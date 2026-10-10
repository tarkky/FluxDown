//! 官方站点 `/api/release` + `/api/changelog` + 校验和清单的解析与版本比较。
//!
//! 渠道对应 SemVer 预发布后缀：稳定版 `vX.Y.Z`，frontier 为 `vX.Y.Z-rc.N`。
//! 比较遵循 SemVer 2.0 §11：`1.3.0 > 1.3.0-rc.2`，`1.4.0-rc.1 > 1.3.0`，
//! 预发布标识按点分段比较（数字段小于字母段），构建元数据（`+meta`）忽略。
//!
//! 官方站点在域名迁移期有多个（[`DEFAULT_API_BASES`]），任一可达即可：检查时错峰竞速，
//! 胜出站点用于同一轮的更新说明、校验和与更新包下载。

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::time::Duration;

use fluxdown_protocol::{ReleaseNoteDto, UpdateManualReason};
use serde::Deserialize;
use serde_json::Value;

use super::install::{InstallTarget, ReleaseComponent};
use crate::http_client::HttpClientError;

/// 官方站点，按优先级：现行域名在前，迁移后的新域名在后；两者长期并存，任一失效时自动换用另一个。
const DEFAULT_API_BASES: [&str; 2] = ["https://fluxdown.zerx.dev", "https://fluxdown.com"];
/// 未检查前（初始状态）展示的发布页。检查后改用胜出站点的 `/changelog`。
pub(super) const RELEASE_PAGE_URL: &str = "https://fluxdown.zerx.dev/changelog";
/// 首选站点在此时长内无结果，才并行请求下一个站点（避免每次都双发请求）。
const FALLBACK_STAGGER: Duration = Duration::from_secs(3);
const CHANGELOG_PER_PAGE: u32 = 50;

/// 更新服务错误。
#[derive(Debug, thiserror::Error)]
pub enum UpdateError {
    #[error("unknown update channel: {0}")]
    InvalidChannel(String),
    #[error("update API request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("update HTTP client unavailable: {0}")]
    Client(#[from] HttpClientError),
    #[error("update API returned status {0}")]
    Status(u16),
    #[error("update API response is invalid: {0}")]
    Decode(String),
    /// 没有可更新的新版本（下载 / 安装请求）。
    #[error("no update available")]
    NoUpdate,
    /// 本机只能手动升级。
    #[error("in-app update unavailable: {0:?}")]
    Manual(UpdateManualReason),
}

/// 官方站点端点；调试构建可经 `FLUXDOWN_UPDATE_BASE_URL` 指向本地服务并跳过主机白名单。
#[derive(Clone, Debug)]
pub(super) struct Endpoint {
    /// 非空，按优先级。
    bases: Arc<[String]>,
    /// 最近一次成功返回 `/api/release` 的站点下标：后续请求与下次检查优先用它。
    active: Arc<AtomicUsize>,
    stagger: Duration,
    trust_any: bool,
}

impl Endpoint {
    pub(super) fn resolve() -> Self {
        #[cfg(debug_assertions)]
        if let Ok(base) = std::env::var("FLUXDOWN_UPDATE_BASE_URL") {
            let base = base.trim().trim_end_matches('/');
            if !base.is_empty() {
                return Self::with_base(base, true);
            }
        }
        Self::with_bases(&DEFAULT_API_BASES, false, FALLBACK_STAGGER)
    }

    pub(super) fn with_base(base: &str, trust_any: bool) -> Self {
        Self::with_bases(&[base], trust_any, FALLBACK_STAGGER)
    }

    pub(super) fn with_bases(bases: &[&str], trust_any: bool, stagger: Duration) -> Self {
        let bases: Arc<[String]> = bases
            .iter()
            .map(|base| base.trim_end_matches('/').to_owned())
            .collect();
        Self {
            bases,
            active: Arc::new(AtomicUsize::new(0)),
            stagger,
            trust_any,
        }
    }

    fn base(&self) -> &str {
        let index = self.active.load(AtomicOrdering::Relaxed);
        self.bases
            .get(index)
            .or_else(|| self.bases.first())
            .map_or("", String::as_str)
    }

    /// 本次检查尝试站点的顺序：上次成功的站点在前，其余按优先级。
    pub(super) fn attempt_order(&self) -> Vec<usize> {
        let active = self
            .active
            .load(AtomicOrdering::Relaxed)
            .min(self.bases.len().saturating_sub(1));
        std::iter::once(active)
            .chain((0..self.bases.len()).filter(|&index| index != active))
            .collect()
    }

    pub(super) fn set_active(&self, index: usize) {
        if index < self.bases.len() {
            self.active.store(index, AtomicOrdering::Relaxed);
        }
    }

    pub(super) fn stagger(&self) -> Duration {
        self.stagger
    }

    pub(super) fn release_url_at(&self, index: usize, channel: &str) -> Option<String> {
        let base = self.bases.get(index)?;
        Some(format!("{base}/api/release?channel={channel}"))
    }

    pub(super) fn release_page_url(&self) -> String {
        format!("{}/changelog", self.base())
    }

    pub(super) fn changelog_url(&self, channel: &str, current_version: &str) -> String {
        format!(
            "{}/api/changelog?per_page={CHANGELOG_PER_PAGE}&since=v{current_version}&channel={channel}",
            self.base()
        )
    }

    /// `{base}/api/download/{name}?tag={tag}`；名称与 tag 均做百分号编码。
    pub(super) fn package_url(&self, name: &str, tag: &str) -> Result<String, UpdateError> {
        let mut url = reqwest::Url::parse(self.base())
            .map_err(|error| UpdateError::Decode(format!("invalid API base: {error}")))?;
        url.path_segments_mut()
            .map_err(|()| UpdateError::Decode("API base cannot be a base URL".to_owned()))?
            .pop_if_empty()
            .extend(["api", "download", name]);
        url.query_pairs_mut().append_pair("tag", tag);
        Ok(url.into())
    }

    /// 站点资产 URL 可能是相对路径（`/api/download/...`，用于地域路由）。
    fn absolute(&self, url: &str) -> String {
        if url.starts_with('/') {
            format!("{}{url}", self.base())
        } else {
            url.to_owned()
        }
    }

    /// 下载地址只接受 https 且主机属于官方分发域：清单被篡改时也不会把用户带到任意 scheme / 主机。
    fn is_trusted(&self, url: &str) -> bool {
        self.trust_any || is_trusted_download_url(url)
    }
}

/// 发布中选中的资产（尚无校验和）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct SelectedAsset {
    pub name: String,
    pub size: u64,
    /// 清单里的绝对直链（可信主机）；手动升级用，缺失为空串。
    pub download_url: String,
}

/// 目标组件（桌面 / 服务端 / 移动端）在渠道上的最新发布。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ComponentRelease {
    pub version: String,
    pub tag: String,
    pub asset: Option<SelectedAsset>,
}

#[derive(Deserialize, Default)]
struct RawComponent {
    #[serde(default)]
    version: String,
    #[serde(default)]
    tag: String,
    /// 资产键 → `{ name, size, download_url }`；不存在的资产为 `null`。
    #[serde(default)]
    assets: BTreeMap<String, Value>,
}

#[derive(Deserialize)]
pub(super) struct RawRelease {
    #[serde(flatten)]
    desktop: RawComponent,
    #[serde(default)]
    server: Option<RawComponent>,
    #[serde(default)]
    mobile: Option<RawComponent>,
}

impl RawRelease {
    /// 取目标组件的发布；服务端 / 移动端发布缺失（`null`）时返回 `None`。
    pub(super) fn component(
        self,
        target: &InstallTarget,
        endpoint: &Endpoint,
    ) -> Option<ComponentRelease> {
        let component = match target.component {
            ReleaseComponent::Desktop => self.desktop,
            ReleaseComponent::Server => self.server?,
            ReleaseComponent::Mobile => self.mobile?,
        };
        if component.version.is_empty() {
            return None;
        }
        let tag = if component.tag.is_empty() {
            format!("v{}", component.version.trim_start_matches('v'))
        } else {
            component.tag
        };
        let asset = select_asset(&component.assets, &target.asset_keys, endpoint);
        Some(ComponentRelease {
            version: component.version,
            tag,
            asset,
        })
    }
}

/// 取 `asset_keys` 中第一个存在（名称非空）的资产。
fn select_asset(
    assets: &BTreeMap<String, Value>,
    keys: &[&'static str],
    endpoint: &Endpoint,
) -> Option<SelectedAsset> {
    keys.iter().find_map(|key| {
        let asset = assets.get(*key)?;
        let name = asset
            .get("name")
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty())?;
        let download_url = asset
            .get("download_url")
            .and_then(Value::as_str)
            .filter(|url| !url.is_empty())
            .map(|url| endpoint.absolute(url))
            .filter(|url| endpoint.is_trusted(url))
            .unwrap_or_default();
        Some(SelectedAsset {
            name: name.to_owned(),
            size: asset.get("size").and_then(Value::as_u64).unwrap_or(0),
            download_url,
        })
    })
}

/// 校验和清单候选文件名：组件哨兵优先，其次合并清单。
pub(super) fn checksum_file_names(component: ReleaseComponent) -> [&'static str; 2] {
    match component {
        ReleaseComponent::Desktop => ["SHA256SUMS-app.txt", "SHA256SUMS.txt"],
        ReleaseComponent::Server => ["SHA256SUMS-server.txt", "SHA256SUMS.txt"],
        ReleaseComponent::Mobile => ["SHA256SUMS-mobile.txt", "SHA256SUMS.txt"],
    }
}

/// 在 `sha256sum` 格式文本中查找文件的校验和（小写十六进制）。
pub(super) fn find_checksum(text: &str, file_name: &str) -> Option<String> {
    text.lines().find_map(|line| {
        let (hash, rest) = line.trim().split_once(char::is_whitespace)?;
        let name = rest.trim_start().trim_start_matches('*');
        let valid = hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit());
        (valid && name == file_name).then(|| hash.to_ascii_lowercase())
    })
}

#[derive(Deserialize)]
pub(super) struct ChangelogResponse {
    #[serde(default)]
    releases: Vec<ChangelogRelease>,
}

impl ChangelogResponse {
    pub(super) fn into_notes(self, current_version: &str) -> Vec<ReleaseNoteDto> {
        release_notes(self.releases, current_version)
    }
}

#[derive(Deserialize)]
struct ChangelogRelease {
    #[serde(default)]
    version: String,
    #[serde(default)]
    published_at: String,
    #[serde(default)]
    body: String,
}

pub(super) fn normalize_channel(channel: &str) -> Result<&'static str, UpdateError> {
    match channel.trim() {
        "" | "stable" => Ok("stable"),
        "frontier" => Ok("frontier"),
        other => Err(UpdateError::InvalidChannel(other.to_owned())),
    }
}

/// `/api/changelog?since=` 是闭区间，过滤掉当前版本本身。
fn release_notes(releases: Vec<ChangelogRelease>, current_version: &str) -> Vec<ReleaseNoteDto> {
    releases
        .into_iter()
        .filter(|release| release.version != current_version)
        .map(|release| ReleaseNoteDto {
            version: release.version,
            published_at: release.published_at,
            body: release.body,
        })
        .collect()
}

fn is_trusted_download_url(url: &str) -> bool {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return false;
    };
    if parsed.scheme() != "https" || !parsed.username().is_empty() || parsed.password().is_some() {
        return false;
    }
    let Some(host) = parsed.host_str() else {
        return false;
    };
    let host = host.to_ascii_lowercase();
    let under = |domain: &str| {
        host == domain
            || host
                .strip_suffix(domain)
                .is_some_and(|prefix| prefix.ends_with('.'))
    };
    under("zerx.dev")
        || under("fluxdown.com")
        || under("github.com")
        || under("githubusercontent.com")
}

/// 预发布标识：数字段按数值比较且恒小于字母段（SemVer 2.0 §11.4）。
#[derive(Debug, PartialEq, Eq)]
enum PreId {
    Num(u64),
    Text(String),
}

#[derive(Debug, PartialEq, Eq)]
struct SemVer {
    core: (u64, u64, u64),
    pre: Vec<PreId>,
}

fn parse_semver(input: &str) -> Result<SemVer, UpdateError> {
    let input = input.trim();
    let input = input.strip_prefix('v').unwrap_or(input);
    let input = input.split('+').next().unwrap_or(input);
    let (core, pre) = match input.split_once('-') {
        Some((core, pre)) => (core, Some(pre)),
        None => (input, None),
    };
    let mut parts = core.split('.');
    let mut next_part = |name: &str| -> Result<u64, UpdateError> {
        parts
            .next()
            .ok_or_else(|| UpdateError::Decode(format!("missing {name} in version: {input}")))?
            .parse::<u64>()
            .map_err(|_| UpdateError::Decode(format!("invalid {name} in version: {input}")))
    };
    let major = next_part("major")?;
    let minor = next_part("minor")?;
    let patch = next_part("patch")?;
    if parts.next().is_some() {
        return Err(UpdateError::Decode(format!("invalid version: {input}")));
    }
    let pre = match pre {
        None => Vec::new(),
        Some("") => {
            return Err(UpdateError::Decode(format!("empty prerelease: {input}")));
        }
        Some(pre) => pre
            .split('.')
            .map(|id| match id.parse::<u64>() {
                Ok(number) => PreId::Num(number),
                Err(_) => PreId::Text(id.to_owned()),
            })
            .collect(),
    };
    Ok(SemVer {
        core: (major, minor, patch),
        pre,
    })
}

fn cmp_pre(a: &[PreId], b: &[PreId]) -> Ordering {
    for (x, y) in a.iter().zip(b.iter()) {
        let ordering = match (x, y) {
            (PreId::Num(m), PreId::Num(n)) => m.cmp(n),
            (PreId::Text(m), PreId::Text(n)) => m.cmp(n),
            (PreId::Num(_), PreId::Text(_)) => Ordering::Less,
            (PreId::Text(_), PreId::Num(_)) => Ordering::Greater,
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    a.len().cmp(&b.len())
}

fn cmp_semver(a: &SemVer, b: &SemVer) -> Ordering {
    match a.core.cmp(&b.core) {
        Ordering::Equal => {}
        ordering => return ordering,
    }
    match (a.pre.is_empty(), b.pre.is_empty()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => cmp_pre(&a.pre, &b.pre),
    }
}

/// `latest` 是否严格高于 `current`；任一方不可解析（如开发版 `dev`）返回错误。
pub fn is_newer(latest: &str, current: &str) -> Result<bool, UpdateError> {
    let latest = parse_semver(latest)?;
    let current = parse_semver(current)?;
    Ok(cmp_semver(&latest, &current) == Ordering::Greater)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use fluxdown_protocol::UpdateInstallKind;
    use serde_json::json;

    use super::*;

    fn target(component: ReleaseComponent, keys: &[&'static str]) -> InstallTarget {
        InstallTarget {
            kind: UpdateInstallKind::Unknown,
            component,
            asset_keys: keys.to_vec(),
            manual_reason: None,
        }
    }

    fn release_json() -> RawRelease {
        serde_json::from_value(json!({
            "version": "1.4.0",
            "tag": "v1.4.0",
            "assets": {
                "setup": { "name": "FluxDown-1.4.0-setup.exe", "size": 10, "download_url": "/api/download/FluxDown-1.4.0-setup.exe" },
                "portable": { "name": "FluxDown-1.4.0-portable.zip", "size": 20, "download_url": "https://evil.example/p.zip" },
                "linux_deb": null
            },
            "server": {
                "version": "1.4.1",
                "tag": "v1.4.1",
                "assets": {
                    "linux_x64": { "name": "fluxdown-server-1.4.1-linux-x64.tar.gz", "size": 30, "download_url": "/api/download/s.tar.gz" }
                }
            }
        }))
        .unwrap()
    }

    #[test]
    fn desktop_component_uses_top_level_assets_in_key_priority() {
        let endpoint = Endpoint::with_base(DEFAULT_API_BASES[0], false);
        let release = release_json()
            .component(
                &target(
                    ReleaseComponent::Desktop,
                    &["linux_deb", "portable", "setup"],
                ),
                &endpoint,
            )
            .unwrap();
        assert_eq!(release.version, "1.4.0");
        assert_eq!(release.tag, "v1.4.0");
        let asset = release.asset.unwrap();
        // `linux_deb` 为 null 被跳过，`portable` 是第一个存在的键；其直链主机不可信 → 空串。
        assert_eq!(asset.name, "FluxDown-1.4.0-portable.zip");
        assert_eq!(asset.size, 20);
        assert_eq!(asset.download_url, "");

        let release = release_json()
            .component(
                &target(ReleaseComponent::Desktop, &["setup", "portable"]),
                &endpoint,
            )
            .unwrap();
        let asset = release.asset.unwrap();
        assert_eq!(asset.name, "FluxDown-1.4.0-setup.exe");
        assert_eq!(
            asset.download_url,
            "https://fluxdown.zerx.dev/api/download/FluxDown-1.4.0-setup.exe"
        );
    }

    #[test]
    fn server_component_reads_server_block() {
        let endpoint = Endpoint::with_base(DEFAULT_API_BASES[0], false);
        let release = release_json()
            .component(
                &target(ReleaseComponent::Server, &["linux_arm64", "linux_x64"]),
                &endpoint,
            )
            .unwrap();
        assert_eq!(release.version, "1.4.1");
        assert_eq!(release.tag, "v1.4.1");
        assert_eq!(
            release.asset.unwrap().name,
            "fluxdown-server-1.4.1-linux-x64.tar.gz"
        );
    }

    #[test]
    fn missing_asset_or_server_block() {
        let endpoint = Endpoint::with_base(DEFAULT_API_BASES[0], false);
        let none = release_json()
            .component(&target(ReleaseComponent::Desktop, &[]), &endpoint)
            .unwrap();
        assert!(none.asset.is_none());
        let raw: RawRelease =
            serde_json::from_value(json!({ "version": "1.0.0", "tag": "v1.0.0", "server": null }))
                .unwrap();
        assert!(
            raw.component(&target(ReleaseComponent::Server, &["x"]), &endpoint)
                .is_none()
        );
    }

    #[test]
    fn debug_endpoint_override_trusts_any_host() {
        let endpoint = Endpoint::with_base("http://127.0.0.1:9/", true);
        let raw: RawRelease = serde_json::from_value(json!({
            "version": "1.0.0", "tag": "v1.0.0",
            "assets": { "setup": { "name": "a", "size": 1, "download_url": "/api/download/a" } }
        }))
        .unwrap();
        let release = raw
            .component(&target(ReleaseComponent::Desktop, &["setup"]), &endpoint)
            .unwrap();
        assert_eq!(
            release.asset.unwrap().download_url,
            "http://127.0.0.1:9/api/download/a"
        );
    }

    #[test]
    fn package_url_encodes_name_and_tag() {
        let endpoint = Endpoint::with_base("https://fluxdown.zerx.dev/", false);
        assert_eq!(
            endpoint
                .package_url("FluxDown 1.4.0.zip", "v1.4.0-rc.1+b")
                .unwrap(),
            "https://fluxdown.zerx.dev/api/download/FluxDown%201.4.0.zip?tag=v1.4.0-rc.1%2Bb"
        );
    }

    #[test]
    fn checksum_sentinels_per_component() {
        assert_eq!(
            checksum_file_names(ReleaseComponent::Desktop),
            ["SHA256SUMS-app.txt", "SHA256SUMS.txt"]
        );
        assert_eq!(
            checksum_file_names(ReleaseComponent::Server),
            ["SHA256SUMS-server.txt", "SHA256SUMS.txt"]
        );
        assert_eq!(
            checksum_file_names(ReleaseComponent::Mobile),
            ["SHA256SUMS-mobile.txt", "SHA256SUMS.txt"]
        );
    }

    #[test]
    fn mobile_component_reads_mobile_block_and_falls_back_to_universal() {
        let endpoint = Endpoint::with_base(DEFAULT_API_BASES[0], false);
        let raw = || -> RawRelease {
            serde_json::from_value(json!({
                "version": "1.4.0",
                "assets": {},
                "mobile": {
                    "version": "1.4.0-rc.2",
                    "tag": "v1.4.0-rc.2",
                    "assets": {
                        "android_arm64": null,
                        "android_universal": { "name": "FluxDown-1.4.0-rc.2-android-universal.apk", "size": 90, "download_url": "/api/download/u.apk?tag=v1.4.0-rc.2" }
                    }
                }
            }))
            .unwrap()
        };
        let release = raw()
            .component(
                &target(
                    ReleaseComponent::Mobile,
                    &["android_arm64", "android_universal"],
                ),
                &endpoint,
            )
            .unwrap();
        assert_eq!(release.version, "1.4.0-rc.2");
        assert_eq!(release.tag, "v1.4.0-rc.2");
        let asset = release.asset.unwrap();
        assert_eq!(asset.name, "FluxDown-1.4.0-rc.2-android-universal.apk");
        assert_eq!(
            asset.download_url,
            "https://fluxdown.zerx.dev/api/download/u.apk?tag=v1.4.0-rc.2"
        );
        let without_mobile: RawRelease =
            serde_json::from_value(json!({ "version": "1.4.0", "mobile": null })).unwrap();
        assert!(
            without_mobile
                .component(
                    &target(ReleaseComponent::Mobile, &["android_universal"]),
                    &endpoint
                )
                .is_none()
        );
    }

    #[test]
    fn active_site_drives_urls_and_attempt_order() {
        let endpoint = Endpoint::with_bases(&DEFAULT_API_BASES, false, Duration::from_secs(3));
        assert_eq!(endpoint.attempt_order(), [0, 1]);
        assert_eq!(
            endpoint.release_url_at(1, "stable").as_deref(),
            Some("https://fluxdown.com/api/release?channel=stable")
        );
        assert_eq!(endpoint.release_url_at(2, "stable"), None);
        endpoint.set_active(1);
        assert_eq!(endpoint.attempt_order(), [1, 0]);
        assert_eq!(
            endpoint.release_page_url(),
            "https://fluxdown.com/changelog"
        );
        assert_eq!(
            endpoint.package_url("a.apk", "v1.0.0").unwrap(),
            "https://fluxdown.com/api/download/a.apk?tag=v1.0.0"
        );
        assert_eq!(
            endpoint.absolute("/api/download/a.apk"),
            "https://fluxdown.com/api/download/a.apk"
        );
        // 越界下标不改变当前站点。
        endpoint.set_active(5);
        assert_eq!(endpoint.attempt_order(), [1, 0]);
    }

    #[test]
    fn checksum_lines_parse_text_and_binary_modes() {
        let hash_a = "A".repeat(64);
        let hash_b = "b".repeat(64);
        let text = format!(
            "{hash_a}  a.zip\n{hash_b} *b.zip\nnot-a-hash  c.zip\n{}  short.zip\n",
            "c".repeat(10)
        );
        assert_eq!(find_checksum(&text, "a.zip"), Some("a".repeat(64)));
        assert_eq!(find_checksum(&text, "b.zip"), Some(hash_b));
        assert_eq!(find_checksum(&text, "c.zip"), None);
        assert_eq!(find_checksum(&text, "short.zip"), None);
        assert_eq!(find_checksum(&text, "missing.zip"), None);
    }

    #[test]
    fn untrusted_download_urls_are_rejected() {
        use super::is_trusted_download_url as trusted;
        assert!(trusted("https://fluxdown.zerx.dev/api/download/x"));
        assert!(trusted("https://fluxdown.com/api/download/x"));
        assert!(trusted("https://dl.fluxdown.com/FluxDownRelease/a.apk"));
        assert!(trusted("https://objects.githubusercontent.com/a"));
        assert!(!trusted("https://evilfluxdown.com/a"));
        assert!(!trusted("https://fluxdown.com.evil.com/a"));
        assert!(!trusted("http://fluxdown.zerx.dev/a"));
        assert!(!trusted("file:///etc/passwd"));
        assert!(!trusted("https://evilzerx.dev/a"));
        assert!(!trusted("https://zerx.dev.evil.com/a"));
        assert!(!trusted("https://user@fluxdown.zerx.dev/a"));
        assert!(!trusted("\\\\host\\share\\a.exe"));
    }

    #[test]
    fn stable_versions_compare_numerically() {
        assert!(is_newer("1.3.0", "1.2.5").unwrap());
        assert!(is_newer("1.2.6", "1.2.5").unwrap());
        assert!(is_newer("1.10.0", "1.9.9").unwrap());
        assert!(!is_newer("1.2.5", "1.2.5").unwrap());
        assert!(!is_newer("1.2.4", "1.2.5").unwrap());
        assert!(is_newer("v2.0.0", "1.99.99").unwrap());
    }

    #[test]
    fn prerelease_precedence_follows_semver() {
        assert!(is_newer("1.3.0", "1.3.0-rc.1").unwrap());
        assert!(!is_newer("1.3.0-rc.2", "1.3.0").unwrap());
        assert!(is_newer("1.3.0-rc.2", "1.3.0-rc.1").unwrap());
        assert!(!is_newer("1.3.0-rc.1", "1.3.0-rc.2").unwrap());
        assert!(is_newer("1.4.0-rc.1", "1.3.0").unwrap());
        assert!(is_newer("1.3.0-rc.1", "1.3.0-alpha").unwrap());
        assert!(is_newer("1.3.0-rc.1.1", "1.3.0-rc.1").unwrap());
        assert!(!is_newer("1.3.0-rc.1", "1.3.0-rc.1").unwrap());
        assert!(is_newer("1.3.0+build.7", "1.2.0+build.9").unwrap());
        assert!(!is_newer("1.3.0+build.7", "1.3.0").unwrap());
    }

    #[test]
    fn invalid_versions_are_rejected() {
        assert!(is_newer("dev", "1.0.0").is_err());
        assert!(is_newer("1.0.0", "dev").is_err());
        assert!(is_newer("1.0", "1.0.0").is_err());
        assert!(is_newer("1.0.0.1", "1.0.0").is_err());
        assert!(is_newer("1.0.0-", "1.0.0").is_err());
    }

    #[test]
    fn channels_normalize_and_reject_unknown() {
        assert_eq!(normalize_channel("").unwrap(), "stable");
        assert_eq!(normalize_channel("stable").unwrap(), "stable");
        assert_eq!(normalize_channel(" frontier ").unwrap(), "frontier");
        assert!(normalize_channel("nightly").is_err());
    }

    #[test]
    fn notes_exclude_current_version() {
        let releases = vec![
            ChangelogRelease {
                version: "1.3.0".to_owned(),
                published_at: "2026-01-01T00:00:00Z".to_owned(),
                body: "new".to_owned(),
            },
            ChangelogRelease {
                version: "1.2.0".to_owned(),
                published_at: String::new(),
                body: "current".to_owned(),
            },
        ];
        let notes = release_notes(releases, "1.2.0");
        assert_eq!(notes.len(), 1);
        assert_eq!(notes[0].version, "1.3.0");
        assert_eq!(notes[0].body, "new");
    }
}
