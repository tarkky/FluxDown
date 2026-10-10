//! 本机设备身份探测：真实主机名，以及 Flutter 时代 `cloud_device_id` 的一次性沿用。
//!
//! 旧 Flutter 客户端通过 `KvStore` 保存设备 id：
//! 便携模式 = `<engine 数据目录>/settings.json`（键无前缀）；安装模式 = `shared_preferences`
//! 插件后端，键带 `flutter.` 前缀：
//! - Windows：`%APPDATA%\FluxDown\FluxDown\shared_preferences.json`（CompanyName / ProductName）；
//! - Linux：`$XDG_DATA_HOME/com.fluxdown.app/shared_preferences.json`（`APPLICATION_ID`）；
//! - macOS：`NSUserDefaults` 域 `com.fluxdown.app`（未沙盒化，经 `defaults read` 读取）。
//!
//! 迁移到 GPUI 后沿用同一个 id，云端仍视为同一台设备，不占用新的设备名额。

use std::path::{Path, PathBuf};

use serde_json::Value;

/// 设备名上限（与 FluxCloud `PATCH /devices` 一致）。
const MAX_DEVICE_NAME_CHARS: usize = 64;
const FALLBACK_DEVICE_NAME: &str = "FluxDown";
const FLUTTER_ID_KEY: &str = "cloud_device_id";
const FLUTTER_NAME_KEY: &str = "cloud_device_name";
#[cfg(target_os = "macos")]
const FLUTTER_PREFERENCES_DOMAIN: &str = "com.fluxdown.app";

/// Flutter 客户端遗留的设备身份。
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct LegacyIdentity {
    pub device_id: String,
    /// 用户在 Flutter「账户」设置里自定义的设备名。
    pub device_name: Option<String>,
}

/// 解析本机设备名：宿主给出的系统设备名（移动端：Android「设备名称」/ 机型、iOS `UIDevice.name`）
/// 优先，否则探测真实主机名；都不可用时回落 `FluxDown`。返回值保证 1..=64 个字符。
///
/// 移动端沙盒里没有可读的主机名（iOS 无 `/proc` / `/etc/hostname`，Android 恒为 `localhost`），
/// 只能由宿主经 UniFFI 传入。
pub(crate) async fn resolve_device_name(host_provided: Option<&str>) -> String {
    if let Some(name) = host_provided.and_then(sanitize_device_name) {
        return name;
    }
    platform_hostname()
        .await
        .and_then(|raw| sanitize_device_name(&raw))
        .unwrap_or_else(|| FALLBACK_DEVICE_NAME.to_owned())
}

/// 取不到任何真实名字时落下的占位名。FluxCloud 同样把它视为占位（再次登录时用真实名自愈），
/// 本地状态里的占位名在每次启动时也会尝试替换成真实名。
pub(crate) fn is_placeholder_device_name(name: &str) -> bool {
    name.trim() == FALLBACK_DEVICE_NAME
}

/// 修剪并截断到设备名上限；空串视为不可用。
pub(crate) fn sanitize_device_name(raw: &str) -> Option<String> {
    let name = raw
        .trim()
        .chars()
        .filter(|character| !character.is_control())
        .take(MAX_DEVICE_NAME_CHARS)
        .collect::<String>();
    let name = name.trim().to_owned();
    (!name.is_empty()).then_some(name)
}

#[cfg(target_os = "windows")]
async fn platform_hostname() -> Option<String> {
    env_hostname("COMPUTERNAME").or_else(|| env_hostname("HOSTNAME"))
}

#[cfg(target_os = "macos")]
async fn platform_hostname() -> Option<String> {
    // 「系统设置 → 共享」里的电脑名称；图形会话通常没有导出 `HOSTNAME`。
    for key in ["ComputerName", "LocalHostName"] {
        if let Some(name) = scutil_get(key).await {
            return Some(name);
        }
    }
    env_hostname("HOSTNAME")
}

#[cfg(target_os = "macos")]
async fn scutil_get(key: &str) -> Option<String> {
    let mut command = tokio::process::Command::new("scutil");
    command.args(["--get", key]).kill_on_drop(true);
    let output = tokio::time::timeout(std::time::Duration::from_secs(3), command.output())
        .await
        .ok()?
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    let trimmed = text.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

#[cfg(not(any(target_os = "windows", target_os = "macos")))]
async fn platform_hostname() -> Option<String> {
    for path in ["/proc/sys/kernel/hostname", "/etc/hostname"] {
        if let Ok(text) = tokio::fs::read_to_string(path).await {
            let trimmed = text.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_owned());
            }
        }
    }
    env_hostname("HOSTNAME")
}

fn env_hostname(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

/// 读取 Flutter 时代遗留的设备身份；找不到 / 不可读 / 不是合法 UUID 都返回 `None`。
///
/// `include_installed=false`（headless `--server`）只看本数据目录内的便携 `settings.json`：
/// 同机安装版 Flutter 的全局偏好属于桌面客户端这台「设备」，服务器实例沿用它会与桌面端
/// 共用 device id，导致同一远程任务被两边重复接单。
pub(crate) async fn load_flutter_identity(
    engine_data_dir: &Path,
    include_installed: bool,
) -> Option<LegacyIdentity> {
    let mut files = vec![engine_data_dir.join("settings.json")];
    if include_installed {
        files.extend(installed_preference_files());
    }
    for path in files {
        let Ok(text) = tokio::fs::read_to_string(&path).await else {
            continue;
        };
        if let Some(identity) = parse_preferences_json(&text) {
            tracing::info!(source = %path.display(), "found Flutter-era cloud device identity");
            return Some(identity);
        }
    }
    #[cfg(target_os = "macos")]
    if include_installed && let Some(identity) = macos_defaults_identity().await {
        tracing::info!("found Flutter-era cloud device identity in NSUserDefaults");
        return Some(identity);
    }
    None
}

#[cfg(target_os = "windows")]
fn installed_preference_files() -> Vec<PathBuf> {
    std::env::var_os("APPDATA")
        .map(|appdata| {
            PathBuf::from(appdata)
                .join("FluxDown")
                .join("FluxDown")
                .join("shared_preferences.json")
        })
        .into_iter()
        .collect()
}

#[cfg(target_os = "linux")]
fn installed_preference_files() -> Vec<PathBuf> {
    std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/share")))
        .map(|data_home| {
            data_home
                .join("com.fluxdown.app")
                .join("shared_preferences.json")
        })
        .into_iter()
        .collect()
}

#[cfg(not(any(target_os = "windows", target_os = "linux")))]
fn installed_preference_files() -> Vec<PathBuf> {
    Vec::new()
}

/// 解析 `settings.json`（键无前缀）或 `shared_preferences.json`（`flutter.` 前缀）。
fn parse_preferences_json(text: &str) -> Option<LegacyIdentity> {
    let root = serde_json::from_str::<Value>(text).ok()?;
    let read = |key: &str| {
        root.get(format!("flutter.{key}"))
            .or_else(|| root.get(key))
            .and_then(Value::as_str)
            .map(str::to_owned)
    };
    identity_from(read(FLUTTER_ID_KEY), read(FLUTTER_NAME_KEY))
}

fn identity_from(id: Option<String>, name: Option<String>) -> Option<LegacyIdentity> {
    let device_id = id?.trim().to_ascii_lowercase();
    // 只接受 UUID：`KvStore` 里的值来自 Flutter 的 UUID v4 生成器；其他形态视为损坏。
    uuid::Uuid::parse_str(&device_id).ok()?;
    Some(LegacyIdentity {
        device_id,
        device_name: name.as_deref().and_then(sanitize_device_name),
    })
}

#[cfg(target_os = "macos")]
async fn macos_defaults_identity() -> Option<LegacyIdentity> {
    let id = defaults_read(FLUTTER_ID_KEY).await;
    let name = defaults_read(FLUTTER_NAME_KEY).await;
    identity_from(id, name)
}

#[cfg(target_os = "macos")]
async fn defaults_read(key: &str) -> Option<String> {
    let mut command = tokio::process::Command::new("defaults");
    command
        .args([
            "read",
            FLUTTER_PREFERENCES_DOMAIN,
            &format!("flutter.{key}"),
        ])
        .kill_on_drop(true);
    let output = tokio::time::timeout(std::time::Duration::from_secs(3), command.output())
        .await
        .ok()?
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    let trimmed = text.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

#[cfg(test)]
mod tests {
    use super::{LegacyIdentity, parse_preferences_json, sanitize_device_name};

    const ID: &str = "3f2b8c1e-5d4a-4e7b-9a10-0c1d2e3f4a5b";

    #[test]
    fn shared_preferences_json_uses_flutter_prefix() {
        let json = format!(
            r#"{{"flutter.cloud_device_id":"{ID}","flutter.cloud_device_name":" 书房 PC "}}"#
        );
        assert_eq!(
            parse_preferences_json(&json),
            Some(LegacyIdentity {
                device_id: ID.to_owned(),
                device_name: Some("书房 PC".to_owned()),
            })
        );
    }

    #[test]
    fn portable_settings_json_has_no_prefix_and_name_is_optional() {
        let json = format!(r#"{{"cloud_device_id":"{}"}}"#, ID.to_uppercase());
        let identity = parse_preferences_json(&json).expect("portable identity");
        assert_eq!(identity.device_id, ID, "ids are normalized to lower case");
        assert_eq!(identity.device_name, None);
    }

    #[test]
    fn non_uuid_missing_or_corrupt_identities_are_ignored() {
        assert_eq!(parse_preferences_json(r#"{"cloud_device_id":"abc"}"#), None);
        assert_eq!(parse_preferences_json(r#"{"cloud_device_id":""}"#), None);
        assert_eq!(parse_preferences_json(r#"{"other":1}"#), None);
        assert_eq!(parse_preferences_json("not json"), None);
    }

    #[test]
    fn device_names_are_trimmed_and_capped_to_the_cloud_limit() {
        assert_eq!(
            sanitize_device_name("  Mac mini\n"),
            Some("Mac mini".to_owned())
        );
        assert_eq!(sanitize_device_name("   "), None);
        let long = "机".repeat(100);
        assert_eq!(
            sanitize_device_name(&long).map(|name| name.chars().count()),
            Some(64)
        );
    }
}
