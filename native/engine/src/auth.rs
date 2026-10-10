//! 插件通用认证凭据存储与请求注入。
//!
//! 插件只负责完成平台特有的登录流程，并把登录结果提交到这里；后续
//! `flux.fetch` 会按插件和站点自动查找并复用凭据。认证结果不放入插件自己的
//! KV 空间，避免每个插件重复实现持久化和过期判断。

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

use crate::db::{Db, DbError};
use crate::logger::log_warn;

/// 插件认证凭据配置键。
///
/// 仅作为旧版本整表存储的迁移兼容键；新写入按认证档案拆成独立 config 行。
pub const AUTH_PROFILES_CONFIG_KEY: &str = "plugin_auth_profiles";

/// 一份可复用的插件认证凭据。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthProfile {
    /// 稳定引用。默认格式为 `plugin_id::site-key`。
    #[serde(default)]
    pub auth_ref: String,
    /// 创建该凭据的插件 ID。
    #[serde(default)]
    pub plugin_id: String,
    /// 站点键（`{scheme}://{host}[:port]`，见 [`site_key`]）。
    #[serde(default)]
    pub site: String,
    /// 平台账户名，可为空。
    #[serde(default)]
    pub account: String,
    /// `basic` / `cookie` / `bearer` / `headers` / `session`。
    #[serde(default)]
    pub kind: String,
    /// Cookie 请求头值。
    #[serde(default)]
    pub cookies: String,
    /// 需要注入的额外请求头。
    #[serde(default)]
    pub headers: HashMap<String, String>,
    /// Bearer access token。保存后自动注入 `Authorization`，除非 headers 已提供。
    #[serde(default)]
    pub access_token: String,
    /// HTTP Basic 用户名（`kind = "basic"` 时使用）。
    #[serde(default)]
    pub username: String,
    /// HTTP Basic 密码（`kind = "basic"` 时使用）。
    #[serde(default)]
    pub password: String,
    /// 平台刷新流程使用的 token，由插件自行消费。
    #[serde(default)]
    pub refresh_token: String,
    /// Unix 秒；为空表示由服务端状态决定有效期（典型 Cookie）。
    #[serde(default)]
    pub expires_at: Option<i64>,
    /// Unix 秒；仅作为插件刷新提示，不会触发通用猜测式刷新。
    #[serde(default)]
    pub refresh_at: Option<i64>,
    /// 平台特有的非敏感元数据。
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

impl AuthProfile {
    /// 当前时间下凭据是否仍可使用。
    pub fn is_valid_at(&self, now: i64) -> bool {
        self.expires_at.is_none_or(|expires_at| expires_at > now)
    }

    /// 将通用认证材料注入请求头。插件自定义 headers 优先于自动生成的 Bearer。
    ///
    /// 调用方须先用 [`site_key`] 校验 `self.site` 与目标请求同源（含 scheme）——
    /// 本方法本身不做该判断，只负责字段→请求头的映射（见
    /// `plugin::bridge::EngineBridge::http_request` 的调用点）。
    pub fn apply_to_headers(&self, headers: &mut HashMap<String, String>) {
        if !self.cookies.is_empty() && !headers.keys().any(|key| key.eq_ignore_ascii_case("cookie"))
        {
            headers.insert("Cookie".to_string(), self.cookies.clone());
        }
        if !self.access_token.is_empty()
            && !headers
                .keys()
                .any(|key| key.eq_ignore_ascii_case("authorization"))
        {
            headers.insert(
                "Authorization".to_string(),
                format!("Bearer {}", self.access_token),
            );
        }
        if self.kind.eq_ignore_ascii_case("basic")
            && (!self.username.is_empty() || !self.password.is_empty())
            && !headers
                .keys()
                .any(|key| key.eq_ignore_ascii_case("authorization"))
        {
            headers.insert(
                "Authorization".to_string(),
                crate::site_auth::basic_auth_value(&self.username, &self.password),
            );
        }
        for (name, value) in &self.headers {
            headers.insert(name.clone(), value.clone());
        }
    }
}

/// 从 URL 生成站点默认认证引用。
pub fn default_auth_ref(plugin_id: &str, url: &str) -> Option<String> {
    site_key(url).map(|site| format!("{plugin_id}::{site}"))
}

/// 从 URL 提取带 scheme 的站点键：`{scheme}://{host}[:port]`。
///
/// scheme 是键的一部分——同一 host 的 http 与 https 是两个不同站点：在
/// https 页面建立的登录态绝不会被隐式复用到同 host 的 http 请求（H-3：防止
/// 明文 http 请求把 Cookie/Bearer 暴露给被动 MITM）。插件想显式允许对 http
/// 站点注入凭据，必须用 `flux.auth.save({site:"http://host"})` 显式声明
/// http（而不是让宿主替它悄悄补 scheme）。
pub fn site_key(url: &str) -> Option<String> {
    let parsed = reqwest::Url::parse(url).ok()?;
    let scheme = parsed.scheme();
    if !matches!(scheme, "http" | "https") {
        return None;
    }
    let host = parsed.host_str()?;
    let host_port = match parsed.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    };
    Some(format!("{scheme}://{host_port}"))
}

/// 规范化 UI/插件输入的站点：接受完整 URL（含 scheme）或裸 `host[:port]`。
///
/// 裸 host（不带 scheme）默认按 `https` 处理——这是登录场景的默认安全假设；
/// 插件要显式登记一个允许明文的站点，必须自己在 `site` 里带上 `http://`。
pub fn normalize_site(input: &str) -> Option<String> {
    let input = input.trim();
    if input.is_empty() {
        return None;
    }
    site_key(input).or_else(|| site_key(&format!("https://{input}")))
}

/// 读取单份认证档案（按新格式单键 → 旧版整表兼容键的顺序查找）。
pub async fn load(db: &Db, auth_ref: &str) -> Result<Option<AuthProfile>, DbError> {
    if let Some(key) = profile_key_for_ref(auth_ref)
        && let Some(json) = db.get_config(&key).await?
    {
        let profile = serde_json::from_str(&json)
            .map_err(|error| DbError::InvalidConfig(format!("认证档案 {key} 解析失败: {error}")))?;
        return Ok(Some(profile));
    }
    Ok(read_legacy_table(db).await?.get(auth_ref).cloned())
}

/// 写入或替换认证档案。
pub async fn save(db: &Db, profile: &AuthProfile) -> Result<(), DbError> {
    migrate_legacy(db).await?;
    let key = profile_config_key(profile);
    let json = serde_json::to_string(profile)
        .map_err(|error| DbError::InvalidConfig(format!("认证档案序列化失败: {error}")))?;
    db.set_config(&key, &json).await
}

/// 删除认证档案。
pub async fn remove(db: &Db, auth_ref: &str) -> Result<(), DbError> {
    migrate_legacy(db).await?;
    if let Some(key) = profile_key_for_ref(auth_ref) {
        db.delete_config(&key).await?;
    }
    Ok(())
}

/// 卸载插件时删除该插件的全部认证档案，包括旧版整表中的条目。
///
/// 只挂在用户主动 [`super::plugin::PluginManager::uninstall`]，不挂在安装
/// 回滚复用的 `purge`（M-4）：回滚不该连带删掉用户已经登录成功的凭据。
pub async fn remove_plugin(db: &Db, plugin_id: &str) -> Result<(), DbError> {
    let prefix = format!("plugin.{plugin_id}.auth.");
    for (key, _) in db.list_config_with_prefix(&prefix).await? {
        db.delete_config(&key).await?;
    }
    let mut legacy = read_legacy_table(db).await?;
    if legacy.is_empty() {
        return Ok(());
    }
    let auth_prefix = format!("{plugin_id}::");
    legacy.retain(|auth_ref, profile| {
        !auth_ref.starts_with(&auth_prefix) && profile.plugin_id != plugin_id
    });
    if legacy.is_empty() {
        db.delete_config(AUTH_PROFILES_CONFIG_KEY).await
    } else {
        let remaining = serde_json::to_string(&legacy)
            .map_err(|error| DbError::InvalidConfig(format!("认证档案旧表序列化失败: {error}")))?;
        db.set_config(AUTH_PROFILES_CONFIG_KEY, &remaining).await
    }
}

fn profile_config_key(profile: &AuthProfile) -> String {
    format!("plugin.{}.auth.{}", profile.plugin_id, profile.site)
}

fn profile_key_for_ref(auth_ref: &str) -> Option<String> {
    let (plugin_id, site) = auth_ref.split_once("::")?;
    if plugin_id.is_empty() || site.is_empty() {
        return None;
    }
    Some(format!("plugin.{plugin_id}.auth.{site}"))
}

/// 读取旧版整表并解析为 `auth_ref → profile` 索引。
///
/// JSON 损坏时记日志并直接清空该键，视同没有旧数据——该键只是迁移兼容层，
/// 让它长期卡死 save/remove/purge/uninstall（M-1）远比丢弃一份已经读不出来
/// 的损坏数据更糟。表不存在或为空同样返回空表，均不算错误。
async fn read_legacy_table(db: &Db) -> Result<BTreeMap<String, AuthProfile>, DbError> {
    let Some(json) = db.get_config(AUTH_PROFILES_CONFIG_KEY).await? else {
        return Ok(BTreeMap::new());
    };
    if json.trim().is_empty() {
        return Ok(BTreeMap::new());
    }
    match serde_json::from_str(&json) {
        Ok(legacy) => Ok(legacy),
        Err(error) => {
            log_warn!("[auth] 旧版认证档案表解析失败，已丢弃: {error}");
            db.delete_config(AUTH_PROFILES_CONFIG_KEY).await?;
            Ok(BTreeMap::new())
        }
    }
}

/// 把旧版整表迁移为按认证档案拆分的独立 config 行。
///
/// 兼容策略：`plugin_auth_profiles` 整表格式与本文件（`native/engine/src/
/// auth.rs`）一样，是随插件系统在同一未发布分支（#664）引入的——`main` 上
/// 从未存在过这张表，因此没有任何已发布版本会带着旧格式站点键升级到本
/// 版本；这里的迁移与下面的 scheme 补全纯属防御性前向兼容（覆盖已经在用
/// nightly/dev 构建的测试者），不代表存在需要迁移的生产数据。
async fn migrate_legacy(db: &Db) -> Result<(), DbError> {
    let legacy = read_legacy_table(db).await?;
    if legacy.is_empty() {
        return Ok(());
    }
    let mut values = BTreeMap::new();
    for (auth_ref, mut profile) in legacy {
        let Some((plugin_id, raw_site)) = auth_ref.split_once("::") else {
            return Err(DbError::InvalidConfig(format!(
                "认证档案 authRef 非法: {auth_ref}"
            )));
        };
        let plugin_id = plugin_id.to_string();
        // H-3 修复前的旧表站点键不含 scheme；统一按 https 补全，与当前
        // site_key() 格式对齐，否则迁移后按新格式查找不到。
        let site = if raw_site.contains("://") {
            raw_site.to_string()
        } else {
            format!("https://{raw_site}")
        };
        let new_auth_ref = format!("{plugin_id}::{site}");
        let Some(key) = profile_key_for_ref(&new_auth_ref) else {
            return Err(DbError::InvalidConfig(format!(
                "认证档案 authRef 非法: {auth_ref}"
            )));
        };
        // 旧表的 map key 才是可查找的权威引用；修复旧版本可能留下的
        // 空/不一致 profile.authRef/pluginId，并强制 site 落到补全 scheme
        // 后的新格式（不能沿用可能仍是旧格式的 profile.site 字段）。
        profile.auth_ref = new_auth_ref;
        if profile.plugin_id.is_empty() {
            profile.plugin_id = plugin_id;
        }
        profile.site = site;
        let value = serde_json::to_string(&profile)
            .map_err(|error| DbError::InvalidConfig(format!("认证档案序列化失败: {error}")))?;
        values.insert(key, value);
    }
    db.set_config_batch_atomic(&values).await?;
    db.delete_config(AUTH_PROFILES_CONFIG_KEY).await
}

fn is_profile_config_key(key: &str) -> bool {
    let Some(rest) = key.strip_prefix("plugin.") else {
        return false;
    };
    let Some((identity, site)) = rest.split_once(".auth.") else {
        return false;
    };
    !identity.is_empty() && !identity.contains('.') && identity.contains('@') && !site.is_empty()
}

/// config 键是否属于敏感的插件认证凭据或站点级 Basic Auth 命名空间。
///
/// REST/RPC 只读接口须过滤该类键，写接口须整体拒绝（而非静默丢弃，见
/// 兼容 API 的 `/api/v1/config` 与 aria2 层）。单点定义，
/// 判据与 [`is_profile_config_key`] 保持一致，供各宿主复用。
pub fn is_sensitive_config_key(key: &str) -> bool {
    key == AUTH_PROFILES_CONFIG_KEY
        || key == crate::site_auth::SITE_AUTH_CONFIG_KEY
        // 互联设备身份私钥 seed（`link::IDENTITY_CONFIG_KEY`），link 命名空间整体不对外。
        || key.starts_with("link.")
        || is_profile_config_key(key)
}

/// 返回当前 Unix 秒。
pub fn now_unix() -> i64 {
    chrono::Utc::now().timestamp()
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn default_ref_uses_normalized_site() {
        assert_eq!(
            default_auth_ref("bilibili@example", "https://Example.COM:443/video"),
            Some("bilibili@example::https://example.com".to_string())
        );
    }

    #[test]
    fn site_key_includes_scheme_and_rejects_cross_scheme_match() {
        assert_eq!(
            site_key("https://example.com/a"),
            Some("https://example.com".to_string())
        );
        assert_eq!(
            site_key("http://example.com/a"),
            Some("http://example.com".to_string())
        );
        // 同 host 不同 scheme 是两个不同站点键——https 登录态不会隐式命中
        // http 请求（H-3）。
        assert_ne!(
            site_key("https://example.com/a"),
            site_key("http://example.com/a")
        );
    }

    #[test]
    fn cookie_and_bearer_are_injected_without_overwriting_explicit_headers() {
        let profile = AuthProfile {
            cookies: "sid=1".to_string(),
            access_token: "token".to_string(),
            ..Default::default()
        };
        let mut headers = HashMap::from([("Authorization".to_string(), "Basic x".to_string())]);
        profile.apply_to_headers(&mut headers);
        assert_eq!(headers.get("Cookie").map(String::as_str), Some("sid=1"));
        assert_eq!(
            headers.get("Authorization").map(String::as_str),
            Some("Basic x")
        );
    }

    #[test]
    fn basic_profile_is_injected() {
        let profile = AuthProfile {
            kind: "basic".to_string(),
            username: "Aladdin".to_string(),
            password: "open sesame".to_string(),
            ..Default::default()
        };
        let mut headers = HashMap::new();
        profile.apply_to_headers(&mut headers);
        assert_eq!(
            headers.get("Authorization").map(String::as_str),
            Some("Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ==")
        );
    }

    #[test]
    fn normalize_site_accepts_url_or_host_and_defaults_bare_host_to_https() {
        assert_eq!(
            normalize_site("https://Example.COM/path"),
            Some("https://example.com".to_string())
        );
        assert_eq!(
            normalize_site("example.com:8443"),
            Some("https://example.com:8443".to_string())
        );
        assert_eq!(
            normalize_site("http://example.com"),
            Some("http://example.com".to_string())
        );
    }

    #[test]
    fn expiry_is_strict() {
        let profile = AuthProfile {
            expires_at: Some(100),
            ..Default::default()
        };
        assert!(profile.is_valid_at(99));
        assert!(!profile.is_valid_at(100));
    }

    #[test]
    fn is_sensitive_config_key_covers_profiles_and_site_auth() {
        assert!(is_sensitive_config_key(AUTH_PROFILES_CONFIG_KEY));
        assert!(is_sensitive_config_key("site_auth_credentials"));
        assert!(is_sensitive_config_key("plugin.a@b.auth.https://x.com"));
        assert!(is_sensitive_config_key("link.identity_secret"));
        assert!(!is_sensitive_config_key("plugin.a@b.enabled"));
        assert!(!is_sensitive_config_key("plugin.dev.a@b"));
    }

    async fn open_test_db() -> (Db, std::path::PathBuf) {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("fluxdown_auth_test_{}_{nanos}", std::process::id()));

        if let Err(error) = std::fs::remove_dir_all(&dir) {
            assert_eq!(
                error.kind(),
                std::io::ErrorKind::NotFound,
                "remove test directory: {error}"
            );
        }
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let db = Db::open(&dir).await.expect("open test db");
        (db, dir)
    }

    #[tokio::test]
    async fn corrupted_legacy_table_is_discarded_instead_of_blocking_save() {
        let (db, dir) = open_test_db().await;
        db.set_config(AUTH_PROFILES_CONFIG_KEY, "not json")
            .await
            .expect("seed corrupted legacy table");
        let profile = AuthProfile {
            plugin_id: "a@b".to_string(),
            site: "https://example.com".to_string(),
            cookies: "sid=1".to_string(),
            ..Default::default()
        };
        save(&db, &profile)
            .await
            .expect("save must not fail-closed on corrupted legacy table");
        assert!(
            db.get_config(AUTH_PROFILES_CONFIG_KEY)
                .await
                .expect("read back")
                .is_none(),
            "corrupted legacy key must be dropped, not left dangling"
        );
        let loaded = load(&db, "a@b::https://example.com")
            .await
            .expect("load")
            .expect("profile persisted under new per-key format");
        assert_eq!(loaded.cookies, "sid=1");

        if let Err(error) = std::fs::remove_dir_all(dir)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            eprintln!("remove auth test directory: {error}");
        }
    }

    #[tokio::test]
    async fn legacy_table_site_keys_are_migrated_with_https_scheme() {
        let (db, dir) = open_test_db().await;
        let legacy = serde_json::json!({
            "a@b::example.com": {
                "authRef": "a@b::example.com",
                "pluginId": "a@b",
                "site": "example.com",
                "cookies": "sid=1",
            }
        });
        db.set_config(AUTH_PROFILES_CONFIG_KEY, &legacy.to_string())
            .await
            .expect("seed legacy table");
        migrate_legacy(&db).await.expect("migrate");
        let migrated = load(&db, "a@b::https://example.com")
            .await
            .expect("load")
            .expect("legacy entry migrated under https-qualified key");
        assert_eq!(migrated.site, "https://example.com");
        assert_eq!(migrated.auth_ref, "a@b::https://example.com");

        if let Err(error) = std::fs::remove_dir_all(dir)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            eprintln!("remove auth test directory: {error}");
        }
    }
}
