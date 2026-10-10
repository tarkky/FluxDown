//! daemon 进程环境配置与 loopback 绑定约束。

use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use fluxdown_protocol::{
    DaemonConfigError, is_public_daemon_config_key, normalize_daemon_config_patch,
};

/// daemon 默认控制端口。
pub const DEFAULT_DAEMON_BIND: &str = "127.0.0.1:17801";

/// daemon 进程级配置。
#[derive(Clone, Debug)]
pub struct DaemonConfig {
    pub bind_addr: SocketAddr,
    pub data_dir_override: Option<PathBuf>,
    pub database_url: Option<String>,
    pub token_file_override: Option<PathBuf>,
    /// `FLUXDOWN_SAVE_DIR`：仅在库中 `default_save_dir` 尚未设置时播种（见 `init_default_config`）。
    pub save_dir_seed: Option<String>,
    /// 演示模式：`Some(url)` 时任务 / 任务组创建只放行该 URL（trim 后精确比较）。
    pub demo_url: Option<String>,
}

/// daemon 配置错误。
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("FLUXDOWN_DAEMON_BIND is invalid: {0}")]
    InvalidBind(String),
    #[error("FLUXDOWN_DAEMON_BIND must be loopback, got {0}")]
    NonLoopback(IpAddr),
    #[error("unknown or daemon-private config field: {0}")]
    UnknownField(String),
    #[error("config field is read-only: {0}")]
    ReadOnlyField(String),
    #[error("invalid config value for {field}: {message}")]
    InvalidValue { field: String, message: String },
}

impl DaemonConfig {
    /// 从环境读取配置并拒绝非 loopback 绑定。
    pub fn from_env() -> Result<Self, ConfigError> {
        let bind_text = std::env::var("FLUXDOWN_DAEMON_BIND")
            .unwrap_or_else(|_| DEFAULT_DAEMON_BIND.to_owned());
        let bind_addr = bind_text
            .parse::<SocketAddr>()
            .map_err(|error| ConfigError::InvalidBind(error.to_string()))?;
        if !bind_addr.ip().is_loopback() {
            return Err(ConfigError::NonLoopback(bind_addr.ip()));
        }
        Ok(Self {
            bind_addr,
            data_dir_override: std::env::var_os("FLUXDOWN_DATA_DIR").map(PathBuf::from),
            database_url: std::env::var("FLUXDOWN_DATABASE_URL")
                .ok()
                .filter(|value| !value.trim().is_empty()),
            token_file_override: std::env::var_os("FLUXDOWN_DAEMON_TOKEN_FILE").map(PathBuf::from),
            save_dir_seed: std::env::var("FLUXDOWN_SAVE_DIR")
                .ok()
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty()),
            demo_url: demo_url_from_env(),
        })
    }

    /// 嵌入式（进程内）daemon 配置：不读环境，数据目录由嵌入方给出。
    ///
    /// 绑定 `127.0.0.1:0`（系统分配的临时 loopback 端口，真实地址经
    /// [`crate::runtime::DaemonReady`] 报告）；无演示模式，无外部数据库。
    #[must_use]
    pub fn embedded(data_dir: PathBuf, save_dir_seed: Option<String>) -> Self {
        Self {
            bind_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            data_dir_override: Some(data_dir),
            database_url: None,
            token_file_override: None,
            save_dir_seed: save_dir_seed
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty()),
            demo_url: None,
        }
    }
}

/// 演示模式 URL：`FLUXDOWN_DEMO_URL` 优先；否则 `FLUXDOWN_DEMO` 为真值时用内置演示文件
/// （`http://127.0.0.1:<FLUXDOWN_BIND 端口>/demo/file`，由 agent 的 `/demo/file` 托管）。
fn demo_url_from_env() -> Option<String> {
    let url = std::env::var("FLUXDOWN_DEMO_URL")
        .ok()
        .as_deref()
        .and_then(parse_demo_url);
    url.or_else(|| {
        std::env::var("FLUXDOWN_DEMO")
            .is_ok_and(|value| flag_truthy(&value))
            .then(|| {
                let bind = std::env::var("FLUXDOWN_BIND").unwrap_or_default();
                builtin_demo_url(&bind)
            })
    })
}

/// 归一化 `FLUXDOWN_DEMO_URL`：去掉首尾空白与误带的包裹引号（Windows cmd 的
/// `set X="v" && …` 会把引号和尾部空格一并写进值），归一化后为空视为未开启。
fn parse_demo_url(raw: &str) -> Option<String> {
    let mut value = raw.trim();
    for quote in ['"', '\''] {
        if value.len() >= 2 && value.starts_with(quote) && value.ends_with(quote) {
            value = value[1..value.len() - 1].trim();
        }
    }
    (!value.is_empty()).then(|| value.to_owned())
}

fn flag_truthy(value: &str) -> bool {
    matches!(
        value
            .trim()
            .trim_matches(['"', '\''])
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// 内置演示 URL：回环访问同进程组托管的 `/demo/file`，端口取 `bind` 末段，缺省 17800。
fn builtin_demo_url(bind: &str) -> String {
    let port = bind
        .rsplit(':')
        .next()
        .and_then(|port| port.parse::<u16>().ok())
        .unwrap_or(17800);
    format!("http://127.0.0.1:{port}/demo/file")
}

/// 演示模式守卫：`demo_url` 已设置且请求 URL 与之不符时返回 `false`（trim 后精确比较，
/// 前缀 / 追加查询串不放行；空 URL 的种子任务在演示模式下同样拒绝）。
#[must_use]
pub fn demo_allows(demo_url: Option<&str>, url: &str) -> bool {
    demo_url.is_none_or(|allowed| url.trim() == allowed)
}

/// 校验并规范化客户端可写的 daemon 设置（键表与值域唯一来源：
/// [`fluxdown_protocol::DAEMON_CONFIG_FIELDS`]）。
///
/// 规范化后的布尔值再按引擎落库编码转写：`bt_seed_enabled` /
/// `bt_auto_reseed` 在引擎侧按 `"0"` 判定关闭（保持既有 `'1'`/`'0'` 编码），
/// 若原样写入 `"false"` 引擎会视为开启。
pub fn validate_config_patch(
    values: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, ConfigError> {
    let mut normalized = normalize_daemon_config_patch(values).map_err(|error| match error {
        DaemonConfigError::UnknownField(key) => ConfigError::UnknownField(key),
        DaemonConfigError::ReadOnly(key) => ConfigError::ReadOnlyField(key),
        DaemonConfigError::InvalidValue { field, message } => {
            ConfigError::InvalidValue { field, message }
        }
    })?;
    for key in ENGINE_NUMERIC_BOOL_KEYS {
        if let Some(value) = normalized.get_mut(*key) {
            *value = if value == "true" { "1" } else { "0" }.to_owned();
        }
    }
    Ok(normalized)
}

/// 引擎以 `"1"`/`"0"` 读取的布尔键（见 `bt_downloader` / `download_manager`
/// 中的 `get_config` 判定）。
const ENGINE_NUMERIC_BOOL_KEYS: &[&str] = &["bt_seed_enabled", "bt_auto_reseed"];

/// 从完整 engine config 中只投影 daemon UI 可见的键（含只读键，如
/// `bt_tracker_sub_cache` / `domain_conn_caps`；daemon 私有键与凭据表不投影）。
#[must_use]
pub fn public_config_values(all: &HashMap<String, String>) -> BTreeMap<String, String> {
    all.iter()
        .filter(|(key, _)| is_public_daemon_config_key(key))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

/// 从持久化配置构建完整 BT 运行配置。
///
/// `bt_seed_time_limit_minutes` / `bt_seed_inactive_time_limit_minutes` 落库
/// 时已是分钟；`*_unit` 键仅记录设置页的展示单位，
/// 引擎 [`fluxdown_engine::bt_downloader::BtConfig`] 直接取分钟值。
/// `bt_seed_enabled` / `bt_auto_reseed` 不在 `BtConfig` 内：引擎在完成 /
/// 启动时实时读库，落库即生效。
#[must_use]
pub fn bt_config_from_map(
    cfg: &HashMap<String, String>,
) -> fluxdown_engine::bt_downloader::BtConfig {
    use fluxdown_engine::bt_downloader::BtMseMode;
    use fluxdown_engine::bt_seeding::SeedingLimitOperator;

    let subscription_enabled = cfg
        .get("bt_tracker_sub_enabled")
        .map(|value| value == "true")
        .unwrap_or(true);
    fluxdown_engine::bt_downloader::BtConfig {
        enabled: cfg
            .get("bt_enabled")
            .map(|value| value == "true")
            .unwrap_or(true),
        enable_dht: cfg
            .get("bt_enable_dht")
            .map(|value| value == "true")
            .unwrap_or(true),
        enable_upnp: cfg
            .get("bt_enable_upnp")
            .map(|value| value == "true")
            .unwrap_or(true),
        port_start: cfg
            .get("bt_port_start")
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or(6881),
        port_end: cfg
            .get("bt_port_end")
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or(6891),
        custom_trackers: cfg.get("bt_custom_trackers").cloned().unwrap_or_default(),
        subscription_trackers: if subscription_enabled {
            cfg.get("bt_tracker_sub_cache").cloned().unwrap_or_default()
        } else {
            String::new()
        },
        seed_ratio_limit: cfg
            .get("bt_seed_ratio_limit")
            .and_then(|value| value.parse::<f64>().ok())
            .unwrap_or(0.0),
        seed_post_ratio_limit: cfg
            .get("bt_seed_post_ratio_limit")
            .and_then(|value| value.parse::<f64>().ok())
            .unwrap_or(0.0),
        seed_time_limit_minutes: cfg
            .get("bt_seed_time_limit_minutes")
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0),
        seed_inactive_time_limit_minutes: cfg
            .get("bt_seed_inactive_time_limit_minutes")
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0),
        seed_limit_operator: cfg
            .get("bt_seed_limit_operator")
            .map(|value| {
                if value.eq_ignore_ascii_case("and") {
                    SeedingLimitOperator::And
                } else {
                    SeedingLimitOperator::Or
                }
            })
            .unwrap_or(SeedingLimitOperator::Or),
        seed_then_action: cfg
            .get("bt_seed_then_action")
            .cloned()
            .unwrap_or_else(|| "stop".to_owned()),
        seed_max_active: cfg
            .get("bt_seed_max_active")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0),
        mse_mode: cfg
            .get("bt_mse_mode")
            .map(String::as_str)
            .map(BtMseMode::from)
            .unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use super::{ConfigError, public_config_values, validate_config_patch};

    #[test]
    fn loopback_predicate_rejects_public_bind() {
        let loopback = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 17801);
        let public = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 17801);
        assert!(loopback.ip().is_loopback());
        assert!(!public.ip().is_loopback());
    }

    #[test]
    fn embedded_config_uses_an_ephemeral_loopback_port_and_no_environment() {
        let dir = std::path::PathBuf::from("data");
        let config = super::DaemonConfig::embedded(dir.clone(), Some("  /downloads ".to_owned()));
        assert!(config.bind_addr.ip().is_loopback());
        assert_eq!(config.bind_addr.port(), 0);
        assert_eq!(config.data_dir_override, Some(dir));
        assert_eq!(config.save_dir_seed.as_deref(), Some("/downloads"));
        assert!(config.database_url.is_none());
        assert!(config.token_file_override.is_none());
        assert!(config.demo_url.is_none());

        let blank =
            super::DaemonConfig::embedded(std::path::PathBuf::from("data"), Some(" ".into()));
        assert!(blank.save_dir_seed.is_none());
    }

    #[test]
    fn sync_catalog_semantics_normalize_without_losing_flutter_values() {
        let values = BTreeMap::from([
            ("auto_max_connections".to_owned(), "0".to_owned()),
            ("max_auto_retries".to_owned(), "-1".to_owned()),
            ("auto_resume_on_start".to_owned(), "true".to_owned()),
            ("ed2k_enable_upnp".to_owned(), "false".to_owned()),
            ("ed2k_server_list".to_owned(), "ed2k://server".to_owned()),
            ("bt_seed_then_action".to_owned(), "delete_files".to_owned()),
            ("bt_mse_mode".to_owned(), "enabled".to_owned()),
        ]);
        let normalized = validate_config_patch(&values).expect("valid synced config");
        assert_eq!(normalized, values);
    }

    #[test]
    fn catalog_keys_new_to_daemon_are_accepted_and_engine_encoded() {
        let values = BTreeMap::from([
            ("bt_seed_enabled".to_owned(), "false".to_owned()),
            ("bt_auto_reseed".to_owned(), "1".to_owned()),
            ("bt_seed_time_limit_unit".to_owned(), "hours".to_owned()),
            (
                "bt_seed_inactive_time_limit_unit".to_owned(),
                "days".to_owned(),
            ),
            ("ed2k_listen_port".to_owned(), " 4662 ".to_owned()),
            ("default_queue_id".to_owned(), "later".to_owned()),
        ]);
        let normalized = validate_config_patch(&values).expect("catalog keys accepted");
        assert_eq!(
            normalized["bt_seed_enabled"], "0",
            "engine reads `!= \"0\"`"
        );
        assert_eq!(normalized["bt_auto_reseed"], "1");
        assert_eq!(normalized["bt_seed_time_limit_unit"], "hours");
        assert_eq!(normalized["bt_seed_inactive_time_limit_unit"], "days");
        assert_eq!(normalized["ed2k_listen_port"], "4662");
        assert_eq!(normalized["default_queue_id"], "later");
    }

    #[test]
    fn read_only_and_unknown_keys_are_rejected_on_patch() {
        for key in [
            "domain_conn_caps",
            "bt_tracker_sub_cache",
            "bt_tracker_sub_updated_at",
            "ed2k_server_sub_cache",
        ] {
            let patch = BTreeMap::from([(key.to_owned(), String::new())]);
            assert!(
                matches!(validate_config_patch(&patch), Err(ConfigError::ReadOnlyField(k)) if k == key),
                "{key} must be rejected as read-only"
            );
        }
        for key in ["site_auth_credentials", "daemon_config_revision", "nope"] {
            let patch = BTreeMap::from([(key.to_owned(), "x".to_owned())]);
            assert!(
                matches!(validate_config_patch(&patch), Err(ConfigError::UnknownField(k)) if k == key),
                "{key} must be rejected as unknown"
            );
        }
        let bad = BTreeMap::from([("ed2k_listen_port".to_owned(), "70000".to_owned())]);
        assert!(matches!(
            validate_config_patch(&bad),
            Err(ConfigError::InvalidValue { field, .. }) if field == "ed2k_listen_port"
        ));
    }

    #[test]
    fn component_mirror_base_patch_rejects_plain_http_with_field_name() {
        let insecure = BTreeMap::from([(
            "component_mirror_base".to_owned(),
            "http://gh-proxy.example.com".to_owned(),
        )]);
        assert!(matches!(
            validate_config_patch(&insecure),
            Err(ConfigError::InvalidValue { field, .. }) if field == "component_mirror_base"
        ));
        let secure = BTreeMap::from([(
            "component_mirror_base".to_owned(),
            "https://gh-proxy.example.com/".to_owned(),
        )]);
        let normalized = validate_config_patch(&secure).expect("https mirror accepted");
        assert_eq!(
            normalized["component_mirror_base"],
            "https://gh-proxy.example.com"
        );
    }

    #[test]
    fn public_projection_keeps_read_only_keys_and_hides_private_ones() {
        let all = HashMap::from([
            ("max_concurrent_tasks".to_owned(), "5".to_owned()),
            ("bt_tracker_sub_cache".to_owned(), "udp://t".to_owned()),
            ("domain_conn_caps".to_owned(), "v3".to_owned()),
            ("proxy_password".to_owned(), "secret".to_owned()),
            ("site_auth_credentials".to_owned(), "{}".to_owned()),
            ("daemon_config_revision".to_owned(), "7".to_owned()),
            ("daemon_migration_link_acked".to_owned(), "1".to_owned()),
        ]);
        let public = public_config_values(&all);
        assert_eq!(public["max_concurrent_tasks"], "5");
        assert_eq!(public["bt_tracker_sub_cache"], "udp://t");
        assert_eq!(public["domain_conn_caps"], "v3");
        assert_eq!(public["proxy_password"], "secret");
        assert!(!public.contains_key("site_auth_credentials"));
        assert!(!public.contains_key("daemon_config_revision"));
        assert!(!public.contains_key("daemon_migration_link_acked"));
    }

    #[test]
    fn bt_config_from_map_parses_bt_enabled() {
        let mut map = std::collections::HashMap::new();
        assert!(super::bt_config_from_map(&map).enabled, "defaults to true");

        map.insert("bt_enabled".to_string(), "false".to_string());
        assert!(!super::bt_config_from_map(&map).enabled, "explicit false");

        map.insert("bt_enabled".to_string(), "true".to_string());
        assert!(super::bt_config_from_map(&map).enabled, "explicit true");
    }
}

#[cfg(test)]
mod demo_tests {
    use super::{builtin_demo_url, demo_allows, parse_demo_url};

    const DEMO: &str = "https://example.com/demo.bin";

    #[test]
    fn demo_url_strips_whitespace_and_wrapping_quotes() {
        let want = Some(DEMO.to_owned());
        assert_eq!(parse_demo_url(DEMO), want);
        assert_eq!(parse_demo_url("\"https://example.com/demo.bin\" "), want);
        assert_eq!(parse_demo_url("'https://example.com/demo.bin'"), want);
        assert_eq!(parse_demo_url("  "), None);
        assert_eq!(parse_demo_url("\"\""), None);
    }

    #[test]
    fn builtin_demo_url_uses_bind_port_over_loopback() {
        assert_eq!(
            builtin_demo_url("0.0.0.0:17800"),
            "http://127.0.0.1:17800/demo/file"
        );
        assert_eq!(
            builtin_demo_url("[::]:9000"),
            "http://127.0.0.1:9000/demo/file"
        );
        assert_eq!(builtin_demo_url(""), "http://127.0.0.1:17800/demo/file");
    }

    #[test]
    fn demo_guard_is_exact_match_after_trim() {
        assert!(demo_allows(None, "https://evil.example/anything.iso"));
        assert!(demo_allows(Some(DEMO), DEMO));
        assert!(demo_allows(Some(DEMO), &format!("  {DEMO}\n")));
        for url in [
            "https://example.com/other.bin",
            "https://example.com/demo.bin?x=1",
            "https://example.com/demo.bin/../secret",
            "",
        ] {
            assert!(!demo_allows(Some(DEMO), url), "{url:?} must be rejected");
        }
    }
}
