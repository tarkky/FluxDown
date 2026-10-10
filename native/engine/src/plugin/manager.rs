//! `PluginManager` —— 插件装载 / 启停 / resolve / notify / 安装 / 设置。
//!
//! `Arc` 共享，注入 `DownloadManager`。`RwLock<Arc<Vec<LoadedPlugin>>>` 读多写少：
//! install/uninstall/toggle 写（整表原子替换），resolve/hook match 读。**不用 arc-swap**
//! （workspace 无此依赖且约束禁新增）。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use serde::Deserialize;
use tokio::sync::RwLock;

use crate::db::Db;
use crate::events::{EngineEvent, EventSink};
use crate::logger::log_info;
use crate::rss::parser::{ParsedFeed, ParsedItem};
use crate::subscription::{
    SubscriptionFetchFuture, SubscriptionFetchRequest, SubscriptionProvider,
};

use super::manifest::{
    PERMISSION_AUTH, PERMISSION_FFMPEG, PERMISSION_YTDLP, PluginManifest, SettingField,
    SettingType, is_safe_relative_path, is_valid_identity,
};
use super::quickjs::HARD_TIMEOUT_CEILING;
use super::runtime::{
    AuthRequest, AuthResult, ExecutionBudget, HostContext, ManifestItem, PluginBridge,
    PluginEntryKind, PluginError, PluginEvent, PluginScript, ResolveManifest, ResolveRequest,
    ResolveResult, ScriptRuntime, SubscriptionRequest,
};

/// 连续超时/超内存达到该次数 → 自动熔断禁用。
const CIRCUIT_BREAKER_THRESHOLD: u32 = 3;

/// 默认 resolve 预算。
const DEFAULT_RESOLVE_BUDGET: ExecutionBudget = ExecutionBudget {
    timeout: Duration::from_secs(10),
    memory_limit_bytes: 64 * 1024 * 1024,
};
/// 默认 hook 预算。
const DEFAULT_HOOK_BUDGET: ExecutionBudget = ExecutionBudget {
    timeout: Duration::from_secs(5),
    memory_limit_bytes: 32 * 1024 * 1024,
};
/// 外部工具（ffmpeg / yt-dlp）授权插件的 hook 墙钟预算：容纳长时子进程（转码 /
/// 下载可达分钟级）。CPU/中断预算仍是 30s（见 [`super::quickjs`]，`await` 不烧
/// CPU、不计入中断顶），内存与普通 hook 一致。
const EXTERNAL_TOOL_HOOK_BUDGET: ExecutionBudget = ExecutionBudget {
    timeout: Duration::from_secs(1830),
    memory_limit_bytes: 32 * 1024 * 1024,
};
/// auth 平面默认预算（M-2）：与 resolve 的 manifest.resolvers[0].timeoutMs 完全
/// 解耦——一个把 resolver 超时调到 500ms 的插件不该连带把登录脚本的墙钟预算
/// 也压到 500ms。可被 manifest `auth.timeoutMs` 下调，30s 硬顶。
const DEFAULT_AUTH_TIMEOUT: Duration = Duration::from_secs(30);

/// 禁用原因（PascalCase 序列化，全文一致）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DisabledReason {
    None,
    Manual,
    CircuitBreaker,
}

impl DisabledReason {
    pub fn as_str(self) -> &'static str {
        match self {
            DisabledReason::None => "None",
            DisabledReason::Manual => "Manual",
            DisabledReason::CircuitBreaker => "CircuitBreaker",
        }
    }
    pub fn parse(s: &str) -> Self {
        match s {
            "Manual" => DisabledReason::Manual,
            "CircuitBreaker" => DisabledReason::CircuitBreaker,
            _ => DisabledReason::None,
        }
    }
}

/// 插件加载状态（PascalCase 序列化，全文一致；wire 契约固定为
/// `"Loaded"`/`"Failed"`，三端客户端按字面量比较，不可改动，见 671#4）。
/// 单点定义比照同文件 [`DisabledReason`] 的先例：生产端（[`PluginManager::list`]）
/// 统一从这里取字面量，杜绝散落的字符串字面量漂移。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginLoadStatus {
    Loaded,
    Failed,
}

impl PluginLoadStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Loaded => "Loaded",
            Self::Failed => "Failed",
        }
    }
}

/// 已加载插件的运行态。
pub struct LoadedPlugin {
    pub manifest: PluginManifest,
    /// 插件目录（安装模式）或 dev 目录。
    pub dir: PathBuf,
    /// devMode：每次调用按最新源码重读（不缓存）。
    pub dev: bool,
    pub enabled: bool,
    pub disabled_reason: DisabledReason,
    /// resolver 入口绝对路径（若声明）。
    resolver_entry: Option<PathBuf>,
    /// hooks 入口绝对路径（若声明）。
    hooks_entry: Option<PathBuf>,
    /// auth 入口绝对路径（若声明）。
    auth_entry: Option<PathBuf>,
    /// subscription provider 入口绝对路径（若声明）。
    subscription_entry: Option<PathBuf>,
    /// 非 dev 模式的缓存源码（加载时读入）。
    resolver_cache: Option<String>,
    hooks_cache: Option<String>,
    auth_cache: Option<String>,
    subscription_cache: Option<String>,
    /// 熔断计数（连续 Timeout/MemoryLimit）。
    timeout_streak: Arc<AtomicU32>,
}

impl LoadedPlugin {
    async fn resolver_source(&self) -> Option<String> {
        match (&self.resolver_entry, self.dev) {
            (Some(p), true) => tokio::fs::read_to_string(p).await.ok(),
            (Some(_), false) => self.resolver_cache.clone(),
            (None, _) => None,
        }
    }
    async fn hooks_source(&self) -> Option<String> {
        match (&self.hooks_entry, self.dev) {
            (Some(p), true) => tokio::fs::read_to_string(p).await.ok(),
            (Some(_), false) => self.hooks_cache.clone(),
            (None, _) => None,
        }
    }

    async fn auth_source(&self) -> Option<String> {
        match (&self.auth_entry, self.dev) {
            (Some(p), true) => tokio::fs::read_to_string(p).await.ok(),
            (Some(_), false) => self.auth_cache.clone(),
            (None, _) => None,
        }
    }

    async fn subscription_source(&self) -> Option<String> {
        match (&self.subscription_entry, self.dev) {
            (Some(p), true) => tokio::fs::read_to_string(p).await.ok(),
            (Some(_), false) => self.subscription_cache.clone(),
            (None, _) => None,
        }
    }
}

/// 动态插件订阅路由。provider 安装、启停或卸载后无需重建引擎，路由每次
/// 调用都向 `PluginManager` 读取当前快照。
pub struct PluginSubscriptionRouter {
    manager: Arc<PluginManager>,
}

impl SubscriptionProvider for PluginSubscriptionRouter {
    fn id(&self) -> &str {
        "*"
    }

    fn fetch(&self, request: SubscriptionFetchRequest) -> SubscriptionFetchFuture {
        let manager = self.manager.clone();
        let provider_id = request.provider_id.clone();
        Box::pin(async move {
            manager
                .fetch_subscription(&provider_id, request)
                .await
                .map_err(|e| e.to_string())
        })
    }
}

/// 传给 UI 的插件视图。
#[derive(Debug, Clone)]
pub struct PluginInfo {
    pub identity: String,
    pub name: String,
    pub version: String,
    pub description: String,
    pub homepage: String,
    pub enabled: bool,
    pub dev_mode: bool,
    pub disabled_reason: String,
    pub settings: Vec<SettingField>,
    /// 当前设置值（key → value 字符串）。
    pub settings_values: Vec<(String, String)>,
    /// manifest 声明的能力权限（供 UI 展示授权，如 `["ffmpeg"]`）。
    pub permissions: Vec<String>,
    /// 是否声明平台登录入口。
    pub auth_supported: bool,
    /// manifest 声明的订阅 provider ID；仅启用插件会被前端作为可选订阅来源展示。
    pub subscription_provider_ids: Vec<String>,
    /// 加载状态：`Loaded` 表示已通过启动时的 manifest/源码加载，`Failed` 表示
    /// 目录仍存在但加载失败。与 `enabled` 分离：手动禁用的插件仍可能已加载。
    pub load_status: String,
    /// 加载失败的可读原因；加载成功时为空。
    pub load_error: String,
}

/// 目录或 dev 配置仍存在、但没有进入运行时快照的插件。
///
/// 失败插件不能混入 `LoadedPlugin`，否则解析链路会误把它当作可运行插件；
/// 但管理页仍需要看到它，才能展示原因并允许用户卸载或修复。
#[derive(Debug, Clone)]
struct FailedPlugin {
    identity: String,
    /// 实际扫描到的插件目录。identity 可能来自非法 manifest，不能反向拼接路径。
    dir: Box<Path>,
    manifest: Option<Box<PluginManifest>>,
    dev_mode: bool,
    error: String,
}

/// 安装来源判别（供 actor 分发规则表）。
pub enum InstallSource {
    Zip(Vec<u8>),
    Dir(PathBuf),
    Dev(PathBuf),
}

/// 插件管理器。
pub struct PluginManager {
    runtime: Arc<dyn ScriptRuntime>,
    bridge: Arc<dyn PluginBridge>,
    plugins: RwLock<Arc<Vec<LoadedPlugin>>>,
    failed_plugins: RwLock<Arc<Vec<FailedPlugin>>>,
    db: Db,
    root: PathBuf,
    app_version: String,
    resolve_budget: ExecutionBudget,
    hook_budget: ExecutionBudget,
    sink: Arc<dyn EventSink>,
    /// 串行化 `load_all`：扫描 + 整表替换非原子，交错的两次重载会用旧快照
    /// 覆盖新的启停/熔断状态。
    load_lock: tokio::sync::Mutex<()>,
}

impl PluginManager {
    /// 构造（不加载）；随后调用 [`Self::load_all`]。
    pub fn new(
        runtime: Arc<dyn ScriptRuntime>,
        bridge: Arc<dyn PluginBridge>,
        db: Db,
        root: PathBuf,
        app_version: String,
        sink: Arc<dyn EventSink>,
    ) -> Self {
        Self {
            runtime,
            bridge,
            plugins: RwLock::new(Arc::new(Vec::new())),
            failed_plugins: RwLock::new(Arc::new(Vec::new())),
            db,
            root,
            app_version,
            resolve_budget: DEFAULT_RESOLVE_BUDGET,
            hook_budget: DEFAULT_HOOK_BUDGET,
            sink,
            load_lock: tokio::sync::Mutex::new(()),
        }
    }

    /// 专用运行时 handle，供 off-actor worker spawn（禁止裸 tokio::spawn）。
    pub fn runtime_handle(&self) -> tokio::runtime::Handle {
        self.runtime.spawn_handle()
    }

    /// 宿主并发上限变化时同步 resolve 并发容量（见 [`ScriptRuntime::set_resolve_capacity`]）。
    pub fn set_resolve_capacity(&self, max_concurrent: usize) {
        self.runtime.set_resolve_capacity(max_concurrent);
    }

    /// 扫描根目录 + `plugin.dev.*` 键，解析并加载全部插件。
    pub async fn load_all(&self) {
        let _serial = self.load_lock.lock().await;
        let mut loaded: Vec<LoadedPlugin> = Vec::new();
        let mut failed: Vec<FailedPlugin> = Vec::new();

        // 1. 安装目录下的子目录。
        if let Ok(mut rd) = tokio::fs::read_dir(&self.root).await {
            while let Ok(Some(entry)) = rd.next_entry().await {
                let path = entry.path();
                let is_internal_dir = path
                    .file_name()
                    .is_some_and(|name| name.to_string_lossy().starts_with('.'));
                if path.is_dir() && !is_internal_dir {
                    match self.load_one(&path, false, None).await {
                        Ok(p) => loaded.push(p),
                        Err(p) => failed.push(p),
                    }
                }
            }
        }

        // 2. dev 插件（plugin.dev.<identity> = abs path）。
        if let Ok(entries) = self.db.list_config_with_prefix("plugin.dev.").await {
            for (_k, path_str) in entries {
                let path = PathBuf::from(&path_str);
                match self
                    .load_one(&path, true, Some(identity_from_dev_key(&_k)))
                    .await
                {
                    Ok(p) => loaded.push(p),
                    Err(p) => failed.push(p),
                }
            }
        }

        let deduped = dedup_plugins_by_identity(loaded, &mut failed);

        *self.plugins.write().await = Arc::new(deduped);
        *self.failed_plugins.write().await = Arc::new(failed);
    }

    /// 加载单个插件目录。失败记 warn 并保留诊断信息（不阻塞其他插件）。
    async fn load_one(
        &self,
        dir: &Path,
        dev: bool,
        identity_hint: Option<&str>,
    ) -> Result<LoadedPlugin, FailedPlugin> {
        let manifest_path = dir.join("manifest.json");
        let bytes = match tokio::fs::read(&manifest_path).await {
            Ok(bytes) => bytes,
            Err(error) => {
                let failure = FailedPlugin {
                    identity: failure_identity(dir, identity_hint),
                    dir: dir.to_path_buf().into_boxed_path(),
                    manifest: None,
                    dev_mode: dev,
                    error: format!("读取 manifest.json 失败: {error}"),
                };
                log_info!("[plugin] 跳过 {dir:?}: {}", failure.error);
                return Err(failure);
            }
        };
        let manifest = match PluginManifest::parse(&bytes) {
            Ok(manifest) => {
                if let Err(error) = manifest.validate() {
                    log_info!("[plugin] 跳过 {dir:?}: manifest 非法: {error}");
                    return Err(FailedPlugin {
                        identity: manifest.identity.clone(),
                        dir: dir.to_path_buf().into_boxed_path(),
                        manifest: Some(Box::new(manifest)),
                        dev_mode: dev,
                        error: error.to_string(),
                    });
                }
                manifest
            }
            Err(error) => {
                log_info!("[plugin] 跳过 {dir:?}: manifest 非法: {error}");
                return Err(FailedPlugin {
                    identity: failure_identity(dir, identity_hint),
                    dir: dir.to_path_buf().into_boxed_path(),
                    manifest: None,
                    dev_mode: dev,
                    error: error.to_string(),
                });
            }
        };

        // minAppVersion 门槛（app_version 未知或 manifest 未声明时从宽放行）。
        if !manifest.min_app_version.is_empty()
            && !self.app_version.is_empty()
            && !super::semver::satisfies_min(&self.app_version, &manifest.min_app_version)
        {
            let required_version = manifest.min_app_version.clone();
            log_info!(
                "[plugin] 跳过 {}: 需 App ≥ {}，当前 {}",
                manifest.identity,
                required_version,
                self.app_version
            );
            return Err(FailedPlugin {
                identity: manifest.identity.clone(),
                dir: dir.to_path_buf().into_boxed_path(),
                manifest: Some(Box::new(manifest)),
                dev_mode: dev,
                error: format!("需要 App ≥ {}，当前 {}", required_version, self.app_version),
            });
        }

        let resolver_entry = manifest.resolvers.first().map(|r| dir.join(&r.entry));
        let hooks_entry = manifest.hooks.as_ref().map(|h| dir.join(&h.entry));
        let auth_entry = manifest.auth.as_ref().map(|a| dir.join(&a.entry));
        let subscription_entry = manifest.subscriptions.first().map(|s| dir.join(&s.entry));

        // 死订阅检查：同时声明 resolver 与订阅 onMetaProbed → warn（带 resolver 的
        // 任务跳过 probe，onMetaProbed 不会触发）。
        if !manifest.resolvers.is_empty()
            && let Some(h) = &manifest.hooks
            && h.events.iter().any(|e| e == "onMetaProbed")
        {
            log_info!(
                "[plugin] {} 同时声明 resolver 与 onMetaProbed；带 resolver 的任务跳过探测，该钩子不会触发",
                manifest.identity
            );
        }

        // 非 dev：加载时读入源码缓存。
        let (resolver_cache, hooks_cache, auth_cache, subscription_cache) = if dev {
            (None, None, None, None)
        } else {
            let rc = match &resolver_entry {
                Some(p) => match tokio::fs::read_to_string(p).await {
                    Ok(s) => Some(s),
                    Err(e) => {
                        log_info!(
                            "[plugin] 跳过 {}: 读取 resolver 失败: {e}",
                            manifest.identity
                        );
                        return Err(FailedPlugin {
                            identity: manifest.identity.clone(),
                            dir: dir.to_path_buf().into_boxed_path(),
                            manifest: Some(Box::new(manifest.clone())),
                            dev_mode: dev,
                            error: format!("读取 resolver 失败: {e}"),
                        });
                    }
                },
                None => None,
            };
            let hc = match &hooks_entry {
                Some(p) => match tokio::fs::read_to_string(p).await {
                    Ok(s) => Some(s),
                    Err(e) => {
                        log_info!("[plugin] 跳过 {}: 读取 hooks 失败: {e}", manifest.identity);
                        return Err(FailedPlugin {
                            identity: manifest.identity.clone(),
                            dir: dir.to_path_buf().into_boxed_path(),
                            manifest: Some(Box::new(manifest.clone())),
                            dev_mode: dev,
                            error: format!("读取 hooks 失败: {e}"),
                        });
                    }
                },
                None => None,
            };
            let ac = match &auth_entry {
                Some(p) => match tokio::fs::read_to_string(p).await {
                    Ok(s) => Some(s),
                    Err(e) => {
                        log_info!("[plugin] 跳过 {}: 读取 auth 失败: {e}", manifest.identity);
                        return Err(FailedPlugin {
                            identity: manifest.identity.clone(),
                            dir: dir.to_path_buf().into_boxed_path(),
                            manifest: Some(Box::new(manifest.clone())),
                            dev_mode: dev,
                            error: format!("读取 auth 失败: {e}"),
                        });
                    }
                },
                None => None,
            };
            let sc = match &subscription_entry {
                Some(p) => match tokio::fs::read_to_string(p).await {
                    Ok(s) => Some(s),
                    Err(e) => {
                        log_info!(
                            "[plugin] 跳过 {}: 读取 subscription 失败: {e}",
                            manifest.identity
                        );
                        return Err(FailedPlugin {
                            identity: manifest.identity.clone(),
                            dir: dir.to_path_buf().into_boxed_path(),
                            manifest: Some(Box::new(manifest.clone())),
                            dev_mode: dev,
                            error: format!("读取 subscription 失败: {e}"),
                        });
                    }
                },
                None => None,
            };
            (rc, hc, ac, sc)
        };

        let identity = manifest.identity.clone();
        let enabled_str = self
            .db
            .get_config(&format!("plugin.{identity}.enabled"))
            .await
            .ok()
            .flatten();
        let reason_str = self
            .db
            .get_config(&format!("plugin.{identity}.disabled_reason"))
            .await
            .ok()
            .flatten();
        let disabled_reason = reason_str
            .as_deref()
            .map(DisabledReason::parse)
            .unwrap_or(DisabledReason::None);
        // 无 enabled 键 = 新装默认启用。
        let enabled = enabled_str.as_deref().map(|v| v == "true").unwrap_or(true);

        Ok(LoadedPlugin {
            manifest,
            dir: dir.to_path_buf(),
            dev,
            enabled,
            disabled_reason,
            resolver_entry,
            hooks_entry,
            auth_entry,
            subscription_entry,
            resolver_cache,
            hooks_cache,
            auth_cache,
            subscription_cache,
            timeout_streak: Arc::new(AtomicU32::new(0)),
        })
    }

    /// 纯 Rust glob 首匹配（按 identity 字典序稳定排序）。返回命中插件 identity。
    pub async fn match_resolver(&self, url: &str) -> Option<String> {
        let snapshot = self.plugins.read().await.clone();
        let mut candidates: Vec<&LoadedPlugin> = snapshot
            .iter()
            .filter(|p| p.enabled && !p.manifest.resolvers.is_empty())
            .collect();
        candidates.sort_by(|a, b| a.manifest.identity.cmp(&b.manifest.identity));
        for p in candidates {
            for pat in p.manifest.resolver_urls() {
                if super::manifest::url_glob_match(pat, url) {
                    return Some(p.manifest.identity.clone());
                }
            }
        }
        None
    }

    /// 同 [`Self::match_resolver`]，但仅当命中插件的 resolver 声明了
    /// `"multi": true` 时才返回——供前置预解析（[`ResolveManifest`]）判定是否
    /// 触发，避免解析昂贵的单文件插件在创建下载时白跑一次（A2 契约）。
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn run(pm: std::sync::Arc<fluxdown_engine::plugin::PluginManager>) {
    /// // 未声明 multi 的插件恒不命中，即使 URL glob 匹配。
    /// let hit = pm.match_multi_resolver("https://pan.example.com/s/abc").await;
    /// assert!(hit.is_none() || hit.is_some());
    /// # }
    /// ```
    pub async fn match_multi_resolver(&self, url: &str) -> Option<String> {
        let snapshot = self.plugins.read().await.clone();
        let mut candidates: Vec<&LoadedPlugin> = snapshot
            .iter()
            .filter(|p| p.enabled && p.manifest.resolvers.first().is_some_and(|r| r.multi))
            .collect();
        candidates.sort_by(|a, b| a.manifest.identity.cmp(&b.manifest.identity));
        for p in candidates {
            for pat in p.manifest.resolver_urls() {
                if super::manifest::url_glob_match(pat, url) {
                    return Some(p.manifest.identity.clone());
                }
            }
        }
        None
    }

    /// 惰性解析。off-actor worker 调用（在插件专用 runtime 上）。
    ///
    /// **fail-closed**：任务带 resolver 绑定但插件已卸载/被禁用/当前版本无
    /// resolver 时返回 `Err`——绝不放行原始页面 URL（那会把网页 HTML 当媒体文件
    /// 存盘）。用户经「忽略插件重试」逃生舱显式清绑定后方可按原始链接下载；
    /// 正常卸载路径由 [`Self::uninstall`] 批量清绑定，不会走到这里。
    pub async fn resolve(
        &self,
        identity: &str,
        mut req: ResolveRequest,
    ) -> Result<Option<ResolveResult>, PluginError> {
        // 从快照克隆所需（避免跨 await 持锁）。
        let (manifest, streak, source, dev_ver) = {
            let snapshot = self.plugins.read().await.clone();
            let Some(p) = snapshot.iter().find(|p| p.manifest.identity == identity) else {
                return Err(PluginError::Runtime(format!(
                    "插件 {identity} 已卸载或加载失败；可在任务菜单选择「忽略插件重试」按原始链接下载"
                )));
            };
            if !p.enabled {
                let reason = match p.disabled_reason {
                    DisabledReason::CircuitBreaker => "已被熔断自动禁用",
                    _ => "已被禁用",
                };
                return Err(PluginError::Runtime(format!(
                    "插件 {identity} {reason}；重新启用插件，或选择「忽略插件重试」按原始链接下载"
                )));
            }
            let source = p.resolver_source().await;
            (
                p.manifest.clone(),
                p.timeout_streak.clone(),
                source,
                p.manifest.version.clone(),
            )
        };
        let Some(source) = source else {
            return Err(PluginError::Runtime(format!(
                "插件 {identity} 当前版本未提供 resolver（或源码读取失败）；可选择「忽略插件重试」按原始链接下载"
            )));
        };

        // required 运行时校验。
        let values = self.load_setting_values(identity).await;
        for f in &manifest.settings {
            if f.required && value_of(&values, f).is_none() {
                return Err(PluginError::MissingRequiredSetting(format!(
                    "插件 {identity} 需先配置「{}」",
                    f.title
                )));
            }
        }

        let settings_json = build_typed_settings_json(&manifest, &values);
        let budget = self.resolve_budget_for(&manifest);
        let script = PluginScript {
            identity: identity.to_string(),
            source,
            entry_fn_hint: PluginEntryKind::Resolve,
            version: dev_ver,
            app_version: self.app_version.clone(),
        };
        // 每次 resolve 都把同一插件/站点映射到稳定认证引用。插件无需把 Cookie
        // 放进自己的 KV，也无需在每次调用时重新登录。只对声明了 auth 权限的
        // 插件填充（M-5）：未授权插件本就会被 `bridge::http_request` 拒绝
        // 携带显式 authRef（见 `req.auth_allowed`），缺省注入只会让它们在
        // `ctx.authRef` 里看到一个自己永远用不了的值，与 runtime.rs 文档
        // 承诺的字段语义不符。
        if req.auth_ref.is_empty()
            && manifest.has_permission(PERMISSION_AUTH)
            && let Some(auth_ref) = crate::auth::default_auth_ref(identity, &req.url)
        {
            req.auth_ref = auth_ref;
        }
        // 二段防递归判定须在 req 被 invoke_resolve 消费前捕获。
        let second_stage = !req.resolver_item.is_empty();

        let result = self
            .runtime
            .invoke_resolve(
                &script,
                req,
                settings_json,
                self.bridge.clone(),
                budget,
                HostContext {
                    // resolve 平面授予 yt-dlp（直链提取的主战场）；ffmpeg 无产物牢笼故不授予。
                    ytdlp_permitted: manifest.has_permission(PERMISSION_YTDLP),
                    auth_permitted: manifest.has_permission(PERMISSION_AUTH),
                    ..Default::default()
                },
            )
            .await;

        // 熔断计数：连续 Timeout/MemoryLimitExceeded 触发自动禁用。QuickJS 内存
        // 超限在 JS 侧表现为「out of memory」异常，quickjs.rs 的 reclassify_oom
        // 已把它归一为 `MemoryLimitExceeded`——OOM 与超时同样计入熔断。
        match &result {
            Err(PluginError::Timeout) | Err(PluginError::MemoryLimitExceeded) => {
                let n = streak.fetch_add(1, Ordering::SeqCst) + 1;
                if n >= CIRCUIT_BREAKER_THRESHOLD {
                    self.trip_circuit_breaker(identity).await;
                }
            }
            _ => {
                streak.store(0, Ordering::SeqCst);
            }
        }

        let result = result?;
        if let Some(res) = &result {
            validate_resolve_output(res, second_stage)?;
        }
        Ok(result)
    }

    /// 执行插件平台登录入口。登录状态由插件通过 `flux.auth.save` 写入宿主，
    /// 本方法只负责驱动 begin/poll/cancel/logout/status 并把挑战返回给 UI。
    ///
    /// `logout` 是唯一在插件被禁用时也放行的 action（M-3）：禁用插件（手动
    /// 关闭或被熔断）仍可能留有 Cookie/Bearer 等登录态，用户必须能清掉它，
    /// 且不需要为此临时重新启用一个已知有问题的插件去跑它的 JS。这种情况下
    /// 完全不进 JS（禁用插件不该被驱动执行任意脚本），只由宿主兜底删除凭据。
    pub async fn authenticate(
        &self,
        identity: &str,
        mut req: AuthRequest,
    ) -> Result<AuthResult, PluginError> {
        if !matches!(
            req.action.as_str(),
            "begin" | "poll" | "cancel" | "logout" | "status"
        ) {
            return Err(PluginError::InvalidOutput(
                "auth action 必须是 begin/poll/cancel/logout/status".to_string(),
            ));
        }

        let (manifest, source, version, enabled) = {
            let snapshot = self.plugins.read().await.clone();
            let Some(plugin) = snapshot.iter().find(|p| p.manifest.identity == identity) else {
                return Err(PluginError::Runtime(format!("插件 {identity} 不存在")));
            };
            if !plugin.manifest.has_permission(PERMISSION_AUTH) {
                return Err(PluginError::Runtime(format!(
                    "插件 {identity} 未声明 auth 权限"
                )));
            }
            let source = plugin.auth_source().await;
            (
                plugin.manifest.clone(),
                source,
                plugin.manifest.version.clone(),
                plugin.enabled,
            )
        };

        if req.auth_ref.is_empty()
            && !req.site.trim().is_empty()
            && let Some(site) = crate::auth::normalize_site(&req.site)
        {
            req.auth_ref = format!("{identity}::{site}");
        }

        if !enabled {
            if req.action != "logout" {
                return Err(PluginError::Runtime(format!("插件 {identity} 未启用")));
            }
            if req.auth_ref.is_empty() {
                return Err(PluginError::InvalidOutput(
                    "logout 需要 authRef 或 site".to_string(),
                ));
            }
            self.bridge.auth_remove(identity, &req.auth_ref).await?;
            return Ok(AuthResult {
                status: "success".to_string(),
                auth_ref: Some(req.auth_ref),
                ..Default::default()
            });
        }
        let Some(source) = source else {
            return Err(PluginError::Runtime(format!(
                "插件 {identity} 未提供 auth 入口"
            )));
        };

        let values = self.load_setting_values(identity).await;
        for field in &manifest.settings {
            if field.required && value_of(&values, field).is_none() {
                return Err(PluginError::MissingRequiredSetting(format!(
                    "插件 {identity} 需先配置「{}」",
                    field.title
                )));
            }
        }

        let script = PluginScript {
            identity: identity.to_string(),
            source,
            entry_fn_hint: PluginEntryKind::Auth,
            version,
            app_version: self.app_version.clone(),
        };
        let action = req.action.clone();
        let requested_auth_ref = req.auth_ref.clone();
        let invocation = self
            .runtime
            .invoke_auth(
                &script,
                req,
                build_typed_settings_json(&manifest, &values),
                self.bridge.clone(),
                self.auth_budget_for(&manifest),
                HostContext {
                    auth_permitted: true,
                    ..Default::default()
                },
            )
            .await;
        if action == "logout" && !requested_auth_ref.is_empty() {
            self.bridge
                .auth_remove(identity, &requested_auth_ref)
                .await?;
        }
        let mut result = invocation?;
        if result.status == "success" && result.auth_ref.is_none() && !requested_auth_ref.is_empty()
        {
            result.auth_ref = Some(requested_auth_ref);
        }
        Ok(result)
    }

    /// 构造动态插件订阅路由；插件安装、启停或卸载后无需重建该路由。
    pub fn subscription_router(self: &Arc<Self>) -> Arc<dyn SubscriptionProvider> {
        Arc::new(PluginSubscriptionRouter {
            manager: self.clone(),
        })
    }

    /// 调用一个插件订阅 provider，并把返回值规范化为引擎条目。
    pub async fn fetch_subscription(
        &self,
        provider_id: &str,
        request: SubscriptionFetchRequest,
    ) -> Result<ParsedFeed, PluginError> {
        let (manifest, streak, source, version, identity) = {
            let snapshot = self.plugins.read().await.clone();
            let Some(p) = snapshot
                .iter()
                .filter(|p| {
                    p.enabled
                        && p.manifest
                            .subscriptions
                            .first()
                            .is_some_and(|s| s.provider_id == provider_id)
                })
                .min_by(|a, b| a.manifest.identity.cmp(&b.manifest.identity))
            else {
                return Err(PluginError::Runtime(format!(
                    "订阅 provider {provider_id} 未安装或已禁用"
                )));
            };
            let source = p.subscription_source().await;
            (
                p.manifest.clone(),
                p.timeout_streak.clone(),
                source,
                p.manifest.version.clone(),
                p.manifest.identity.clone(),
            )
        };
        let Some(source) = source else {
            return Err(PluginError::Runtime(format!(
                "订阅 provider {provider_id} 的脚本读取失败"
            )));
        };

        let values = self.load_setting_values(&identity).await;
        for field in &manifest.settings {
            if field.required && value_of(&values, field).is_none() {
                return Err(PluginError::MissingRequiredSetting(format!(
                    "插件 {identity} 需先配置「{}」",
                    field.title
                )));
            }
        }

        manifest
            .subscriptions
            .first()
            .ok_or_else(|| PluginError::Runtime("订阅 provider 声明缺失".to_string()))?;
        let budget = self.subscription_budget_for(&manifest);
        let script = PluginScript {
            identity: identity.clone(),
            source,
            entry_fn_hint: PluginEntryKind::Subscription,
            version,
            app_version: self.app_version.clone(),
        };
        let result = self
            .runtime
            .invoke_subscription(
                &script,
                SubscriptionRequest {
                    provider_id: provider_id.to_string(),
                    source_id: request.source_id,
                    url: request.url,
                    provider_config: if request.provider_config.trim().is_empty() {
                        "{}".to_string()
                    } else {
                        request.provider_config
                    },
                    cookies: request.cookies,
                    user_agent: request.user_agent,
                },
                build_typed_settings_json(&manifest, &values),
                self.bridge.clone(),
                budget,
                HostContext {
                    ytdlp_permitted: manifest.has_permission(PERMISSION_YTDLP),
                    auth_permitted: manifest.has_permission(PERMISSION_AUTH),
                    ..Default::default()
                },
            )
            .await;

        match &result {
            Err(PluginError::Timeout) | Err(PluginError::MemoryLimitExceeded) => {
                let n = streak.fetch_add(1, Ordering::SeqCst) + 1;
                if n >= CIRCUIT_BREAKER_THRESHOLD {
                    self.trip_circuit_breaker(&identity).await;
                }
            }
            _ => streak.store(0, Ordering::SeqCst),
        }

        let raw = result?;
        parse_subscription_output(&raw)
    }

    /// 通知平面：遍历声明该事件且 match 命中的启用插件，逐个在插件 runtime 上 spawn
    /// invoke_hook。**全部 fire-and-forget，本函数立即返回**。
    pub async fn notify(&self, event: PluginEvent) {
        let snapshot = self.plugins.read().await.clone();
        let ev_name = event.declared_name();
        let url = event.url().to_string();
        for idx in 0..snapshot.len() {
            let p = &snapshot[idx];
            if !p.enabled {
                continue;
            }
            let Some(hooks) = &p.manifest.hooks else {
                continue;
            };
            if !hooks.events.iter().any(|e| e == ev_name) {
                continue;
            }
            // match.urls（缺省 = 全匹配）。
            let matches = match &hooks.match_decl {
                Some(m) => m
                    .urls
                    .iter()
                    .any(|pat| super::manifest::url_glob_match(pat, &url)),
                None => true,
            };
            if !matches {
                continue;
            }

            let runtime = self.runtime.clone();
            let bridge = self.bridge.clone();
            let db = self.db.clone();
            let manifest = p.manifest.clone();
            let identity = p.manifest.identity.clone();
            let version = p.manifest.version.clone();
            let app_version = self.app_version.clone();
            // ffmpeg 门：授权 + 有产物文件（onDone 才有）→ 注入 flux.ffmpeg 并抬升
            // hook 墙钟预算；牢笼根 = 产物所在目录。其余事件/未授权 → 无 ffmpeg。
            let ffmpeg_permitted = p.manifest.has_permission(PERMISSION_FFMPEG);
            let ytdlp_permitted = p.manifest.has_permission(PERMISSION_YTDLP);
            let auth_permitted = p.manifest.has_permission(PERMISSION_AUTH);
            let ffmpeg_root = match &event {
                PluginEvent::Done { file_path, .. } => {
                    Path::new(file_path).parent().map(Path::to_path_buf)
                }
                _ => None,
            };
            let host = HostContext {
                ffmpeg_permitted,
                ffmpeg_root,
                ytdlp_permitted,
                auth_permitted,
            };
            // 授权外部工具（ffmpeg 有产物牢笼 / yt-dlp 任意上下文）→ 抬升墙钟预算。
            let budget = if (ffmpeg_permitted && host.ffmpeg_root.is_some()) || ytdlp_permitted {
                EXTERNAL_TOOL_HOOK_BUDGET
            } else {
                self.hook_budget
            };
            let event = event.clone();
            // onDone 活动指示：带产物钩子可能长时（ffmpeg 转码），旁路上报
            // 开始/结束供 UI 显示「插件处理中」，不触碰任务状态机。
            let activity_task_id = match &event {
                PluginEvent::Done { task_id, .. } => Some(task_id.clone()),
                _ => None,
            };
            let activity_plugin_id = p.manifest.identity.clone();
            let sink = self.sink.clone();
            let handle = self.runtime.spawn_handle();
            let dev = p.dev;
            let hooks_entry = p.hooks_entry.clone();
            let hooks_cache = p.hooks_cache.clone();

            handle.spawn(async move {
                if let Some(tid) = &activity_task_id {
                    sink.emit(EngineEvent::PluginHookActivity {
                        task_id: tid.clone(),
                        plugin_id: activity_plugin_id.clone(),
                        running: true,
                    });
                }
                let source = match (hooks_entry, dev) {
                    (Some(path), true) => tokio::fs::read_to_string(&path).await.ok(),
                    (Some(_), false) => hooks_cache,
                    (None, _) => None,
                };
                if let Some(source) = source {
                    let values = load_setting_values_db(&db, &identity).await;
                    let settings_json = build_typed_settings_json(&manifest, &values);
                    let script = PluginScript {
                        identity,
                        source,
                        entry_fn_hint: PluginEntryKind::Hook,
                        version,
                        app_version,
                    };
                    runtime
                        .invoke_hook(&script, event, settings_json, bridge, budget, host)
                        .await;
                }
                if let Some(tid) = &activity_task_id {
                    sink.emit(EngineEvent::PluginHookActivity {
                        task_id: tid.clone(),
                        plugin_id: activity_plugin_id,
                        running: false,
                    });
                }
            });
        }
    }

    /// resolve 预算：manifest timeoutMs 可下调，30s 硬顶。
    fn resolve_budget_for(&self, manifest: &PluginManifest) -> ExecutionBudget {
        let timeout = manifest
            .resolvers
            .first()
            .and_then(|r| r.timeout_ms)
            .map(Duration::from_millis)
            .unwrap_or(self.resolve_budget.timeout)
            .min(HARD_TIMEOUT_CEILING);
        ExecutionBudget {
            timeout,
            memory_limit_bytes: self.resolve_budget.memory_limit_bytes,
        }
    }

    /// auth 预算：manifest `auth.timeoutMs` 可下调，默认 [`DEFAULT_AUTH_TIMEOUT`]，
    /// 30s 硬顶。与 [`Self::resolve_budget_for`] 完全独立——不读 resolver 的
    /// timeoutMs（M-2）。
    fn auth_budget_for(&self, manifest: &PluginManifest) -> ExecutionBudget {
        let timeout = manifest
            .auth
            .as_ref()
            .and_then(|a| a.timeout_ms)
            .map(Duration::from_millis)
            .unwrap_or(DEFAULT_AUTH_TIMEOUT)
            .min(HARD_TIMEOUT_CEILING);
        ExecutionBudget {
            timeout,
            memory_limit_bytes: self.resolve_budget.memory_limit_bytes,
        }
    }

    /// subscription 预算：manifest 可下调默认 resolve 预算。
    fn subscription_budget_for(&self, manifest: &PluginManifest) -> ExecutionBudget {
        let timeout = manifest
            .subscriptions
            .first()
            .and_then(|s| s.timeout_ms)
            .map(Duration::from_millis)
            .unwrap_or(self.resolve_budget.timeout)
            .min(HARD_TIMEOUT_CEILING);
        ExecutionBudget {
            timeout,
            memory_limit_bytes: self.resolve_budget.memory_limit_bytes,
        }
    }

    // ---------------------------------------------------------------------
    // 安装 / 卸载 / 启停 / 设置
    // ---------------------------------------------------------------------

    /// 从 zip 字节安装。
    pub async fn install_from_zip(&self, bytes: Vec<u8>) -> Result<String, PluginError> {
        let outcome = super::install::install_from_zip_with_backup(&self.root, &bytes)?;
        self.finish_install_outcome(outcome).await
    }

    /// 从目录安装（不剥壳，path 须直接含 manifest.json）。
    pub async fn install_from_dir(&self, path: &Path) -> Result<String, PluginError> {
        let outcome = super::install::install_from_dir_with_backup(&self.root, path)?;
        self.finish_install_outcome(outcome).await
    }

    /// dev 安装（写 plugin.dev.<identity>=abs(path)，不拷贝）。
    pub async fn install_dev(&self, path: &Path) -> Result<String, PluginError> {
        let manifest_path = path.join("manifest.json");
        let bytes = std::fs::read(&manifest_path)
            .map_err(|e| PluginError::ManifestInvalid(format!("读取 manifest 失败: {e}")))?;
        let manifest = PluginManifest::parse(&bytes)?;
        manifest.validate()?;
        let identity = manifest.identity.clone();
        let abs = std::fs::canonicalize(path)
            .map_err(|e| PluginError::ManifestInvalid(format!("解析路径失败: {e}")))?;
        let dev_key = format!("plugin.dev.{identity}");
        // 失败只回退本次写入的 dev 登记；同 identity 的已安装目录与
        // `plugin.<id>.*` 设置/KV 不属于这次 dev 安装，绝不能清。
        let previous = self
            .db
            .get_config(&dev_key)
            .await
            .map_err(|e| PluginError::Runtime(e.to_string()))?;
        self.db
            .set_config(&dev_key, &abs.to_string_lossy())
            .await
            .map_err(|e| PluginError::Runtime(e.to_string()))?;
        if let Err(error) = self.activate_dev_install(&identity).await {
            let rollback = match previous {
                Some(old) => self.db.set_config(&dev_key, &old).await,
                None => self.db.delete_config(&dev_key).await,
            };
            if let Err(rollback_error) = rollback {
                crate::logger::report_error("plugin", "restore_dev_registration", &rollback_error);
            }
            self.load_all().await;
            return Err(error);
        }
        Ok(identity)
    }

    /// dev 安装的加载 + 校验：dev 副本必须真正加载成功（否则同 identity 的已安装
    /// 副本会被顶替/保留，而 dev 版未生效却报成功）。
    async fn activate_dev_install(&self, identity: &str) -> Result<(), PluginError> {
        self.load_all().await;
        if let Some(error) = self.dev_load_failure(identity).await {
            return Err(error);
        }
        self.finish_install(identity).await
    }

    /// 本 identity 的 dev 登记对应的加载失败（忽略被 dev 顶替的已安装副本那条
    /// identity 重复记录）。
    async fn dev_load_failure(&self, identity: &str) -> Option<PluginError> {
        self.failed_plugins
            .read()
            .await
            .iter()
            .find(|p| p.identity == identity && p.dev_mode)
            .map(|p| PluginError::LoadFailed(p.error.clone()))
    }

    /// 重新加载一个 dev 插件：重读 manifest 与全部入口源码并做与安装相同的
    /// compile / pattern 校验。
    ///
    /// dev 插件的 JS 入口每次调用都按最新源码重读，但 manifest（名称 / 版本 /
    /// 匹配规则 / 设置项 / 入口路径 / 权限）只在 [`Self::load_all`] 时解析，
    /// 改了它就必须显式重载。与 [`Self::install_dev`] 不同：校验失败**保留**
    /// dev 登记与启停状态（开发中的半成品不该被清掉），只把错误回报给调用方；
    /// 之前加载失败的插件修好后经此恢复。
    pub async fn reload_dev(&self, identity: &str) -> Result<(), PluginError> {
        let registered = self
            .db
            .get_config(&format!("plugin.dev.{identity}"))
            .await
            .map_err(|e| PluginError::Runtime(e.to_string()))?
            .is_some();
        if !registered {
            return Err(PluginError::NotDevPlugin(identity.to_string()));
        }
        self.load_all().await;
        if let Some(error) = self.dev_load_failure(identity).await {
            return Err(error);
        }
        self.validate_loaded(identity).await
    }

    async fn finish_install_outcome(
        &self,
        outcome: super::install::InstallOutcome,
    ) -> Result<String, PluginError> {
        let identity = outcome.identity().to_string();
        if let Err(error) = self.finish_install(&identity).await {
            // 升级失败恢复旧目录；新装失败只删本次新建的目录。设置/KV 与同 identity
            // 的 dev 登记不是这次安装写的，绝不能清。
            if let Err(rollback_error) = super::install::rollback_install(&outcome) {
                crate::logger::report_error("plugin", "rollback_install", &rollback_error);
            }
            self.load_all().await;
            return Err(error);
        }
        if let Err(error) = super::install::commit_install(&outcome) {
            if let Err(rollback_error) = super::install::rollback_install(&outcome) {
                crate::logger::report_error("plugin", "rollback_install", &rollback_error);
            }
            self.load_all().await;
            return Err(error);
        }
        Ok(identity)
    }

    /// 安装共同后续：compile 校验 + enabled 规则 + 整表重载。
    async fn finish_install(&self, identity: &str) -> Result<(), PluginError> {
        // 读回 disabled_reason 判断 enabled 规则。
        let reason = self
            .db
            .get_config(&format!("plugin.{identity}.disabled_reason"))
            .await
            .map_err(|e| PluginError::Runtime(e.to_string()))?
            .as_deref()
            .map(DisabledReason::parse)
            .unwrap_or(DisabledReason::None);
        let has_enabled_key = self
            .db
            .get_config(&format!("plugin.{identity}.enabled"))
            .await
            .map_err(|e| PluginError::Runtime(e.to_string()))?
            .is_some();

        // compile / pattern 校验各 entry：先临时重载以拿到源码，再校验。
        // 任一校验失败由调用方回滚：升级恢复旧目录，新安装清理新目录，
        // 避免残留一个「已装但校验失败」且默认启用的插件。
        self.load_all().await;
        self.validate_loaded(identity).await?;

        // enabled 写入规则：
        // - 新装（无 enabled 键）或熔断 → enabled=1, reason=None（升级即解熔断）
        // - Manual → 维持 disabled 不动（不覆盖用户主动关闭）
        if !has_enabled_key || reason == DisabledReason::CircuitBreaker {
            self.write_enabled(identity, true, DisabledReason::None)
                .await?;
        }
        // 最终重载让内存态与 config 一致。
        self.load_all().await;
        Ok(())
    }

    /// 对当前快照中的插件做 compile / pattern 校验（不改任何状态）。
    async fn validate_loaded(&self, identity: &str) -> Result<(), PluginError> {
        let snapshot = self.plugins.read().await.clone();
        let Some(p) = snapshot.iter().find(|p| p.manifest.identity == identity) else {
            return Err(PluginError::ManifestInvalid(
                "安装后未能加载插件（校验失败或版本门槛不满足）".to_string(),
            ));
        };
        for src in [
            p.resolver_source().await,
            p.hooks_source().await,
            p.auth_source().await,
            p.subscription_source().await,
        ]
        .into_iter()
        .flatten()
        {
            self.runtime.check_compile(&src)?;
        }
        for f in &p.manifest.settings {
            if let Some(pat) = &f.pattern
                && !self.runtime.regex_valid(pat)
            {
                return Err(PluginError::ManifestInvalid(format!(
                    "setting '{}': pattern 非法（JS RegExp 编译失败）",
                    f.key
                )));
            }
        }
        Ok(())
    }

    /// 卸载（用户主动）：删目录 + 清 config 键 + 清任务 resolver 绑定 + 清认证凭据。
    ///
    /// 清绑定 = 对受影响任务批量应用「忽略插件、按原始链接重跑」逃生舱；不清则
    /// 留下 orphaned 绑定，resume 走 fail-closed 报错（见 [`Self::resolve`]）。
    /// 凭据清理只挂在这里（用户主动卸载），不挂在 [`Self::purge`] 本身（M-4）。
    /// 安装失败的回滚不走 purge（只回退本次写入的目录/dev 登记），避免误删同
    /// identity 已有插件的目录、设置与凭据。
    pub async fn uninstall(&self, identity: &str) -> Result<(), PluginError> {
        self.db
            .clear_tasks_resolver(identity)
            .await
            .map_err(|e| PluginError::Runtime(e.to_string()))?;
        self.purge(identity).await?;
        self.bridge.remove_plugin_workspace(identity).await;
        if let Err(e) = crate::auth::remove_plugin(&self.db, identity).await {
            // 只记日志不 `?`：目录/配置键已经清干净，卸载本身已经完成；凭据
            // 清理失败（如旧版整表损坏——已由 auth::read_legacy_table 兜底，
            // 这里只剩真正的 db I/O 错误）不该让用户看到卸载报错回滚假象。
            log_info!("[plugin] 清理插件 {identity} 认证凭据失败（已忽略）: {e:#}");
        }
        Ok(())
    }

    /// 删目录 + 清 `plugin.<identity>.` 前缀全部 config 键 + 重载。仅供
    /// [`Self::uninstall`] 使用。
    async fn purge(&self, identity: &str) -> Result<(), PluginError> {
        // 删安装目录（dev 不删源，仅删配置键）。
        let failed_plugin = self
            .failed_plugins
            .read()
            .await
            .iter()
            .find(|plugin| plugin.identity == identity && !plugin.dev_mode)
            .map(|plugin| (false, plugin.dir.to_path_buf()));
        let dir = match &failed_plugin {
            Some((true, _)) => None,
            Some((false, dir)) => Some(dir.clone()),
            None if is_valid_identity(identity) => Some(self.root.join(identity)),
            None => {
                return Err(PluginError::ManifestInvalid(
                    "插件 identity 非法，拒绝拼接卸载路径".to_string(),
                ));
            }
        };
        if let Some(dir) = dir
            && dir.exists()
            && let Err(error) = tokio::fs::remove_dir_all(&dir).await
            && error.kind() != std::io::ErrorKind::NotFound
        {
            // 删除目录可能已部分成功，返回原错误前重建可执行快照。
            self.load_all().await;
            return Err(PluginError::Runtime(format!("删除插件目录失败: {error}")));
        }
        let result = self.purge_config(identity).await;
        // 配置枚举/删除失败也必须重载：安装目录已删除，不能继续消费旧 JS 缓存。
        // 重载只负责收尾，卸载仍返回配置清理的原始错误。
        self.load_all().await;
        result
    }

    async fn purge_config(&self, identity: &str) -> Result<(), PluginError> {
        // config 键清理：identity 不合法时禁止前缀扫描（671#1）。失败插件的
        // identity 可能来自未通过 validate() 的 manifest，或干脆是扫到的目录
        // 名（见 `failure_identity`/`FailedPlugin::identity` 文档）——例如插件
        // 开发者常在插件根目录放一个无 manifest.json 的 "dev" 工作区目录，会
        // 被当成失败插件、identity 就是字面 "dev"；若仍按 `plugin.{identity}.`
        // 前缀扫描删除，会精确撞上 `load_all` 用来枚举 dev 插件注册的保留前缀
        // `plugin.dev.`，把全部 dev 模式插件注册一次性清空。identity 不合法时
        // 改走精确键删除：只清这个 identity 自己能推导出的固定几个键（有效
        // identity 的插件从不会有已知设置遗留在失败态——`update_settings` 只
        // 认 `self.plugins` 里的已加载插件，不合法 identity 不可能有 setting
        // 键要清）。
        if is_valid_identity(identity) {
            for prefix in [
                format!("plugin.{identity}."),
                format!("plugin.dev.{identity}"),
            ] {
                let entries = self
                    .db
                    .list_config_with_prefix(&prefix)
                    .await
                    .map_err(|e| PluginError::Runtime(e.to_string()))?;

                for (k, _) in entries {
                    // 认证凭据（`plugin.<id>.auth.<site>`）虽然共享这个前缀
                    // 命名空间，但只能由用户主动 uninstall 清（M-4）——
                    // purge 是安装回滚复用路径，不该连带删掉已登录成功的
                    // 凭据。
                    if crate::auth::is_sensitive_config_key(&k) {
                        continue;
                    }
                    self.db
                        .delete_config(&k)
                        .await
                        .map_err(|e| PluginError::Runtime(e.to_string()))?;
                }

                // plugin.dev.<id> 是精确键（无尾点），单独删。
                self.db
                    .delete_config(&prefix)
                    .await
                    .map_err(|e| PluginError::Runtime(e.to_string()))?;
            }
        } else {
            for key in [
                format!("plugin.{identity}.enabled"),
                format!("plugin.{identity}.disabled_reason"),
                format!("plugin.dev.{identity}"),
            ] {
                self.db
                    .delete_config(&key)
                    .await
                    .map_err(|e| PluginError::Runtime(e.to_string()))?;
            }
        }
        Ok(())
    }

    /// 手动开关（写 enabled + disabled_reason）。
    pub async fn set_enabled(&self, identity: &str, enabled: bool) -> Result<(), PluginError> {
        let reason = if enabled {
            DisabledReason::None
        } else {
            DisabledReason::Manual
        };
        self.write_enabled(identity, enabled, reason).await?;
        self.load_all().await;
        Ok(())
    }

    async fn write_enabled(
        &self,
        identity: &str,
        enabled: bool,
        reason: DisabledReason,
    ) -> Result<(), PluginError> {
        let values = std::collections::BTreeMap::from([
            (format!("plugin.{identity}.enabled"), enabled.to_string()),
            (
                format!("plugin.{identity}.disabled_reason"),
                reason.as_str().to_string(),
            ),
        ]);
        self.db
            .set_config_batch_atomic(&values)
            .await
            .map_err(|e| PluginError::Runtime(e.to_string()))
    }

    /// 熔断：自动禁用 + emit 事件。
    async fn trip_circuit_breaker(&self, identity: &str) {
        if let Err(error) = self
            .write_enabled(identity, false, DisabledReason::CircuitBreaker)
            .await
        {
            crate::logger::report_error("plugin", "persist_circuit_breaker", &error);
            return;
        }
        self.load_all().await;
        self.sink.emit(EngineEvent::PluginAutoDisabled {
            identity: identity.to_string(),
            reason: DisabledReason::CircuitBreaker.as_str().to_string(),
        });
        log_info!("[plugin] {identity} 连续超时，已自动熔断禁用");
    }

    /// 批量设置，all-or-nothing。
    pub async fn update_settings(
        &self,
        identity: &str,
        entries: &[(String, String)],
    ) -> Result<(), PluginError> {
        let manifest = {
            let snapshot = self.plugins.read().await.clone();
            snapshot
                .iter()
                .find(|p| p.manifest.identity == identity)
                .map(|p| p.manifest.clone())
        };
        let Some(manifest) = manifest else {
            return Err(PluginError::Runtime(format!("插件 {identity} 未找到")));
        };
        // 全量校验（任一失败即整体拒绝，不写任何键）。
        for (key, value) in entries {
            let field = manifest.settings.iter().find(|f| &f.key == key);
            let Some(field) = field else {
                return Err(PluginError::InvalidSetting {
                    key: key.clone(),
                    message: format!("未知设置项 '{key}'"),
                });
            };
            self.validate_value(field, value)?;
        }
        let values = entries
            .iter()
            .map(|(key, value)| (format!("plugin.{identity}.setting.{key}"), value.clone()))
            .collect();
        self.db
            .set_config_batch_atomic(&values)
            .await
            .map_err(|e| PluginError::Runtime(e.to_string()))?;
        Ok(())
    }

    /// 单设置项校验（类型/required/pattern/min-max/select/toggle）。
    fn validate_value(&self, field: &SettingField, value: &str) -> Result<(), PluginError> {
        let bad = |m: String| {
            Err(PluginError::InvalidSetting {
                key: field.key.clone(),
                message: m,
            })
        };
        match field.ty {
            SettingType::Boolean => {
                if value != "true" && value != "false" {
                    return bad(format!("'{}' 必须为 true/false", field.key));
                }
            }
            SettingType::Number => {
                let v = value
                    .parse::<f64>()
                    .ok()
                    .filter(|v| v.is_finite())
                    .ok_or_else(|| PluginError::InvalidSetting {
                        key: field.key.clone(),
                        message: format!("'{}' 不是有效数字", field.key),
                    })?;
                if let Some(lo) = field.min
                    && v < lo
                {
                    return bad(format!("'{}' 小于下限 {lo}", field.key));
                }
                if let Some(hi) = field.max
                    && v > hi
                {
                    return bad(format!("'{}' 大于上限 {hi}", field.key));
                }
            }
            SettingType::String => {
                // select：成员必须 ∈ options。
                if field.effective_widget() == super::manifest::SettingWidget::Select
                    && !field.options.iter().any(|o| o.value == value)
                {
                    return bad(format!("'{}' 不是合法选项", field.key));
                }
                // pattern。
                if let Some(pat) = &field.pattern
                    && !self.runtime.regex_test(pat, value)
                {
                    return bad(format!("'{}' 不匹配 pattern", field.key));
                }
            }
        }
        Ok(())
    }

    /// 列出全部插件（供 UI）。
    pub async fn list(&self) -> Vec<PluginInfo> {
        let snapshot = self.plugins.read().await.clone();
        let failed_snapshot = self.failed_plugins.read().await.clone();
        let mut out = Vec::with_capacity(snapshot.len() + failed_snapshot.len());
        for p in snapshot.iter() {
            let values = self.load_setting_values(&p.manifest.identity).await;
            out.push(PluginInfo {
                identity: p.manifest.identity.clone(),
                name: p.manifest.name.clone(),
                version: p.manifest.version.clone(),
                description: p.manifest.description.clone(),
                homepage: p.manifest.homepage.clone(),
                enabled: p.enabled,
                dev_mode: p.dev,
                disabled_reason: p.disabled_reason.as_str().to_string(),
                settings: p.manifest.settings.clone(),
                settings_values: values.into_iter().collect(),
                permissions: p.manifest.permissions.clone(),
                auth_supported: p.manifest.auth.is_some(),
                subscription_provider_ids: p
                    .manifest
                    .subscriptions
                    .iter()
                    .map(|subscription| subscription.provider_id.clone())
                    .collect(),
                load_status: PluginLoadStatus::Loaded.as_str().to_string(),
                load_error: String::new(),
            });
        }
        for p in failed_snapshot.iter() {
            // enabled 强制 false（671#6）：失败插件从未真正跑起来过，
            // `plugin_state()` 的「无 enabled 键 = 新装默认启用」缺省语义是为
            // 已加载插件设计的，套在失败插件上会呈现「加载失败」徽章与
            // 「已启用」开关同框的矛盾态。disabled_reason 仍如实返回（用户
            // 手动禁用/熔断的历史原因，供 UI 展示）。
            let (_, disabled_reason) = self.plugin_state(&p.identity).await;
            let (
                name,
                version,
                description,
                homepage,
                settings,
                permissions,
                auth_supported,
                subscription_provider_ids,
            ) = match &p.manifest {
                Some(manifest) => (
                    manifest.name.clone(),
                    manifest.version.clone(),
                    manifest.description.clone(),
                    manifest.homepage.clone(),
                    manifest.settings.clone(),
                    manifest.permissions.clone(),
                    manifest.auth.is_some(),
                    manifest
                        .subscriptions
                        .iter()
                        .map(|subscription| subscription.provider_id.clone())
                        .collect(),
                ),
                None => (
                    p.identity.clone(),
                    String::new(),
                    String::new(),
                    String::new(),
                    Vec::new(),
                    Vec::new(),
                    false,
                    Vec::new(),
                ),
            };
            let values = self.load_setting_values(&p.identity).await;
            out.push(PluginInfo {
                identity: p.identity.clone(),
                name,
                version,
                description,
                homepage,
                enabled: false,
                dev_mode: p.dev_mode,
                disabled_reason: disabled_reason.as_str().to_string(),
                settings,
                settings_values: values.into_iter().collect(),
                permissions,
                auth_supported,
                subscription_provider_ids,
                load_status: PluginLoadStatus::Failed.as_str().to_string(),
                load_error: truncate_load_error(&p.error),
            });
        }
        out.sort_by(|a, b| a.identity.cmp(&b.identity));
        out
    }

    async fn plugin_state(&self, identity: &str) -> (bool, DisabledReason) {
        let enabled = self
            .db
            .get_config(&format!("plugin.{identity}.enabled"))
            .await
            .ok()
            .flatten()
            .map(|value| value == "true")
            .unwrap_or(true);
        let reason = self
            .db
            .get_config(&format!("plugin.{identity}.disabled_reason"))
            .await
            .ok()
            .flatten()
            .as_deref()
            .map(DisabledReason::parse)
            .unwrap_or(DisabledReason::None);
        (enabled, reason)
    }

    /// 按 identity 查插件 manifest 声明的能力权限（供安装后依赖提醒）。
    ///
    /// 插件不存在时返回空（调用方视为无依赖）。
    pub async fn permissions_of(&self, identity: &str) -> Vec<String> {
        let snapshot = self.plugins.read().await.clone();
        snapshot
            .iter()
            .find(|p| p.manifest.identity == identity)
            .map(|p| p.manifest.permissions.clone())
            .unwrap_or_default()
    }

    /// 逃生舱窄接口：清该任务 resolver_plugin_id（不改插件全局状态）。
    pub async fn clear_task_resolver(&self, task_id: &str) -> Result<(), PluginError> {
        self.db
            .set_task_resolver(task_id, "")
            .await
            .map_err(|e| PluginError::Runtime(e.to_string()))
    }

    /// 读取某插件的设置值（config `plugin.<id>.setting.*`）。
    async fn load_setting_values(&self, identity: &str) -> HashMap<String, String> {
        load_setting_values_db(&self.db, identity).await
    }
}

/// 读取设置值（自由函数，供 notify spawn 任务复用，无需 &self）。
async fn load_setting_values_db(db: &Db, identity: &str) -> HashMap<String, String> {
    let prefix = format!("plugin.{identity}.setting.");
    let mut map = HashMap::new();
    if let Ok(entries) = db.list_config_with_prefix(&prefix).await {
        for (k, v) in entries {
            if let Some(key) = k.strip_prefix(&prefix) {
                map.insert(key.to_string(), v);
            }
        }
    }
    map
}

/// 按 identity 去重（476#1）：安装目录扫描与 dev 键扫描互相独立，同一
/// identity 若既有已安装目录、又有 `plugin.dev.<identity>` 覆盖（或误留了两条
/// dev 注册），此前会原样塞进快照，让管理页显示两条同标识记录、且卸载其一
/// 时看起来「删不掉」（另一条仍在快照里）。这里保证同一 identity 只保留
/// 一条：dev 覆盖优先于安装目录（开发者主动登记的本地调试版本应生效）；
/// 被顶替的一份转入 `failed`，仍可在管理页看到并允许用户显式卸载清理磁盘/
/// 配置残留，不静默丢弃。纯函数，独立于 I/O 可测。
fn dedup_plugins_by_identity(
    loaded: Vec<LoadedPlugin>,
    failed: &mut Vec<FailedPlugin>,
) -> Vec<LoadedPlugin> {
    let mut identity_index: HashMap<String, usize> = HashMap::with_capacity(loaded.len());
    let mut deduped: Vec<LoadedPlugin> = Vec::with_capacity(loaded.len());
    for plugin in loaded {
        match identity_index.get(&plugin.manifest.identity).copied() {
            Some(idx) => {
                // dev 覆盖已安装目录：换入 dev 版本，被顶替的安装目录转入 failed。
                let displaced = if plugin.dev && !deduped[idx].dev {
                    std::mem::replace(&mut deduped[idx], plugin)
                } else {
                    plugin
                };
                failed.push(FailedPlugin {
                    identity: displaced.manifest.identity.clone(),
                    dir: displaced.dir.clone().into_boxed_path(),
                    manifest: Some(Box::new(displaced.manifest.clone())),
                    dev_mode: displaced.dev,
                    error: "identity 重复：与另一已加载插件冲突，可在此卸载清理残留目录/配置"
                        .to_string(),
                });
            }
            None => {
                identity_index.insert(plugin.manifest.identity.clone(), deduped.len());
                deduped.push(plugin);
            }
        }
    }
    deduped
}

fn identity_from_dev_key(key: &str) -> &str {
    key.strip_prefix("plugin.dev.").unwrap_or(key)
}

fn failure_identity(dir: &Path, identity_hint: Option<&str>) -> String {
    identity_hint
        .map(str::to_owned)
        .or_else(|| {
            dir.file_name()
                .map(|name| name.to_string_lossy().into_owned())
        })
        .unwrap_or_else(|| "unknown".to_string())
}

/// `FailedPlugin.error` 的可读渲染上限（671#9）：来源之一是
/// `manifest.validate()` 的错误文本，会原样插值插件可控的 manifest 字符串
/// （identity/version/icon 路径等，见 `manifest.rs` 的 `ManifestInvalid`
/// 消息），未经任何长度上限就会随每次插件列表刷新原样进入 `PluginInfo` /
/// REST `PluginDto` 并被客户端逐字渲染。截断只在这一处生效——
/// `list()` 是 `FailedPlugin.error` 唯一的对外读出口。
const MAX_LOAD_ERROR_LEN: usize = 1024;

fn truncate_load_error(error: &str) -> String {
    if error.len() <= MAX_LOAD_ERROR_LEN {
        return error.to_string();
    }
    let mut end = MAX_LOAD_ERROR_LEN;
    while end > 0 && !error.is_char_boundary(end) {
        end -= 1;
    }
    let mut truncated = error[..end].to_string();
    truncated.push('…');
    truncated
}

/// 取字段生效值：config 存值 → manifest default → None。
fn value_of(values: &HashMap<String, String>, field: &SettingField) -> Option<String> {
    values
        .get(&field.key)
        .cloned()
        .or_else(|| field.default.clone())
}

/// 构建类型化设置 JSON 对象（string→JS string、number→JS number、boolean→JS bool）。
fn build_typed_settings_json(
    manifest: &PluginManifest,
    values: &HashMap<String, String>,
) -> String {
    let mut obj = serde_json::Map::new();
    for f in &manifest.settings {
        let raw = value_of(values, f);
        let jv = match f.ty {
            SettingType::Boolean => serde_json::Value::Bool(raw.as_deref() == Some("true")),
            SettingType::Number => match raw.as_deref().and_then(|s| s.parse::<f64>().ok()) {
                Some(n) => serde_json::Number::from_f64(n)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null),
                None => serde_json::Value::Null,
            },
            SettingType::String => match raw {
                Some(s) => serde_json::Value::String(s),
                None => serde_json::Value::Null,
            },
        };
        obj.insert(f.key.clone(), jv);
    }
    serde_json::Value::Object(obj).to_string()
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct SubscriptionOutput {
    title: String,
    link: String,
    items: Vec<SubscriptionOutputItem>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct SubscriptionOutputItem {
    guid: String,
    title: String,
    link: String,
    enclosure_url: String,
    /// 可选二段解析标识；非空时核心用条目 link 调 resolver，保留插件的精确规格。
    resolver_item: String,
    enclosure_length: i64,
    pub_date: i64,
}

/// 订阅输出校验：条目总数 ≤1000；单条非法（guid 空/超长、URL scheme 不在
/// 白名单、字段超长、长度为负）**跳过并记日志**而不是整批失败——订阅是无人
/// 值守链路，上游一条脏数据不该让整个源进入退避；但若条目非空且**全部**
/// 非法，视为插件输出系统性错误，返回 `InvalidOutput`。
fn parse_subscription_output(raw: &str) -> Result<ParsedFeed, PluginError> {
    let output: SubscriptionOutput = serde_json::from_str(raw)
        .map_err(|e| PluginError::InvalidOutput(format!("订阅返回值非法: {e}")))?;
    if output.items.len() > 1000 {
        return Err(PluginError::InvalidOutput(
            "订阅条目数量超过 1000".to_string(),
        ));
    }
    let total = output.items.len();
    let mut items = Vec::with_capacity(total);
    let mut first_reason: Option<String> = None;
    let mut rejected = 0usize;
    for item in output.items {
        match validate_subscription_item(item) {
            Ok(item) => items.push(item),
            Err(reason) => {
                rejected += 1;
                if first_reason.is_none() {
                    first_reason = Some(reason);
                }
            }
        }
    }
    if rejected > 0 {
        let reason = first_reason.unwrap_or_default();
        if items.is_empty() {
            return Err(PluginError::InvalidOutput(format!(
                "订阅条目全部非法（{rejected} 条），首条原因: {reason}"
            )));
        }
        crate::logger::log_error!(
            "[plugin] subscription output: skipped {} of {} items, first reason: {}",
            rejected,
            total,
            reason
        );
    }
    Ok(ParsedFeed {
        title: output.title,
        link: output.link,
        items,
    })
}

/// 单条订阅条目校验：guid 非空 ≤2048；title ≤1024；resolver_item ≤2048；
/// `enclosureUrl` / `link` 至少一个非空且都过 [`check_output_url`]（同 resolve
/// 平面：scheme 白名单 + ≤8KB）；`enclosureLength` 非负。
fn validate_subscription_item(item: SubscriptionOutputItem) -> Result<ParsedItem, String> {
    if item.guid.is_empty() || item.guid.len() > 2048 {
        return Err("guid 必须非空且不超过 2048 字节".to_string());
    }
    if item.title.len() > 1024 {
        return Err(format!("条目 {} 的 title 超过 1024 字节", item.guid));
    }
    if item.resolver_item.len() > 2048 {
        return Err(format!("条目 {} 的 resolverItem 超过 2048 字节", item.guid));
    }
    if item.link.is_empty() && item.enclosure_url.is_empty() {
        return Err(format!("条目 {} 缺少 link / enclosureUrl", item.guid));
    }
    if !item.resolver_item.is_empty() && item.link.is_empty() {
        return Err(format!(
            "条目 {} 含 resolverItem 时必须提供 link",
            item.guid
        ));
    }
    for (name, url) in [("link", &item.link), ("enclosureUrl", &item.enclosure_url)] {
        if !url.is_empty()
            && let Err(e) = check_output_url(url)
        {
            return Err(format!("条目 {} 的 {name} 非法: {e}", item.guid));
        }
    }
    if item.enclosure_length < 0 {
        return Err(format!("条目 {} 的 enclosureLength 不可为负数", item.guid));
    }
    Ok(ParsedItem {
        guid: item.guid,
        title: item.title,
        link: item.link,
        enclosure_url: item.enclosure_url,
        enclosure_type: String::new(),
        resolver_item: item.resolver_item,
        enclosure_length: item.enclosure_length,
        pub_date: item.pub_date,
    })
}

/// resolve 输出校验：url scheme ∈{http,https,ftp,magnet,ed2k}、长度 ≤8KB；
/// file_name 拒绝 `/ \ ..` 与控制字符；`manifest` 与 `url`/`variants`/
/// `audio_url` 互斥，且二段解析（`second_stage`，对应
/// [`ResolveRequest::resolver_item`] 非空）不可返回 `manifest`（防递归裂变，
/// D6/§7.6）。全 fail-closed：任一违规整体拒绝该次 resolve 结果。
fn validate_resolve_output(res: &ResolveResult, second_stage: bool) -> Result<(), PluginError> {
    if let Some(manifest) = &res.manifest {
        if second_stage {
            return Err(PluginError::InvalidOutput(
                "二段解析（resolverItem 非空）不可返回 manifest：防递归裂变".to_string(),
            ));
        }
        if !res.url.is_empty() || !res.variants.is_empty() || res.audio_url.is_some() {
            return Err(PluginError::InvalidOutput(
                "manifest 与 url/variants/audioUrl 互斥".to_string(),
            ));
        }
        return validate_manifest(manifest);
    }
    // 变体存在时顶层 url 允许为空（选中变体后覆盖）；非空时仍须合法。
    if res.variants.is_empty() || !res.url.is_empty() {
        check_output_url(&res.url)?;
    }
    if res.variants.len() > 50 {
        return Err(PluginError::InvalidOutput(format!(
            "variants 过多: {} > 50",
            res.variants.len()
        )));
    }
    for v in &res.variants {
        if v.label.is_empty() || v.label.chars().count() > 200 {
            return Err(PluginError::InvalidOutput(
                "variant label 须非空且 ≤200 字符".to_string(),
            ));
        }
        check_output_url(&v.url)?;
        if let Some(a) = &v.audio_url
            && !a.is_empty()
        {
            check_output_url(a)?;
        }
        if let Some(name) = &v.file_name {
            check_file_name(name)?;
        }
    }
    if let Some(a) = &res.audio_url
        && !a.is_empty()
    {
        check_output_url(a)?;
    }
    if let Some(name) = &res.file_name {
        check_file_name(name)?;
    }
    Ok(())
}

fn check_file_name(name: &str) -> Result<(), PluginError> {
    if name.contains('/')
        || name.contains('\\')
        || name.contains("..")
        || name.chars().any(|c| c.is_control())
    {
        return Err(PluginError::InvalidOutput(format!(
            "file_name 非法: {name}"
        )));
    }
    Ok(())
}

fn check_output_url(url: &str) -> Result<(), PluginError> {
    if url.len() > 8 * 1024 {
        return Err(PluginError::InvalidOutput("url 超过 8KB".to_string()));
    }
    let scheme = url.split(':').next().unwrap_or("").to_ascii_lowercase();
    if !matches!(
        scheme.as_str(),
        "http" | "https" | "ftp" | "magnet" | "ed2k"
    ) {
        return Err(PluginError::InvalidOutput(format!(
            "url scheme 不允许: {scheme}"
        )));
    }
    Ok(())
}

/// [`ResolveResult::manifest`] 校验：items 1..=1000；item.id 非空 ≤200；
/// item.name 非空且过 [`check_file_name`]；path 空或
/// [`is_safe_relative_path`]，按 `/` 计深度 ≤8，`path+"/"+name` 总长 ≤180
/// 字符（给 `save_dir` 前缀留余量，规避 Windows `MAX_PATH` 260 限制）；
/// kind ∈ {"", "file"}；per-item variants ≤50，variant id/label 非空。
/// fail-closed：任一条目违规拒绝整个 manifest（不做部分放行）。
fn validate_manifest(m: &ResolveManifest) -> Result<(), PluginError> {
    if m.items.is_empty() || m.items.len() > 1000 {
        return Err(PluginError::InvalidOutput(format!(
            "manifest items 数量非法: {}（须 1..=1000）",
            m.items.len()
        )));
    }
    for item in &m.items {
        validate_manifest_item(item)?;
    }
    Ok(())
}

fn validate_manifest_item(item: &ManifestItem) -> Result<(), PluginError> {
    if item.id.is_empty() || item.id.chars().count() > 200 {
        return Err(PluginError::InvalidOutput(format!(
            "manifest item id 非法: '{}'",
            item.id
        )));
    }
    if item.name.is_empty() {
        return Err(PluginError::InvalidOutput(
            "manifest item name 不可为空".to_string(),
        ));
    }
    check_file_name(&item.name)?;
    if !item.path.is_empty() && !is_safe_relative_path(&item.path) {
        return Err(PluginError::InvalidOutput(format!(
            "manifest item path 非法: '{}'",
            item.path
        )));
    }
    let depth = if item.path.is_empty() {
        0
    } else {
        item.path.split('/').count()
    };
    if depth > 8 {
        return Err(PluginError::InvalidOutput(format!(
            "manifest item path 深度超限: {depth} > 8"
        )));
    }
    let full_len = if item.path.is_empty() {
        item.name.chars().count()
    } else {
        item.path.chars().count() + 1 + item.name.chars().count()
    };
    if full_len > 180 {
        return Err(PluginError::InvalidOutput(format!(
            "manifest item 落盘路径过长: {full_len} > 180"
        )));
    }
    if !item.kind.is_empty() && item.kind != "file" {
        return Err(PluginError::InvalidOutput(format!(
            "manifest item kind 非法: '{}'",
            item.kind
        )));
    }
    if item.variants.len() > 50 {
        return Err(PluginError::InvalidOutput(format!(
            "manifest item variants 过多: {} > 50",
            item.variants.len()
        )));
    }
    for v in &item.variants {
        if v.id.is_empty() || v.label.is_empty() {
            return Err(PluginError::InvalidOutput(
                "manifest variant id/label 不可为空".to_string(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::{
        FailedPlugin, LoadedPlugin, dedup_plugins_by_identity, parse_subscription_output,
        validate_resolve_output,
    };
    use crate::plugin::PluginManifest;
    use crate::plugin::{
        ManifestItem, ManifestVariant, PluginError, ResolveManifest, ResolveResult, ResolveVariant,
    };
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;

    fn test_manifest(identity: &str) -> PluginManifest {
        PluginManifest {
            identity: identity.to_string(),
            name: identity.to_string(),
            version: "1.0.0".to_string(),
            description: String::new(),
            homepage: String::new(),
            icon: String::new(),
            min_app_version: String::new(),
            resolvers: Vec::new(),
            subscriptions: Vec::new(),
            hooks: None,
            auth: None,
            settings: Vec::new(),
            permissions: Vec::new(),
        }
    }

    fn test_loaded_plugin(identity: &str, dev: bool, dir: &str) -> LoadedPlugin {
        LoadedPlugin {
            manifest: test_manifest(identity),
            dir: PathBuf::from(dir),
            dev,
            enabled: true,
            disabled_reason: super::DisabledReason::None,
            resolver_entry: None,
            hooks_entry: None,
            auth_entry: None,
            subscription_entry: None,
            resolver_cache: None,
            hooks_cache: None,
            auth_cache: None,
            subscription_cache: None,
            timeout_streak: Arc::new(AtomicU32::new(0)),
        }
    }

    // 476#1：同 identity 的安装目录 + dev 注册只保留一条（dev 覆盖安装目录），
    // 被顶替的一份进入 failed 列表而不是静默丢弃或重复出现在快照里。
    #[test]
    fn dedup_plugins_by_identity_prefers_dev_over_installed() {
        let installed = test_loaded_plugin("acme@demo", false, "/root/acme@demo");
        let dev = test_loaded_plugin("acme@demo", true, "/home/dev/demo");
        let mut failed: Vec<FailedPlugin> = Vec::new();

        let deduped = dedup_plugins_by_identity(vec![installed, dev], &mut failed);

        assert_eq!(deduped.len(), 1, "only one entry per identity may survive");
        assert!(
            deduped[0].dev,
            "dev registration must win over installed dir"
        );
        assert_eq!(
            failed.len(),
            1,
            "displaced duplicate must be recorded, not dropped"
        );
        assert!(
            !failed[0].dev_mode,
            "the displaced entry is the installed-dir copy"
        );
        assert_eq!(failed[0].identity, "acme@demo");
    }

    // 两个不同 identity 必须都保留、互不影响。
    #[test]
    fn dedup_plugins_by_identity_keeps_distinct_identities() {
        let a = test_loaded_plugin("acme@demo", false, "/root/acme@demo");
        let b = test_loaded_plugin("other@demo", false, "/root/other@demo");
        let mut failed: Vec<FailedPlugin> = Vec::new();

        let deduped = dedup_plugins_by_identity(vec![a, b], &mut failed);

        assert_eq!(deduped.len(), 2);
        assert!(failed.is_empty());
    }

    fn variant(label: &str, url: &str) -> ResolveVariant {
        ResolveVariant {
            label: label.into(),
            url: url.into(),
            ..Default::default()
        }
    }

    fn item(id: &str, name: &str, path: &str) -> ManifestItem {
        ManifestItem {
            id: id.into(),
            name: name.into(),
            path: path.into(),
            ..Default::default()
        }
    }

    /// 单条非法条目跳过、合法条目保留；URL scheme 白名单与 resolve 平面一致。
    #[test]
    fn subscription_output_skips_invalid_items_but_keeps_valid_ones() {
        let raw = r#"{"title":"T","items":[
            {"guid":"ok","link":"https://a.test/1","enclosureUrl":"https://a.test/1.torrent"},
            {"guid":"","link":"https://a.test/2"},
            {"guid":"bad-scheme","link":"javascript:alert(1)"},
            {"guid":"no-url"},
            {"guid":"neg","link":"https://a.test/3","enclosureLength":-1}
        ]}"#;
        let feed = parse_subscription_output(raw).expect("partial output is accepted");
        assert_eq!(feed.items.len(), 1);
        assert_eq!(feed.items[0].guid, "ok");
    }

    /// 条目非空但全部非法：视为插件输出系统性错误，整轮失败。
    #[test]
    fn subscription_output_fails_when_every_item_is_invalid() {
        let raw = r#"{"items":[{"guid":"","link":"https://a.test/1"},{"guid":"x"}]}"#;
        assert!(matches!(
            parse_subscription_output(raw),
            Err(PluginError::InvalidOutput(_))
        ));
        // 空 feed（无条目）是合法的：新源尚无内容不算失败。
        assert!(parse_subscription_output(r#"{"items":[]}"#).is_ok());
    }

    #[test]
    fn subscription_output_requires_link_for_resolver_item() {
        let raw = r#"{"items":[{
            "guid":"resolver-without-link",
            "resolverItem":"episode@1080p",
            "enclosureUrl":"https://cdn.example/episode.torrent"
        }]}"#;
        assert!(matches!(
            parse_subscription_output(raw),
            Err(PluginError::InvalidOutput(_))
        ));
    }

    /// 有 variants 时顶层 url 允许为空（选中变体后覆盖）。
    #[test]
    fn variants_allow_empty_top_level_url() {
        let res = ResolveResult {
            variants: vec![variant("1080p", "https://v.example.com/a")],
            ..Default::default()
        };
        assert!(validate_resolve_output(&res, false).is_ok());
    }

    /// 无 variants 且顶层 url 为空 → 拒（原有单直链语义不放松）。
    #[test]
    fn empty_url_without_variants_rejected() {
        assert!(validate_resolve_output(&ResolveResult::default(), false).is_err());
    }

    /// 变体 label 为空 / url scheme 非法 → 拒。
    #[test]
    fn invalid_variant_rejected() {
        let empty_label = ResolveResult {
            variants: vec![variant("", "https://v.example.com/a")],
            ..Default::default()
        };
        assert!(validate_resolve_output(&empty_label, false).is_err());
        let bad_scheme = ResolveResult {
            variants: vec![variant("x", "javascript:alert(1)")],
            ..Default::default()
        };
        assert!(validate_resolve_output(&bad_scheme, false).is_err());
    }

    /// 变体数量 > 50 → 拒。
    #[test]
    fn too_many_variants_rejected() {
        let res = ResolveResult {
            variants: (0..51)
                .map(|i| variant(&format!("v{i}"), "https://v.example.com/a"))
                .collect(),
            ..Default::default()
        };
        assert!(validate_resolve_output(&res, false).is_err());
    }

    /// manifest 合法清单（1 段路径 + 无规格）→ 通过。
    #[test]
    fn valid_manifest_passes() {
        let res = ResolveResult {
            manifest: Some(ResolveManifest {
                name: "share".into(),
                items: vec![item("f1", "a.mp4", "sub")],
            }),
            ..Default::default()
        };
        assert!(validate_resolve_output(&res, false).is_ok());
    }

    /// manifest 与 url/variants/audioUrl 互斥 → 各自拒绝。
    #[test]
    fn manifest_mutually_exclusive_with_url_fields() {
        let with_url = ResolveResult {
            url: "https://x.example.com/a".into(),
            manifest: Some(ResolveManifest {
                items: vec![item("f1", "a.mp4", "")],
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(validate_resolve_output(&with_url, false).is_err());

        let with_variants = ResolveResult {
            variants: vec![variant("x", "https://x.example.com/a")],
            manifest: Some(ResolveManifest {
                items: vec![item("f1", "a.mp4", "")],
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(validate_resolve_output(&with_variants, false).is_err());

        let with_audio = ResolveResult {
            audio_url: Some("https://x.example.com/a.m4a".into()),
            manifest: Some(ResolveManifest {
                items: vec![item("f1", "a.mp4", "")],
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(validate_resolve_output(&with_audio, false).is_err());
    }

    /// 二段解析（`resolverItem` 非空，`second_stage=true`）返回 manifest → 拒
    /// （防递归裂变，D6/§7.6）。
    #[test]
    fn second_stage_manifest_rejected() {
        let res = ResolveResult {
            manifest: Some(ResolveManifest {
                items: vec![item("f1", "a.mp4", "")],
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(validate_resolve_output(&res, true).is_err());
    }

    /// items 为空 / 超过 1000 → 拒。
    #[test]
    fn manifest_item_count_out_of_range_rejected() {
        let empty = ResolveResult {
            manifest: Some(ResolveManifest::default()),
            ..Default::default()
        };
        assert!(validate_resolve_output(&empty, false).is_err());

        let too_many = ResolveResult {
            manifest: Some(ResolveManifest {
                items: (0..1001)
                    .map(|i| item(&format!("f{i}"), "a.mp4", ""))
                    .collect(),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(validate_resolve_output(&too_many, false).is_err());
    }

    /// item id/name 为空、path 不安全（含 `..`）、path 深度超 8、
    /// `path+name` 总长超 180、kind 非法、variant id 为空 → 各自拒绝。
    #[test]
    fn manifest_item_field_violations_rejected() {
        let cases: Vec<ResolveManifest> = vec![
            ResolveManifest {
                items: vec![item("", "a.mp4", "")],
                ..Default::default()
            },
            ResolveManifest {
                items: vec![item("f1", "", "")],
                ..Default::default()
            },
            ResolveManifest {
                items: vec![item("f1", "a.mp4", "../escape")],
                ..Default::default()
            },
            ResolveManifest {
                items: vec![item("f1", "a.mp4", "a/b/c/d/e/f/g/h/i")],
                ..Default::default()
            },
            ResolveManifest {
                items: vec![item("f1", &"a".repeat(200), "")],
                ..Default::default()
            },
            ResolveManifest {
                items: vec![ManifestItem {
                    kind: "folder".into(),
                    ..item("f1", "a.mp4", "")
                }],
                ..Default::default()
            },
            ResolveManifest {
                items: vec![ManifestItem {
                    variants: vec![ManifestVariant {
                        id: String::new(),
                        label: "1080p".into(),
                        size: None,
                    }],
                    ..item("f1", "a.mp4", "")
                }],
                ..Default::default()
            },
        ];
        for m in cases {
            let res = ResolveResult {
                manifest: Some(m),
                ..Default::default()
            };
            assert!(validate_resolve_output(&res, false).is_err());
        }
    }

    /// item.variants 数量 > 50 → 拒。
    #[test]
    fn manifest_item_too_many_variants_rejected() {
        let m = ResolveManifest {
            items: vec![ManifestItem {
                variants: (0..51)
                    .map(|i| ManifestVariant {
                        id: format!("v{i}"),
                        label: format!("v{i}"),
                        size: None,
                    })
                    .collect(),
                ..item("f1", "a.mp4", "")
            }],
            ..Default::default()
        };
        let res = ResolveResult {
            manifest: Some(m),
            ..Default::default()
        };
        assert!(validate_resolve_output(&res, false).is_err());
    }
}
