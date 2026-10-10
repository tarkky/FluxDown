//! 局域网直连（L1）服务：把 `fluxdown_link::LinkManager` 装配进 agent。
//!
//! 职责：
//! - **响应端**：兼容 API 的 `/ping`（`linkFingerprint`）、`pair/hello`、`pair/reveal`、
//!   `pair/confirm`、数据面 `link/tasks` 与 `link/info`；入站配对请求（揭示被受理后）进快照 /
//!   事件，由 `agent.link.approve` 决定；
//!   接单时按契约做目录回退（非本机风格绝对路径 → 本机默认目录；带目录建任务失败 → 去掉目录重试）。
//! - **发起端**：mDNS 发现（`LinkDiscoveredChanged`）、手动地址探测、配对（SAS 由 UI 核对）、
//!   解除配对、在线探测（`LinkDeviceInfo.online` 真实值）、下发任务、经已认证链路交换
//!   对端默认目录 / 路径风格。
//!
//! 桌面默认只监听 `127.0.0.1`：此时局域网内的其它设备连不上本机，配对码结果的 `addresses`
//! 为空、不做 mDNS 广播、配对时不自报回连地址；`--server` 模式与 `lanEnabled` 时正常工作。

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use fluxdown_api::routes::{API_LINK_INFO, API_LINK_TASKS};
use fluxdown_api::service::{ApiError, link_unsupported};
use fluxdown_link::pairing::{CODE_TTL_SECS, DECISION_WINDOW_SECS};
use fluxdown_link::{
    DiscoveredPeer, DiscoveryKind, LinkEngineEvent, LinkError, LinkManager, LinkOptions,
    LinkResult, PairConfirmOutcome, PeerAddress, PeerInfo, SelfInfo, WireHello, WireReveal,
};
use fluxdown_protocol::{
    AgentEvent, ApplicationErrorCode, CreateTaskRequest, DaemonCreateTaskParams, ErrorReason,
    LinkAddressParams, LinkApproveParams, LinkAuth, LinkCodeResponse, LinkDeviceInfo,
    LinkDeviceParams, LinkDiscoveredPeer, LinkDiscoveryParams, LinkDispatchParams,
    LinkDispatchResult, LinkPairBeginParams, LinkPairBeginResponse, LinkPairConfirmOutcome,
    LinkPairConfirmRequest, LinkPairFinishParams, LinkPairFinishResponse, LinkPairHelloRequest,
    LinkPairHelloResponse, LinkPairRevealRequest, LinkPairRevealResponse, LinkPairingCodeDto,
    LinkPairingRequestDto, LinkPingInfo, LinkTaskRequest, PathStyle, RpcErrorData, method,
};
use serde_json::{Value, json};
use tokio::sync::{Mutex, Notify, mpsc};
use tokio_util::sync::CancellationToken;

use super::storage::{AgentLinkStorage, devices_with_online, local_path_style_text};
use crate::daemon_client::DaemonClient;
use crate::event_hub::AgentEventHub;
use crate::state::{AgentState, StateStore};

/// 同时保留的待确认入站配对请求上限（超出丢最旧）。
const PAIRING_REQUEST_CAP: usize = 16;
/// 已配对设备在线状态的后台轮询间隔。
const ONLINE_POLL_INTERVAL: Duration = Duration::from_secs(45);
/// 一轮在线探测（含并发的信息交换）的整体时限：个别设备长时间不可达不拖慢整批。
const PROBE_ROUND_TIMEOUT: Duration = Duration::from_secs(8);
/// 兼容 API `link_pair_hello` 的 URL / 文件名 / 目录长度上限（同 FluxCloud 下发契约）。
const MAX_URL_LEN: usize = 8192;
const MAX_FILE_NAME_LEN: usize = 255;
const MAX_SAVE_DIR_LEN: usize = 1024;

fn now_unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| i64::try_from(elapsed.as_millis()).ok())
        .unwrap_or(0)
}

/// 本机建下载任务的入口（生产实现走 daemon；测试注入记录型实现）。
#[async_trait]
pub trait LinkTaskCreator: Send + Sync {
    /// 以「无人值守」方式创建任务，返回任务 ID。
    async fn create_task(&self, request: CreateTaskRequest) -> Result<String, ApiError>;
}

/// 经 daemon JSON-RPC 创建任务。
pub struct DaemonTaskCreator {
    daemon: Arc<DaemonClient>,
}

impl DaemonTaskCreator {
    #[must_use]
    pub fn new(daemon: Arc<DaemonClient>) -> Self {
        Self { daemon }
    }
}

#[async_trait]
impl LinkTaskCreator for DaemonTaskCreator {
    async fn create_task(&self, request: CreateTaskRequest) -> Result<String, ApiError> {
        let created: Value = self
            .daemon
            .call_detailed(
                method::DAEMON_TASK_CREATE,
                Some(DaemonCreateTaskParams {
                    request,
                    torrent_blob_id: None,
                    // 对端下发的任务没有人在本机确认选择框：全选文件直接开始。
                    unattended: true,
                    hint_file_size: None,
                }),
            )
            .await
            .map_err(crate::api_host::object_error)?;
        created
            .get("taskId")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| ApiError::Internal("daemon returned no taskId".to_owned()))
    }
}

/// 服务装配参数。
pub struct LinkServiceParts {
    pub events: AgentEventHub,
    pub state: Arc<Mutex<AgentState>>,
    pub store: Arc<StateStore>,
    pub tasks: Arc<dyn LinkTaskCreator>,
    /// 网关实际监听地址（决定本机是否可被局域网访问，以及 mDNS 广播端口）。
    pub bound: SocketAddr,
    /// `--server` 模式（headless / NAS）。
    pub server_mode: bool,
}

/// 互联操作的失败：互联层错误 + 服务层校验错误。
#[derive(Debug, thiserror::Error)]
pub enum LinkOpError {
    #[error(transparent)]
    Link(#[from] LinkError),
    /// 保存目录不是目标设备路径风格下的绝对路径。
    #[error("save directory is not an absolute path for the target device")]
    SaveDirUnavailable,
    /// 参数非法（字段名）。
    #[error("invalid {0}")]
    Invalid(&'static str),
}

/// RPC 错误映射的上下文：同一种网络失败在配对里是「对端不可达」，在已配对设备上是「设备离线」。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorContext {
    Pairing,
    Peer,
    Local,
}

/// 互联操作错误 → `agent.link.*` 的稳定错误详情（含本地化用的 `reason`）。
#[must_use]
pub fn rpc_error(error: &LinkOpError, context: ErrorContext) -> RpcErrorData {
    use ApplicationErrorCode as Code;
    let unreachable_reason = match context {
        ErrorContext::Pairing => Some(ErrorReason::PairingPeerUnreachable),
        ErrorContext::Peer => Some(ErrorReason::PeerOffline),
        ErrorContext::Local => None,
    };
    let (code, retryable, reason) = match error {
        LinkOpError::SaveDirUnavailable => {
            return RpcErrorData {
                field: Some("saveDir".to_owned()),
                ..RpcErrorData::new(Code::InvalidArgument, false)
                    .with_reason(ErrorReason::SaveDirUnavailable)
            };
        }
        LinkOpError::Invalid(field) => {
            return RpcErrorData {
                field: Some((*field).to_owned()),
                ..RpcErrorData::new(Code::InvalidArgument, false)
            };
        }
        LinkOpError::Link(error) => match error {
            LinkError::InvalidCode => (
                Code::InvalidArgument,
                false,
                Some(ErrorReason::PairingCodeInvalid),
            ),
            LinkError::SessionExpired => (
                Code::NotFound,
                false,
                Some(ErrorReason::PairingSessionExpired),
            ),
            LinkError::PairingTimeout => (
                Code::Timeout,
                false,
                Some(ErrorReason::PairingSessionExpired),
            ),
            LinkError::BadSignature => (
                Code::InvalidArgument,
                false,
                Some(ErrorReason::PairingSignatureInvalid),
            ),
            LinkError::SelfPairing => {
                (Code::InvalidArgument, false, Some(ErrorReason::PairingSelf))
            }
            LinkError::Throttled => (Code::Unavailable, true, Some(ErrorReason::PairingThrottled)),
            LinkError::RejectedByPeer => {
                (Code::Conflict, false, Some(ErrorReason::PairingRejected))
            }
            LinkError::NotFluxDown(_) => (
                Code::InvalidArgument,
                false,
                Some(ErrorReason::PairingNotFluxDown),
            ),
            // 配对阶段：地址上出示的身份与发现指纹不符，按身份校验失败提示；
            // 已配对设备上：拨通的不是那台设备，按离线处理。
            LinkError::IdentityMismatch(_) => {
                let reason = if context == ErrorContext::Pairing {
                    ErrorReason::PairingSignatureInvalid
                } else {
                    ErrorReason::PeerOffline
                };
                (Code::Conflict, false, Some(reason))
            }
            LinkError::CommitmentMismatch => (
                Code::InvalidArgument,
                false,
                Some(ErrorReason::PairingSignatureInvalid),
            ),
            LinkError::UnsupportedVersion => (
                Code::Conflict,
                false,
                Some(ErrorReason::PairingVersionMismatch),
            ),
            LinkError::Unauthorized => {
                (Code::Unauthorized, false, Some(ErrorReason::PeerNotPaired))
            }
            LinkError::NotPaired => (Code::NotFound, false, Some(ErrorReason::PeerNotPaired)),
            LinkError::Unreachable | LinkError::Io(_) => {
                (Code::Unavailable, true, unreachable_reason)
            }
            LinkError::BadPayload(_) => (Code::InvalidArgument, false, None),
            LinkError::Store(_) => (Code::Internal, false, None),
            LinkError::Unavailable => (Code::Unavailable, true, None),
        },
    };
    let data = RpcErrorData::new(code, retryable);
    match reason {
        Some(reason) => data.with_reason(reason),
        None => data,
    }
}

/// 把 `Result<T, LinkOpError>` 转成 RPC 结果值；失败时把带完整根因的原因记进日志
/// （RPC 错误对象只携带稳定的 code + reason，不带自由文本）。
pub fn rpc_value<T: serde::Serialize>(
    result: Result<T, LinkOpError>,
    context: ErrorContext,
) -> Result<Value, RpcErrorData> {
    match result {
        Ok(value) => serde_json::to_value(value)
            .map_err(|_| RpcErrorData::new(ApplicationErrorCode::Internal, false)),
        Err(error) => {
            tracing::warn!(error = %error, "agent.link request failed");
            Err(rpc_error(&error, context))
        }
    }
}

/// 互联层错误 → 兼容 API 错误（保留历史响应状态码）。
fn api_error(error: LinkError) -> ApiError {
    match error {
        LinkError::Unauthorized | LinkError::NotPaired => ApiError::Unauthorized,
        LinkError::InvalidCode
        | LinkError::BadSignature
        | LinkError::BadPayload(_)
        | LinkError::SelfPairing
        | LinkError::SessionExpired
        | LinkError::Throttled
        | LinkError::RejectedByPeer
        | LinkError::PairingTimeout
        | LinkError::IdentityMismatch(_)
        | LinkError::CommitmentMismatch
        | LinkError::UnsupportedVersion
        | LinkError::NotFluxDown(_) => ApiError::BadRequest(error.to_string()),
        LinkError::Unreachable | LinkError::Unavailable => ApiError::Unavailable,
        LinkError::Io(_) | LinkError::Store(_) => ApiError::Internal(error.to_string()),
    }
}

fn op_api_error(error: LinkOpError) -> ApiError {
    match error {
        LinkOpError::Link(error) => api_error(error),
        LinkOpError::SaveDirUnavailable => ApiError::BadRequest(
            "save directory is not an absolute path for the target device".to_owned(),
        ),
        LinkOpError::Invalid(field) => ApiError::BadRequest(format!("invalid {field}")),
    }
}

fn discovered_dto(peer: DiscoveredPeer) -> LinkDiscoveredPeer {
    LinkDiscoveredPeer {
        fingerprint: peer.fingerprint,
        name: peer.name,
        platform: peer.platform,
        host: peer.host,
        port: peer.port,
        app_version: peer.app_version,
        source: match peer.kind {
            DiscoveryKind::Mdns => "mdns",
            DiscoveryKind::Manual => "manual",
        }
        .to_owned(),
    }
}

fn non_empty(text: String) -> Option<String> {
    if text.is_empty() { None } else { Some(text) }
}

fn valid_url(url: &str) -> bool {
    !url.is_empty()
        && url.len() <= MAX_URL_LEN
        && !url.chars().any(|c| c.is_whitespace() || c.is_control())
}

/// 远端（已配对设备 / 云端）下发的文件名：单一路径分量，且不是 Windows 保留设备名。
#[must_use]
pub fn valid_file_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_FILE_NAME_LEN
        && !name.contains(['/', '\\', '\0'])
        && name != "."
        && name != ".."
        && !is_reserved_device_name(name)
}

fn is_reserved_device_name(name: &str) -> bool {
    let stem = name
        .split('.')
        .next()
        .unwrap_or_default()
        .trim_end_matches([' ', '\t'])
        .to_ascii_uppercase();
    match stem.as_str() {
        "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$" => true,
        other => {
            let digit = other
                .strip_prefix("COM")
                .or_else(|| other.strip_prefix("LPT"));
            digit.is_some_and(|rest| {
                let mut chars = rest.chars();
                matches!(chars.next(), Some('1'..='9' | '¹' | '²' | '³')) && chars.next().is_none()
            })
        }
    }
}

fn valid_save_dir(dir: &str) -> bool {
    dir.len() <= MAX_SAVE_DIR_LEN && !dir.contains('\0')
}

/// 目录不含 `..` 段，且不是 UNC / 设备命名空间路径（`\\host\share`、`\\?\`、`\\.\`）：
/// 后者在 Windows 上创建目录会主动触发对外 SMB 认证。
fn is_plain_local_dir(dir: &str) -> bool {
    if dir.contains('\0') || dir.split(['/', '\\']).any(|segment| segment == "..") {
        return false;
    }
    let mut chars = dir.chars();
    let is_separator = |c: char| c == '/' || c == '\\';
    !(chars.next().is_some_and(is_separator) && chars.next().is_some_and(is_separator))
}

/// 接单目录：对端给的保存目录必须是**本机路径风格**的绝对路径且通过 [`is_plain_local_dir`]
/// 才采用，否则（含空串）返回 `None`，由 daemon 使用本机默认目录（例如 Windows 设备下发
/// `C:\Downloads` 给 macOS）。云端接单与局域网互联共用这一规则。
#[must_use]
pub fn resolve_receive_dir(save_dir: &str) -> Option<String> {
    let dir = save_dir.trim();
    (!dir.is_empty() && PathStyle::current().is_absolute(dir) && is_plain_local_dir(dir))
        .then(|| dir.to_owned())
}

fn build_task_request(
    url: &str,
    file_name: &str,
    save_dir: &str,
) -> Result<CreateTaskRequest, ApiError> {
    serde_json::from_value(json!({ "url": url, "fileName": file_name, "saveDir": save_dir }))
        .map_err(|error| ApiError::Internal(format!("could not build task request: {error}")))
}

/// 本机网关可被局域网设备直连的基址（`http://ip:port`）：只监听回环 → 空；绑定了具体
/// 非回环地址 → 就是它；绑定通配地址 → 经 UDP 出站网卡探测得到各网段本机 IP。
#[must_use]
pub fn lan_base_urls(bound: SocketAddr) -> Vec<String> {
    let ip = bound.ip();
    if ip.is_loopback() {
        return Vec::new();
    }
    if !ip.is_unspecified() {
        return vec![format!("http://{bound}")];
    }
    let mut urls: Vec<String> = Vec::new();
    for target in ["8.8.8.8", "192.168.0.1", "10.0.0.1", "172.16.0.1"] {
        for candidate in fluxdown_link::discovery::local_direct_addrs(target, bound.port()) {
            let Ok(addr) = candidate.parse::<SocketAddr>() else {
                continue;
            };
            let unusable = match addr.ip() {
                IpAddr::V4(v4) => v4.is_loopback() || v4.is_unspecified() || v4.is_link_local(),
                IpAddr::V6(v6) => v6.is_loopback() || v6.is_unspecified(),
            };
            let url = format!("http://{addr}");
            if !unusable && !urls.contains(&url) {
                urls.push(url);
            }
        }
    }
    urls
}

fn mdns_enabled() -> bool {
    std::env::var("FLUXDOWN_MDNS").map_or(true, |value| {
        !matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "off" | "no"
        )
    })
}

fn trusted_proxies_from_env() -> Vec<IpAddr> {
    std::env::var("FLUXDOWN_TRUSTED_PROXIES")
        .map(|value| {
            value
                .split(',')
                .filter_map(|entry| entry.trim().parse::<IpAddr>().ok())
                .collect()
        })
        .unwrap_or_default()
}

/// 设备列表变化判定用的指纹项：(指纹, 名称, 在线, 默认目录, 路径风格)。
type RosterSignature = (String, String, bool, Option<String>, Option<PathStyle>);

/// 用于判断设备列表是否真的变化的指纹（忽略在线期间每轮都会刷新的 `last_seen_at`）。
fn roster_signature(devices: &[LinkDeviceInfo]) -> Vec<RosterSignature> {
    devices
        .iter()
        .map(|device| {
            (
                device.fingerprint.clone(),
                device.name.clone(),
                device.online,
                device.default_save_dir.clone(),
                device.path_style,
            )
        })
        .collect()
}

/// agent 的局域网直连服务。
pub struct LinkService {
    events: AgentEventHub,
    state: Arc<Mutex<AgentState>>,
    store: Arc<StateStore>,
    tasks: Arc<dyn LinkTaskCreator>,
    bound: StdMutex<SocketAddr>,
    server_mode: bool,
    trusted_proxies: Vec<IpAddr>,
    manager: OnceLock<Arc<LinkManager>>,
    started: Notify,
    online: StdMutex<HashMap<String, bool>>,
    requests: StdMutex<Vec<LinkPairingRequestDto>>,
    code_generation: AtomicU64,
}

fn lock<T>(mutex: &StdMutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl LinkService {
    #[must_use]
    pub fn new(parts: LinkServiceParts) -> Arc<Self> {
        Arc::new(Self {
            events: parts.events,
            state: parts.state,
            store: parts.store,
            tasks: parts.tasks,
            bound: StdMutex::new(parts.bound),
            server_mode: parts.server_mode,
            trusted_proxies: trusted_proxies_from_env(),
            manager: OnceLock::new(),
            started: Notify::new(),
            online: StdMutex::new(HashMap::new()),
            requests: StdMutex::new(Vec::new()),
            code_generation: AtomicU64::new(0),
        })
    }

    /// 刷新已验证成功的网关监听端口；保留 IP、身份、配对与可达范围。
    ///
    /// 启动前只记录最新地址，不等待 daemon 或互联初始化。广告更新失败时保留旧地址；
    /// 传入旧地址可用于网关事务回滚。
    pub async fn update_bound(&self, bound: SocketAddr) -> Result<(), LinkOpError> {
        let mut current = lock(&self.bound);
        let mut expected = *current;
        expected.set_port(bound.port());
        if expected != bound {
            return Err(LinkOpError::Invalid("bound"));
        }
        if let Some(manager) = self.manager.get() {
            manager.update_api_port(bound.port())?;
        }
        *current = bound;
        Ok(())
    }

    /// 加载（或首次生成）本机身份并启动互联。必须在 legacy 迁移完成后调用：迁移可能
    /// 写入 daemon 时代的身份与名册，提前生成身份会被随后的迁移覆盖。
    pub async fn start(self: &Arc<Self>) -> LinkResult<()> {
        if self.manager.get().is_some() {
            return Ok(());
        }
        let device_name = self.state.lock().await.device_name.clone();
        let name = std::env::var("FLUXDOWN_LINK_NAME")
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| {
                if self.server_mode {
                    "FluxDown Server".to_owned()
                } else if device_name.trim().is_empty() {
                    "FluxDown".to_owned()
                } else {
                    device_name
                }
            });
        let self_info = SelfInfo {
            name,
            platform: Some(if self.server_mode {
                "server".to_owned()
            } else {
                std::env::consts::OS.to_owned()
            }),
            app_version: Some(fluxdown_protocol::APP_VERSION.to_owned()),
        };
        let bound = *lock(&self.bound);
        let options = LinkOptions {
            api_port: bound.port(),
            reachable: !bound.ip().is_loopback(),
            advertise: mdns_enabled(),
        };
        let (tx, rx) = mpsc::channel::<LinkEngineEvent>(64);
        let storage = Arc::new(AgentLinkStorage::new(
            self.state.clone(),
            self.store.clone(),
        ));
        let manager = LinkManager::load(storage, self_info, options, tx).await?;
        tracing::info!(
            fingerprint = manager.fingerprint(),
            lan_reachable = options.reachable,
            "device link ready"
        );
        {
            // load 会等待状态存储；初始化期间重绑不能被它挡住。发布前在同一短锁下追上
            // 最新端口，避免 update_bound 看见空 OnceLock 后初始化又发布旧端口。
            let bound = lock(&self.bound);
            manager.update_api_port(bound.port())?;
            if self.manager.set(manager).is_err() {
                return Ok(());
            }
        }
        tokio::spawn(self.clone().pump(rx));
        self.started.notify_one();
        self.publish_devices().await;
        Ok(())
    }

    /// 已配对设备在线状态的后台轮询；`start` 完成后才开始。
    pub async fn run(self: Arc<Self>, cancel: CancellationToken) {
        tokio::select! {
            () = cancel.cancelled() => return,
            () = self.started.notified() => {}
        }
        let mut tick = tokio::time::interval(ONLINE_POLL_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                _ = tick.tick() => {
                    if let Err(error) = self.probe_all(false).await {
                        tracing::debug!(error = %error, "linked device probe round failed");
                    }
                }
            }
        }
    }

    pub(super) fn manager(&self) -> Result<&Arc<LinkManager>, LinkError> {
        self.manager.get().ok_or(LinkError::Unavailable)
    }

    /// 兼容 API 面：未启动 = 本宿主不支持设备互联。
    fn api_manager(&self) -> Result<&Arc<LinkManager>, ApiError> {
        self.manager.get().ok_or_else(link_unsupported)
    }

    /// 转发 [`LinkManager`] 事件到快照 / 事件流。
    async fn pump(self: Arc<Self>, mut rx: mpsc::Receiver<LinkEngineEvent>) {
        while let Some(event) = rx.recv().await {
            match event {
                LinkEngineEvent::Discovered(_) => self.publish_discovered(),
                LinkEngineEvent::Paired(record) => {
                    tracing::info!(
                        name = %record.name,
                        fingerprint = %record.short_fingerprint(),
                        "device paired"
                    );
                    self.drop_requests(|request| request.peer_fingerprint == record.fingerprint);
                    self.publish_devices().await;
                    // 双方都拿到对端的默认目录 / 路径风格；失败（旧版对端、暂时不可达）静默。
                    let service = self.clone();
                    let fingerprint = record.fingerprint;
                    tokio::spawn(async move {
                        if service.exchange_info(&fingerprint).await {
                            service.publish_devices().await;
                        }
                    });
                }
                LinkEngineEvent::Unpaired(fingerprint) => {
                    lock(&self.online).remove(&fingerprint);
                    self.publish_devices().await;
                }
                LinkEngineEvent::IncomingCancelled { session_id } => {
                    self.drop_requests(|request| request.session_id == session_id);
                }
                LinkEngineEvent::IncomingPairing {
                    session_id,
                    sas,
                    peer_name,
                    peer_platform,
                    peer_fingerprint,
                } => self.add_request(LinkPairingRequestDto {
                    session_id,
                    peer_name,
                    peer_fingerprint,
                    peer_platform,
                    sas,
                    expires_at_unix_ms: now_unix_ms().saturating_add(
                        i64::try_from(DECISION_WINDOW_SECS * 1000).unwrap_or(60_000),
                    ),
                }),
                LinkEngineEvent::Error(message) => {
                    tracing::warn!(error = %message, "device link subsystem error");
                }
            }
        }
    }

    // ── 快照 / 事件 ────────────────────────────────────────────────────────

    async fn devices(&self) -> Vec<LinkDeviceInfo> {
        let online = lock(&self.online).clone();
        let state = self.state.lock().await;
        devices_with_online(&state, &online)
    }

    async fn publish_devices(&self) {
        let devices = self.devices().await;
        self.events
            .publish(AgentEvent::LinkedDevicesChanged(devices));
    }

    fn publish_discovered(&self) {
        let peers = self
            .manager
            .get()
            .map(|manager| {
                manager
                    .discovered_peers()
                    .into_iter()
                    .map(discovered_dto)
                    .collect()
            })
            .unwrap_or_default();
        self.events
            .publish(AgentEvent::LinkDiscoveredChanged(peers));
    }

    fn publish_requests(&self) {
        let requests = lock(&self.requests).clone();
        self.events
            .publish(AgentEvent::LinkPairingRequestsChanged(requests));
    }

    fn add_request(self: &Arc<Self>, request: LinkPairingRequestDto) {
        let session_id = request.session_id.clone();
        let expires_in = Duration::from_secs(DECISION_WINDOW_SECS);
        {
            let mut requests = lock(&self.requests);
            requests.retain(|existing| existing.session_id != session_id);
            requests.push(request);
            while requests.len() > PAIRING_REQUEST_CAP {
                requests.remove(0);
            }
        }
        self.publish_requests();
        // 核验窗口过后请求自然作废：从快照里移除，UI 不再显示一个已无法批准的弹窗。
        let service = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(expires_in).await;
            service.drop_requests(|request| request.session_id == session_id);
        });
    }

    fn drop_requests(&self, matches: impl Fn(&LinkPairingRequestDto) -> bool) {
        let removed = {
            let mut requests = lock(&self.requests);
            let before = requests.len();
            requests.retain(|request| !matches(request));
            requests.len() != before
        };
        if removed {
            self.publish_requests();
        }
    }

    // ── 本机信息 ───────────────────────────────────────────────────────────

    /// 本机默认下载目录 / 路径风格（经已认证链路自报给已配对对端）。
    fn local_peer_info(&self) -> PeerInfo {
        let configured = self.events.inspect(|snapshot| {
            snapshot
                .daemon
                .config
                .values
                .get("default_save_dir")
                .map(|dir| dir.trim().to_owned())
        });
        let default_save_dir = configured.filter(|dir| !dir.is_empty()).or_else(|| {
            directories::UserDirs::new().and_then(|dirs| {
                dirs.download_dir()
                    .map(|dir| dir.to_string_lossy().into_owned())
            })
        });
        PeerInfo {
            default_save_dir,
            path_style: Some(local_path_style_text().to_owned()),
        }
    }

    /// 与对端交换设备信息；返回是否拿到了对端信息。
    async fn exchange_info(&self, fingerprint: &str) -> bool {
        let Ok(manager) = self.manager() else {
            return false;
        };
        match manager
            .exchange_peer_info(fingerprint, &self.local_peer_info())
            .await
        {
            Ok(Some(_)) => true,
            Ok(None) => false,
            Err(error) => {
                tracing::debug!(fingerprint, error = %error, "peer info exchange failed");
                false
            }
        }
    }

    /// 兼容 API 是否应采信转发头（见 `ApiHost::link_trusted_proxy`）。
    #[must_use]
    pub fn trusted_proxy(&self, peer: IpAddr) -> bool {
        peer.is_loopback() || self.trusted_proxies.contains(&peer)
    }

    // ── agent.link.* ───────────────────────────────────────────────────────

    /// 配对码过期后收起 mDNS 广播（除非之后又生成了更新的码 / 用户已主动停止）。
    fn arm_advertising_timeout(self: &Arc<Self>) {
        let generation = self.code_generation.fetch_add(1, Ordering::AcqRel) + 1;
        let service = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(CODE_TTL_SECS)).await;
            if service.code_generation.load(Ordering::Acquire) == generation
                && let Ok(manager) = service.manager()
            {
                manager.stop_advertising();
            }
        });
    }

    /// `agent.link.pairingCode`：生成配对码并（局域网可达时）开始 mDNS 广播。
    pub async fn pairing_code(self: &Arc<Self>) -> Result<LinkPairingCodeDto, LinkOpError> {
        let manager = self.manager()?;
        let code = manager.generate_code();
        self.arm_advertising_timeout();
        Ok(LinkPairingCodeDto {
            code,
            expires_at_unix_ms: now_unix_ms()
                .saturating_add(i64::try_from(CODE_TTL_SECS * 1000).unwrap_or(120_000)),
            addresses: lan_base_urls(*lock(&self.bound)),
            fingerprint: manager.fingerprint().to_owned(),
            device_name: manager.self_name().to_owned(),
        })
    }

    /// `agent.link.stopPairing`：作废配对码并停止广播。
    pub fn stop_pairing(&self) -> Result<(), LinkOpError> {
        self.code_generation.fetch_add(1, Ordering::AcqRel);
        self.manager()?.stop_pairing();
        Ok(())
    }

    /// `agent.link.discovery.set`。
    pub fn set_discovery(&self, enabled: bool) -> Result<(), LinkOpError> {
        let manager = self.manager()?;
        if enabled {
            manager.start_discovery()?;
        } else {
            manager.stop_discovery();
        }
        // 开启会清空发现快照、停止后也清空：UI 据此重置列表。
        self.publish_discovered();
        Ok(())
    }

    /// `agent.link.probe`。
    pub async fn probe(&self, address: &str) -> Result<LinkDiscoveredPeer, LinkOpError> {
        let address = PeerAddress::parse(address).map_err(|_| LinkOpError::Invalid("address"))?;
        Ok(discovered_dto(self.manager()?.probe(&address).await?))
    }

    /// `agent.link.pairBegin`。
    pub async fn pair_begin(
        &self,
        address: &str,
        code: &str,
    ) -> Result<LinkPairBeginResponse, LinkOpError> {
        let address = PeerAddress::parse(address).map_err(|_| LinkOpError::Invalid("address"))?;
        let code = code.trim();
        if code.is_empty() {
            return Err(LinkOpError::Invalid("code"));
        }
        let result = self.manager()?.begin_pairing(&address, code).await?;
        Ok(LinkPairBeginResponse {
            token: result.token,
            sas: result.sas,
            peer_name: result.peer_name,
            peer_fingerprint: result.peer_fingerprint,
        })
    }

    /// `agent.link.pairFinish`：发起端核对 SAS 后确认 / 放弃。放弃或对端拒绝 → `None`。
    pub async fn pair_finish(
        &self,
        token: &str,
        accept: bool,
    ) -> Result<Option<LinkDeviceInfo>, LinkOpError> {
        let manager = self.manager()?;
        let Some(record) = manager.confirm_pairing(token, accept).await? else {
            return Ok(None);
        };
        let online = manager.is_online(&record.fingerprint).await;
        lock(&self.online).insert(record.fingerprint.clone(), online);
        self.publish_devices().await;
        Ok(Some(super::storage::device_info(&record, online)))
    }

    /// `agent.link.approve`：响应端对入站配对请求的决定。
    pub fn approve(&self, session_id: &str, accept: bool) -> Result<(), LinkOpError> {
        let result = self.manager()?.approve_incoming(session_id, accept);
        // 无论决定是否成功送达（会话可能已过期），这条请求都不再需要 UI 处理。
        self.drop_requests(|request| request.session_id == session_id);
        Ok(result?)
    }

    /// `agent.link.remove`。
    pub async fn remove(&self, fingerprint: &str) -> Result<(), LinkOpError> {
        let manager = self.manager()?;
        if !manager.remove_device(fingerprint).await? {
            return Err(LinkError::NotPaired.into());
        }
        lock(&self.online).remove(fingerprint);
        self.publish_devices().await;
        Ok(())
    }

    /// `agent.link.refresh`：探测全部已配对设备在线状态并刷新对端信息，同时推送 `LinkedDevicesChanged`。
    pub async fn refresh(&self) -> Result<Vec<LinkDeviceInfo>, LinkOpError> {
        Ok(self.probe_all(true).await?)
    }

    /// 并发探测名册里全部设备。`force_info` 为真时对每个在线设备重新交换设备信息，否则只对
    /// 还不知道对端信息的设备交换。名册或在线状态变化才推送事件（`force_info` 时总是推送）。
    async fn probe_all(&self, force_info: bool) -> LinkResult<Vec<LinkDeviceInfo>> {
        let manager = self.manager()?;
        let records = manager.list_devices().await?;
        let before = self.devices().await;
        let probes = records.iter().map(|record| async move {
            // 超时只判定这一台设备离线，不连累同轮里已经探测成功的其他设备。
            let probe = async {
                let online = manager.is_online(&record.fingerprint).await;
                if online && (force_info || record.info == PeerInfo::default()) {
                    self.exchange_info(&record.fingerprint).await;
                }
                online
            };
            let online = tokio::time::timeout(PROBE_ROUND_TIMEOUT, probe)
                .await
                .unwrap_or(false);
            (record.fingerprint.clone(), online)
        });
        let results = futures_util::future::join_all(probes).await;
        *lock(&self.online) = results.into_iter().collect();
        let after = self.devices().await;
        if force_info || roster_signature(&before) != roster_signature(&after) {
            self.events
                .publish(AgentEvent::LinkedDevicesChanged(after.clone()));
        }
        Ok(after)
    }

    /// `agent.link.dispatch`：把下载下发到已配对设备，返回对端新任务 ID。
    pub async fn dispatch(
        &self,
        fingerprint: &str,
        url: &str,
        file_name: Option<&str>,
        save_dir: Option<&str>,
    ) -> Result<String, LinkOpError> {
        let url = url.trim();
        if !valid_url(url) {
            return Err(LinkOpError::Invalid("url"));
        }
        let file_name = file_name.map(str::trim).filter(|name| !name.is_empty());
        if file_name.is_some_and(|name| !valid_file_name(name)) {
            return Err(LinkOpError::Invalid("fileName"));
        }
        let save_dir = save_dir.map(str::trim).filter(|dir| !dir.is_empty());
        if save_dir.is_some_and(|dir| !valid_save_dir(dir)) {
            return Err(LinkOpError::Invalid("saveDir"));
        }
        let manager = self.manager()?;
        let record = manager
            .get_device(fingerprint)
            .await?
            .ok_or(LinkError::NotPaired)?;
        if let Some(dir) = save_dir {
            // 目标路径风格未知（旧版对端）时不阻拦：由接收端按契约回退到它的默认目录。
            let style = record
                .info
                .path_style
                .as_deref()
                .and_then(|style| match style {
                    "windows" => Some(PathStyle::Windows),
                    "posix" => Some(PathStyle::Posix),
                    _ => None,
                })
                .or_else(|| {
                    record
                        .platform
                        .as_deref()
                        .and_then(PathStyle::from_platform)
                });
            if style.is_some_and(|style| !style.is_absolute(dir)) {
                return Err(LinkOpError::SaveDirUnavailable);
            }
        }
        Ok(manager
            .dispatch(fingerprint, url, save_dir, file_name)
            .await?)
    }

    /// `agent.link.*` RPC 分发：解析参数、调用对应操作、按稳定 `reason` 映射错误。
    pub async fn rpc(
        self: &Arc<Self>,
        method_name: &str,
        params: Option<Value>,
    ) -> Result<Value, RpcErrorData> {
        fn parse<T: serde::de::DeserializeOwned>(params: Option<Value>) -> Result<T, RpcErrorData> {
            serde_json::from_value(params.unwrap_or_else(|| json!({})))
                .map_err(|_| RpcErrorData::new(ApplicationErrorCode::InvalidArgument, false))
        }
        let ok = |result: Result<(), LinkOpError>, context| {
            rpc_value(result.map(|()| json!({ "ok": true })), context)
        };
        match method_name {
            method::AGENT_LINK_PAIRING_CODE => {
                rpc_value(self.pairing_code().await, ErrorContext::Local)
            }
            method::AGENT_LINK_STOP_PAIRING => ok(self.stop_pairing(), ErrorContext::Local),
            method::AGENT_LINK_DISCOVERY_SET => {
                let params: LinkDiscoveryParams = parse(params)?;
                ok(self.set_discovery(params.enabled), ErrorContext::Local)
            }
            method::AGENT_LINK_PROBE => {
                let params: LinkAddressParams = parse(params)?;
                rpc_value(self.probe(&params.address).await, ErrorContext::Pairing)
            }
            method::AGENT_LINK_PAIR_BEGIN => {
                let params: LinkPairBeginParams = parse(params)?;
                rpc_value(
                    self.pair_begin(&params.address, &params.code).await,
                    ErrorContext::Pairing,
                )
            }
            method::AGENT_LINK_PAIR_FINISH => {
                let params: LinkPairFinishParams = parse(params)?;
                rpc_value(
                    self.pair_finish(&params.token, params.accept)
                        .await
                        .map(|device| LinkPairFinishResponse {
                            paired: device.is_some(),
                            device,
                        }),
                    ErrorContext::Pairing,
                )
            }
            method::AGENT_LINK_APPROVE => {
                let params: LinkApproveParams = parse(params)?;
                ok(
                    self.approve(&params.session_id, params.accept),
                    ErrorContext::Pairing,
                )
            }
            method::AGENT_LINK_REMOVE => {
                let params: LinkDeviceParams = parse(params)?;
                ok(self.remove(&params.fingerprint).await, ErrorContext::Peer)
            }
            method::AGENT_LINK_REFRESH => rpc_value(self.refresh().await, ErrorContext::Local),
            method::AGENT_LINK_DISPATCH => {
                let params: LinkDispatchParams = parse(params)?;
                rpc_value(
                    self.dispatch(
                        &params.fingerprint,
                        &params.url,
                        params.file_name.as_deref(),
                        params.save_dir.as_deref(),
                    )
                    .await
                    .map(|task_id| LinkDispatchResult { task_id }),
                    ErrorContext::Peer,
                )
            }
            _ => Err(RpcErrorData::new(ApplicationErrorCode::Unsupported, false)),
        }
    }

    // ── 兼容 API（Flutter / CLI / 其它设备的 `/api/v1/link/*` 与 `/ping`）──────

    #[must_use]
    pub fn api_ping_info(&self) -> Option<LinkPingInfo> {
        let manager = self.manager.get()?;
        Some(LinkPingInfo {
            fingerprint: manager.fingerprint().to_owned(),
            name: manager.self_name().to_owned(),
            platform: manager.self_platform().unwrap_or_default().to_owned(),
        })
    }

    pub async fn api_pair_hello(
        &self,
        req: LinkPairHelloRequest,
        source: Option<IpAddr>,
    ) -> Result<LinkPairHelloResponse, ApiError> {
        let manager = self.api_manager()?;
        let wire = WireHello {
            protocol_version: req.protocol_version,
            code: req.code,
            initiator_id_pub: req.initiator_id_pub,
            initiator_commit: req.initiator_commit,
            initiator_sig: req.initiator_sig,
            name: req.name,
            platform: non_empty(req.platform),
            app_version: non_empty(req.app_version),
            initiator_addrs: req.initiator_addrs,
        };
        let resp = manager.pair_hello_wire(wire, source).map_err(api_error)?;
        Ok(LinkPairHelloResponse {
            protocol_version: resp.protocol_version,
            session_id: resp.session_id,
            responder_eph_pub: resp.responder_eph_pub,
            responder_nonce: resp.responder_nonce,
            responder_id_pub: resp.responder_id_pub,
            name: resp.name,
            platform: resp.platform.unwrap_or_default(),
            app_version: resp.app_version.unwrap_or_default(),
        })
    }

    pub async fn api_pair_reveal(
        &self,
        req: LinkPairRevealRequest,
    ) -> Result<LinkPairRevealResponse, ApiError> {
        let manager = self.api_manager()?;
        let resp = manager
            .pair_reveal_wire(WireReveal {
                session_id: req.session_id,
                initiator_eph_pub: req.initiator_eph_pub,
                initiator_nonce: req.initiator_nonce,
            })
            .map_err(api_error)?;
        Ok(LinkPairRevealResponse {
            responder_sig: resp.responder_sig,
        })
    }

    pub async fn api_pair_confirm(
        &self,
        req: LinkPairConfirmRequest,
    ) -> Result<LinkPairConfirmOutcome, ApiError> {
        let manager = self.api_manager()?;
        let outcome = manager
            .pair_confirm(&req.session_id, req.confirm)
            .await
            .map_err(api_error)?;
        // 终局（批准 / 拒绝 / 超时 / 发起方放弃）之后这条入站请求都不必再展示。
        self.drop_requests(|request| request.session_id == req.session_id);
        Ok(match outcome {
            PairConfirmOutcome::Paired => LinkPairConfirmOutcome::Paired,
            PairConfirmOutcome::Declined => LinkPairConfirmOutcome::Declined,
            PairConfirmOutcome::Rejected => LinkPairConfirmOutcome::Rejected,
            PairConfirmOutcome::TimedOut => LinkPairConfirmOutcome::TimedOut,
        })
    }

    pub fn api_approve(&self, session_id: &str, accept: bool) -> Result<(), ApiError> {
        self.approve(session_id, accept).map_err(op_api_error)
    }

    /// 已配对设备下发下载任务（数据面，链路 HMAC 鉴权 + AEAD 解密）。
    pub async fn api_create_task(&self, auth: LinkAuth, body: Vec<u8>) -> Result<String, ApiError> {
        let manager = self.api_manager()?;
        let request = manager
            .authorize(
                "POST",
                API_LINK_TASKS,
                &auth.device,
                auth.ts,
                &auth.nonce,
                &body,
                &auth.tag,
                &auth.enc,
            )
            .await
            .map_err(api_error)?;
        let task: LinkTaskRequest = serde_json::from_slice(&request.body)
            .map_err(|error| ApiError::BadRequest(format!("invalid link task payload: {error}")))?;
        let url = task.url.trim();
        if !valid_url(url) {
            return Err(ApiError::BadRequest("invalid url".to_owned()));
        }
        let file_name = task.file_name.trim();
        if !file_name.is_empty() && !valid_file_name(file_name) {
            return Err(ApiError::BadRequest("invalid fileName".to_owned()));
        }
        if !valid_save_dir(&task.save_dir) {
            return Err(ApiError::BadRequest("invalid saveDir".to_owned()));
        }
        self.create_local_task(url, file_name, resolve_receive_dir(&task.save_dir))
            .await
    }

    /// 接单建任务：带目录失败 → 去掉目录（使用本机默认目录）重试一次。
    async fn create_local_task(
        &self,
        url: &str,
        file_name: &str,
        save_dir: Option<String>,
    ) -> Result<String, ApiError> {
        let first = build_task_request(url, file_name, save_dir.as_deref().unwrap_or_default())?;
        match self.tasks.create_task(first).await {
            Ok(task_id) => Ok(task_id),
            Err(error) if save_dir.is_some() => {
                tracing::warn!(
                    error = %error,
                    "link task rejected with the requested directory; retrying with the default directory"
                );
                self.tasks
                    .create_task(build_task_request(url, file_name, "")?)
                    .await
            }
            Err(error) => Err(error),
        }
    }

    /// `POST /api/v1/link/info`：交换设备信息（数据面，请求 / 响应均为密文）。
    pub async fn api_peer_info(&self, auth: LinkAuth, body: Vec<u8>) -> Result<Vec<u8>, ApiError> {
        let manager = self.api_manager()?;
        let request = manager
            .authorize(
                "POST",
                API_LINK_INFO,
                &auth.device,
                auth.ts,
                &auth.nonce,
                &body,
                &auth.tag,
                &auth.enc,
            )
            .await
            .map_err(api_error)?;
        let sealed = manager
            .answer_peer_info(&request, &self.local_peer_info())
            .await
            .map_err(api_error)?;
        // 请求方把它的默认目录 / 路径风格也告诉了我们：名册变了，推给 UI。
        self.publish_devices().await;
        Ok(sealed)
    }

    pub fn api_generate_code(self: &Arc<Self>) -> Result<LinkCodeResponse, ApiError> {
        let manager = self.api_manager()?;
        let code = manager.generate_code();
        self.arm_advertising_timeout();
        Ok(LinkCodeResponse {
            code,
            ttl_seconds: i64::try_from(CODE_TTL_SECS).unwrap_or(120),
        })
    }

    pub fn api_stop_advertising(&self) -> Result<(), ApiError> {
        self.api_manager()?.stop_advertising();
        Ok(())
    }

    pub fn api_discovery(&self, start: bool) -> Result<(), ApiError> {
        self.api_manager()?;
        self.set_discovery(start).map_err(op_api_error)
    }

    pub fn api_discovered(&self) -> Result<Vec<LinkDiscoveredPeer>, ApiError> {
        Ok(self
            .api_manager()?
            .discovered_peers()
            .into_iter()
            .map(discovered_dto)
            .collect())
    }

    pub async fn api_probe(&self, host: &str, port: u16) -> Result<LinkDiscoveredPeer, ApiError> {
        let address = PeerAddress::from_host_port(host, port).map_err(api_error)?;
        self.api_manager()?
            .probe(&address)
            .await
            .map(discovered_dto)
            .map_err(api_error)
    }

    pub async fn api_pair_begin(
        &self,
        host: &str,
        port: u16,
        code: &str,
    ) -> Result<LinkPairBeginResponse, ApiError> {
        let address = PeerAddress::from_host_port(host, port).map_err(api_error)?;
        self.api_manager()?;
        self.pair_begin(&address.to_candidate(), code)
            .await
            .map_err(op_api_error)
    }

    pub async fn api_pair_finish(
        &self,
        token: &str,
        accept: bool,
    ) -> Result<Option<LinkDeviceInfo>, ApiError> {
        self.api_manager()?;
        self.pair_finish(token, accept).await.map_err(op_api_error)
    }

    pub async fn api_devices(&self) -> Result<Vec<LinkDeviceInfo>, ApiError> {
        self.api_manager()?;
        self.refresh().await.map_err(op_api_error)
    }

    pub async fn api_remove_device(&self, fingerprint: &str) -> Result<bool, ApiError> {
        self.api_manager()?;
        match self.remove(fingerprint).await {
            Ok(()) => Ok(true),
            Err(LinkOpError::Link(LinkError::NotPaired)) => Ok(false),
            Err(error) => Err(op_api_error(error)),
        }
    }

    pub async fn api_dispatch(
        &self,
        fingerprint: &str,
        url: &str,
        save_dir: Option<&str>,
        file_name: Option<&str>,
    ) -> Result<String, ApiError> {
        self.api_manager()?;
        self.dispatch(fingerprint, url, file_name, save_dir)
            .await
            .map_err(op_api_error)
    }
}
