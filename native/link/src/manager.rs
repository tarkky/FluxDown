//! [`LinkManager`] —— 设备互联子系统门面。
//!
//! 聚合身份、名册存储（[`LinkStorage`]）、配对协议（响应方 + 发起方）、mDNS 发现、
//! 可扩展传输栈，供宿主（agent / 冻结的 server）驱动。宿主只跟本门面 +
//! 一个事件通道打交道。
//!
//! # 角色
//! - **响应方**（被添加设备）：生成配对码、处理 `hello`/`confirm`、mDNS 广播。
//! - **发起方**（正在添加设备）：mDNS 浏览、`begin_pairing`（发 hello、算 SAS）、
//!   `confirm_pairing`（发 confirm、落库）。
//! - **数据面**：`dispatch`（把下载下发给已配对设备，先 AEAD 加密再走传输栈）、
//!   `authorize`（校验入站链路请求的 HMAC 鉴权并解密）、`exchange_peer_info`
//!   （经已认证链路交换默认目录 / 路径风格）。

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use tokio::sync::mpsc;

use super::address::PeerAddress;
use super::crypto::{
    LINK_AUTH_SKEW_SECS, derive_link_aead_key, link_auth_tag, open_link_body, seal_link_body,
    verify_link_auth_tag,
};
use super::discovery::{self, MdnsAdvertiser, MdnsBrowser};
use super::error::{LinkError, LinkResult};
use super::identity::LinkIdentity;
use super::pairing::{
    HelloRequest, HelloResponse, PAIRING_PROTOCOL_VERSION, PairingInitiator, PairingResponder,
    RevealRequest, RevealResponse, SelfInfo,
};
use super::storage::LinkStorage;
use super::transport::TransportStack;
use super::types::{DiscoveredPeer, PeerCandidate, PeerInfo, PeerRecord, TransportKind};
use super::wire::{self, classify_failure, read_json_object, send_failure};

/// 数据面：经已认证链路交换设备信息的路径（与 `fluxdown_api::routes::API_LINK_INFO` 一致）。
pub const LINK_INFO_PATH: &str = "/api/v1/link/info";
/// 数据面：下发下载任务的路径（与 `fluxdown_api::routes::API_LINK_TASKS` 一致）。
pub const LINK_TASKS_PATH: &str = "/api/v1/link/tasks";

/// 设备互联事件，由宿主消费并转换为协议通知。
#[derive(Debug, Clone)]
pub enum LinkEngineEvent {
    /// mDNS/手动发现到一台设备。
    Discovered(DiscoveredPeer),
    /// 一台设备完成配对并入册（含 link_secret，宿主转 UI 前须剥除敏感字段）。
    Paired(PeerRecord),
    /// 一台设备被解除配对（fingerprint）。
    Unpaired(String),
    /// 有设备正在向本机发起配对，等待本机用户核对 SAS 后批准/拒绝。
    IncomingPairing {
        session_id: String,
        sas: String,
        peer_name: String,
        peer_platform: Option<String>,
        /// 发起方 Ed25519 身份指纹（设备 ID）。
        peer_fingerprint: String,
    },
    /// 子系统错误（供 UI 提示）。
    Error(String),
    /// 发起方放弃/拒绝了一次入站配对：响应方会话已移除，宿主应关闭对应的待确认请求。
    IncomingCancelled { session_id: String },
}

/// 响应方处理一次入站 `confirm` 的终局。四种都是协议的正常结果，**不是错误**——
/// 发起方要据此给出准确提示，不能一律显示「会话过期」。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PairConfirmOutcome {
    /// 本机用户批准，发起方已入册。
    Paired,
    /// 发起方自己传了 `confirm=false`。
    Declined,
    /// 本机用户核对 SAS 后拒绝。
    Rejected,
    /// 等待本机用户核验超时（60s 决策窗口耗尽）。
    TimedOut,
}

impl PairConfirmOutcome {
    /// 是否真的完成了配对。
    #[must_use]
    pub fn paired(self) -> bool {
        matches!(self, Self::Paired)
    }

    /// 供 HTTP 响应体透出的稳定判别串（发起方据此还原语义）；`Paired`/`Declined`
    /// 无需额外理由。
    #[must_use]
    pub fn reason(self) -> Option<&'static str> {
        match self {
            Self::Rejected => Some("rejected"),
            Self::TimedOut => Some("timeout"),
            Self::Paired | Self::Declined => None,
        }
    }
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// 发现快照去重键：优先用指纹（mDNS TXT 记录携带，稳定）；探测阶段指纹未知
/// 时退化到 `host:port`。
fn discovered_peer_key(p: &DiscoveredPeer) -> String {
    match &p.fingerprint {
        Some(fp) => fp.clone(),
        None => format!("{}:{}", p.host, p.port),
    }
}

/// 按 [`discovered_peer_key`] 去重更新发现快照：已存在则原地覆盖（刷新地址/
/// 名称等可能变化的字段），否则追加。
fn upsert_discovered(snapshot: &mut Vec<DiscoveredPeer>, peer: DiscoveredPeer) {
    let key = discovered_peer_key(&peer);
    match snapshot.iter_mut().find(|e| discovered_peer_key(e) == key) {
        Some(existing) => *existing = peer,
        None => snapshot.push(peer),
    }
}

/// 发起方待确认会话（[`PendingInit`]）存活上限。**必须**与响应方
/// `pairing.rs` 的 `SESSION_TTL`（180s）保持一致——双端各自独立维护一套
/// 过期判定，此前这个 180 是硬编码在 [`LinkManager::begin_pairing`] 剪枝
/// 调用里的魔法数，[`LinkManager::confirm_pairing`] 需要同一个值做本地
/// 过期检查（此前它完全不做本地过期检查，只依赖对端 `SESSION_TTL` 兜
/// 底），因此提升为具名常量集中定义，未来两端 TTL 一起改，不会各自漂移。
const PENDING_INIT_TTL: std::time::Duration = std::time::Duration::from_secs(180);

/// 手动探测（`/ping`）得到的设备指纹的保留时长：配对时用来校验对端出示的身份，
/// 过久的探测结果不再可信（设备可能已换身份）。
const PROBED_FINGERPRINT_TTL: std::time::Duration = std::time::Duration::from_secs(600);

/// 探测指纹表的条数上限，超出时淘汰最旧的一条，保证内存占用有界。
const MAX_PROBED_FINGERPRINTS: usize = 64;

/// 已配对设备 Direct 候选端点集合的总数上限。mDNS 重新发现命中已配对
/// 指纹时会把新地址去重后插到候选表首位、旧候选整体保留作回退（见
/// `LinkManager::start_discovery` 的转发任务），若不设上限，设备长期
/// 在多个网络间漂移会让候选表无界增长。取一个小值：日常场景「当前
/// 网段 + 一两个历史网段」远用不到 4 个，仍能覆盖「NAS 同时插着物理
/// 网卡和虚拟机 host-only 网卡」这类多网卡场景的合理候选数。
const MAX_DIRECT_CANDIDATES: usize = 4;

/// 发起方待确认会话（begin_pairing 与 confirm_pairing 之间的状态）。
struct PendingInit {
    initiator: PairingInitiator,
    session_id: String,
    peer: PeerAddress,
    created: std::time::Instant,
}

/// [`LinkManager`] 的宿主相关选项。
#[derive(Debug, Clone, Copy)]
pub struct LinkOptions {
    /// 本机 fluxdown API 端口（mDNS 广播 + 自报候选用）。
    pub api_port: u16,
    /// 本机 API 是否可被局域网内其它设备访问。`false`（如桌面 agent 默认只监听
    /// 127.0.0.1）时不做 mDNS 广播、配对时也不向对端自报回连地址——那些地址对端
    /// 根本连不上。
    pub reachable: bool,
    /// 是否允许 mDNS 广播（`FLUXDOWN_MDNS=off` 的开关）。`false` 时仍可用配对码 +
    /// 手动地址配对，只是不在局域网内广播本机。
    pub advertise: bool,
}

impl LinkOptions {
    /// 局域网可达且允许广播（保持既有发现行为）。
    #[must_use]
    pub fn reachable(api_port: u16) -> Self {
        Self {
            api_port,
            reachable: true,
            advertise: true,
        }
    }
}

/// 设备互联门面。宿主持 `Arc<LinkManager>`。
pub struct LinkManager {
    identity: LinkIdentity,
    self_info: SelfInfo,
    store: Arc<dyn LinkStorage>,
    responder: PairingResponder,
    transport: TransportStack,
    client: reqwest::Client,
    api_port: AtomicU16,
    reachable: bool,
    advertise: bool,
    events: mpsc::Sender<LinkEngineEvent>,
    advertiser: Mutex<Option<MdnsAdvertiser>>,
    browser: Mutex<Option<MdnsBrowser>>,
    pending: Mutex<HashMap<String, PendingInit>>,
    /// 数据面防重放：时窗内已见过的 `(device:nonce, ts)`，authorize 剪枝保持有界。
    ///
    /// **纯内存态，无持久化**：进程重启即清空全部历史。残余风险——若攻击者
    /// 截获过一份合法的 `(ts, nonce, tag)`，理论上可在响应方重启后、原始
    /// `ts` 仍落在 `LINK_AUTH_SKEW_SECS`（120s）容忍窗口内时重放一次。攻击
    /// 窗口极窄（需精确命中「重启后 120s 内」这个时机，且重启本身不频繁），
    /// 判定为可接受风险：加持久化的工程成本（落盘 + 跨重启一致性）远大于
    /// 收窄这个窗口带来的收益。这是权衡后的结论，不是遗漏。
    seen_nonces: Mutex<Vec<(String, i64)>>,
    /// 发现快照（发起方侧）：`start_discovery` 转发任务里按 [`upsert_discovered`]
    /// 去重更新；`start_discovery` 调用时清空；`probe` 的结果不入此快照。
    /// `Arc` 包裹以便转发任务（`tokio::spawn` 的 `'static` 闭包）持有写句柄。
    discovered: Arc<Mutex<Vec<DiscoveredPeer>>>,
    /// mDNS 广播给出、尚未经认证链路验证的回连候选（指纹 → 候选）。仅存内存，
    /// 只在拨号时排在已验证候选之后；认证请求经它成功后才由
    /// [`Self::promote_hinted`] 写入名册。每个已配对指纹至多一条，天然有界。
    hinted: Arc<Mutex<HashMap<String, PeerCandidate>>>,
    /// 手动探测（`/ping`）得到的设备指纹（`PeerAddress::base_url` → 指纹 + 记录时刻）。
    /// 与 [`Self::discovered`] 一起，是发起方配对时比对对端身份的“发现指纹”来源，见
    /// [`Self::known_fingerprints`]。带 TTL 与条数上限。
    probed: Mutex<HashMap<String, (String, std::time::Instant)>>,
}

impl LinkManager {
    /// 从存储加载（或首次生成并持久化）本机身份，构造门面。
    pub async fn load(
        storage: Arc<dyn LinkStorage>,
        self_info: SelfInfo,
        options: LinkOptions,
        events: mpsc::Sender<LinkEngineEvent>,
    ) -> LinkResult<Arc<Self>> {
        let identity = Self::load_or_create_identity(storage.as_ref()).await?;
        let client = wire::http_client();
        let responder = PairingResponder::new(identity.clone(), self_info.clone());
        let transport = TransportStack::direct_only(client.clone());
        let mgr = Arc::new(Self {
            identity,
            self_info,
            store: storage,
            responder,
            transport,
            client,
            api_port: AtomicU16::new(options.api_port),
            reachable: options.reachable,
            advertise: options.advertise,
            events,
            advertiser: Mutex::new(None),
            browser: Mutex::new(None),
            pending: Mutex::new(HashMap::new()),
            seen_nonces: Mutex::new(Vec::new()),
            discovered: Arc::new(Mutex::new(Vec::new())),
            hinted: Arc::new(Mutex::new(HashMap::new())),
            probed: Mutex::new(HashMap::new()),
        });
        Self::spawn_gc(&mgr);
        Ok(mgr)
    }

    /// 启动后台周期清理任务：每 60s 剪枝 `pending`/`seen_nonces`，并转发给
    /// 响应方剪枝其 `codes`/`sessions`（见
    /// [`super::pairing::PairingResponder::prune_expired`]）。此前这几张
    /// 状态表都只在「恰好又发生一次同类调用」时顺带清理，半途放弃的配对/
    /// 过期 nonce 记录会一直驻留到进程重启才被回收。
    ///
    /// 持 `Weak` 而非 `Arc`：循环体自身绝不能成为 `LinkManager` 存活的
    /// 理由——宿主释放最后一个 `Arc` 后，下一次 `upgrade()` 必然失败，循环
    /// 随之退出；否则这个 `'static` 后台任务会永久多攥一份 `Arc`，
    /// `LinkManager`（及其持有的 `advertiser`/`browser` 等资源）就再也
    /// 不会被析构。
    fn spawn_gc(mgr: &Arc<Self>) {
        let weak = Arc::downgrade(mgr);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                tick.tick().await;
                let Some(mgr) = weak.upgrade() else {
                    break;
                };
                mgr.prune_expired();
            }
        });
    }

    async fn load_or_create_identity(storage: &dyn LinkStorage) -> LinkResult<LinkIdentity> {
        if let Some(seed) = storage.load_identity_seed().await? {
            return Ok(LinkIdentity::from_secret_bytes(&seed));
        }
        let identity = LinkIdentity::generate();
        storage.save_identity_seed(&identity.secret_bytes()).await?;
        Ok(identity)
    }

    /// 本机设备指纹（设备 ID）。
    #[must_use]
    pub fn fingerprint(&self) -> &str {
        self.identity.fingerprint()
    }

    /// 本机展示名（供 `/ping` 透出）。
    #[must_use]
    pub fn self_name(&self) -> &str {
        &self.self_info.name
    }

    /// 更新实际 API 端口及已开启的 mDNS 广播；不改变可达范围、身份、配对或浏览状态。
    ///
    /// 广播更新失败时保留旧端口并恢复旧广告。未开始广播时只更新未来的自报候选与广播端口。
    pub fn update_api_port(&self, api_port: u16) -> LinkResult<()> {
        let advertiser = self.advertiser.lock().map_err(|_| LinkError::Unavailable)?;
        let old_port = self.api_port.load(Ordering::Relaxed);
        if let Some(advertiser) = advertiser.as_ref() {
            let update = |port| {
                advertiser.update_port(
                    port,
                    self.identity.fingerprint(),
                    &self.self_info.name,
                    self.self_info.platform.as_deref(),
                    self.self_info.app_version.as_deref(),
                )
            };
            if let Err(error) = update(api_port) {
                // register 的通知 socket 失败时，命令可能已经排队；必须显式恢复旧广告。
                update(old_port).map_err(|rollback| {
                    LinkError::Io(format!(
                        "mDNS port update failed: {error}; restoring previous port failed: {rollback}"
                    ))
                })?;
                return Err(error);
            }
        }
        self.api_port.store(api_port, Ordering::Relaxed);
        Ok(())
    }

    /// 本机平台（供 `/ping` 透出）。
    #[must_use]
    pub fn self_platform(&self) -> Option<&str> {
        self.self_info.platform.as_deref()
    }

    // ── 响应方（被添加设备侧）─────────────────────────────────────────────

    /// 生成一次性配对码（在被添加设备 UI 展示），并确保 mDNS 广播已开启。
    pub fn generate_code(&self) -> String {
        self.ensure_advertising();
        self.responder.generate_code()
    }

    /// 处理入站 `hello`（wire 形式，base64 编解码全在引擎内完成，宿主纯字段搬运）。
    /// `source`：请求来源地址，直接透传给 [`PairingResponder::handle_hello`] 做按来源
    /// 分桶节流；拿不到时传 `None`。
    ///
    /// 这一步只回出本次会话的临时公钥与随机数：响应方此刻还看不到发起方的临时公钥，
    /// 没有 SAS，也不会广播 `IncomingPairing`——见 [`Self::pair_reveal_wire`]。
    pub fn pair_hello_wire(
        &self,
        w: WireHello,
        source: Option<IpAddr>,
    ) -> LinkResult<WireHelloResponse> {
        // 版本门禁先于字段解码：旧版发起方的 hello 没有 `initiatorCommit` 等新字段，
        // 先解码只会得到含糊的「载荷非法」，而不是明确的版本不兼容。
        if w.protocol_version != PAIRING_PROTOCOL_VERSION {
            return Err(LinkError::UnsupportedVersion);
        }
        let req = HelloRequest {
            protocol_version: w.protocol_version,
            code: w.code,
            initiator_id_pub: decode_b64_array::<32>(&w.initiator_id_pub)?,
            initiator_commit: decode_b64_array::<32>(&w.initiator_commit)?,
            initiator_sig: decode_b64_array::<64>(&w.initiator_sig)?,
            name: w.name,
            platform: w.platform,
            app_version: w.app_version,
            initiator_addrs: w.initiator_addrs,
        };
        let resp = self.responder.handle_hello(&req, source)?;
        Ok(WireHelloResponse {
            protocol_version: resp.protocol_version,
            session_id: resp.session_id,
            responder_eph_pub: B64.encode(resp.responder_eph_pub),
            responder_nonce: B64.encode(resp.responder_nonce),
            responder_id_pub: B64.encode(resp.responder_id_pub),
            name: resp.name,
            platform: resp.platform,
            app_version: resp.app_version,
        })
    }

    /// 处理入站 `reveal`（wire 形式）：核对承诺、算出 SAS，随后向宿主广播
    /// `IncomingPairing`（本机用户从这一刻起核对 SAS），并把响应方对完整转录的签名回给
    /// 发起方。回复里不含 SAS——SAS 只在两端各自的屏幕上展示。
    pub fn pair_reveal_wire(&self, w: WireReveal) -> LinkResult<WireRevealResponse> {
        let req = RevealRequest {
            session_id: w.session_id,
            initiator_eph_pub: decode_b64_array::<32>(&w.initiator_eph_pub)?,
            initiator_nonce: decode_b64_array::<32>(&w.initiator_nonce)?,
        };
        let accepted = self.responder.handle_reveal(&req)?;
        self.emit_incoming_pairing(
            req.session_id,
            accepted.sas,
            accepted.peer_name,
            accepted.peer_platform,
            &accepted.peer_id_pub,
        );
        Ok(WireRevealResponse {
            responder_sig: B64.encode(accepted.response.responder_sig),
        })
    }

    /// 处理入站 `confirm`：本机用户批准则把发起方入册并广播 `Paired` 事件。
    ///
    /// 返回 [`PairConfirmOutcome`] 而非 `bool`：本机用户**拒绝**与**核验超时**都是
    /// 协议的正常终局（不是服务端错误），必须能被发起方区分。此前它们经 `Err` 冒泡成
    /// HTTP 400，发起方的 [`Self::confirm_pairing`] 见非 2xx 一律映射成
    /// `SessionExpired`，用户看到的是「会话过期」而不是「对方拒绝了配对」。
    pub async fn pair_confirm(
        &self,
        session_id: &str,
        confirm: bool,
    ) -> LinkResult<PairConfirmOutcome> {
        match self.responder.handle_confirm(session_id, confirm).await {
            Ok(Some(record)) => {
                self.store.upsert(&record).await?;
                if self
                    .events
                    .send(LinkEngineEvent::Paired(record))
                    .await
                    .is_err()
                {
                    tracing::debug!("link event receiver closed after pairing");
                }
                Ok(PairConfirmOutcome::Paired)
            }
            // 发起方自己传了 confirm=false —— 它当然知道自己拒绝了，无需额外语义。
            Ok(None) => {
                // 发起方放弃/拒绝：响应方会话已被删除，通知宿主关闭待确认弹窗。
                if self
                    .events
                    .send(LinkEngineEvent::IncomingCancelled {
                        session_id: session_id.to_string(),
                    })
                    .await
                    .is_err()
                {
                    tracing::debug!("link event receiver closed after pairing cancellation");
                }
                Ok(PairConfirmOutcome::Declined)
            }
            Err(LinkError::RejectedByPeer) => Ok(PairConfirmOutcome::Rejected),
            Err(LinkError::PairingTimeout) => Ok(PairConfirmOutcome::TimedOut),
            Err(e) => Err(e),
        }
    }

    /// 批准/拒绝一次入站配对请求（本机用户核对 SAS 后调用）。转发到响应方
    /// 的本地决策记录，唤醒 [`super::pairing::PairingResponder::handle_confirm`]
    /// 里等待用户核验、且已收到发起方 `confirm=true` 的那次 HTTP 请求。
    pub fn approve_incoming(&self, session_id: &str, accept: bool) -> LinkResult<()> {
        self.responder.set_local_decision(session_id, accept)
    }

    /// 后台广播「有入站配对待核对」事件。`events` 是 async `mpsc::Sender`，
    /// 本方法的调用方 `pair_reveal_wire` 是同步 fn，沿用
    /// [`Self::ensure_advertising`] 已有的 `tokio::spawn` 写法把发送挪到
    /// 后台，不阻塞 reveal 的返回路径。
    fn emit_incoming_pairing(
        &self,
        session_id: String,
        sas: String,
        peer_name: String,
        peer_platform: Option<String>,
        peer_id_pub: &[u8; 32],
    ) {
        let tx = self.events.clone();
        let event = LinkEngineEvent::IncomingPairing {
            session_id,
            sas,
            peer_name,
            peer_platform,
            peer_fingerprint: super::crypto::fingerprint(peer_id_pub),
        };
        tokio::spawn(async move {
            if tx.send(event).await.is_err() {
                tracing::debug!("link event receiver closed before incoming pairing notification");
            }
        });
    }

    /// 确保 mDNS 广播运行（幂等）。本机 API 局域网不可达（[`LinkOptions::reachable`]
    /// 为 `false`）时不广播。失败仅记 Error 事件，不阻断配对（手动地址可兜底）。
    fn ensure_advertising(&self) {
        if !self.reachable || !self.advertise {
            return;
        }
        let mut guard = match self.advertiser.lock() {
            Ok(g) => g,
            Err(_) => return,
        };
        if guard.is_some() {
            return;
        }
        match MdnsAdvertiser::start(
            self.api_port.load(Ordering::Relaxed),
            self.identity.fingerprint(),
            &self.self_info.name,
            self.self_info.platform.as_deref(),
            self.self_info.app_version.as_deref(),
        ) {
            Ok(a) => *guard = Some(a),
            Err(e) => {
                let tx = self.events.clone();
                let msg = e.to_string();
                tokio::spawn(async move {
                    if tx.send(LinkEngineEvent::Error(msg)).await.is_err() {
                        tracing::debug!(
                            "link event receiver closed before advertising failure notification"
                        );
                    }
                });
            }
        }
    }

    /// 主动开启 mDNS 广播（宿主在启用本地互联时调用）。
    pub fn start_advertising(&self) {
        self.ensure_advertising();
    }

    /// 停止 mDNS 广播（配对码过期 / 用户关闭配对界面时调用）。与
    /// [`Self::stop_discovery`] 对称：持锁置 `None`，[`MdnsAdvertiser`] 的
    /// `Drop` 负责实际 shutdown；此前只有 `ensure_advertising`/
    /// `start_advertising`，广播一旦开启就持续到进程退出，本机指纹/设备名
    /// 一直暴露在局域网上，即使用户已经关掉了配对界面。
    pub fn stop_advertising(&self) {
        if let Ok(mut guard) = self.advertiser.lock() {
            *guard = None; // Drop → daemon.shutdown()
        }
    }

    /// 停止配对：立即作废当前配对码并停止 mDNS 广播。
    pub fn stop_pairing(&self) {
        self.responder.revoke_codes();
        self.stop_advertising();
    }
    // ── 发现（发起方侧）───────────────────────────────────────────────────

    /// 开始 mDNS 浏览：发现的设备经事件通道以 `Discovered` 汇出，并同步去重
    /// 更新发现快照。幂等——重复调用不重启浏览，但仍清空发现快照，与管理面
    /// `POST /link/discovery {"action":"start"}` 的语义对齐。
    /// 命中已配对指纹的广播不入快照/不发事件，而是刷新该设备的 Direct 候选
    /// 地址（见下）。
    pub fn start_discovery(&self) -> LinkResult<()> {
        if let Ok(mut snapshot) = self.discovered.lock() {
            snapshot.clear();
        }
        let mut guard = self.browser.lock().map_err(|_| LinkError::Unavailable)?;
        if guard.is_some() {
            return Ok(());
        }
        let (tx, mut rx) = mpsc::channel::<DiscoveredPeer>(64);
        let out = self.events.clone();
        let hinted = Arc::clone(&self.hinted);
        let self_fp = self.identity.fingerprint().to_string();
        let discovered = Arc::clone(&self.discovered);
        let store = self.store.clone();
        tokio::spawn(async move {
            while let Some(peer) = rx.recv().await {
                // 过滤掉本机自身的广播。
                if peer.fingerprint.as_deref() == Some(self_fp.as_str()) {
                    continue;
                }
                // 命中已配对名册：多半是设备 DHCP 换了 IP 后重新广播——刷新
                // 其 Direct 候选地址，不当作「可添加的新设备」推进 discovered
                // 快照/事件通道（否则已配对设备会污染「发现列表」UI）。
                if let Some(fp) = peer.fingerprint.as_deref()
                    && let Ok(Some(record)) = store.get(fp).await
                {
                    let Ok(address) = PeerAddress::from_host_port(&peer.host, peer.port) else {
                        continue;
                    };
                    let fresh = PeerCandidate {
                        kind: TransportKind::Direct,
                        address: address.to_candidate(),
                    };
                    // mDNS 通告的指纹是公开值，地址未经任何认证：不写库、不挤占
                    // 已验证候选，只作为内存中的待验证线索；认证请求经它成功后
                    // 才持久化。已知地址/重复线索直接跳过。
                    if record.candidates.contains(&fresh) {
                        continue;
                    }
                    if let Ok(mut map) = hinted.lock() {
                        map.insert(fp.to_string(), fresh);
                    }
                    continue;
                }
                if let Ok(mut snapshot) = discovered.lock() {
                    upsert_discovered(&mut snapshot, peer.clone());
                }
                if out.send(LinkEngineEvent::Discovered(peer)).await.is_err() {
                    break;
                }
            }
        });
        *guard = Some(MdnsBrowser::start(tx)?);
        Ok(())
    }

    /// 停止 mDNS 浏览。
    pub fn stop_discovery(&self) {
        if let Ok(mut guard) = self.browser.lock() {
            *guard = None; // Drop → daemon.shutdown()
        }
    }

    /// 当前发现快照（发起方侧 UI 轮询用）；`start_discovery` 时清空，浏览期间
    /// 持续去重更新。
    #[must_use]
    pub fn discovered_peers(&self) -> Vec<DiscoveredPeer> {
        self.discovered
            .lock()
            .map(|g| g.clone())
            .unwrap_or_default()
    }

    /// 手动地址探测（mDNS 失效兜底）：`/ping` 一台设备，返回其信息（不配对）。探测到的
    /// 设备指纹会被记住一小段时间，随后对同一地址发起配对时用来校验对端身份。
    pub async fn probe(&self, address: &PeerAddress) -> LinkResult<DiscoveredPeer> {
        let peer = discovery::probe(&self.client, address).await?;
        if let Some(fingerprint) = &peer.fingerprint {
            self.remember_probe(address, fingerprint);
        }
        Ok(peer)
    }

    /// 记住一次探测得到的指纹；先剪掉过期项，满员时淘汰最旧的一条。
    fn remember_probe(&self, address: &PeerAddress, fingerprint: &str) {
        let Ok(mut probed) = self.probed.lock() else {
            return;
        };
        probed.retain(|_, (_, at)| at.elapsed() < PROBED_FINGERPRINT_TTL);
        let key = address.base_url();
        if probed.len() >= MAX_PROBED_FINGERPRINTS
            && !probed.contains_key(&key)
            && let Some(oldest) = probed
                .iter()
                .min_by_key(|(_, (_, at))| *at)
                .map(|(k, _)| k.clone())
        {
            probed.remove(&oldest);
        }
        probed.insert(key, (fingerprint.to_string(), std::time::Instant::now()));
    }

    /// 发起方已知的、`address` 上设备的指纹：mDNS 发现快照里落在该地址的条目，加上最近
    /// 一次对该地址手动探测得到的指纹。配对握手里响应方出示的身份必须命中其中之一；
    /// 没有任何发现记录（直接输入地址配对）时返回空，不做这项比对。
    fn known_fingerprints(&self, address: &PeerAddress) -> Vec<String> {
        let base = address.base_url();
        let mut known = Vec::new();
        if let Ok(probed) = self.probed.lock()
            && let Some((fingerprint, at)) = probed.get(&base)
            && at.elapsed() < PROBED_FINGERPRINT_TTL
        {
            known.push(fingerprint.clone());
        }
        if let Ok(snapshot) = self.discovered.lock() {
            known.extend(snapshot.iter().filter_map(|peer| {
                let fingerprint = peer.fingerprint.as_ref()?;
                let peer_address = PeerAddress::from_host_port(&peer.host, peer.port).ok()?;
                (peer_address.base_url() == base).then(|| fingerprint.clone())
            }));
        }
        known
    }

    // ── 配对（发起方侧）───────────────────────────────────────────────────

    /// 发起配对：向 `address` 依次发送 `hello`（带配对码与临时公钥承诺）和 `reveal`
    /// （揭示临时公钥），返回 `(token, sas, 对端名)`。UI 展示 SAS 供用户与对端核对，
    /// 随后调 [`confirm_pairing`]。
    ///
    /// 发起方已知该地址上设备的发现指纹（mDNS 快照或最近一次手动探测）时，对端出示的
    /// 身份必须与之一致，否则 [`LinkError::IdentityMismatch`]——此时还没有揭示临时
    /// 公钥。对端配对协议版本不符（含旧版对端）→ [`LinkError::UnsupportedVersion`]。
    ///
    /// 错误语义（见 [`crate::wire`]）：网络失败 → [`LinkError::Io`]；对端（或其反代）返回
    /// 非 FluxDown 的 4xx / 重定向 / HTML → [`LinkError::NotFluxDown`]；只有对端 FluxDown
    /// 明确回了「配对码无效」才是 [`LinkError::InvalidCode`]。
    pub async fn begin_pairing(
        &self,
        address: &PeerAddress,
        code: &str,
    ) -> LinkResult<BeginPairingResult> {
        let mut initiator = PairingInitiator::new(self.identity.clone());
        let addrs = self.self_addrs_towards(address).await;
        let hello = initiator.build_hello(code, &self.self_info, addrs);

        let body = serde_json::json!({
            "protocolVersion": hello.protocol_version,
            "code": hello.code,
            "initiatorIdPub": B64.encode(hello.initiator_id_pub),
            "initiatorCommit": B64.encode(hello.initiator_commit),
            "initiatorSig": B64.encode(hello.initiator_sig),
            "name": hello.name,
            "platform": hello.platform.clone().unwrap_or_default(),
            "appVersion": hello.app_version.clone().unwrap_or_default(),
            "initiatorAddrs": hello.initiator_addrs,
        });
        let json = self
            .post_pairing_step(address, "/api/v1/link/pair/hello", &body)
            .await
            .map_err(hello_rejection)?;
        let hello_resp = parse_hello_response(&json)?;

        let responder_addr = address.to_candidate();
        let known = self.known_fingerprints(address);
        let reveal = initiator.on_hello_response(&hello_resp, &responder_addr, &known)?;

        let body = serde_json::json!({
            "sessionId": reveal.session_id,
            "initiatorEphPub": B64.encode(reveal.initiator_eph_pub),
            "initiatorNonce": B64.encode(reveal.initiator_nonce),
        });
        let json = self
            .post_pairing_step(address, "/api/v1/link/pair/reveal", &body)
            .await?;
        let reveal_resp = parse_reveal_response(&json)?;
        let sas = initiator.on_reveal_response(&reveal_resp)?;

        let token = uuid::Uuid::new_v4().simple().to_string();
        let peer_name = hello_resp.name.clone();
        if let Ok(mut pending) = self.pending.lock() {
            // 剪枝：丢弃用户已放弃、超过会话时窗的待确认项，避免无界增长 + 及时释放临时密钥。
            pending.retain(|_, p| p.created.elapsed() < PENDING_INIT_TTL);
            // 剪枝后仍达到硬上限（短时间内连续发起多次配对但都未确认）——
            // 拒绝新请求而非无界增长；正常使用不可能同时挂 32 个待确认配对。
            if pending.len() >= 32 {
                return Err(LinkError::Unavailable);
            }
            pending.insert(
                token.clone(),
                PendingInit {
                    initiator,
                    session_id: hello_resp.session_id.clone(),
                    peer: address.clone(),
                    created: std::time::Instant::now(),
                },
            );
        }
        Ok(BeginPairingResult {
            token,
            sas,
            peer_name,
            peer_fingerprint: super::crypto::fingerprint(&hello_resp.responder_id_pub),
        })
    }

    /// 向对端的配对端点 POST 一步握手消息（`hello` / `reveal`），返回成功响应的 JSON
    /// 对象；失败按 [`crate::wire`] 的规则分类。
    async fn post_pairing_step(
        &self,
        address: &PeerAddress,
        path: &str,
        body: &serde_json::Value,
    ) -> LinkResult<serde_json::Value> {
        let resp = self
            .client
            .post(address.url(path))
            .json(body)
            .timeout(std::time::Duration::from_secs(8))
            .send()
            .await
            .map_err(|e| send_failure(&e))?;
        if !resp.status().is_success() {
            return Err(classify_failure(resp).await);
        }
        read_json_object(resp).await
    }

    /// 本机朝向 `peer` 的可回连地址（`initiatorAddrs`，供对端存为回连候选）。本机 API
    /// 局域网不可达、或对端是 https 站点（多半是公网反代，回连无意义）时为空。
    async fn self_addrs_towards(&self, peer: &PeerAddress) -> Vec<String> {
        if !self.reachable || peer.is_https() {
            return Vec::new();
        }
        let host = peer.host();
        let ip_text = if host.parse::<IpAddr>().is_ok() {
            host.to_string()
        } else {
            // 域名先解析成 IP（下面的出站网卡探测需要 IP）。
            match tokio::net::lookup_host((host, peer.port())).await {
                Ok(mut resolved) => resolved
                    .next()
                    .map(|addr| addr.ip().to_string())
                    .unwrap_or_default(),
                Err(_) => String::new(),
            }
        };
        discovery::local_direct_addrs(&ip_text, self.api_port.load(Ordering::Relaxed))
    }

    /// SAS 核对后确认/拒绝配对。`accept=true` 且对端确认成功 → 落库 + 广播 Paired。
    pub async fn confirm_pairing(
        &self,
        token: &str,
        accept: bool,
    ) -> LinkResult<Option<PeerRecord>> {
        let pending = {
            let mut guard = self.pending.lock().map_err(|_| LinkError::Unavailable)?;
            guard.remove(token)
        };
        let Some(pending) = pending else {
            return Err(LinkError::SessionExpired);
        };
        // 本地也校验过期：此前只靠对端 SESSION_TTL 兜底，双端各自维护一套
        // 过期逻辑，未来任一端常量独立调整就会行为不一致。本机用户迟迟不
        // 核验 SAS 时在这里直接短路，不必再发起一次注定失败、还要等待
        // 下方最多 70s 超时的网络往返。
        if pending.created.elapsed() > PENDING_INIT_TTL {
            return Err(LinkError::SessionExpired);
        }
        let body = serde_json::json!({ "sessionId": pending.session_id, "confirm": accept });
        let resp = self
            .client
            .post(pending.peer.url("/api/v1/link/pair/confirm"))
            .json(&body)
            // 对端响应方现在要等本机用户在 confirm 阶段核验 SAS 并点击批准/
            // 拒绝（PairingResponder::handle_confirm 等待用户决策，上限
            // 60s），因此这里的超时必须盖过那 60s 决策窗口再留出网络往返
            // 余量，否则发起方会在对端用户点确认前就先行超时掉线。
            .timeout(std::time::Duration::from_secs(70))
            .send()
            .await
            .map_err(|e| send_failure(&e))?;
        if !resp.status().is_success() {
            return Err(classify_failure(resp).await);
        }
        if !accept {
            return Ok(None);
        }
        let json = read_json_object(resp).await?;
        // 对端 confirm 响应体 `{"success":true,"paired":<bool>,"reason":<"rejected"|
        // "timeout"|null>}`。对端用户拒绝与核验超时都是**协议正常终局**，对端以 2xx +
        // `paired=false` 返回（而非 4xx），否则这里的非 2xx 分支会把它们一律压成
        // SessionExpired，用户看到的是「会话过期」而不是「对方拒绝了配对」。
        let paired = json
            .get("paired")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if !paired {
            let reason = json.get("reason").and_then(|v| v.as_str()).unwrap_or("");
            return Err(match reason {
                "timeout" => LinkError::PairingTimeout,
                _ => LinkError::RejectedByPeer,
            });
        }
        let record = pending.initiator.finalize()?;
        self.store.upsert(&record).await?;
        if self
            .events
            .send(LinkEngineEvent::Paired(record.clone()))
            .await
            .is_err()
        {
            tracing::debug!("link event receiver closed after pairing");
        }
        Ok(Some(record))
    }

    // ── 名册 ──────────────────────────────────────────────────────────────

    /// 全部已配对设备。
    pub async fn list_devices(&self) -> LinkResult<Vec<PeerRecord>> {
        self.store.list().await
    }

    /// 按指纹读取单台已配对设备。
    pub async fn get_device(&self, fingerprint: &str) -> LinkResult<Option<PeerRecord>> {
        self.store.get(fingerprint).await
    }

    /// 解除配对（删除设备），广播 Unpaired。
    pub async fn remove_device(&self, fingerprint: &str) -> LinkResult<bool> {
        let removed = self.store.remove(fingerprint).await?;
        if removed
            && self
                .events
                .send(LinkEngineEvent::Unpaired(fingerprint.to_string()))
                .await
                .is_err()
        {
            tracing::debug!("link event receiver closed after unpairing");
        }
        Ok(removed)
    }

    /// 把 mDNS 待验证候选追加到名册候选之后（仅用于本次拨号，不持久化、不越过
    /// 已验证候选）。
    fn with_hinted(&self, mut record: PeerRecord) -> PeerRecord {
        if let Ok(map) = self.hinted.lock()
            && let Some(hint) = map.get(&record.fingerprint)
            && !record.candidates.contains(hint)
        {
            record.candidates.push(hint.clone());
        }
        record
    }

    /// 认证请求（链路密钥 HMAC/AEAD 往返成功）经 `base_url` 完成后，若该地址正是
    /// 待验证候选，则将其置首并持久化；否则不动名册。
    async fn promote_hinted(&self, fingerprint: &str, base_url: &str) {
        let hint = match self.hinted.lock() {
            Ok(map) => map.get(fingerprint).cloned(),
            Err(_) => None,
        };
        let Some(hint) = hint else {
            return;
        };
        let matches = PeerAddress::parse(&hint.address)
            .map(|a| a.base_url() == base_url)
            .unwrap_or(false);
        if !matches {
            return;
        }
        let record = match self.store.get(fingerprint).await {
            Ok(Some(record)) => record,
            Ok(None) => return,
            Err(error) => {
                tracing::warn!(fingerprint, %error, "cannot load linked peer for candidate promotion");
                return;
            }
        };
        let candidates = promoted_candidates(&record.candidates, hint);
        if candidates != record.candidates
            && let Err(error) = self.store.update_candidates(fingerprint, &candidates).await
        {
            tracing::warn!(fingerprint, %error, "cannot persist verified linked peer candidate");
            return;
        }
        if let Ok(mut map) = self.hinted.lock()
            && map.get(fingerprint) == candidates.first()
        {
            map.remove(fingerprint);
        }
    }

    /// 探测一台已配对设备是否在线（走传输栈拨号），成功则刷新 last_seen。
    pub async fn is_online(&self, fingerprint: &str) -> bool {
        let Ok(Some(record)) = self.store.get(fingerprint).await else {
            return false;
        };
        let record = self.with_hinted(record);
        match self.transport.connect(&record).await {
            Ok(_) => {
                if let Err(error) = self.store.touch(fingerprint, now_unix()).await {
                    tracing::warn!(fingerprint, %error, "cannot persist linked peer last_seen");
                }
                true
            }
            Err(LinkError::Unreachable) => false,
            Err(error) => {
                // 身份不符等：不是「暂时离线」，需要留下线索。
                tracing::warn!(fingerprint, error = %error, "linked device probe rejected");
                false
            }
        }
    }

    // ── 数据面 ────────────────────────────────────────────────────────────

    /// 把一个下载任务下发给已配对设备（发起方数据面）。走传输栈解析可达
    /// base_url，请求体先加密（[`derive_link_aead_key`] + [`seal_link_body`]）
    /// 再用每对独立链路密钥对**密文**做 HMAC 鉴权（encrypt-then-MAC），POST
    /// 对端 `/api/v1/link/tasks`。返回新任务 ID。
    pub async fn dispatch(
        &self,
        fingerprint: &str,
        url: &str,
        save_dir: Option<&str>,
        file_name: Option<&str>,
    ) -> LinkResult<String> {
        let record = self
            .store
            .get(fingerprint)
            .await?
            .ok_or(LinkError::NotPaired)?;
        let record = self.with_hinted(record);
        let conn = self.transport.connect(&record).await?;
        // 明文序列化**一次**，加密**一次**——同一份密文字节既用于 HMAC 也
        // 用于发送，保证签名覆盖的字节与对端收到并校验的字节完全一致
        // （Option 空值序列化为 ""，非 null，否则响应方 `LinkTaskRequest`
        // (非 Option String) 反序列化会 400）。
        let body_json = serde_json::json!({
            "url": url,
            "saveDir": save_dir.unwrap_or_default(),
            "fileName": file_name.unwrap_or_default(),
        });
        let plaintext = serde_json::to_vec(&body_json).unwrap_or_default();
        let resp = self
            .post_sealed(
                &record,
                &conn.base_url,
                LINK_TASKS_PATH,
                &plaintext,
                std::time::Duration::from_secs(15),
            )
            .await?;
        if !resp.status().is_success() {
            return Err(classify_failure(resp).await);
        }
        let json = read_json_object(resp).await?;
        let task_id = json
            .get("taskId")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| LinkError::Io("missing taskId in dispatch response".into()))?
            .to_string();
        self.promote_hinted(fingerprint, &conn.base_url).await;
        if let Err(error) = self.store.touch(fingerprint, now_unix()).await {
            tracing::warn!(fingerprint, %error, "cannot persist linked peer last_seen");
        }
        Ok(task_id)
    }

    /// 数据面出站请求：对明文 AEAD 加密（[`derive_link_aead_key`] + [`seal_link_body`]），
    /// 再用每对独立链路密钥对**密文**做 HMAC 鉴权（encrypt-then-MAC），POST 到
    /// `{base_url}{path}`。对端必须按同一顺序校验（先验 HMAC 再解密），否则攻击者能在
    /// 密文没被认证前就篡改，让对端白白解密一次。
    async fn post_sealed(
        &self,
        record: &PeerRecord,
        base_url: &str,
        path: &str,
        plaintext: &[u8],
        timeout: std::time::Duration,
    ) -> LinkResult<reqwest::Response> {
        let ts = now_unix();
        let nonce = uuid::Uuid::new_v4().simple().to_string();
        let aead_key = derive_link_aead_key(&record.link_secret);
        let sealed = seal_link_body(&aead_key, plaintext);
        let tag = link_auth_tag(&record.link_secret, "POST", path, ts, &nonce, &sealed);
        self.client
            .post(format!("{base_url}{path}"))
            .header("X-FluxLink-Device", self.identity.fingerprint())
            .header("X-FluxLink-Ts", ts.to_string())
            .header("X-FluxLink-Nonce", nonce)
            .header("X-FluxLink-Auth", tag)
            .header("X-FluxLink-Enc", "v1")
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .body(sealed)
            .timeout(timeout)
            .send()
            .await
            .map_err(|e| send_failure(&e))
    }

    /// 经已认证链路与对端**交换设备信息**（默认下载目录 / 路径风格）：本机信息 `mine`
    /// 随请求发给对端（对端据此更新它名册里的本机信息），对端信息随响应回来并写入本机名册。
    ///
    /// 请求与响应体都是 AEAD 密文：对端信息无法被局域网嗅探，也无法被中间人伪造。
    /// 对端不认识该端点（旧版 Flutter hub / 旧 server）→ `Ok(None)`，调用方按「未知」处理。
    pub async fn exchange_peer_info(
        &self,
        fingerprint: &str,
        mine: &PeerInfo,
    ) -> LinkResult<Option<PeerInfo>> {
        let record = self
            .store
            .get(fingerprint)
            .await?
            .ok_or(LinkError::NotPaired)?;
        let record = self.with_hinted(record);
        let conn = self.transport.connect(&record).await?;
        let resp = self
            .post_sealed(
                &record,
                &conn.base_url,
                LINK_INFO_PATH,
                &encode_peer_info(mine),
                std::time::Duration::from_secs(8),
            )
            .await?;
        let status = resp.status();
        if matches!(status.as_u16(), 404 | 405 | 501) {
            return Ok(None);
        }
        if !status.is_success() {
            return Err(classify_failure(resp).await);
        }
        let sealed = resp.bytes().await.map_err(|e| send_failure(&e))?;
        let aead_key = derive_link_aead_key(&record.link_secret);
        let Some(info) =
            open_link_body(&aead_key, &sealed).and_then(|plaintext| decode_peer_info(&plaintext))
        else {
            // 不是本链路密钥加密的合法信息包（对端是别的服务 / 旧版本兜底页）。
            tracing::debug!(fingerprint, "peer info response could not be opened");
            return Ok(None);
        };
        self.promote_hinted(fingerprint, &conn.base_url).await;
        self.store.set_peer_info(fingerprint, &info).await?;
        if let Err(error) = self.store.touch(fingerprint, now_unix()).await {
            tracing::warn!(fingerprint, %error, "cannot persist linked peer last_seen");
        }
        Ok(Some(info))
    }

    /// 响应端处理已通过 [`Self::authorize`] 的 `POST /api/v1/link/info`：记下请求方自报信息，
    /// 返回用链路密钥加密的本机信息 `mine`（响应体，`application/octet-stream`）。
    pub async fn answer_peer_info(
        &self,
        request: &LinkRequest,
        mine: &PeerInfo,
    ) -> LinkResult<Vec<u8>> {
        let record = self
            .store
            .get(&request.device)
            .await?
            .ok_or(LinkError::Unauthorized)?;
        if let Some(theirs) = decode_peer_info(&request.body) {
            self.store.set_peer_info(&request.device, &theirs).await?;
        }
        let aead_key = derive_link_aead_key(&record.link_secret);
        Ok(seal_link_body(&aead_key, &encode_peer_info(mine)))
    }

    /// 校验入站链路数据面请求：时间戳时窗 → 取设备记录 → 验 HMAC（覆盖
    /// 密文摘要，encrypt-then-MAC）→ nonce 防重放 → 解密。成功返回发起方
    /// 指纹 + **已解密**的明文 body（见 [`LinkRequest`]）。
    ///
    /// `enc` 是 `X-FluxLink-Enc` 请求头的原始值：数据面 body 恒为密文，
    /// 双端同版本发布、不兼容明文旧客户端——缺这个头或头值不是 `"v1"` 一律
    /// 当鉴权失败拒绝，不留明文回退路径（回退路径等于给降级攻击开门）。
    #[allow(clippy::too_many_arguments)]
    pub async fn authorize(
        &self,
        method: &str,
        path: &str,
        device_fp: &str,
        ts: i64,
        nonce: &str,
        body: &[u8],
        tag: &str,
        enc: &str,
    ) -> LinkResult<LinkRequest> {
        if enc != "v1" {
            return Err(LinkError::Unauthorized);
        }
        let now = now_unix();
        // i128 比较，防攻击者构造的极端 ts 触发 i64 溢出（debug 下 panic）。
        if (now as i128 - ts as i128).abs() > LINK_AUTH_SKEW_SECS as i128 {
            return Err(LinkError::Unauthorized);
        }
        let record = self
            .store
            .get(device_fp)
            .await?
            .ok_or(LinkError::Unauthorized)?;
        // encrypt-then-MAC：`body` 此刻仍是密文，先验 HMAC（覆盖密文摘要）
        // ——与发起方 [`Self::dispatch`] 对同一份密文字节算 tag 的顺序一致，
        // 签名覆盖的字节与此处校验的字节完全相同。
        if !verify_link_auth_tag(&record.link_secret, method, path, ts, nonce, body, tag) {
            return Err(LinkError::Unauthorized);
        }
        // 防重放：同 (device, nonce) 在时窗内仅接受一次；顺带按时窗剪枝保持有界。
        {
            let mut seen = self
                .seen_nonces
                .lock()
                .map_err(|_| LinkError::Unavailable)?;
            seen.retain(|(_, seen_ts)| now - *seen_ts <= LINK_AUTH_SKEW_SECS);
            let key = format!("{device_fp}:{nonce}");
            if seen.iter().any(|(k, _)| *k == key) {
                return Err(LinkError::Unauthorized);
            }
            seen.push((key, now));
        }
        // HMAC 通过后才解密：密文完整性此刻已由 HMAC 保证，解密在正常流程
        // 下不会失败（同一 `record.link_secret` 派生的 AEAD 密钥）；仍显式
        // 处理——解密失败统一按鉴权失败处理，不额外泄露「HMAC 过但解密
        // 失败」这种细分信息。
        let aead_key = derive_link_aead_key(&record.link_secret);
        let plaintext = open_link_body(&aead_key, body).ok_or(LinkError::Unauthorized)?;
        Ok(LinkRequest {
            device: device_fp.to_string(),
            body: plaintext,
        })
    }

    /// 剪枝全部「只靠恰好又发生一次同类调用才顺带清理」的过期状态：
    /// `pending`（本机发起、SAS 核对/confirm 未完成的待确认会话）、
    /// `seen_nonces`（数据面防重放时窗）、`probed`（手动探测指纹），并转发给响应方
    /// 剪枝其 `codes`/`sessions`。由 [`Self::spawn_gc`] 每 60s 调用一次；半途
    /// 放弃的配对与过期 nonce 记录此前会一直驻留到进程重启才被回收。
    pub fn prune_expired(&self) {
        if let Ok(mut pending) = self.pending.lock() {
            pending.retain(|_, p| p.created.elapsed() < PENDING_INIT_TTL);
        }
        if let Ok(mut seen) = self.seen_nonces.lock() {
            let now = now_unix();
            seen.retain(|(_, seen_ts)| now - *seen_ts <= LINK_AUTH_SKEW_SECS);
        }
        if let Ok(mut probed) = self.probed.lock() {
            probed.retain(|_, (_, at)| at.elapsed() < PROBED_FINGERPRINT_TTL);
        }
        self.responder.prune_expired();
    }
}

/// [`LinkManager::begin_pairing`] 的结果：待确认令牌 + 供核对的 SAS + 对端信息。
#[derive(Debug, Clone)]
pub struct BeginPairingResult {
    pub token: String,
    pub sas: String,
    pub peer_name: String,
    pub peer_fingerprint: String,
}

/// [`LinkManager::authorize`] 成功后的产出：通过链路鉴权的一次入站数据面
/// 请求——发起方指纹 + **已解密**的明文 body。此前 `authorize` 只返回发起
/// 方指纹（`LinkResult<String>`），宿主拿到指纹后仍用自己手里的原始请求
/// 体字节反序列化；数据面加密后那份原始字节是密文，宿主必须改用这里
/// 返回的解密结果——把「鉴权通过」与「拿到明文」在返回值里绑在一起，
/// 杜绝调用方漏改、继续拿密文当明文解析的错误用法。
#[derive(Debug, Clone)]
pub struct LinkRequest {
    /// 发起方设备指纹。
    pub device: String,
    /// 已解密的明文请求体。
    pub body: Vec<u8>,
}

fn b64_to_array<const N: usize>(json: &serde_json::Value, key: &str) -> LinkResult<[u8; N]> {
    let s = json
        .get(key)
        .and_then(|v| v.as_str())
        .ok_or_else(|| LinkError::BadPayload(format!("missing {key}")))?;
    let bytes = B64
        .decode(s)
        .map_err(|_| LinkError::BadPayload(format!("bad base64 {key}")))?;
    <[u8; N]>::try_from(bytes.as_slice())
        .map_err(|_| LinkError::BadPayload(format!("bad length {key}")))
}

/// 旧版响应方把本端按新协议构造的 `hello` 当成无法解析的载荷拒绝时返回的 message 前缀
/// （`invalid link hello payload: …`，由 API 层的 hello 处理器生成）。本端发出的 hello
/// 恒为良构，所以这类拒绝只可能意味着对端不认识新协议。
const HELLO_PAYLOAD_REJECTION_PREFIX: &str = "invalid link hello payload";

/// 把对端对 `hello` 的拒绝归类：旧版响应方的载荷拒绝 → [`LinkError::UnsupportedVersion`]，
/// 其余错误原样返回。
fn hello_rejection(error: LinkError) -> LinkError {
    match error {
        LinkError::BadPayload(message) if message.starts_with(HELLO_PAYLOAD_REJECTION_PREFIX) => {
            LinkError::UnsupportedVersion
        }
        other => other,
    }
}

fn parse_hello_response(json: &serde_json::Value) -> LinkResult<HelloResponse> {
    let get_str = |k: &str| {
        json.get(k)
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(str::to_string)
    };
    // 没有 sessionId 说明对端根本不是 FluxDown 响应方——比如反代把请求转给了别的
    // JSON 服务。
    let session_id = get_str("sessionId")
        .ok_or_else(|| LinkError::NotFluxDown("hello response has no sessionId".into()))?;
    // 版本先于其它字段校验：旧版响应方的回复没有 protocolVersion（还带着明文 SAS 与
    // 它自己的签名），不能按新协议去解读。
    let protocol_version = json
        .get("protocolVersion")
        .and_then(serde_json::Value::as_u64)
        .and_then(|v| u32::try_from(v).ok())
        .unwrap_or(0);
    if protocol_version != PAIRING_PROTOCOL_VERSION {
        return Err(LinkError::UnsupportedVersion);
    }
    Ok(HelloResponse {
        protocol_version,
        session_id,
        responder_eph_pub: b64_to_array::<32>(json, "responderEphPub")?,
        responder_nonce: b64_to_array::<32>(json, "responderNonce")?,
        responder_id_pub: b64_to_array::<32>(json, "responderIdPub")?,
        name: get_str("name").unwrap_or_default(),
        platform: get_str("platform"),
        app_version: get_str("appVersion"),
    })
}

fn parse_reveal_response(json: &serde_json::Value) -> LinkResult<RevealResponse> {
    Ok(RevealResponse {
        responder_sig: b64_to_array::<64>(json, "responderSig")?,
    })
}

/// 入站 `hello` 的 wire 形式（base64 字符串字段），供 HTTP 宿主纯字段搬运。
#[derive(Debug, Clone)]
pub struct WireHello {
    pub protocol_version: u32,
    pub code: String,
    pub initiator_id_pub: String,
    pub initiator_commit: String,
    pub initiator_sig: String,
    pub name: String,
    pub platform: Option<String>,
    pub app_version: Option<String>,
    pub initiator_addrs: Vec<String>,
}

/// 出站 `hello` 回复的 wire 形式（base64 字符串字段）。
#[derive(Debug, Clone)]
pub struct WireHelloResponse {
    pub protocol_version: u32,
    pub session_id: String,
    pub responder_eph_pub: String,
    pub responder_nonce: String,
    pub responder_id_pub: String,
    pub name: String,
    pub platform: Option<String>,
    pub app_version: Option<String>,
}

/// 入站 `reveal` 的 wire 形式（base64 字符串字段）。
#[derive(Debug, Clone)]
pub struct WireReveal {
    pub session_id: String,
    pub initiator_eph_pub: String,
    pub initiator_nonce: String,
}

/// 出站 `reveal` 回复的 wire 形式：响应方对完整转录的签名（base64）。
#[derive(Debug, Clone)]
pub struct WireRevealResponse {
    pub responder_sig: String,
}

fn decode_b64_array<const N: usize>(s: &str) -> LinkResult<[u8; N]> {
    let bytes = B64
        .decode(s)
        .map_err(|_| LinkError::BadPayload("bad base64".into()))?;
    <[u8; N]>::try_from(bytes.as_slice()).map_err(|_| LinkError::BadPayload("bad length".into()))
}

/// 一个字符串字段的长度上限（对端自报值一律不信任，超限即丢弃）。
const PEER_INFO_MAX_DIR_LEN: usize = 1024;

/// 对端信息的线上形式（加密前的明文 JSON）：`{"v":1,"defaultSaveDir":…,"pathStyle":…}`。
fn encode_peer_info(info: &PeerInfo) -> Vec<u8> {
    let mut json = serde_json::json!({ "v": 1 });
    if let Some(dir) = &info.default_save_dir {
        json["defaultSaveDir"] = serde_json::Value::String(dir.clone());
    }
    if let Some(style) = &info.path_style {
        json["pathStyle"] = serde_json::Value::String(style.clone());
    }
    serde_json::to_vec(&json).unwrap_or_default()
}

/// 解析对端信息；不是 v1 信息包（含请求方反射回来的其它数据面明文）→ `None`。
/// 字段一律做长度 / 取值收敛：目录 ≤ 1024 字节且无 NUL，风格只认 `windows` / `posix`。
fn decode_peer_info(plaintext: &[u8]) -> Option<PeerInfo> {
    let json: serde_json::Value = serde_json::from_slice(plaintext).ok()?;
    if json.get("v").and_then(serde_json::Value::as_i64) != Some(1) {
        return None;
    }
    let default_save_dir = json
        .get("defaultSaveDir")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|dir| !dir.is_empty() && dir.len() <= PEER_INFO_MAX_DIR_LEN && !dir.contains('\0'))
        .map(str::to_owned);
    let path_style = json
        .get("pathStyle")
        .and_then(serde_json::Value::as_str)
        .filter(|style| matches!(*style, "windows" | "posix"))
        .map(str::to_owned);
    Some(PeerInfo {
        default_save_dir,
        path_style,
    })
}

/// 已验证的新候选置首，去重后截断到 [`MAX_DIRECT_CANDIDATES`]。
fn promoted_candidates(old: &[PeerCandidate], verified: PeerCandidate) -> Vec<PeerCandidate> {
    let mut candidates = Vec::with_capacity(old.len() + 1);
    candidates.push(verified.clone());
    candidates.extend(old.iter().filter(|c| **c != verified).cloned());
    candidates.truncate(MAX_DIRECT_CANDIDATES);
    candidates
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::crypto::{derive_link_aead_key, link_auth_tag, seal_link_body};
    use crate::storage::memory::MemoryLinkStorage;
    use crate::types::DiscoveryKind;

    async fn mgr_with_device(secret: Vec<u8>) -> (Arc<LinkManager>, String) {
        mgr_with_storage(secret, Arc::new(MemoryLinkStorage::default())).await
    }

    async fn mgr_with_storage(
        secret: Vec<u8>,
        storage: Arc<MemoryLinkStorage>,
    ) -> (Arc<LinkManager>, String) {
        let (tx, _rx) = mpsc::channel(8);
        let mgr = LinkManager::load(
            storage,
            SelfInfo {
                name: "me".into(),
                platform: None,
                app_version: None,
            },
            LinkOptions::reachable(17800),
            tx,
        )
        .await
        .unwrap();
        let fp = "peerfp".to_string();
        mgr.store
            .upsert(&PeerRecord {
                fingerprint: fp.clone(),
                identity_pub: vec![1u8; 32],
                name: "peer".into(),
                platform: None,
                link_secret: secret,
                candidates: vec![],
                paired_at: 0,
                last_seen_at: 0,
                info: PeerInfo::default(),
            })
            .await
            .unwrap();
        (mgr, fp)
    }

    /// 按发起方 [`LinkManager::dispatch`] 同款 encrypt-then-MAC 流程构造
    /// 密文 body + tag：先加密、再对**密文**算 HMAC——测试里的「合法请求」
    /// 必须复刻这个顺序，否则测出来的是另一套协议。
    fn seal_and_tag(
        secret: &[u8],
        path: &str,
        ts: i64,
        nonce: &str,
        plaintext: &[u8],
    ) -> (Vec<u8>, String) {
        let sealed = seal_link_body(&derive_link_aead_key(secret), plaintext);
        let tag = link_auth_tag(secret, "POST", path, ts, nonce, &sealed);
        (sealed, tag)
    }

    #[tokio::test]
    async fn api_port_update_refreshes_pairing_candidates_without_resetting_identity_or_auth() {
        let secret = vec![7u8; 32];
        let (mgr, fp) = mgr_with_device(secret.clone()).await;
        let identity = mgr.fingerprint().to_owned();
        let peer = PeerAddress::parse("127.0.0.1:17800").unwrap();
        let path = LINK_TASKS_PATH;
        let ts = now_unix();
        let plaintext = br#"{"url":"http://example.test/file"}"#;
        let (sealed, tag) = seal_and_tag(&secret, path, ts, "before-port-update", plaintext);
        mgr.authorize(
            "POST",
            path,
            &fp,
            ts,
            "before-port-update",
            &sealed,
            &tag,
            "v1",
        )
        .await
        .unwrap();
        assert_eq!(mgr.self_addrs_towards(&peer).await, vec!["127.0.0.1:17800"]);

        mgr.update_api_port(17801).unwrap();
        assert_eq!(mgr.self_addrs_towards(&peer).await, vec!["127.0.0.1:17801"]);
        assert_eq!(mgr.fingerprint(), identity);
        assert!(
            mgr.advertiser
                .lock()
                .is_ok_and(|advertiser| advertiser.is_none()),
            "updating a port must not start advertising"
        );
        // 端口切换不能清除防重放状态，也不能使已有 link_secret 失效。
        assert!(matches!(
            mgr.authorize(
                "POST",
                path,
                &fp,
                ts,
                "before-port-update",
                &sealed,
                &tag,
                "v1"
            )
            .await,
            Err(LinkError::Unauthorized)
        ));
        let (sealed, tag) = seal_and_tag(&secret, path, ts, "after-port-update", plaintext);
        let authorized = mgr
            .authorize(
                "POST",
                path,
                &fp,
                ts,
                "after-port-update",
                &sealed,
                &tag,
                "v1",
            )
            .await
            .unwrap();
        assert_eq!(authorized.device, fp);
        assert_eq!(authorized.body, plaintext);

        mgr.update_api_port(17800).unwrap();
        assert_eq!(mgr.self_addrs_towards(&peer).await, vec!["127.0.0.1:17800"]);
    }

    #[tokio::test]
    async fn api_port_update_does_not_expand_reachability_or_revoke_pairing_codes() {
        let (mgr, _events) = responder_mgr().await;
        let code = mgr.generate_code();
        mgr.update_api_port(17801).unwrap();
        let peer = PeerAddress::parse("127.0.0.1:17800").unwrap();
        assert!(mgr.self_addrs_towards(&peer).await.is_empty());
        assert!(
            mgr.advertiser
                .lock()
                .is_ok_and(|advertiser| advertiser.is_none())
        );
        let mut initiator = PairingInitiator::new(LinkIdentity::generate());
        let hello = initiator.build_hello(&code, &mgr.self_info, Vec::new());
        let response = mgr.responder.handle_hello(&hello, None).unwrap();
        assert_eq!(
            crate::crypto::fingerprint(&response.responder_id_pub),
            mgr.fingerprint()
        );
    }

    #[tokio::test]
    async fn authorize_accepts_valid_then_rejects_replay_tamper_and_skew() {
        let secret = vec![7u8; 32];
        let (mgr, fp) = mgr_with_device(secret.clone()).await;
        let path = "/api/v1/link/tasks";
        let ts = now_unix();
        let plaintext = br#"{"url":"http://x/f"}"#;
        let (sealed, tag) = seal_and_tag(&secret, path, ts, "n1", plaintext);

        // 合法请求通过，且解密还原出原始明文。
        let authorized = mgr
            .authorize("POST", path, &fp, ts, "n1", &sealed, &tag, "v1")
            .await
            .unwrap();
        assert_eq!(authorized.device, fp);
        assert_eq!(authorized.body, plaintext);
        // 同 nonce 重放被拒（防重放）。
        assert!(matches!(
            mgr.authorize("POST", path, &fp, ts, "n1", &sealed, &tag, "v1")
                .await,
            Err(LinkError::Unauthorized)
        ));
        // 篡改密文（哪怕只翻一个字节）→ HMAC 覆盖密文摘要，篡改后 tag 不再
        // 匹配 → 拒（encrypt-then-MAC 的完整性保护在生效）。
        let mut tampered = sealed.clone();
        let last = tampered.len() - 1;
        tampered[last] ^= 0xFF;
        assert!(matches!(
            mgr.authorize("POST", path, &fp, ts, "n2", &tampered, &tag, "v1")
                .await,
            Err(LinkError::Unauthorized)
        ));
        // 过期时间戳 → 拒。
        let old_ts = ts - LINK_AUTH_SKEW_SECS - 5;
        let (old_sealed, old_tag) = seal_and_tag(&secret, path, old_ts, "n3", plaintext);
        assert!(matches!(
            mgr.authorize("POST", path, &fp, old_ts, "n3", &old_sealed, &old_tag, "v1")
                .await,
            Err(LinkError::Unauthorized)
        ));
        // 未配对设备 → 拒。
        let (sealed4, tag4) = seal_and_tag(&secret, path, ts, "n4", plaintext);
        assert!(matches!(
            mgr.authorize("POST", path, "unknown", ts, "n4", &sealed4, &tag4, "v1")
                .await,
            Err(LinkError::Unauthorized)
        ));
    }

    #[tokio::test]
    async fn authorize_rejects_missing_or_invalid_enc_header() {
        // 数据面不留明文回退路径：缺 `X-FluxLink-Enc` 头或头值不是 "v1"，
        // 哪怕 HMAC/nonce/时间戳全部合法也一律拒绝——回归防止未来有人为
        // 兼容旧客户端悄悄加回明文分支（降级攻击）。
        let secret = vec![7u8; 32];
        let (mgr, fp) = mgr_with_device(secret.clone()).await;
        let path = "/api/v1/link/tasks";
        let ts = now_unix();
        let (sealed, tag) = seal_and_tag(&secret, path, ts, "n1", br#"{"url":"http://x/f"}"#);

        assert!(matches!(
            mgr.authorize("POST", path, &fp, ts, "n1", &sealed, &tag, "")
                .await,
            Err(LinkError::Unauthorized)
        ));
        assert!(matches!(
            mgr.authorize("POST", path, &fp, ts, "n1", &sealed, &tag, "v0")
                .await,
            Err(LinkError::Unauthorized)
        ));
    }

    #[tokio::test]
    async fn prune_expired_evicts_stale_seen_nonces() {
        // pending 的过期判定已由 confirm_pairing 的本地 PENDING_INIT_TTL
        // 检查覆盖；这里覆盖 prune_expired 对 seen_nonces 的时窗剪枝——
        // 超过 LINK_AUTH_SKEW_SECS 的防重放记录必须被清掉，否则该表在
        // 进程存活期内随数据面请求量无界增长。
        let secret = vec![7u8; 32];
        let (mgr, fp) = mgr_with_device(secret).await;
        let old_ts = now_unix() - LINK_AUTH_SKEW_SECS - 5;
        if let Ok(mut seen) = mgr.seen_nonces.lock() {
            seen.push((format!("{fp}:stale"), old_ts));
        }
        mgr.prune_expired();
        assert!(mgr.seen_nonces.lock().unwrap().is_empty());
    }

    fn direct(addr: &str) -> PeerCandidate {
        PeerCandidate {
            kind: TransportKind::Direct,
            address: addr.to_string(),
        }
    }

    #[tokio::test]
    async fn metadata_persistence_failure_keeps_verified_hint_until_retry_succeeds()
    -> LinkResult<()> {
        let storage = Arc::new(MemoryLinkStorage::default());
        let (manager, fingerprint) = mgr_with_storage(vec![7; 32], storage.clone()).await;
        let candidate = direct("10.0.0.9:17800");
        manager
            .hinted
            .lock()
            .map_err(|error| LinkError::Store(error.to_string()))?
            .insert(fingerprint.clone(), candidate.clone());
        let address = PeerAddress::parse(&candidate.address)?.base_url();
        storage
            .fail_metadata_writes
            .store(true, std::sync::atomic::Ordering::Relaxed);
        manager.promote_hinted(&fingerprint, &address).await;
        assert_eq!(
            manager
                .hinted
                .lock()
                .map_err(|error| LinkError::Store(error.to_string()))?
                .get(&fingerprint),
            Some(&candidate)
        );
        assert!(
            manager
                .store
                .get(&fingerprint)
                .await?
                .ok_or(LinkError::NotPaired)?
                .candidates
                .is_empty()
        );
        storage
            .fail_metadata_writes
            .store(false, std::sync::atomic::Ordering::Relaxed);
        manager.promote_hinted(&fingerprint, &address).await;
        assert!(
            !manager
                .hinted
                .lock()
                .map_err(|error| LinkError::Store(error.to_string()))?
                .contains_key(&fingerprint)
        );
        assert_eq!(
            manager
                .store
                .get(&fingerprint)
                .await?
                .ok_or(LinkError::NotPaired)?
                .candidates,
            vec![candidate]
        );
        Ok(())
    }

    #[tokio::test]
    async fn peer_info_response_does_not_report_success_before_metadata_is_persisted()
    -> LinkResult<()> {
        let storage = Arc::new(MemoryLinkStorage::default());
        let (manager, fingerprint) = mgr_with_storage(vec![7; 32], storage.clone()).await;
        storage
            .fail_metadata_writes
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let request = LinkRequest {
            device: fingerprint.clone(),
            body: encode_peer_info(&PeerInfo {
                default_save_dir: Some("/downloads".into()),
                path_style: Some("posix".into()),
            }),
        };
        let result = manager
            .answer_peer_info(&request, &PeerInfo::default())
            .await;
        assert!(matches!(result, Err(LinkError::Store(_))));
        assert_eq!(
            manager
                .store
                .get(&fingerprint)
                .await?
                .ok_or(LinkError::NotPaired)?
                .info
                .default_save_dir,
            None
        );
        Ok(())
    }

    #[tokio::test]
    async fn unverified_hint_never_displaces_stored_candidates() {
        let (mgr, fp) = mgr_with_device(vec![7u8; 32]).await;
        mgr.store
            .update_candidates(&fp, &[direct("10.0.0.2:17800")])
            .await
            .unwrap();
        mgr.hinted
            .lock()
            .unwrap()
            .insert(fp.clone(), direct("10.0.0.9:17800"));
        let record = mgr.store.get(&fp).await.unwrap().unwrap();
        let dial = mgr.with_hinted(record);
        assert_eq!(
            dial.candidates,
            vec![direct("10.0.0.2:17800"), direct("10.0.0.9:17800")]
        );
        let stored = mgr.store.get(&fp).await.unwrap().unwrap();
        assert_eq!(stored.candidates, vec![direct("10.0.0.2:17800")]);
    }

    #[tokio::test]
    async fn hint_is_persisted_only_after_authenticated_success_on_its_address() {
        let (mgr, fp) = mgr_with_device(vec![7u8; 32]).await;
        mgr.store
            .update_candidates(&fp, &[direct("10.0.0.2:17800")])
            .await
            .unwrap();
        mgr.hinted
            .lock()
            .unwrap()
            .insert(fp.clone(), direct("10.0.0.9:17800"));
        let other = PeerAddress::parse("10.0.0.2:17800").unwrap().base_url();
        mgr.promote_hinted(&fp, &other).await;
        let stored = mgr.store.get(&fp).await.unwrap().unwrap();
        assert_eq!(stored.candidates, vec![direct("10.0.0.2:17800")]);

        let hinted = PeerAddress::parse("10.0.0.9:17800").unwrap().base_url();
        mgr.promote_hinted(&fp, &hinted).await;
        let stored = mgr.store.get(&fp).await.unwrap().unwrap();
        assert_eq!(
            stored.candidates,
            vec![direct("10.0.0.9:17800"), direct("10.0.0.2:17800")]
        );
    }

    fn wire_hello(hello: &HelloRequest) -> WireHello {
        WireHello {
            protocol_version: hello.protocol_version,
            code: hello.code.clone(),
            initiator_id_pub: B64.encode(hello.initiator_id_pub),
            initiator_commit: B64.encode(hello.initiator_commit),
            initiator_sig: B64.encode(hello.initiator_sig),
            name: hello.name.clone(),
            platform: hello.platform.clone(),
            app_version: hello.app_version.clone(),
            initiator_addrs: hello.initiator_addrs.clone(),
        }
    }

    async fn responder_mgr() -> (Arc<LinkManager>, mpsc::Receiver<LinkEngineEvent>) {
        let (tx, rx) = mpsc::channel(8);
        let mgr = LinkManager::load(
            Arc::new(MemoryLinkStorage::default()),
            SelfInfo {
                name: "nas".into(),
                platform: Some("linux".into()),
                app_version: None,
            },
            LinkOptions {
                api_port: 17800,
                reachable: false,
                advertise: false,
            },
            tx,
        )
        .await
        .unwrap();
        (mgr, rx)
    }

    #[tokio::test]
    async fn incoming_pairing_is_announced_only_after_reveal_with_the_shared_sas() {
        // 响应方在 hello 阶段还看不到发起方的临时公钥，没有 SAS 可展示：入站配对请求
        // 只在 reveal 被受理之后才广播，且广播的 SAS 与发起方算出的一致。
        let (mgr, mut events) = responder_mgr().await;
        let code = mgr.generate_code();
        let initiator_id = LinkIdentity::generate();
        let mut initiator = PairingInitiator::new(initiator_id.clone());
        let hello = initiator.build_hello(
            &code,
            &SelfInfo {
                name: "laptop".into(),
                platform: Some("macos".into()),
                app_version: None,
            },
            vec![],
        );

        let resp = mgr.pair_hello_wire(wire_hello(&hello), None).unwrap();
        tokio::task::yield_now().await;
        assert!(events.try_recv().is_err(), "hello alone must not announce");

        // 发起方按对端 JSON 的形状解析 hello 回复（与 begin_pairing 同一条解析路径）。
        let json = serde_json::json!({
            "protocolVersion": resp.protocol_version,
            "sessionId": resp.session_id,
            "responderEphPub": resp.responder_eph_pub,
            "responderNonce": resp.responder_nonce,
            "responderIdPub": resp.responder_id_pub,
            "name": resp.name,
            "platform": resp.platform,
            "appVersion": resp.app_version,
        });
        let hello_resp = parse_hello_response(&json).unwrap();
        let reveal = initiator
            .on_hello_response(&hello_resp, "10.0.0.1:17800", &[])
            .unwrap();
        let revealed = mgr
            .pair_reveal_wire(WireReveal {
                session_id: reveal.session_id.clone(),
                initiator_eph_pub: B64.encode(reveal.initiator_eph_pub),
                initiator_nonce: B64.encode(reveal.initiator_nonce),
            })
            .unwrap();
        let reveal_resp =
            parse_reveal_response(&serde_json::json!({ "responderSig": revealed.responder_sig }))
                .unwrap();
        let initiator_sas = initiator.on_reveal_response(&reveal_resp).unwrap();

        let event = tokio::time::timeout(std::time::Duration::from_secs(2), events.recv())
            .await
            .unwrap()
            .unwrap();
        match event {
            LinkEngineEvent::IncomingPairing {
                session_id,
                sas,
                peer_name,
                peer_fingerprint,
                ..
            } => {
                assert_eq!(session_id, hello_resp.session_id);
                assert_eq!(sas, initiator_sas);
                assert_eq!(peer_name, "laptop");
                assert_eq!(peer_fingerprint, initiator_id.fingerprint());
            }
            other => panic!("expected IncomingPairing, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn hello_from_other_protocol_version_is_rejected_before_field_decoding() {
        // 旧版发起方的 hello 没有新字段（解码后是空串）：要得到明确的版本不兼容，
        // 而不是含糊的载荷非法。
        let (mgr, _events) = responder_mgr().await;
        let code = mgr.generate_code();
        let stale = WireHello {
            protocol_version: 0,
            code: code.clone(),
            initiator_id_pub: String::new(),
            initiator_commit: String::new(),
            initiator_sig: String::new(),
            name: "old".into(),
            platform: None,
            app_version: None,
            initiator_addrs: vec![],
        };
        assert!(matches!(
            mgr.pair_hello_wire(stale, None),
            Err(LinkError::UnsupportedVersion)
        ));
    }

    #[test]
    fn legacy_peer_replies_are_reported_as_unsupported_version() {
        // 旧版响应方的 hello 回复：有 sessionId、明文 SAS 与自己的签名，没有 protocolVersion。
        let legacy = serde_json::json!({
            "sessionId": "abc",
            "responderEphPub": B64.encode([1u8; 32]),
            "responderIdPub": B64.encode([2u8; 32]),
            "responderSig": B64.encode([3u8; 64]),
            "name": "old-nas",
            "sas": "123456",
        });
        assert!(matches!(
            parse_hello_response(&legacy),
            Err(LinkError::UnsupportedVersion)
        ));
        // 根本不是 FluxDown 的 JSON 仍是 NotFluxDown，不被误判成版本问题。
        assert!(matches!(
            parse_hello_response(&serde_json::json!({ "ok": true })),
            Err(LinkError::NotFluxDown(_))
        ));

        // 旧版响应方会把新 hello 当作无法解析的载荷拒绝。
        let rejected = LinkError::BadPayload(
            "invalid link hello payload: missing field `initiatorEphPub` at line 1 column 80"
                .into(),
        );
        assert!(matches!(
            hello_rejection(rejected),
            LinkError::UnsupportedVersion
        ));
        assert!(matches!(
            hello_rejection(LinkError::InvalidCode),
            LinkError::InvalidCode
        ));
        assert!(matches!(
            hello_rejection(LinkError::BadPayload("unrelated".into())),
            LinkError::BadPayload(_)
        ));
    }

    #[test]
    fn new_pairing_errors_round_trip_through_the_wire_message_contract() {
        // 发起方只看得到对端的 HTTP message：新错误必须能被还原，而不是一律变成含糊的载荷错误。
        for error in [LinkError::UnsupportedVersion, LinkError::CommitmentMismatch] {
            let restored = LinkError::from_wire_message(&error.to_string());
            assert_eq!(
                restored.map(|e| std::mem::discriminant(&e)),
                Some(std::mem::discriminant(&error))
            );
        }
    }

    #[tokio::test]
    async fn known_fingerprints_cover_probe_and_discovery_for_that_address_only() {
        let (mgr, _events) = responder_mgr().await;
        let addr = PeerAddress::parse("10.0.0.2:17800").unwrap();
        let elsewhere = PeerAddress::parse("10.0.0.3:17800").unwrap();
        assert!(mgr.known_fingerprints(&addr).is_empty());

        mgr.remember_probe(&addr, "fp-probed");
        let peer = |fingerprint: Option<&str>, host: &str| DiscoveredPeer {
            fingerprint: fingerprint.map(str::to_string),
            name: "peer".into(),
            platform: None,
            host: host.into(),
            port: 17800,
            app_version: None,
            kind: DiscoveryKind::Mdns,
        };
        {
            let mut snapshot = mgr.discovered.lock().unwrap();
            snapshot.push(peer(Some("fp-mdns"), "10.0.0.2"));
            // 没有指纹的条目与其它地址的条目都不提供这个地址的指纹。
            snapshot.push(peer(None, "10.0.0.2"));
            snapshot.push(peer(Some("fp-other"), "10.0.0.3"));
        }

        let mut known = mgr.known_fingerprints(&addr);
        known.sort();
        assert_eq!(known, vec!["fp-mdns".to_string(), "fp-probed".to_string()]);
        let mut elsewhere_known = mgr.known_fingerprints(&elsewhere);
        elsewhere_known.sort();
        assert_eq!(elsewhere_known, vec!["fp-other".to_string()]);
    }

    #[tokio::test]
    async fn probed_fingerprints_are_bounded_and_expire() {
        let (mgr, _events) = responder_mgr().await;
        for i in 0..(MAX_PROBED_FINGERPRINTS + 8) {
            let addr = PeerAddress::parse(&format!("10.1.0.{}:17800", i + 1)).unwrap();
            mgr.remember_probe(&addr, &format!("fp-{i}"));
        }
        assert_eq!(mgr.probed.lock().unwrap().len(), MAX_PROBED_FINGERPRINTS);
        // 满员淘汰的是最旧的，最新的仍在。
        let newest =
            PeerAddress::parse(&format!("10.1.0.{}:17800", MAX_PROBED_FINGERPRINTS + 8)).unwrap();
        assert_eq!(
            mgr.known_fingerprints(&newest),
            vec![format!("fp-{}", MAX_PROBED_FINGERPRINTS + 7)]
        );

        // 过期的探测结果不再参与比对，并被后台剪枝回收。
        let Some(expired_at) = std::time::Instant::now()
            .checked_sub(PROBED_FINGERPRINT_TTL + std::time::Duration::from_secs(1))
        else {
            return;
        };
        for (_, at) in mgr.probed.lock().unwrap().values_mut() {
            *at = expired_at;
        }
        assert!(mgr.known_fingerprints(&newest).is_empty());
        mgr.prune_expired();
        assert!(mgr.probed.lock().unwrap().is_empty());
    }
}
