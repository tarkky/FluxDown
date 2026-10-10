//! 本地设备互联（device link）—— P2P 局域网配对 + mDNS 发现 + 可扩展直连传输。
//!
//! 与宿主无关：宿主（agent / 冻结的 server）只需实现 [`LinkStorage`]
//! （身份种子 + 已配对名册的持久化），并驱动 [`LinkManager`]。
//!
//! # 分层（可扩展性设计）
//! - [`identity`]：Ed25519 本机身份（设备 ID = 公钥指纹，TOFU 固定）。
//! - [`address`]：对端地址模型（http/https、域名、反代 base path）。
//! - [`discovery`]：发现层：mDNS 广播/浏览 + 手动地址 `/ping` 探测。
//! - [`pairing`]：配对协议（一次性码 + 临时公钥承诺-揭示 + X25519 ECDH + SAS + 身份签名）。
//! - [`transport`]：**数据面传输 seam** —— Direct(v1) / 未来 iroh、relay 插拔。
//! - [`storage`]：持久化 trait（身份种子 + 已配对设备名册）。
//! - [`crypto`]：指纹 / SAS / 链路密钥 / HMAC 鉴权原语。

pub mod address;
pub mod crypto;
pub mod discovery;
pub mod error;
pub mod identity;
pub mod manager;
pub mod pairing;
pub mod storage;
pub mod transport;
pub mod types;
pub mod wire;

pub use address::PeerAddress;
pub use error::{LinkError, LinkResult};
pub use identity::LinkIdentity;
pub use manager::{
    BeginPairingResult, LinkEngineEvent, LinkManager, LinkOptions, LinkRequest, PairConfirmOutcome,
    WireHello, WireHelloResponse, WireReveal, WireRevealResponse,
};
pub use pairing::{
    HelloRequest, HelloResponse, PAIRING_PROTOCOL_VERSION, PairingInitiator, PairingResponder,
    RevealAccepted, RevealRequest, RevealResponse, SelfInfo,
};
pub use storage::LinkStorage;
pub use transport::{DirectTransport, PeerConn, Transport, TransportStack};
pub use types::{
    DiscoveredPeer, DiscoveryKind, PeerCandidate, PeerInfo, PeerRecord, TransportKind,
};
