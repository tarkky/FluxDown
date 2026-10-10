//! 本地设备互联（device link）错误类型。

/// 设备互联子系统的统一错误。HTTP 宿主层把它映射为响应状态码，
/// agent `link/service.rs` 把它转成 `LinkEvent` 的错误信号。
#[derive(Debug, thiserror::Error)]
pub enum LinkError {
    /// 配对码不存在 / 已过期 / 已被使用。
    #[error("pairing code invalid or expired")]
    InvalidCode,

    /// 配对会话不存在或已过期（confirm 阶段找不到 hello 建立的会话）。
    #[error("pairing session not found or expired")]
    SessionExpired,

    /// 对端提交的身份签名校验失败（Ed25519 verify 失败）——身份与临时密钥绑定被破坏。
    #[error("peer identity signature verification failed")]
    BadSignature,

    /// 发起方揭示的临时公钥/随机数与其 hello 里先行发出的承诺不符——握手被篡改
    /// 或发起方不守协议，会话随即作废。
    #[error("pairing commitment does not match the revealed values")]
    CommitmentMismatch,

    /// 对端配对协议版本与本机不兼容。配对只认当前协议版本，不做降级：旧版对端
    /// 没有临时公钥承诺，任何兼容分支都会让握手退回可被中间人操纵 SAS 的形态。
    #[error(
        "unsupported link pairing protocol version; update both devices to the same FluxDown release"
    )]
    UnsupportedVersion,

    /// 双端持有同一身份（共享同一引擎数据库的进程互相配对）——设备不能与自己配对。
    #[error("cannot pair a device with itself (both ends share the same link identity)")]
    SelfPairing,

    /// 短时窗内错误配对码尝试过多——在线暴力猜码节流（与有效码生命周期解耦）。
    #[error("too many failed pairing attempts, try again later")]
    Throttled,

    /// 对端设备上的用户拒绝了本次配对（SAS 不一致或主动取消）。
    #[error("pairing rejected on the peer device")]
    RejectedByPeer,

    /// 等待对端设备用户核验 SAS 超时。
    #[error("timed out waiting for confirmation on the peer device")]
    PairingTimeout,

    /// 拨通的地址上不是已配对的那台设备（指纹不符）——TOFU 身份复核失败。
    #[error("peer identity mismatch at {0}")]
    IdentityMismatch(String),

    /// 载荷字段非法（长度不符 / base64 解码失败 / 缺字段等）。
    #[error("invalid link payload: {0}")]
    BadPayload(String),

    /// 数据面链路鉴权失败（HMAC 不匹配 / 时间戳过期 / 设备未配对）。
    #[error("link authentication failed")]
    Unauthorized,

    /// 目标设备当前不可达（所有传输策略都失败）。
    #[error("peer unreachable")]
    Unreachable,

    /// 拨通的地址不是 FluxDown 服务（非 JSON / 非 FluxDown 错误体 / 重定向 / HTML 等），
    /// 典型场景：反代把请求转给了别的服务、协议（http/https）或端口填错。
    /// 与「配对码错误」严格区分——它说明对端根本没有处理这次配对请求。
    #[error("endpoint is not a FluxDown service: {0}")]
    NotFluxDown(String),

    /// 目标设备未配对（本机名册里没有该指纹）。
    #[error("peer is not paired")]
    NotPaired,

    /// 底层持久化错误。
    #[error("link store error: {0}")]
    Store(String),

    /// 网络 / IO 错误（探测、HTTP 请求、mDNS）。
    #[error("link io error: {0}")]
    Io(String),

    /// 该宿主未启用设备互联能力。
    #[error("device link not available on this host")]
    Unavailable,
}

/// 便捷别名。
pub type LinkResult<T> = Result<T, LinkError>;

impl LinkError {
    /// 把对端 HTTP 错误响应里的 `message` 还原成本端错误值。
    ///
    /// 为什么需要它：配对是**跨设备**协议，发起方拿到的只是对端的状态码 + message。
    /// 此前发起方把对端的任何 400 一律改写成 [`LinkError::InvalidCode`]，于是对端
    /// 真正返回的「猜码过多已被节流」被伪装成「配对码错误」——用户会一遍遍重试一个
    /// 其实完全正确的码。这些 `#[error(...)]` 字符串本就是稳定契约（Web 客户端的
    /// `isLinkUnsupportedError` 同样按字符串比对，见 web/src/lib/link.ts 文件头注），
    /// 这里把该契约集中成一处双向映射，避免各调用点散落字符串判断。
    #[must_use]
    pub fn from_wire_message(message: &str) -> Option<Self> {
        match message.trim() {
            "pairing code invalid or expired" => Some(Self::InvalidCode),
            "pairing session not found or expired" => Some(Self::SessionExpired),
            "peer identity signature verification failed" => Some(Self::BadSignature),
            "pairing commitment does not match the revealed values" => {
                Some(Self::CommitmentMismatch)
            }
            "unsupported link pairing protocol version; update both devices to the same FluxDown release" => {
                Some(Self::UnsupportedVersion)
            }
            "cannot pair a device with itself (both ends share the same link identity)" => {
                Some(Self::SelfPairing)
            }
            "too many failed pairing attempts, try again later" => Some(Self::Throttled),
            "pairing rejected on the peer device" => Some(Self::RejectedByPeer),
            "timed out waiting for confirmation on the peer device" => Some(Self::PairingTimeout),
            _ => None,
        }
    }
}
