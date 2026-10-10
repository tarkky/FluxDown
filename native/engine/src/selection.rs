//! 需要宿主(有 UI 的一端)介入决策的选择点:HLS 画质选择、BT 文件选择。
//!
//! 现状(已核实,见项目历史会话记录):HLS/BT 的用户选择答案是通过独立
//! 后到的客户端选择消息(`SelectHlsQuality`/`SelectBtFiles`)分别投递的,与
//! "发起选择"调用点解耦,不是一次 `.await` 闭环完成。因此 [`HostSelection`]
//! 同时包含"发起等待"与"投递答案"两类方法。

use std::time::Duration;

use crate::model::{BtFileEntry, HlsQualityOption, ResolveVariantOption};

/// 一次宿主选择请求的结果。
///
/// 三态设计(而非现状二态:要么拿到答案要么用默认值):
/// - [`NoSelectorConfigured`](SelectionOutcome::NoSelectorConfigured) 让
///   headless 场景在**进入等待前**短路,不必真的等待超时。
/// - [`TimedOutDefaulted`](SelectionOutcome::TimedOutDefaulted) 与
///   [`UserChose`](SelectionOutcome::UserChose)/
///   [`NoSelectorConfigured`](SelectionOutcome::NoSelectorConfigured) 在日志中
///   可区分,避免运维把"真超时"和"设计如此的无人值守"混为一谈。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectionOutcome<T> {
    /// 用户在超时前做出了选择。
    UserChose(T),
    /// 等待超时,回退到调用方提供的默认值。
    TimedOutDefaulted(T),
    /// 当前宿主未配置任何选择器(headless),直接使用默认值,未进入等待。
    NoSelectorConfigured(T),
}

impl<T> SelectionOutcome<T> {
    /// 取出内部值,不区分具体是哪一种结果。
    ///
    /// # Examples
    ///
    /// ```
    /// use fluxdown_engine::selection::SelectionOutcome;
    ///
    /// let outcome = SelectionOutcome::UserChose(3);
    /// assert_eq!(outcome.into_inner(), 3);
    /// ```
    pub fn into_inner(self) -> T {
        match self {
            SelectionOutcome::UserChose(v)
            | SelectionOutcome::TimedOutDefaulted(v)
            | SelectionOutcome::NoSelectorConfigured(v) => v,
        }
    }
}

/// 需要宿主介入的选择点:HLS 画质选择、BT 文件选择。
///
/// 由宿主实现并注入 [`crate::Engine`]。方法采用 `async_trait`(而非原生
/// async fn in trait):[`crate::Engine`] 需要以 `Arc<dyn HostSelection>`
/// 存字段并跨多任务共享,截至当前 Rust 稳定版本原生 async fn in trait 仍
/// 非 dyn 兼容(`E0038`)。
#[async_trait::async_trait]
pub trait HostSelection: Send + Sync {
    /// 发起 HLS 画质选择等待;`timeout` 到期后返回 `TimedOutDefaulted`。
    async fn select_hls_quality(
        &self,
        task_id: &str,
        options: &[HlsQualityOption],
        timeout: Duration,
    ) -> SelectionOutcome<i32>;

    /// 发起 BT 文件选择等待;`timeout` 为 `None` 时保留现状"无限等待"语义,
    /// 为 `Some(d)` 时到期返回 `TimedOutDefaulted`（供宿主指定有限超时）。
    async fn select_bt_files(
        &self,
        task_id: &str,
        files: &[BtFileEntry],
        timeout: Option<Duration>,
    ) -> SelectionOutcome<Vec<i32>>;

    /// 发起插件 resolve 变体（画质/格式）选择等待；`timeout` 到期后返回
    /// `TimedOutDefaulted`（默认值 = 调用方传入的 `default_index`，通常为 0，
    /// 即插件按自身偏好排在首位的变体）。
    async fn select_resolve_variant(
        &self,
        task_id: &str,
        options: &[ResolveVariantOption],
        default_index: i32,
        timeout: Duration,
    ) -> SelectionOutcome<i32>;

    /// 投递宿主收到的 HLS 画质选择答案，唤醒对应
    /// [`select_hls_quality`](HostSelection::select_hls_quality) 的等待。
    fn provide_hls_selection(&self, task_id: &str, selected_index: i32);

    /// 投递宿主收到的 BT 文件选择答案，
    /// 唤醒对应 [`select_bt_files`](HostSelection::select_bt_files) 的等待。
    fn provide_bt_selection(&self, task_id: &str, selected_indices: Vec<i32>);

    /// 投递插件 resolve 变体选择答案（由收到 `SelectResolveVariant` 信号的宿主
    /// 侧调用），唤醒对应
    /// [`select_resolve_variant`](HostSelection::select_resolve_variant) 的等待。
    fn provide_variant_selection(&self, task_id: &str, selected_index: i32);

    /// 当前是否有能作答交互选择的宿主界面在线。为 false 时引擎不会为「文件已存在」
    /// 询问让出并发槽挂起任务，直接按回退值（重命名）继续。默认 false（headless /
    /// CLI 本地模式 / 测试）。
    fn can_prompt(&self) -> bool {
        false
    }

    /// 发起「文件已存在」询问等待；`timeout` 到期返回
    /// `TimedOutDefaulted(FileExistsChoice::Rename)`。默认实现不等待，直接
    /// `NoSelectorConfigured(Rename)`。
    async fn select_file_exists(
        &self,
        _task_id: &str,
        _conflict: &FileConflict,
        _timeout: Duration,
    ) -> SelectionOutcome<FileExistsChoice> {
        SelectionOutcome::NoSelectorConfigured(FileExistsChoice::Rename)
    }
}

/// 「文件已存在」询问的答复。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileExistsChoice {
    /// 自动追加序号另存（也是超时 / 无人值守回退值）。
    Rename,
    /// 完成时替换旧文件；授权只绑定询问时的文件名。
    Overwrite,
    /// 不下载，任务记为完成并采纳已有文件。
    Skip,
    /// 用户取消：任务暂停，下次真正起跑时重新询问。
    Cancel,
}

/// 「文件已存在」询问的上下文，全部在主机侧算好（远程客户端看不到主机文件系统）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileConflict {
    /// 发生冲突的目标文件名。
    pub file_name: String,
    /// 保存目录。
    pub save_dir: String,
    /// 已有文件大小（字节）。
    pub existing_size: Option<u64>,
    /// 已有文件修改时间（Unix 毫秒）。
    pub existing_modified_unix_ms: Option<i64>,
    /// 新下载的大小（字节），未知为 `None`。
    pub incoming_size: Option<i64>,
    /// 选「重命名」时的预览名（询问时刻计算）。
    pub rename_preview: String,
    /// 该协议是否允许「跳过」（与全局 skip 支持范围一致：HTTP/FTP）。
    pub allow_skip: bool,
}
