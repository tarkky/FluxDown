use std::{
    cell::{Cell, RefCell},
    collections::{HashMap, HashSet, VecDeque},
    rc::Rc,
    time::{Duration, Instant},
};

use fluxdown_protocol::{RemoteCommandAction, RemoteCommandParams, RemoteTaskStatus};
use fluxdown_ui_components::{CheckState, FluxIcon, check_mark, tabular_numbers};
use fluxdown_ui_icon_pack::FileKind;
use fluxdown_ui_theme::active_theme;
use gpui::{
    AnyElement, App, ClickEvent, Context, Div, Edges, Entity, FocusHandle, FontWeight, Hsla,
    InteractiveElement as _, IntoElement, Modifiers, MouseButton, MouseDownEvent, MouseMoveEvent,
    ParentElement, Pixels, Render, ScrollWheelEvent, SharedString, Stateful,
    StatefulInteractiveElement as _, Styled, Task, WeakEntity, Window, div,
    prelude::FluentBuilder as _, px,
};
use gpui_component::{
    ElementExt as _, Icon, IconName, Sizable as _, Size, h_flex,
    menu::{PopupMenu, PopupMenuItem},
    scroll::Scrollbar,
    spinner::Spinner,
    table::{Column, DataTable, TableDelegate, TableState},
    tooltip::Tooltip,
    v_flex,
};

use crate::{
    batch::{MAX_IN_FLIGHT, coalesce_commands, retry_copy, retry_delay},
    components::{
        conflict_badge::conflict_badge, file_icon::task_file_icon, task_drag::DraggedTasks,
    },
    controller::DownloadsCommand,
    model::{
        CategoryIndex, DownloadFilter, DownloadTaskView, RowId, RowKey, SidebarSelection,
        TaskProtocol, TaskSource, TaskState, TaskStore,
        counts::SidebarCounts,
        dispatch::DispatchSummary,
        format_bytes,
        row_order::{InteractionHold, RowOrder},
        view_prefs::{
            DateBucket, SortDir, ViewDensity, ViewGroupBy, ViewPrefs, ViewSortKey, state_group_key,
        },
    },
    pages::downloads::DownloadView,
    strings::{DownloadStrings, error_text},
};

/// 固定左侧选择列宽（含表格左侧留白）：平时显示文件类型图标，行悬停 / 已选中 /
/// 存在任意选中时换成复选框。
pub(crate) const SELECTION_COLUMN_WIDTH: f32 = 36.;
/// 表头拖拽调宽的上限（下限按列取 [`DownloadColumnKind::min_width`]）。
const MAX_COLUMN_WIDTH: f32 = 480.;
/// 文件名列拖宽上限（长文件名需要比其他列更宽的空间）。
const FILE_NAME_MAX_WIDTH: f32 = 1600.;
/// 表头高：取 `density.control`（默认 28），随文字增大；DataTable 的 `Size` 同时决定
/// 表头行高，任务行高由 `render_tr` 按密度覆盖。
pub(crate) fn table_header_height(cx: &App) -> Pixels {
    active_theme(cx).density().control
}
/// 表格右侧留白（`render_last_empty_col`），与选择列内含的左侧留白对称（≈ spacing.sm）。
const TABLE_TRAILING_GUTTER: f32 = 8.;
/// 表头列分隔线高（常驻的拖宽提示，悬停时由拖宽柄全高高亮接管）。
const COLUMN_DIVIDER_HEIGHT: f32 = 14.;
/// 数据列左右内边距：显式设给每列，进度条宽度据此推算。
const CELL_PADDING_X: f32 = 8.;
/// 进度列百分比区宽：容纳 12px 等宽数字「99%」。
const PROGRESS_LABEL_WIDTH: f32 = 32.;
/// 进度条与百分比的间距。
const PROGRESS_GAP: f32 = 8.;
/// 暂停态进度条：`statusPaused` 的 40%。
const PAUSED_BAR_ALPHA: f32 = 0.4;
/// 行悬停操作按钮边长。
const ROW_ACTION_SIZE: f32 = 24.;
/// 舒适密度下文件图标相对 `icon.lg`（16）的倍数：24px，与双行文字块（约 34px）协调。
const FILE_ICON_COMFORTABLE_SCALE: f32 = 1.5;
/// 宽松密度下文件图标相对 `icon.lg` 的倍数：32px，与三行主列（名称 / 进度条 / 元信息）协调。
const FILE_ICON_RELAXED_SCALE: f32 = 2.;
/// 选中底色左右内缩（inset 样式）。
const SELECTED_INSET_X: f32 = 4.;
/// 任务行 group 名：选择列复选框与行操作按钮随行悬停显隐。
const ROW_GROUP: &str = "download-task-row";
/// 表头全选格 group 名：悬停该格时显示全选框。
const SELECT_ALL_GROUP: &str = "download-select-all";
/// 超过一天的剩余时间估算不可信，不显示。
const MAX_ETA_SECS: u64 = 86_400;
/// 表头按下与抬起的位移超过它视为拖动列（不触发排序）。
const HEADER_CLICK_SLOP: f32 = 4.;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DownloadColumnKind {
    FileName,
    Progress,
    Size,
    Speed,
    Eta,
    Status,
    Created,
    Protocol,
    Source,
    Queue,
}

impl DownloadColumnKind {
    /// 默认列顺序（`columns` 偏好为空或缺列时按此补齐）。
    pub(crate) const ALL: [Self; 10] = [
        Self::FileName,
        Self::Size,
        Self::Progress,
        Self::Status,
        Self::Created,
        Self::Speed,
        Self::Eta,
        Self::Protocol,
        Self::Source,
        Self::Queue,
    ];

    pub(crate) fn key(self) -> &'static str {
        match self {
            Self::FileName => "file_name",
            Self::Progress => "progress",
            Self::Size => "size",
            Self::Speed => "speed",
            Self::Eta => "eta",
            Self::Status => "status",
            Self::Created => "created",
            Self::Protocol => "protocol",
            Self::Source => "source",
            Self::Queue => "queue",
        }
    }

    fn from_key(key: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.key() == key)
    }

    /// 速度 / 剩余时间已并入「状态」列的活动文案，默认隐藏（仍可在列设置打开）。
    fn default_visible(self) -> bool {
        matches!(
            self,
            Self::FileName | Self::Size | Self::Progress | Self::Status | Self::Created
        )
    }

    /// 宽松密度把大小 / 进度 / 速度 / 剩余时间并入文件名主列（Web `RELAXED_MERGED_COLUMNS` 镜像）。
    fn merged_in_relaxed(self) -> bool {
        matches!(self, Self::Size | Self::Progress | Self::Speed | Self::Eta)
    }

    /// 列在当前密度下是否实际渲染：宽松密度主列恒显示、被并入的列不单独显示；
    /// 列偏好（`visible`）本身不改，切回其他密度原样恢复。
    fn shown(self, visible: bool, density: ViewDensity) -> bool {
        match density {
            ViewDensity::Relaxed if self == Self::FileName => true,
            ViewDensity::Relaxed => visible && !self.merged_in_relaxed(),
            ViewDensity::Comfortable | ViewDensity::Compact => visible,
        }
    }

    /// 文件名列的值只在容器宽度未知时使用；已知后由 [`DownloadTableDelegate`]
    /// 按容器宽回算（吸收剩余宽度），用户拖过后改用 `ViewPrefs::file_name_width`。
    fn default_width(self) -> f32 {
        match self {
            Self::FileName => 240.,
            Self::Progress => 150.,
            Self::Size => 84.,
            Self::Speed => 90.,
            Self::Eta => 84.,
            Self::Status => 140.,
            Self::Created => 150.,
            Self::Protocol => 64.,
            Self::Source => 148.,
            Self::Queue => 88.,
        }
    }

    fn min_width(self) -> f32 {
        match self {
            Self::FileName => 160.,
            Self::Progress => 110.,
            Self::Size => 64.,
            Self::Speed => 72.,
            Self::Eta => 64.,
            Self::Status => 84.,
            Self::Created => 140.,
            Self::Protocol => 56.,
            Self::Source => 96.,
            Self::Queue => 72.,
        }
    }

    /// 文字缩放 `scale` 下列的显示宽：等于默认宽的列（用户没拖过）整体随文字放大，
    /// 其余保持用户宽度，但不低于放大后的最小宽。`scale` 为 1 时恒等于 `width`
    /// （不小于 [`Self::min_width`]）。
    fn display_width(self, width: f32, scale: f32) -> f32 {
        let min_width = self.min_width() * scale;
        if (width - self.default_width()).abs() < 0.5 {
            (width * scale).max(min_width)
        } else {
            width.max(min_width)
        }
    }

    /// 数字列：单元格与表头右对齐。
    fn is_numeric(self) -> bool {
        matches!(self, Self::Size | Self::Speed | Self::Eta | Self::Created)
    }

    /// 点击表头切换到的排序键。
    fn sort_key(self) -> Option<ViewSortKey> {
        match self {
            Self::FileName => Some(ViewSortKey::Name),
            Self::Progress => Some(ViewSortKey::Progress),
            Self::Size => Some(ViewSortKey::Size),
            Self::Speed => Some(ViewSortKey::Speed),
            Self::Created => Some(ViewSortKey::Created),
            Self::Status => Some(ViewSortKey::Status),
            _ => None,
        }
    }

    /// 首次切到该列排序时的方向：名称 A→Z，数值类从大到新。
    fn default_sort_dir(self) -> SortDir {
        match self {
            Self::FileName => SortDir::Asc,
            _ => SortDir::Desc,
        }
    }

    pub(crate) fn label(self, strings: &DownloadStrings) -> SharedString {
        match self {
            Self::FileName => strings.col_file_name.clone(),
            Self::Progress => strings.col_progress.clone(),
            Self::Size => strings.col_size.clone(),
            Self::Speed => strings.col_speed.clone(),
            Self::Eta => strings.col_eta.clone(),
            Self::Status => strings.col_status.clone(),
            Self::Created => strings.col_created.clone(),
            Self::Protocol => strings.col_protocol.clone(),
            Self::Source => strings.col_source.clone(),
            Self::Queue => strings.col_queue.clone(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct DownloadColumn {
    pub(crate) kind: DownloadColumnKind,
    pub(crate) width: f32,
    /// 用户开关（右上角列设置）；是否显示只由它决定，超宽走横向滚动。
    pub(crate) visible: bool,
}

impl DownloadColumn {
    fn new(kind: DownloadColumnKind) -> Self {
        Self {
            kind,
            width: kind.default_width(),
            visible: kind.default_visible(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct DraggedColumnMenuItem {
    pub(crate) kind: DownloadColumnKind,
    pub(crate) label: SharedString,
}

impl Render for DraggedColumnMenuItem {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = active_theme(cx);
        let tokens = theme.tokens();
        h_flex()
            .px(tokens.spacing.md)
            .py(tokens.spacing.xs)
            .gap(tokens.spacing.sm)
            .rounded(tokens.radius.sm)
            .border(theme.extended().stroke.thin)
            .border_color(tokens.colors.border)
            .bg(tokens.colors.surface)
            .shadow(tokens.shadow.sm.clone())
            .text_size(tokens.typography.sm.size)
            .line_height(tokens.typography.sm.line_height)
            .text_color(tokens.colors.surface_foreground)
            .child(Icon::new(FluxIcon::GripVertical).size(theme.extended().icon.md))
            .child(self.label.clone())
    }
}

/// 表格可见行：任务行或分组头。
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum VisibleRow {
    Task(RowId),
    GroupHeader {
        key: String,
        label: SharedString,
        count: usize,
        collapsed: bool,
    },
}

/// 分组计算结果。
struct GroupBucket {
    key: String,
    label: SharedString,
    order: i64,
}

/// 上次计算的侧栏计数及其有效期键（存储 generation + 分类索引身份）。
struct CountsCache {
    generation: u64,
    categories: Rc<CategoryIndex>,
    counts: Rc<SidebarCounts>,
}

/// 按 key 分桶：桶顺序为 key 首次出现的顺序，桶元数据取首个成员的；用 key → 下标的
/// 哈希表定位，分桶数很多时（如按站点分组）仍是线性。
fn group_by_key<B, V>(
    items: impl IntoIterator<Item = (B, V)>,
    key_of: impl Fn(&B) -> &str,
) -> Vec<(B, Vec<V>)> {
    let mut index: HashMap<String, usize> = HashMap::new();
    let mut groups: Vec<(B, Vec<V>)> = Vec::new();
    for (bucket, value) in items {
        match index.get(key_of(&bucket)) {
            Some(&ix) => groups[ix].1.push(value),
            None => {
                index.insert(key_of(&bucket).to_owned(), groups.len());
                groups.push((bucket, vec![value]));
            }
        }
    }
    groups
}

/// 侧栏选中项到表格筛选的投影。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TableFilter {
    Download(DownloadFilter),
    Queue(String),
    Device(String),
    Group(String),
}

impl From<&SidebarSelection> for TableFilter {
    fn from(selection: &SidebarSelection) -> Self {
        match selection {
            SidebarSelection::Download(filter) => Self::Download(filter.clone()),
            SidebarSelection::Queue(id) => Self::Queue(id.clone()),
            SidebarSelection::Device(id) => Self::Device(id.clone()),
        }
    }
}

/// 右键菜单单任务事实：足以判定菜单项可见性的最小状态快照，与
/// [`DownloadTaskView`] 解耦以便纯函数测试。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TaskMenuFacts {
    pub(crate) is_local: bool,
    /// 任务可被控制（暂停 / 继续 / 删除）。远程任务云端状态未知时为 `false`。
    pub(crate) controllable: bool,
    pub(crate) state: TaskState,
    pub(crate) boosted: bool,
    /// `url` 是否为 BT 哨兵值 `torrent-file://…`（不可重新下载）。
    pub(crate) is_torrent_sentinel: bool,
    /// 失败态且错误消息带插件重试前缀。
    pub(crate) is_plugin_retry_error: bool,
    /// 已完成但产物已不在下载目录（文件跟踪扫描结果）。
    pub(crate) file_missing: bool,
}

/// 引擎/daemon 插件系统失败任务的错误消息前缀。
pub(crate) const PLUGIN_ERROR_PREFIX: &str = "[插件]";

/// 任务右键菜单条目；数组顺序即渲染顺序。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MenuEntry {
    Resume,
    Pause,
    IgnorePluginRetry,
    Boost,
    CancelBoost,
    OpenFile,
    OpenFolder,
    Rename,
    Redownload,
    CopyUrl,
    MoveToQueue,
    Separator,
    Delete,
    DeleteWithFiles,
    /// 在主窗口停靠详情面板中查看（独立窗口经面板头部「在独立窗口打开」）。
    ShowDetail,
}

/// 纯函数：按选中任务集合的事实计算应显示的右键菜单项。
///
/// 多选时每一项都要求对**全部**选中任务成立（交集语义），resume/pause 例外
/// ——只要选中集合中**存在**一个可恢复/可暂停的任务就显示（与工具栏按钮行为
/// 一致：工具栏无条件对每个选中项下发命令，无效状态的任务被服务端忽略）。
pub(crate) fn context_menu_items(selection: &[TaskMenuFacts]) -> Vec<MenuEntry> {
    if selection.is_empty() {
        return Vec::new();
    }
    let mut items = Vec::new();
    if selection.iter().any(|task| {
        task.controllable && !matches!(task.state, TaskState::Completed | TaskState::Downloading)
    }) {
        items.push(MenuEntry::Resume);
    }
    if selection.iter().any(|task| {
        task.controllable && matches!(task.state, TaskState::Downloading | TaskState::Pending)
    }) {
        items.push(MenuEntry::Pause);
    }
    if let [only] = selection {
        if only.state == TaskState::Failed && only.is_plugin_retry_error {
            items.push(MenuEntry::IgnorePluginRetry);
        }
        if only.is_local {
            items.push(if only.boosted {
                MenuEntry::CancelBoost
            } else {
                MenuEntry::Boost
            });
        }
    }
    if selection
        .iter()
        .all(|task| task.is_local && task.state == TaskState::Completed && !task.file_missing)
    {
        items.push(MenuEntry::OpenFile);
    }
    if selection.iter().all(|task| task.is_local) {
        items.push(MenuEntry::OpenFolder);
    }
    if let [only] = selection
        && only.is_local
    {
        items.push(MenuEntry::Rename);
    }
    if selection.iter().all(|task| {
        task.is_local
            && matches!(task.state, TaskState::Completed | TaskState::Failed)
            && !task.is_torrent_sentinel
    }) {
        items.push(MenuEntry::Redownload);
    }
    items.push(MenuEntry::CopyUrl);
    if selection.iter().all(|task| task.is_local) {
        items.push(MenuEntry::MoveToQueue);
    }
    items.push(MenuEntry::Separator);
    if selection.iter().any(|task| task.controllable) {
        items.push(MenuEntry::Delete);
        items.push(MenuEntry::DeleteWithFiles);
    }
    if selection.iter().any(|task| task.is_local) {
        items.push(MenuEntry::ShowDetail);
    }
    items
}

/// 构造一个分组头右键菜单项：点击时把 `group_id` 回调进 [`DownloadView`] 的方法
/// （无需 `Window`，用于批量命令类操作）。
fn group_menu_item(
    label: SharedString,
    icon: FluxIcon,
    host: &WeakEntity<DownloadView>,
    group_id: &str,
    action: fn(&mut DownloadView, String, &mut Context<DownloadView>),
) -> PopupMenuItem {
    let host = host.clone();
    let group_id = group_id.to_owned();
    PopupMenuItem::new(label)
        .icon(icon)
        .on_click(move |_, _, cx| {
            let group_id = group_id.clone();

            let Ok(()) = host.update(cx, |view, cx| action(view, group_id, cx)) else {
                // 视图已释放，结束这次回调而不再更新状态。
                return;
            };
        })
}

/// 同 [`group_menu_item`]，但回调需要 `Window`（打开确认对话框 / 独立窗口）。
fn group_menu_item_windowed(
    label: SharedString,
    icon: FluxIcon,
    host: &WeakEntity<DownloadView>,
    group_id: &str,
    action: fn(&mut DownloadView, String, &mut Window, &mut Context<DownloadView>),
) -> PopupMenuItem {
    let host = host.clone();
    let group_id = group_id.to_owned();
    PopupMenuItem::new(label)
        .icon(icon)
        .on_click(move |_, window, cx| {
            let group_id = group_id.clone();

            let Ok(()) = host.update(cx, |view, cx| action(view, group_id, window, cx)) else {
                // 视图已释放，结束这次回调而不再更新状态。
                return;
            };
        })
}

pub(crate) struct DownloadTableDelegate {
    pub(crate) strings: DownloadStrings,
    pub(crate) columns: Vec<DownloadColumn>,
    store: Rc<TaskStore>,
    categories: Rc<CategoryIndex>,
    /// 侧栏计数缓存（侧栏渲染经不可变引用读取，故内部可变）。
    counts: RefCell<Option<CountsCache>>,
    visible: Vec<VisibleRow>,
    seen_generation: u64,
    /// 上次重算时存储的结构计数；未变说明只有行内容变化，可沿用上次的行顺序。
    seen_structure: u64,
    /// 全量排序所得的分组前顺序，不是 RowOrder 保持期内显示的顺序。
    sorted_rows: Vec<RowId>,
    seen_sort_values: u64,
    seen_view_fields: u64,
    sorted_group_date: Option<chrono::NaiveDate>,
    #[cfg(test)]
    force_full_sort: bool,
    #[cfg(test)]
    full_sort_count: usize,
    view_dirty: bool,
    /// 行顺序稳定器：指针活动期间 / 动态排序键限频内推迟内容变化引起的重排。
    row_order: RowOrder,
    /// 被推迟的重排的补偿定时器（到期后强制按最新排序重排）。
    reorder_timer: Option<Task<()>>,
    selected_tasks: HashSet<RowKey>,
    selection_anchor: Option<RowKey>,
    /// 显式多选模式：仅 Cmd/Ctrl+点击或全选进入，普通单击 / 清空选择退出；
    /// 行首复选框只在该模式且有选中项时显示（与 Web `multiSelect` 对齐）。
    multi_select: bool,
    filter: TableFilter,
    query: String,
    prefs: ViewPrefs,
    /// queue_id → 名称（列 / 分组标签）。
    queue_names: Vec<(String, String)>,
    /// group_id → 名称。
    group_names: HashMap<String, String>,
    /// 远程任务目标设备显示名：设备 id / 指纹 → 已消歧的名称。
    device_names: HashMap<String, String>,
    /// 表格容器宽（`render_download_table` 在 prepaint 测得、帧外写入）；0 = 尚未测得。
    viewport_width: f32,
    /// 最近一次交给表格的文件名列宽（`column()` 写入）；与期望值不同才需 refresh。
    applied_file_width: Cell<f32>,
    /// 最近一次交给表格的文字缩放（界面缩放 × 字号缩放，`column()` 写入）：
    /// 列的最小宽 / 默认宽按它放大，与当前主题不同即需 refresh 重建列。
    text_scale: Cell<f32>,
    /// 列配置（宽度 / 可见性 / 顺序 / 排序指示）需要重建表头 `col_groups`。
    /// 纯行数据变化不置位：`TableState::refresh` 会用代理宽度覆盖表格内部
    /// 实时宽度，进度节拍若每次都刷新会把用户正在拖拽的列宽弹回去。
    columns_dirty: bool,
    /// 上级页面弱引用：右键菜单需要参数的命令（移动到队列 / 忽略插件重试 /
    /// 任务组操作）经它回调 `DownloadView` 的方法执行；`None` 时这些项不渲染。
    host: Option<WeakEntity<DownloadView>>,
    /// 右键菜单动作分派目标；`None` 时菜单项不带 action_context（仍可点击，
    /// 只是动作从菜单自身的焦点路径冒泡）。
    action_context: Option<FocusHandle>,
}

impl DownloadTableDelegate {
    pub(crate) fn new(strings: DownloadStrings, store: Rc<TaskStore>) -> Self {
        Self {
            strings,
            columns: DownloadColumnKind::ALL
                .into_iter()
                .map(DownloadColumn::new)
                .collect(),
            store,
            categories: Rc::new(CategoryIndex::default()),
            counts: RefCell::new(None),
            visible: Vec::new(),
            seen_generation: u64::MAX,
            seen_structure: u64::MAX,
            sorted_rows: Vec::new(),
            seen_sort_values: u64::MAX,
            seen_view_fields: u64::MAX,
            sorted_group_date: None,
            #[cfg(test)]
            force_full_sort: false,
            #[cfg(test)]
            full_sort_count: 0,
            view_dirty: true,
            row_order: RowOrder::default(),
            reorder_timer: None,
            selected_tasks: HashSet::new(),
            selection_anchor: None,
            multi_select: false,
            filter: TableFilter::Download(DownloadFilter::ALL),
            query: String::new(),
            prefs: ViewPrefs::default(),
            queue_names: Vec::new(),
            group_names: HashMap::new(),
            device_names: HashMap::new(),
            viewport_width: 0.,
            applied_file_width: Cell::new(0.),
            text_scale: Cell::new(1.),
            columns_dirty: false,
            host: None,
            action_context: None,
        }
    }

    pub(crate) fn set_strings(&mut self, strings: DownloadStrings) {
        self.strings = strings;
        self.view_dirty = true;
    }

    /// 注入宿主弱引用；右键菜单中需要参数的命令经它回调
    /// [`DownloadView`] 的方法执行。
    pub(crate) fn set_host(&mut self, host: WeakEntity<DownloadView>) {
        self.host = Some(host);
    }

    /// 注入右键菜单动作分派目标（根元素的 focus handle）。
    pub(crate) fn set_action_context(&mut self, handle: FocusHandle) {
        self.action_context = Some(handle);
    }

    pub(crate) fn set_categories(&mut self, categories: Rc<CategoryIndex>) {
        if !Rc::ptr_eq(&self.categories, &categories) {
            self.categories = categories;
            self.view_dirty = true;
        }
    }

    pub(crate) fn set_queue_names(&mut self, queues: Vec<(String, String)>) {
        if self.queue_names != queues {
            self.queue_names = queues;
            self.view_dirty = true;
        }
    }

    pub(crate) fn set_group_names(&mut self, groups: HashMap<String, String>) {
        if self.group_names != groups {
            self.group_names = groups;
            self.view_dirty = true;
        }
    }

    pub(crate) fn set_device_names(&mut self, names: HashMap<String, String>) {
        if self.device_names != names {
            self.device_names = names;
            self.view_dirty = true;
        }
    }

    pub(crate) fn set_filter(&mut self, filter: TableFilter) {
        if self.filter != filter {
            self.filter = filter;
            self.view_dirty = true;
        }
    }

    pub(crate) fn set_query(&mut self, query: &str) {
        let query = query.trim().to_lowercase();
        if self.query != query {
            self.query = query;
            self.view_dirty = true;
        }
    }

    pub(crate) fn prefs(&self) -> &ViewPrefs {
        &self.prefs
    }

    /// 替换视图偏好（密度 / 分组 / 排序 / 列 / 折叠）。
    pub(crate) fn set_prefs(&mut self, prefs: ViewPrefs) {
        if self.prefs == prefs {
            return;
        }
        self.apply_column_prefs(&prefs.columns);
        self.columns_dirty = true;
        self.prefs = prefs;
        self.view_dirty = true;
    }

    pub(crate) fn prefs_mut(&mut self) -> &mut ViewPrefs {
        self.view_dirty = true;
        &mut self.prefs
    }

    /// 列配置是否需要 `TableState::refresh`（读取后清零）。
    pub(crate) fn take_columns_dirty(&mut self) -> bool {
        std::mem::take(&mut self.columns_dirty)
    }

    /// 表头拖拽调宽结束后，把表格内部列宽（含首列勾选列）写回代理列配置，
    /// 作为后续刷新与持久化的事实源。返回是否有变化。
    ///
    /// 文件名列默认由容器宽回算（吸收剩余宽度）；表格给出的宽度与最近一次应用值
    /// 不同即说明用户拖过它，此后固定为 `prefs.file_name_width`，不再随容器伸缩。
    /// 其他列变宽后，下一帧 `render_download_table` 的 prepaint 发现未固定的
    /// 文件名列宽过期，帧外 refresh 补足。拖拽进行中代理宽度不变，因此不会触发
    /// refresh 把拖拽中的列宽弹回。
    pub(crate) fn sync_column_widths(&mut self, widths: &[Pixels]) -> bool {
        let applied_file_width = self.applied_file_width.get();
        let text_scale = self.text_scale.get();
        let mut file_name_width = None;
        let mut changed = false;
        let density = self.prefs.density;
        let shown = self
            .columns
            .iter_mut()
            .filter(|column| column.kind.shown(column.visible, density));
        for (column, width) in shown.zip(widths.iter().skip(1)) {
            let width = f32::from(*width);
            if column.kind == DownloadColumnKind::FileName {
                if (width - applied_file_width).abs() >= 0.5 {
                    file_name_width = Some(width.clamp(
                        DownloadColumnKind::FileName.min_width() * text_scale,
                        FILE_NAME_MAX_WIDTH,
                    ));
                }
                continue;
            }
            // 表格给出的是放大后的显示宽；与显示宽一致说明用户没拖过，不把放大值写成偏好。
            if (width - column.kind.display_width(column.width, text_scale)).abs() < 0.5 {
                continue;
            }
            let width = width.clamp(
                column.kind.min_width() * text_scale,
                MAX_COLUMN_WIDTH * text_scale,
            );
            if (column.width - width).abs() > f32::EPSILON {
                column.width = width;
                changed = true;
            }
        }
        if let Some(width) = file_name_width {
            self.prefs.file_name_width = Some(width);
            self.applied_file_width.set(width);
            changed = true;
        }
        changed
    }

    /// 容器宽为 `viewport` 时文件名列应得的宽度。用户拖过则取固定宽；否则
    /// `max(min_width, 容器宽 − 选择列 − 其他可见列 − 右侧留白 − 竖滚动条预留)`，
    /// 容器宽未知（≤ 0）时退回列配置宽度。
    fn file_name_width_for(&self, viewport: f32) -> f32 {
        let text_scale = self.text_scale.get();
        let min_width = DownloadColumnKind::FileName.min_width() * text_scale;
        if let Some(width) = self.prefs.file_name_width {
            return width.clamp(min_width, FILE_NAME_MAX_WIDTH);
        }
        if viewport <= 0. {
            return self
                .columns
                .iter()
                .find(|column| column.kind == DownloadColumnKind::FileName)
                .map_or(min_width, |column| column.width.max(min_width));
        }
        let others: f32 = self
            .columns
            .iter()
            .filter(|column| {
                column.kind != DownloadColumnKind::FileName
                    && column.kind.shown(column.visible, self.prefs.density)
            })
            .map(|column| column.kind.display_width(column.width, text_scale))
            .sum();
        let reserved =
            SELECTION_COLUMN_WIDTH + others + TABLE_TRAILING_GUTTER + f32::from(Scrollbar::width());
        (viewport - reserved).max(min_width)
    }

    fn file_name_visible(&self) -> bool {
        self.columns.iter().any(|column| {
            column.kind == DownloadColumnKind::FileName
                && column.kind.shown(column.visible, self.prefs.density)
        })
    }

    /// 文字缩放（主题的界面缩放 × 字号缩放）与上次交给表格的列不一致，需重建列。
    fn text_scale_stale(&self, text_scale: f32) -> bool {
        (self.text_scale.get() - text_scale).abs() > f32::EPSILON
    }

    /// prepaint 判定：容器宽变化，或文件名列宽与期望不符（例如刚同步了其他列宽）。
    fn viewport_needs_sync(&self, viewport: f32) -> bool {
        (self.viewport_width - viewport).abs() >= 0.5
            || (self.file_name_visible()
                && (self.file_name_width_for(viewport) - self.applied_file_width.get()).abs()
                    >= 0.5)
    }

    /// 按当前容器宽计算文件名列宽并记为「已应用」（`column()` 构建表格列时调用）。
    fn apply_file_name_width(&self) -> f32 {
        let width = self.file_name_width_for(self.viewport_width);
        self.applied_file_width.set(width);
        width
    }

    /// 写入容器宽；返回表格是否需要 `refresh` 以应用新的文件名列宽。
    pub(crate) fn set_viewport_width(&mut self, viewport: f32) -> bool {
        self.viewport_width = viewport;
        self.file_name_visible()
            && (self.file_name_width_for(viewport) - self.applied_file_width.get()).abs() >= 0.5
    }

    /// 表头点击排序，三档循环：列默认方向 → 反方向 → 恢复默认排序（智能：添加顺序）。
    /// 返回偏好是否改变。
    fn toggle_sort(&mut self, kind: DownloadColumnKind) -> bool {
        let Some(key) = kind.sort_key() else {
            return false;
        };
        let default_dir = kind.default_sort_dir();
        if self.prefs.sort_key != key {
            self.prefs.sort_key = key;
            self.prefs.sort_dir = default_dir;
        } else if self.prefs.sort_dir == default_dir {
            self.prefs.sort_dir = match default_dir {
                SortDir::Asc => SortDir::Desc,
                SortDir::Desc => SortDir::Asc,
            };
        } else {
            self.prefs.sort_key = ViewSortKey::Smart;
            self.prefs.sort_dir = SortDir::default();
        }
        self.view_dirty = true;
        true
    }

    fn apply_column_prefs(&mut self, prefs: &[crate::model::view_prefs::ColumnPref]) {
        if prefs.is_empty() {
            return;
        }
        let mut columns: Vec<DownloadColumn> = prefs
            .iter()
            .filter_map(|pref| {
                let kind = DownloadColumnKind::from_key(&pref.key)?;
                let mut column = DownloadColumn::new(kind);
                column.visible = pref.visible;
                if pref.width >= kind.min_width() {
                    column.width = pref.width;
                }
                Some(column)
            })
            .collect();
        for kind in DownloadColumnKind::ALL {
            if !columns.iter().any(|column| column.kind == kind) {
                columns.push(DownloadColumn::new(kind));
            }
        }
        if !columns.iter().any(|column| column.visible) {
            columns[0].visible = true;
        }
        self.columns = columns;
    }

    /// 当前列配置 → 偏好条目。
    pub(crate) fn column_prefs(&self) -> Vec<crate::model::view_prefs::ColumnPref> {
        self.columns
            .iter()
            .map(|column| crate::model::view_prefs::ColumnPref {
                key: column.kind.key().to_owned(),
                visible: column.visible,
                width: column.width,
            })
            .collect()
    }

    /// 存储变化或视图参数变化时重算可见行。返回是否重算。
    pub(crate) fn refresh_view(&mut self) -> bool {
        self.refresh_view_at(Instant::now())
    }

    fn refresh_view_at(&mut self, now: Instant) -> bool {
        let generation = self.store.generation();
        if !self.view_dirty && generation == self.seen_generation {
            return false;
        }
        let structure = self.store.structure_generation();
        let content_only = !self.view_dirty && structure == self.seen_structure;
        self.seen_generation = generation;
        self.seen_structure = structure;
        self.view_dirty = false;
        self.visible = self.compute_visible(content_only, now);
        // 选中集只保留当前筛选 + 搜索下仍在视图内的任务（已删除、被侧栏切换 / 搜索 /
        // 状态变化筛掉的一律移出），选择条计数与快捷键批量动作都只作用于看得见的任务。
        // 折叠分组内的任务仍属于当前视图，不因折叠而取消选中。
        let mut selected = std::mem::take(&mut self.selected_tasks);
        selected.retain(|key| {
            self.store
                .get(key)
                .is_some_and(|task| self.matches_filter(&task) && self.matches_query(&task))
        });
        self.selected_tasks = selected;
        self.selection_anchor = self
            .selection_anchor
            .take()
            .filter(|key| self.selected_tasks.contains(key));
        true
    }

    /// 本地日期跨过零点：按日期分组时「今天 / 昨天」等分桶随之改变，强制重算可见行。
    /// 返回是否需要重绘；不按日期分组时不做任何事。
    pub(crate) fn refresh_for_new_day(&mut self) -> bool {
        if self.prefs.group_by != ViewGroupBy::Date {
            return false;
        }
        self.view_dirty = true;
        self.refresh_view()
    }

    /// 表格内指针活动（移动 / 滚动 / 按下）的记录句柄：推迟由行内容变化引起的重排，
    /// 避免行在光标下跳走。
    pub(crate) fn interaction_hold(&self) -> InteractionHold {
        self.row_order.interaction_hold()
    }

    fn reorder_deadline(&self) -> Option<Instant> {
        self.row_order.deadline(self.prefs.sort_key.is_live())
    }

    fn needs_reorder_timer(&self) -> bool {
        self.reorder_timer.is_none() && self.reorder_deadline().is_some()
    }

    fn matches_query(&self, task: &DownloadTaskView) -> bool {
        if self.query.is_empty() {
            return true;
        }
        task.name_fold.contains(&self.query)
            || task.url_fold.contains(&self.query)
            || task.site_fold.contains(&self.query)
    }

    fn matches_filter(&self, task: &DownloadTaskView) -> bool {
        match &self.filter {
            TableFilter::Download(filter) => filter.matches(task, &self.categories),
            TableFilter::Queue(queue_id) => {
                task.source == TaskSource::Local && task.queue_id == *queue_id
            }
            TableFilter::Device(device) => SidebarSelection::device_matches(device, task),
            TableFilter::Group(group_id) => task.group_id == *group_id,
        }
    }

    fn compute_visible(&mut self, content_only: bool, now: Instant) -> Vec<VisibleRow> {
        let store = Rc::clone(&self.store);
        let local = store.local();
        let remote = store.remote();
        let sort_values = store.sort_generation(self.prefs.sort_key);
        let view_fields = store.view_fields_generation();
        let group_date =
            (self.prefs.group_by == ViewGroupBy::Date).then(|| chrono::Local::now().date_naive());
        let reuse_sorted = content_only
            && sort_values == self.seen_sort_values
            && view_fields == self.seen_view_fields
            && group_date == self.sorted_group_date;
        #[cfg(test)]
        let reuse_sorted = reuse_sorted && !self.force_full_sort;
        let mut rows: Vec<(RowId, &DownloadTaskView)> = if reuse_sorted {
            self.sorted_rows
                .iter()
                .filter_map(|&id| {
                    let task = match id {
                        RowId::Local(ix) => local.get(ix),
                        RowId::Remote(ix) => remote.get(ix),
                    };
                    task.map(|task| (id, task))
                })
                .collect()
        } else {
            local
                .iter()
                .enumerate()
                .map(|(ix, task)| (RowId::Local(ix), task))
                .chain(
                    remote
                        .iter()
                        .enumerate()
                        .map(|(ix, task)| (RowId::Remote(ix), task)),
                )
                .filter(|(_, task)| self.matches_filter(task) && self.matches_query(task))
                .collect()
        };
        if !reuse_sorted {
            rows.sort_by(|(_, left), (_, right)| self.prefs.compare(left, right));
            self.sorted_rows.clear();
            self.sorted_rows.extend(rows.iter().map(|(id, _)| *id));
            self.seen_sort_values = sort_values;
            self.seen_view_fields = view_fields;
            self.sorted_group_date = group_date;
            #[cfg(test)]
            {
                self.full_sort_count += 1;
            }
        }
        // 即使排序输入不变，也照常应用保持期：过期顺序和动态键的重排时钟必须一致。
        let live_key = self.prefs.sort_key.is_live();
        let rows = self.row_order.apply(rows, content_only, live_key, now);

        if self.prefs.group_by == ViewGroupBy::None {
            return rows
                .into_iter()
                .map(|(id, _)| VisibleRow::Task(id))
                .collect();
        }

        let mut buckets = group_by_key(
            rows.into_iter()
                .map(|(id, task)| (self.group_bucket(task), id)),
            |bucket: &GroupBucket| bucket.key.as_str(),
        );
        buckets.sort_by(|(left, _), (right, _)| {
            left.order
                .cmp(&right.order)
                .then_with(|| left.label.cmp(&right.label))
        });
        let mut visible = Vec::new();
        for (bucket, ids) in buckets {
            let collapsed = self.prefs.is_group_collapsed(&bucket.key);
            visible.push(VisibleRow::GroupHeader {
                key: bucket.key,
                label: bucket.label,
                count: ids.len(),
                collapsed,
            });
            if !collapsed {
                visible.extend(ids.into_iter().map(VisibleRow::Task));
            }
        }
        visible
    }

    fn group_bucket(&self, task: &DownloadTaskView) -> GroupBucket {
        let strings = &self.strings;
        match self.prefs.group_by {
            ViewGroupBy::None => GroupBucket {
                key: String::new(),
                label: SharedString::default(),
                order: 0,
            },
            ViewGroupBy::Status => GroupBucket {
                key: format!("status:{}", state_group_key(task.state)),
                label: strings.state_label(task.state),
                order: i64::from(task.state.status_rank()),
            },
            ViewGroupBy::Date => {
                let bucket = DateBucket::of(task.created_at_secs);
                GroupBucket {
                    key: format!("date:{}", bucket.key()),
                    label: strings.date_bucket_label(bucket),
                    order: bucket as i64,
                }
            }
            ViewGroupBy::Type => {
                let id = self.categories.category_of(task);
                let (label, order) = self
                    .categories
                    .rules()
                    .iter()
                    .enumerate()
                    .find(|(_, rule)| rule.dto.id == id)
                    .map_or_else(
                        || (strings.category_other.clone(), i64::MAX),
                        |(ix, rule)| (strings.category_label(&rule.dto), ix as i64),
                    );
                GroupBucket {
                    key: format!("type:{id}"),
                    label,
                    order,
                }
            }
            ViewGroupBy::Queue => {
                if task.source == TaskSource::Remote {
                    return GroupBucket {
                        key: "queue:remote".to_owned(),
                        label: strings.remote_tasks.clone(),
                        order: i64::MAX,
                    };
                }
                let (label, order) = self
                    .queue_names
                    .iter()
                    .enumerate()
                    .find(|(_, (id, _))| *id == task.queue_id)
                    .map_or_else(
                        || (SharedString::from(task.queue_id.clone()), i64::MAX - 1),
                        |(ix, (_, name))| (SharedString::from(name.clone()), ix as i64),
                    );
                GroupBucket {
                    key: format!("queue:{}", task.queue_id),
                    label,
                    order,
                }
            }
            ViewGroupBy::Site => {
                let site = task.source_site();
                let label = if site.is_empty() {
                    strings.site_unknown(task)
                } else {
                    SharedString::from(site.to_owned())
                };
                GroupBucket {
                    key: format!("site:{}", label),
                    label,
                    order: 0,
                }
            }
            ViewGroupBy::Group => {
                if task.group_id.is_empty() {
                    return GroupBucket {
                        key: "group:".to_owned(),
                        label: strings.ungrouped.clone(),
                        order: i64::MAX,
                    };
                }
                let label = self
                    .group_names
                    .get(&task.group_id)
                    .cloned()
                    .unwrap_or_else(|| task.group_id.clone());
                GroupBucket {
                    key: format!("group:{}", task.group_id),
                    label: SharedString::from(label),
                    order: 0,
                }
            }
        }
    }

    fn visible_task_ids(&self) -> impl Iterator<Item = RowId> + '_ {
        self.visible.iter().filter_map(|row| match row {
            VisibleRow::Task(id) => Some(*id),
            VisibleRow::GroupHeader { .. } => None,
        })
    }

    fn visible_task_keys(&self) -> Vec<RowKey> {
        self.visible_task_ids()
            .filter_map(|id| self.store.row(id).map(|row| row.key.clone()))
            .collect()
    }

    /// 侧栏计数：按存储 generation 与分类索引缓存，一次扫描得出全部桶。
    fn sidebar_counts(&self) -> Rc<SidebarCounts> {
        let generation = self.store.generation();
        let mut cache = self.counts.borrow_mut();
        if let Some(cached) = cache.as_ref()
            && cached.generation == generation
            && Rc::ptr_eq(&cached.categories, &self.categories)
        {
            return Rc::clone(&cached.counts);
        }
        let local = self.store.local();
        let remote = self.store.remote();
        let counts = Rc::new(SidebarCounts::compute(
            local.iter().chain(remote.iter()),
            &self.categories,
        ));
        *cache = Some(CountsCache {
            generation,
            categories: Rc::clone(&self.categories),
            counts: Rc::clone(&counts),
        });
        counts
    }

    pub(crate) fn count_matching(&self, filter: &DownloadFilter) -> usize {
        self.sidebar_counts().filter(filter)
    }

    pub(crate) fn count_in_queue(&self, queue_id: &str) -> usize {
        self.sidebar_counts().queue(queue_id)
    }

    /// 设备计数：与表格筛选同一规则（[`SidebarSelection::device_matches`]）。
    pub(crate) fn count_device(&self, device_id: &str) -> usize {
        self.sidebar_counts().device(device_id)
    }

    fn shown_columns_count(&self) -> usize {
        self.columns
            .iter()
            .filter(|column| column.kind.shown(column.visible, self.prefs.density))
            .count()
    }

    fn shown_column(&self, col_ix: usize) -> Option<&DownloadColumn> {
        if col_ix == 0 {
            return None;
        }
        self.columns
            .iter()
            .filter(|column| column.kind.shown(column.visible, self.prefs.density))
            .nth(col_ix - 1)
    }

    fn move_shown_column(&mut self, from_ix: usize, to_ix: usize) {
        if from_ix == 0 || to_ix == 0 {
            return;
        }
        let positions: Vec<_> = self
            .columns
            .iter()
            .enumerate()
            .filter_map(|(ix, column)| {
                column
                    .kind
                    .shown(column.visible, self.prefs.density)
                    .then_some(ix)
            })
            .collect();
        let Some(&from_position) = positions.get(from_ix - 1) else {
            return;
        };
        let Some(&to_position) = positions.get(to_ix - 1) else {
            return;
        };
        let column = self.columns.remove(from_position);
        self.columns.insert(to_position, column);
    }

    pub(crate) fn move_column_kind(&mut self, from: DownloadColumnKind, to: DownloadColumnKind) {
        let Some(from_ix) = self.columns.iter().position(|column| column.kind == from) else {
            return;
        };
        let Some(to_ix) = self.columns.iter().position(|column| column.kind == to) else {
            return;
        };
        if from_ix == to_ix {
            return;
        }
        let column = self.columns.remove(from_ix);
        self.columns.insert(to_ix, column);
    }

    pub(crate) fn set_column_visible(&mut self, kind: DownloadColumnKind, visible: bool) -> bool {
        if !visible && self.columns.iter().filter(|column| column.visible).count() <= 1 {
            return false;
        }
        let Some(column) = self.columns.iter_mut().find(|column| column.kind == kind) else {
            return false;
        };
        if column.visible == visible {
            return false;
        }
        column.visible = visible;
        true
    }

    /// 恢复默认列集；文件名列同时回到「吸收剩余宽度」模式。
    pub(crate) fn reset_columns(&mut self) {
        self.columns = DownloadColumnKind::ALL
            .into_iter()
            .map(DownloadColumn::new)
            .collect();
        self.prefs.file_name_width = None;
    }

    pub(crate) fn select_all_tasks(&mut self) {
        self.selected_tasks.clear();
        self.selected_tasks.extend(self.visible_task_keys());
        self.selection_anchor = None;
        self.multi_select = true;
    }

    pub(crate) fn clear_selection(&mut self) {
        self.selected_tasks.clear();
        self.selection_anchor = None;
        self.multi_select = false;
    }

    /// 是否处于显式多选模式（且仍有选中项）：控制行首复选框、表头全选框与表头选择条。
    pub(crate) fn in_multi_select(&self) -> bool {
        self.multi_select && !self.selected_tasks.is_empty()
    }

    pub(crate) fn select_task(&mut self, key: RowKey, modifiers: Modifiers) {
        if modifiers.shift
            && let Some(anchor) = self.selection_anchor.clone()
        {
            let keys = self.visible_task_keys();
            if let Some(anchor_index) = keys.iter().position(|item| *item == anchor)
                && let Some(task_index) = keys.iter().position(|item| *item == key)
            {
                let (start, end) = if anchor_index <= task_index {
                    (anchor_index, task_index)
                } else {
                    (task_index, anchor_index)
                };
                self.selected_tasks.clear();
                self.selected_tasks
                    .extend(keys[start..=end].iter().cloned());
                self.selection_anchor = Some(key);
                return;
            }
        }

        if modifiers.secondary() {
            if !self.selected_tasks.remove(&key) {
                self.selected_tasks.insert(key.clone());
            }
            self.multi_select = true;
        } else {
            self.selected_tasks.clear();
            self.selected_tasks.insert(key.clone());
            self.multi_select = false;
        }
        self.selection_anchor = Some(key);
    }

    pub(crate) fn row_key_at(&self, row_ix: usize) -> Option<RowKey> {
        match self.visible.get(row_ix)? {
            VisibleRow::Task(id) => self.store.row(*id).map(|row| row.key.clone()),
            VisibleRow::GroupHeader { .. } => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn group_key_at(&self, row_ix: usize) -> Option<&str> {
        match self.visible.get(row_ix)? {
            VisibleRow::GroupHeader { key, .. } => Some(key),
            VisibleRow::Task(_) => None,
        }
    }

    pub(crate) fn select_task_for_context_menu(&mut self, key: RowKey) {
        if !self.selected_tasks.contains(&key) {
            self.selected_tasks.clear();
            self.selected_tasks.insert(key.clone());
            self.multi_select = false;
        }
        self.selection_anchor = Some(key);
    }

    pub(crate) fn selected_keys(&self) -> Vec<RowKey> {
        let mut keys: Vec<RowKey> = self.selected_tasks.iter().cloned().collect();
        keys.sort();
        keys
    }

    /// 恰好选中一个任务时返回它（停靠详情面板跟随选中用；O(1)，表格高频通知下可放心调用）。
    pub(crate) fn single_selected_key(&self) -> Option<&RowKey> {
        if self.selected_tasks.len() == 1 {
            self.selected_tasks.iter().next()
        } else {
            None
        }
    }

    /// 「详情」要展示的本地任务：优先选区锚点（右键 / 最后点击的行），否则按序第一个本地选中项。
    pub(crate) fn detail_candidate(&self) -> Option<RowKey> {
        self.selection_anchor
            .as_ref()
            .filter(|key| key.is_local() && self.selected_tasks.contains(*key))
            .cloned()
            .or_else(|| self.selected_keys().into_iter().find(RowKey::is_local))
    }

    /// 选中集合投影（选择条 / 工具栏）：只统计仍存在于 store 的选中任务
    /// （已删除任务不算），完整遍历以得到数量与各类可用性。
    pub(crate) fn selection_summary(&self) -> SelectionSummary {
        let mut summary = SelectionSummary::default();
        let mut any_unopenable = false;
        for key in &self.selected_tasks {
            let Some(row) = self.store.get(key) else {
                continue;
            };
            summary.count += 1;
            summary.any = true;
            summary.any_local |= key.is_local();
            any_unopenable |= !row.has_local_file();
            match row.state {
                TaskState::Downloading | TaskState::Pending => summary.any_active = true,
                TaskState::Paused | TaskState::Failed => summary.any_resumable = true,
                TaskState::Completed => {}
            }
        }
        summary.all_openable = summary.any && !any_unopenable;
        summary
    }

    /// 选中任务里仍存在且可打开文件的 key（本地 + 已完成 + 文件仍在下载目录）。
    fn openable_selected_keys(&self) -> Vec<RowKey> {
        self.selected_keys()
            .into_iter()
            .filter(|key| self.store.get(key).is_some_and(|row| row.has_local_file()))
            .collect()
    }

    /// 拖出到系统文件管理器的本机路径：锚点行在选区内时带上整个选区，否则只拖锚点行；
    /// 只取本地、已完成且未被判定丢失的任务（磁盘现状由拖出时再探测）。
    pub(crate) fn drag_paths(&self, anchor: &RowKey) -> Vec<std::path::PathBuf> {
        let keys = if self.selected_tasks.contains(anchor) {
            self.selected_keys()
        } else {
            vec![anchor.clone()]
        };
        keys.iter()
            .filter_map(|key| {
                let row = self.store.get(key)?;
                if row.has_local_file() {
                    row.local_file_path()
                } else {
                    None
                }
            })
            .collect()
    }

    pub(crate) fn toggle_group_collapsed(&mut self, key: &str) {
        self.prefs.toggle_group_collapsed(key);
        self.view_dirty = true;
    }

    /// 文件大类的类别名。
    fn kind_label(&self, kind: FileKind) -> SharedString {
        let strings = &self.strings;
        match kind {
            FileKind::Video => &strings.category_video,
            FileKind::Audio => &strings.category_audio,
            FileKind::Document => &strings.category_document,
            FileKind::Image => &strings.category_image,
            FileKind::Archive | FileKind::DiskImage => &strings.category_archive,
            FileKind::Application | FileKind::Mobile => &strings.category_program,
            FileKind::Other => &strings.category_other,
        }
        .clone()
    }

    /// 状态列主文案：下载中显示「速度 · 剩余时间」（只显示已知部分），其余为状态名。
    fn status_label(&self, task: &DownloadTaskView) -> SharedString {
        match task.remote_status {
            // 取消的远程任务不是「失败」；云端新增的未知状态不冒充任何已知状态。
            Some(RemoteTaskStatus::Canceled) => return self.strings.status_canceled.clone(),
            Some(RemoteTaskStatus::Unknown) => return SharedString::from("—"),
            _ => {}
        }
        if task.state != TaskState::Downloading {
            return self.strings.task_state_label(task);
        }
        // 宽松密度的速度 / 剩余时间已在主列元信息行，状态列只给状态名。
        if self.prefs.density == ViewDensity::Relaxed {
            return self.strings.status_downloading.clone();
        }
        let speed = task
            .speed_bytes_per_second
            .filter(|speed| *speed > 0)
            .map(|speed| format!("{}/s", format_bytes(speed)));
        let eta = task
            .eta_seconds
            .filter(|seconds| *seconds <= MAX_ETA_SECS)
            .map(|seconds| self.strings.format_eta(seconds));
        match (speed, eta) {
            (Some(speed), Some(eta)) => SharedString::from(format!("{speed} · {eta}")),
            (Some(speed), None) => SharedString::from(speed),
            (None, Some(eta)) => eta,
            (None, None) => self.strings.status_downloading.clone(),
        }
    }

    /// 状态列第二行（舒适密度）/ 紧凑密度的悬停提示：已下 / 总量、并发连接或 BT
    /// 节点、失败原因首行。
    fn status_detail(&self, task: &DownloadTaskView) -> Option<String> {
        match task.state {
            TaskState::Downloading => {
                let bytes = bytes_progress(task);
                Some(match self.transfer_detail(task) {
                    Some(transfers) => format!("{bytes} · {transfers}"),
                    None => bytes,
                })
            }
            TaskState::Paused => Some(bytes_progress(task)),
            TaskState::Failed => task
                .error_message
                .lines()
                .next()
                .map(str::trim)
                .filter(|line| !line.is_empty())
                .map(str::to_owned),
            TaskState::Pending => self.strings.queued_label(task),
            TaskState::Completed => None,
        }
    }

    /// 宽松密度状态列第二行：只给主列没有的信息（并发连接 / 排队位置 / 失败原因）。
    fn relaxed_status_detail(&self, task: &DownloadTaskView) -> Option<String> {
        match task.state {
            TaskState::Downloading => self.transfer_detail(task),
            TaskState::Failed | TaskState::Pending => self.status_detail(task),
            TaskState::Paused | TaskState::Completed => None,
        }
    }

    /// 宽松密度主列元信息行：未完成为「已下 / 总量 · 百分比 [· 速度 · 剩余]」，完成为
    /// 「大小 · 类别」，末尾接来源域名；未知字段直接省略（Web `relaxedMeta` 镜像）。
    fn relaxed_meta(&self, task: &DownloadTaskView) -> String {
        let mut parts: Vec<String> = Vec::with_capacity(6);
        if task.state == TaskState::Completed {
            if task.size_bytes > 0 {
                parts.push(task.size.clone());
            }
            parts.push(self.kind_label(task.kind).to_string());
        } else {
            parts.push(bytes_progress(task));
            parts.push(percent_label(task.progress).to_string());
            if task.state == TaskState::Downloading {
                if let Some(speed) = task.speed_bytes_per_second.filter(|speed| *speed > 0) {
                    parts.push(format!("{}/s", format_bytes(speed)));
                }
                if let Some(seconds) = task.eta_seconds.filter(|seconds| *seconds <= MAX_ETA_SECS) {
                    parts.push(self.strings.format_eta(seconds).to_string());
                }
            }
        }
        let site = task.source_site();
        if !site.is_empty() {
            parts.push(site.to_owned());
        }
        parts.join(" · ")
    }

    /// 实时并发：本地分段连接数与 BT 已连节点数。
    fn transfer_detail(&self, task: &DownloadTaskView) -> Option<String> {
        let active = task.active_transfers();
        let peers = task
            .runtime
            .as_ref()
            .filter(|_| task.runtime_connected && task.protocol == TaskProtocol::Bt)
            .and_then(|runtime| runtime.connected_peers);
        match (active, peers) {
            (Some(active), Some(peers)) => Some(format!(
                "{active} {} · {peers} {}",
                self.strings.active_transfers, self.strings.connected_peers
            )),
            (Some(active), None) => Some(format!("{active} {}", self.strings.active_transfers)),
            (None, Some(peers)) => Some(format!("{peers} {}", self.strings.connected_peers)),
            (None, None) => None,
        }
    }

    /// 固定左列：平时显示系统文件图标（取不到时回退为按类型的图标，元数据加载中为
    /// Spinner）；行悬停时经 `group_hover` 换成复选框；行已选中或存在任意选中时复选框常显。
    /// 隐藏（`invisible`）的元素不绘制、不注册鼠标监听，不会拦截行点击。
    fn render_selection_cell(
        &self,
        row_ix: usize,
        task: &DownloadTaskView,
        window: &mut Window,
        cx: &mut Context<TableState<Self>>,
    ) -> AnyElement {
        let theme = active_theme(cx);
        let muted = theme.tokens().colors.muted_foreground;
        let hover_border = theme.tokens().colors.foreground.opacity(0.7);
        let icon_sizes = theme.extended().icon;
        let key = task.key.clone();
        let selected = self.selected_tasks.contains(&key);
        let checkbox_pinned = self.in_multi_select();
        let glyph = (!checkbox_pinned).then(|| {
            if task.metadata_pending {
                return Spinner::new()
                    .with_size(icon_sizes.md)
                    .icon(IconName::LoaderCircle)
                    .color(muted)
                    .into_any_element();
            }
            // 双行 / 三行密度给文件图标更大的尺寸（与文字块等高感），紧凑单行与文字同高。
            let size = match self.prefs.density {
                ViewDensity::Relaxed => icon_sizes.lg * FILE_ICON_RELAXED_SCALE,
                ViewDensity::Comfortable => icon_sizes.lg * FILE_ICON_COMFORTABLE_SCALE,
                ViewDensity::Compact => icon_sizes.lg,
            };
            task_file_icon(task, size, window, cx)
        });
        let checkbox = div()
            .id(("download-task-multi-select", row_ix))
            .p(px(4.))
            .cursor_pointer()
            .child(
                check_mark(
                    if selected {
                        CheckState::Checked
                    } else {
                        CheckState::Unchecked
                    },
                    cx,
                )
                .when(!selected, |mark| {
                    mark.hover(move |style| style.border_color(hover_border))
                }),
            )
            .on_click(cx.listener(move |table, _: &ClickEvent, _, cx| {
                cx.stop_propagation();
                let delegate = table.delegate_mut();
                if !delegate.selected_tasks.remove(&key) {
                    delegate.selected_tasks.insert(key.clone());
                }
                delegate.selection_anchor = Some(key.clone());
                cx.notify();
            }));
        // 复选框只在多选状态（已有选中项，经 Cmd/Ctrl+点击进入）显示；悬浮不再露出。
        div()
            .size_full()
            .relative()
            .when_some(glyph, |this, glyph| {
                this.child(h_flex().absolute().inset_0().justify_center().child(glyph))
            })
            .when(checkbox_pinned, |this| {
                this.child(
                    h_flex()
                        .absolute()
                        .inset_0()
                        .justify_center()
                        .on_mouse_down(MouseButton::Left, |_, _, cx| cx.stop_propagation())
                        .child(checkbox),
                )
            })
            .into_any_element()
    }

    fn render_file_cell(&self, task: &DownloadTaskView, cx: &App) -> AnyElement {
        if self.prefs.density == ViewDensity::Relaxed {
            return self.render_relaxed_file_cell(task, cx);
        }
        let theme = active_theme(cx);
        let tokens = theme.tokens();
        let (name, name_color) = if task.metadata_pending {
            (
                self.strings.metadata_loading.clone(),
                tokens.colors.muted_foreground,
            )
        } else {
            (
                SharedString::from(task.name.clone()),
                // 文件已不在下载目录：文件名退为次要色，与状态列「文件已删除」呼应。
                if task.is_file_missing() {
                    tokens.colors.muted_foreground
                } else {
                    tokens.colors.foreground
                },
            )
        };
        let meta = (self.prefs.density.two_line() && !task.metadata_pending).then(|| {
            let category = self.kind_label(task.kind);
            let site = task.source_site();
            if site.is_empty() {
                category
            } else {
                SharedString::from(format!("{category} · {site}"))
            }
        });
        v_flex()
            .size_full()
            .min_w_0()
            .justify_center()
            .child(
                div()
                    .min_w_0()
                    .truncate()
                    .text_size(tokens.typography.sm.size)
                    .line_height(tokens.typography.sm.line_height)
                    .font_weight(tokens.typography.sm.weight)
                    .text_color(name_color)
                    .child(name),
            )
            .when_some(meta, |this, meta| {
                this.child(
                    div()
                        .min_w_0()
                        .truncate()
                        .text_size(tokens.typography.xs.size)
                        .line_height(tokens.typography.xs.line_height)
                        .text_color(theme.extended().colors.text_tertiary)
                        .child(meta),
                )
            })
            .into_any_element()
    }

    /// 宽松密度主列：文件名（中粗）/ 通栏分段进度条（完成态省略）/ 元信息三行。
    /// 大小、进度、速度、剩余时间列在此密度下并入本列（见 [`DownloadColumnKind::shown`]）。
    fn render_relaxed_file_cell(&self, task: &DownloadTaskView, cx: &App) -> AnyElement {
        let theme = active_theme(cx);
        let tokens = theme.tokens();
        let (name, name_color) = if task.metadata_pending {
            (
                self.strings.metadata_loading.clone(),
                tokens.colors.muted_foreground,
            )
        } else if task.is_file_missing() {
            (
                SharedString::from(task.name.clone()),
                tokens.colors.muted_foreground,
            )
        } else {
            (
                SharedString::from(task.name.clone()),
                tokens.colors.foreground,
            )
        };
        let show_bar = task.state != TaskState::Completed && !task.metadata_pending;
        let bar_width = (self.applied_file_width.get() - 2. * CELL_PADDING_X).max(0.);
        v_flex()
            .size_full()
            .min_w_0()
            .justify_center()
            .gap(tokens.spacing.xs)
            .child(
                div()
                    .min_w_0()
                    .truncate()
                    .text_size(tokens.typography.sm.size)
                    .line_height(tokens.typography.sm.line_height)
                    .font_weight(FontWeight::MEDIUM)
                    .text_color(name_color)
                    .child(name),
            )
            .when(show_bar, |this| {
                this.child(
                    crate::components::segment_progress::render_segment_progress(
                        task.runtime.as_deref(),
                        task.progress,
                        bar_width,
                        theme.components().progress_height,
                        theme.components().progress_radius,
                        progress_bar_color(task.state, cx),
                        progress_track_color(cx),
                    ),
                )
            })
            .when(!task.metadata_pending, |this| {
                this.child(
                    div()
                        .min_w_0()
                        .truncate()
                        .text_size(tokens.typography.xs.size)
                        .line_height(tokens.typography.xs.line_height)
                        .font_features(tabular_numbers())
                        .text_color(theme.extended().colors.text_tertiary)
                        .child(self.relaxed_meta(task)),
                )
            })
            .into_any_element()
    }

    /// 活动列：主文案按状态着色（仅下载中 primary、失败 destructive），双行密度
    /// 第二行给详情；紧凑密度把详情放进悬停提示，失败总是提示完整原因。
    fn render_status_cell(&self, task: &DownloadTaskView, cx: &App) -> AnyElement {
        let theme = active_theme(cx);
        let typography = &theme.tokens().typography;
        let relaxed = self.prefs.density == ViewDensity::Relaxed;
        let two_line = self.prefs.density.two_line();
        let detail = if relaxed {
            self.relaxed_status_detail(task)
        } else {
            self.status_detail(task)
        };
        let tooltip = if task.state == TaskState::Failed && !task.error_message.trim().is_empty() {
            Some(SharedString::from(task.error_message.clone()))
        } else if two_line {
            None
        } else {
            detail.clone().map(SharedString::from)
        };
        let label_style = if relaxed {
            &typography.sm
        } else {
            &typography.xs
        };
        v_flex()
            .id(SharedString::from(format!(
                "download-status-{}",
                task.key.task_id()
            )))
            .size_full()
            .min_w_0()
            .justify_center()
            .when(relaxed, |this| this.gap(theme.tokens().spacing.xs))
            .font_features(tabular_numbers())
            .child(if task.conflict_pending {
                h_flex()
                    .min_w_0()
                    .child(conflict_badge(
                        task.key.task_id(),
                        relaxed,
                        &self.strings,
                        self.host.as_ref(),
                        cx,
                    ))
                    .into_any_element()
            } else {
                div()
                    .min_w_0()
                    .truncate()
                    .text_size(label_style.size)
                    .line_height(label_style.line_height)
                    .text_color(task_status_color(task, cx))
                    .child(self.status_label(task))
                    .into_any_element()
            })
            .when_some(detail.filter(|_| two_line), |this, detail| {
                this.child(
                    div()
                        .min_w_0()
                        .truncate()
                        .text_size(typography.xs.size)
                        .line_height(typography.xs.line_height)
                        .text_color(theme.extended().colors.text_tertiary)
                        .child(detail),
                )
            })
            .when_some(
                tooltip.filter(|_| !task.conflict_pending),
                |this, tooltip| {
                    this.tooltip(move |window, cx| Tooltip::new(tooltip.clone()).build(window, cx))
                },
            )
            .into_any_element()
    }

    /// 进度单元格：`components.progress` 高度 / 圆角的条 + 整数百分比；完成态整格留空
    /// （完成只由状态列表达）。
    fn render_progress_cell(&self, task: &DownloadTaskView, cx: &App) -> AnyElement {
        if task.state == TaskState::Completed {
            return div().into_any_element();
        }
        let theme = active_theme(cx);
        let tokens = theme.tokens();
        let colors = &tokens.colors;
        let bar_color = progress_bar_color(task.state, cx);
        let text_scale = f32::from(theme.text_extent(1.));
        let column_width = self
            .columns
            .iter()
            .find(|column| column.kind == DownloadColumnKind::Progress)
            .map_or(
                DownloadColumnKind::Progress.min_width() * text_scale,
                |column| column.kind.display_width(column.width, text_scale),
            );
        // 百分比区随文字放大；条宽 = 列内容宽（扣左右内边距）− 百分比区 − 间距，与下方布局同一组常量。
        let label_width = theme.text_extent(PROGRESS_LABEL_WIDTH);
        let bar_width =
            (column_width - 2. * CELL_PADDING_X - f32::from(label_width) - PROGRESS_GAP).max(0.);
        h_flex()
            .size_full()
            .min_w_0()
            .gap(px(PROGRESS_GAP))
            .child(
                crate::components::segment_progress::render_segment_progress(
                    task.runtime.as_deref(),
                    task.progress,
                    bar_width,
                    theme.components().progress_height,
                    theme.components().progress_radius,
                    bar_color,
                    progress_track_color(cx),
                ),
            )
            .child(
                div()
                    .flex_none()
                    .w(label_width)
                    .text_right()
                    .text_size(tokens.typography.xs.size)
                    .line_height(tokens.typography.xs.line_height)
                    .font_features(tabular_numbers())
                    .text_color(colors.muted_foreground)
                    .child(percent_label(task.progress)),
            )
            .into_any_element()
    }

    /// 行悬停操作（最后一个可见列右端浮层）：暂停 / 继续 / 重试 / 打开 + 在文件夹中
    /// 显示；停靠详情面板未打开时再加「详情」（面板打开后单击行即切换详情，按钮多余），
    /// 舒适与紧凑密度一致。底色与行悬停一致（选中时叠加选中色），除「详情」外点击不
    /// 改变选中。
    fn render_row_actions(&self, task: &DownloadTaskView, cx: &App) -> Option<AnyElement> {
        let host = self.host.as_ref()?;
        let is_local = task.key.is_local();
        let with_detail = !self.prefs.detail_open;
        let mut actions = row_actions(task.state, is_local, task.file_missing, with_detail)
            .filter(|action| {
                is_local
                    || action
                        .remote_action()
                        .is_some_and(|remote| task.remote_can(remote))
            })
            .peekable();
        actions.peek()?;
        let theme = active_theme(cx);
        let tokens = theme.tokens();
        let extended = theme.extended();
        let (muted, foreground) = (tokens.colors.muted_foreground, tokens.colors.foreground);
        let pressed = extended.colors.nav_selected;
        let selected = self.selected_tasks.contains(&task.key);
        let buttons = actions.map(|action| {
            let (icon, tooltip) = match action {
                RowAction::Pause => (FluxIcon::Pause, self.strings.pause.clone()),
                RowAction::Resume => (FluxIcon::Play, self.strings.resume.clone()),
                RowAction::Retry => (FluxIcon::RotateCw, self.strings.resume.clone()),
                RowAction::Open => (FluxIcon::ExternalLink, self.strings.open_file.clone()),
                RowAction::Reveal => (FluxIcon::FolderOpen, self.strings.open_folder.clone()),
                RowAction::Detail => (FluxIcon::PanelRight, self.strings.detail.clone()),
            };
            let host = host.clone();
            let key = task.key.clone();
            h_flex()
                .id(action.id())
                .flex_none()
                .size(px(ROW_ACTION_SIZE))
                .justify_center()
                .rounded(tokens.radius.sm)
                .cursor_pointer()
                .text_color(muted)
                .hover(move |style| style.bg(pressed).text_color(foreground))
                .tooltip(move |window, cx| Tooltip::new(tooltip.clone()).build(window, cx))
                .on_mouse_down(MouseButton::Left, |_, _, cx| cx.stop_propagation())
                .on_click(move |_, window, cx| {
                    cx.stop_propagation();
                    let key = key.clone();

                    let Ok(()) = host.update(cx, |view, cx| {
                        let Some(command) = action.command() else {
                            view.show_row_detail(key, window, cx);
                            return;
                        };
                        let command = view
                            .controller
                            .store()
                            .get(&key)
                            .and_then(|row| task_command(&row, command));
                        if let Some(command) = command {
                            view.execute_commands(vec![command], cx);
                        }
                    }) else {
                        // 视图已释放，结束这次回调而不再更新状态。
                        return;
                    };
                })
                .child(Icon::new(icon).size(extended.icon.md))
        });
        Some(
            h_flex()
                .absolute()
                .top_0()
                .right_0()
                .bottom_0()
                .gap(tokens.spacing.xxs)
                .pl(tokens.spacing.xs)
                .bg(extended.colors.row_hover)
                .invisible()
                .group_hover(ROW_GROUP, |style| style.visible())
                .when(selected, |this| {
                    this.child(div().absolute().inset_0().bg(tokens.colors.accent))
                })
                .children(buttons)
                .into_any_element(),
        )
    }

    /// 分组头：xs 中等字重正文色标签 + 三级色计数，28 高内容贴底，行槽位多出的
    /// 高度作上方留白；不加底色块。
    fn render_group_header(
        &self,
        row_ix: usize,
        key: &str,
        label: &SharedString,
        count: usize,
        collapsed: bool,
        cx: &mut Context<TableState<Self>>,
    ) -> AnyElement {
        let group_progress =
            key.strip_prefix("group:")
                .filter(|id| !id.is_empty())
                .map(|group_id| {
                    self.store
                        .local()
                        .iter()
                        .filter(|task| task.group_id == group_id)
                        .fold((0usize, 0usize), |(completed, total), task| {
                            let completed = if task.state == TaskState::Completed {
                                completed + 1
                            } else {
                                completed
                            };
                            (completed, total + 1)
                        })
                });
        let key = key.to_owned();
        let on_click = cx.listener(move |table, _: &ClickEvent, _, cx| {
            let delegate = table.delegate_mut();
            delegate.toggle_group_collapsed(&key);
            delegate.refresh_view();
            // 折叠记忆随视图偏好持久化；表格实体此刻正被更新，放到帧外调用。
            if let Some(host) = delegate.host.clone() {
                cx.defer(move |cx| {
                    let Ok(()) = host.update(cx, |view, cx| view.schedule_persist_prefs(cx)) else {
                        // 视图已释放，结束这次回调而不再更新状态。
                        return;
                    };
                });
            }
            table.refresh(cx);
            cx.notify();
        });
        let theme = active_theme(cx);
        let tokens = theme.tokens();
        let extended = theme.extended();
        let tertiary = extended.colors.text_tertiary;
        div()
            .id(("download-group-header", row_ix))
            .size_full()
            .flex()
            .flex_col()
            .justify_end()
            .cursor_pointer()
            .on_mouse_down(MouseButton::Left, |_, _, cx| cx.stop_propagation())
            .on_click(on_click)
            .child(
                h_flex()
                    .h(theme.density().control)
                    .min_w_0()
                    .gap(tokens.spacing.xs)
                    .text_size(tokens.typography.xs.size)
                    .line_height(tokens.typography.xs.line_height)
                    .child(
                        Icon::new(if collapsed {
                            FluxIcon::ChevronRight
                        } else {
                            FluxIcon::ChevronDown
                        })
                        .size(extended.icon.sm)
                        .text_color(tertiary),
                    )
                    .child(
                        div()
                            .min_w_0()
                            .truncate()
                            .font_weight(FontWeight::MEDIUM)
                            .text_color(tokens.colors.foreground)
                            .child(label.clone()),
                    )
                    .child(
                        div()
                            .flex_none()
                            .font_features(tabular_numbers())
                            .text_color(tertiary)
                            .child(count.to_string()),
                    )
                    .when_some(group_progress, |this, (completed, total)| {
                        this.child(
                            div()
                                .flex_none()
                                .font_features(tabular_numbers())
                                .text_color(tertiary)
                                .child(format!("{completed}/{total}")),
                        )
                    }),
            )
            .into_any_element()
    }

    /// 选中集合 → 任务事实；选择滞后于存储事件时任一键缺失即返回 `None`。
    fn selected_menu_facts(&self) -> Option<(Vec<RowKey>, Vec<TaskMenuFacts>)> {
        let selected = self.selected_keys();
        if selected.is_empty() {
            return None;
        }
        let facts = selected
            .iter()
            .map(|key| self.task_menu_facts(key))
            .collect::<Option<Vec<_>>>()?;
        Some((selected, facts))
    }

    fn task_menu_facts(&self, key: &RowKey) -> Option<TaskMenuFacts> {
        let row = self.store.get(key)?;
        Some(TaskMenuFacts {
            is_local: key.is_local(),
            controllable: row.remote_status != Some(RemoteTaskStatus::Unknown),
            state: row.state,
            boosted: row.boosted,
            is_torrent_sentinel: row.url.starts_with("torrent-file://"),
            is_plugin_retry_error: row.state == TaskState::Failed
                && row.error_message.starts_with(PLUGIN_ERROR_PREFIX),
            file_missing: row.is_file_missing(),
        })
    }

    /// 任务行右键菜单：按 [`context_menu_items`] 的纯函数结果逐项渲染。
    fn task_context_menu(&self, menu: PopupMenu) -> PopupMenu {
        let Some((selected, facts)) = self.selected_menu_facts() else {
            return menu;
        };
        context_menu_items(&facts)
            .into_iter()
            .fold(menu, |menu, entry| {
                self.apply_menu_entry(menu, entry, &selected)
            })
    }

    fn apply_menu_entry(
        &self,
        menu: PopupMenu,
        entry: MenuEntry,
        selected: &[RowKey],
    ) -> PopupMenu {
        match entry {
            MenuEntry::Resume => menu.menu_with_icon(
                self.strings.resume.clone(),
                FluxIcon::Play,
                Box::new(crate::actions::ResumeSelected),
            ),
            MenuEntry::Pause => menu.menu_with_icon(
                self.strings.pause.clone(),
                FluxIcon::Pause,
                Box::new(crate::actions::PauseSelected),
            ),
            MenuEntry::IgnorePluginRetry => {
                let (Some(host), Some(task_id)) = (
                    self.host.clone(),
                    selected.first().map(|key| key.task_id().to_owned()),
                ) else {
                    return menu;
                };
                menu.item(
                    PopupMenuItem::new(self.strings.ignore_plugin_retry.clone())
                        .icon(FluxIcon::CircleAlert)
                        .on_click(move |_, window, cx| {
                            let task_id = task_id.clone();

                            let Ok(()) = host.update(cx, |view, cx| {
                                view.confirm_ignore_plugin_retry(task_id, window, cx);
                            }) else {
                                // 视图已释放，结束这次回调而不再更新状态。
                                return;
                            };
                        }),
                )
            }
            MenuEntry::Boost | MenuEntry::CancelBoost => {
                let cancel = matches!(entry, MenuEntry::CancelBoost);
                menu.menu_with_icon(
                    if cancel {
                        self.strings.cancel_boost.clone()
                    } else {
                        self.strings.boost_download.clone()
                    },
                    FluxIcon::Zap,
                    Box::new(crate::actions::ToggleBoostSelected),
                )
            }
            MenuEntry::OpenFile => menu.menu_with_icon(
                self.strings.open_file.clone(),
                FluxIcon::ExternalLink,
                Box::new(crate::actions::OpenSelected),
            ),
            MenuEntry::OpenFolder => menu.menu_with_icon(
                self.strings.open_folder.clone(),
                FluxIcon::FolderOpen,
                Box::new(crate::actions::RevealSelected),
            ),
            MenuEntry::Rename => menu.menu_with_icon(
                self.strings.rename_task.clone(),
                FluxIcon::Pen,
                Box::new(crate::actions::RenameSelected),
            ),
            MenuEntry::Redownload => menu.menu_with_icon(
                self.strings.redownload_task.clone(),
                FluxIcon::RotateCw,
                Box::new(crate::actions::RedownloadSelected),
            ),
            MenuEntry::CopyUrl => menu.menu_with_icon(
                self.strings.copy_url.clone(),
                FluxIcon::Copy,
                Box::new(crate::actions::CopySelectedUrl),
            ),
            MenuEntry::MoveToQueue => self.append_move_to_queue(menu),
            MenuEntry::Separator => menu.separator(),
            MenuEntry::Delete => menu.menu_with_icon(
                self.strings.delete_task.clone(),
                FluxIcon::Trash2,
                Box::new(crate::actions::DeleteSelected),
            ),
            MenuEntry::DeleteWithFiles => menu.menu_with_icon(
                self.strings.delete_task_and_file.clone(),
                FluxIcon::Trash2,
                Box::new(crate::actions::DeleteSelectedWithFiles),
            ),
            MenuEntry::ShowDetail => menu.menu_with_icon(
                self.strings.detail.clone(),
                FluxIcon::PanelRight,
                Box::new(crate::actions::ShowSelectedDetail),
            ),
        }
    }

    /// 「移动到队列」：固定版 gpui-component 的 `PopupMenu::submenu` 需要
    /// `Context<PopupMenu>`，而 `TableDelegate::context_menu` 只提供
    /// `Context<TableState<Self>>`（两者是不同实体的上下文，无法互转），因此
    /// 这里无法构造真正的嵌套子菜单，改为「小节标题 + 缩进平铺项」。
    fn append_move_to_queue(&self, menu: PopupMenu) -> PopupMenu {
        let Some(host) = self.host.clone() else {
            return menu;
        };
        if self.queue_names.is_empty() {
            return menu;
        }
        let mut menu = menu.label(self.strings.move_to_queue.clone());
        for (queue_id, name) in &self.queue_names {
            let host = host.clone();
            let queue_id = queue_id.clone();
            menu = menu.item(
                PopupMenuItem::new(SharedString::from(format!("    {name}"))).on_click(
                    move |_, _, cx| {
                        let queue_id = queue_id.clone();

                        let Ok(()) =
                            host.update(cx, |view, cx| view.move_selected_to_queue(queue_id, cx))
                        else {
                            // 视图已释放，结束这次回调而不再更新状态。
                            return;
                        };
                    },
                ),
            );
        }
        menu
    }

    /// 分组头行右键菜单（P1.8）；`group_id` 为空（未分组桶）不显示菜单。
    fn group_context_menu(&self, group_id: &str, menu: PopupMenu) -> PopupMenu {
        if group_id.is_empty() {
            return menu;
        }
        let Some(host) = self.host.as_ref() else {
            return menu;
        };
        let has_failed_member = self
            .store
            .local()
            .iter()
            .any(|task| task.group_id == group_id && task.state == TaskState::Failed);

        let mut menu = menu.item(group_menu_item(
            self.strings.group_pause_all.clone(),
            FluxIcon::Pause,
            host,
            group_id,
            DownloadView::group_pause_all,
        ));
        menu = menu.item(group_menu_item(
            self.strings.group_resume_all.clone(),
            FluxIcon::Play,
            host,
            group_id,
            DownloadView::group_resume_all,
        ));
        if has_failed_member {
            menu = menu.item(group_menu_item(
                self.strings.group_retry_failed.clone(),
                FluxIcon::RotateCw,
                host,
                group_id,
                DownloadView::group_retry_failed,
            ));
        }
        menu = menu.item(group_menu_item(
            self.strings.group_open_folder.clone(),
            FluxIcon::FolderOpen,
            host,
            group_id,
            DownloadView::group_open_folder,
        ));
        menu = menu.item(group_menu_item_windowed(
            self.strings.group_copy_source_link.clone(),
            FluxIcon::Copy,
            host,
            group_id,
            DownloadView::group_copy_source_link,
        ));
        menu = menu.separator();
        menu = menu.item(group_menu_item(
            self.strings.group_delete.clone(),
            FluxIcon::Trash2,
            host,
            group_id,
            DownloadView::group_delete,
        ));
        menu = menu.item(group_menu_item_windowed(
            self.strings.group_delete_with_files.clone(),
            FluxIcon::Trash2,
            host,
            group_id,
            DownloadView::confirm_group_delete_with_files,
        ));
        menu.item(group_menu_item_windowed(
            self.strings.open_group_in_window.clone(),
            FluxIcon::AppWindow,
            host,
            group_id,
            DownloadView::open_group_in_window,
        ))
    }
}

impl TableDelegate for DownloadTableDelegate {
    fn columns_count(&self, _cx: &App) -> usize {
        self.shown_columns_count() + 1
    }

    fn rows_count(&self, _cx: &App) -> usize {
        self.visible.len()
    }

    /// 空态：图标 + 标题 + 引导。
    fn render_empty(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<TableState<Self>>,
    ) -> impl IntoElement {
        let theme = active_theme(cx);
        let tokens = theme.tokens();
        v_flex()
            .size_full()
            .items_center()
            .justify_center()
            .gap(tokens.spacing.sm)
            .child(
                Icon::new(FluxIcon::Download)
                    .size(px(32.))
                    .text_color(theme.extended().colors.text_tertiary),
            )
            .child(
                div()
                    .text_size(tokens.typography.sm.size)
                    .line_height(tokens.typography.sm.line_height)
                    .font_weight(FontWeight::MEDIUM)
                    .text_color(tokens.colors.foreground)
                    .child(self.strings.empty_title.clone()),
            )
            .child(
                div()
                    .text_size(tokens.typography.xs.size)
                    .line_height(tokens.typography.xs.line_height)
                    .text_color(tokens.colors.muted_foreground)
                    .child(self.strings.empty_subtitle.clone()),
            )
    }

    /// 排序指示由 `render_th` 自绘，列不设 `sort`（去掉 DataTable 常驻排序图标）。
    fn column(&self, col_ix: usize, cx: &App) -> Column {
        // 列的最小宽 / 默认宽随文字放大；记下本次应用的缩放，主题变化时由 prepaint 触发重建。
        let text_scale = f32::from(active_theme(cx).text_extent(1.));
        self.text_scale.set(text_scale);
        if col_ix == 0 {
            // 不设 `fixed_left`：gpui-component 会给固定列画一条 `border` 色竖线，
            // 而文件名列已吸收剩余宽度，常规列集不会出现横向滚动。
            return Column::new("selection", "")
                .width(px(SELECTION_COLUMN_WIDTH))
                .min_width(px(SELECTION_COLUMN_WIDTH))
                .max_width(px(SELECTION_COLUMN_WIDTH))
                .resizable(false)
                .movable(false)
                .selectable(false)
                .p_0();
        }

        let Some(column) = self.shown_column(col_ix) else {
            return Column::new("missing", "").resizable(false).movable(false);
        };
        let base = Column::new(column.kind.key(), column.kind.label(&self.strings))
            .min_width(px(column.kind.min_width() * text_scale))
            .paddings(Edges {
                top: px(0.),
                right: px(CELL_PADDING_X),
                bottom: px(0.),
                left: px(CELL_PADDING_X),
            });
        if column.kind == DownloadColumnKind::FileName {
            // 默认吸收剩余宽度（其他列拖宽后由它补足）；拖过后固定为用户宽度。
            return base
                .width(px(self.apply_file_name_width()))
                .max_width(px(FILE_NAME_MAX_WIDTH));
        }
        base.width(px(column.kind.display_width(column.width, text_scale)))
            .max_width(px(MAX_COLUMN_WIDTH * text_scale))
    }

    fn render_th(
        &mut self,
        col_ix: usize,
        _window: &mut Window,
        cx: &mut Context<TableState<Self>>,
    ) -> impl IntoElement {
        if col_ix == 0 {
            let keys = self.visible_task_keys();
            let selected_count = keys
                .iter()
                .filter(|key| self.selected_tasks.contains(*key))
                .count();
            // 非多选模式（如普通单击选中一行）表头全选框按未选处理，不随单选点亮。
            let state = match selected_count {
                _ if !self.in_multi_select() => CheckState::Unchecked,
                0 => CheckState::Unchecked,
                n if n == keys.len() => CheckState::Checked,
                _ => CheckState::Indeterminate,
            };
            let hover_border = active_theme(cx).tokens().colors.foreground.opacity(0.7);
            // 全选框平时隐藏，悬停表头该格或已有选中时出现，避免表头常驻一个空框。
            return h_flex()
                .id("select-all-download-tasks")
                .group(SELECT_ALL_GROUP)
                .size_full()
                .justify_center()
                .cursor_pointer()
                .on_mouse_down(MouseButton::Left, |_, _, cx| cx.stop_propagation())
                .child(
                    div()
                        .when(state == CheckState::Unchecked, |this| {
                            this.invisible()
                                .group_hover(SELECT_ALL_GROUP, |style| style.visible())
                        })
                        .child(check_mark(state, cx).when(
                            state == CheckState::Unchecked,
                            |mark| {
                                mark.group_hover(SELECT_ALL_GROUP, move |style| {
                                    style.border_color(hover_border)
                                })
                            },
                        )),
                )
                .on_click(cx.listener(move |table, _: &ClickEvent, _, cx| {
                    let delegate = table.delegate_mut();
                    if state == CheckState::Checked {
                        delegate.clear_selection();
                    } else {
                        delegate.select_all_tasks();
                    }
                    cx.notify();
                }))
                .into_any_element();
        }

        let Some(kind) = self.shown_column(col_ix).map(|column| column.kind) else {
            return div().into_any_element();
        };
        let numeric = kind.is_numeric();
        let sort_key = kind.sort_key();
        // 只有当前排序列显示方向箭头；智能（默认）排序不对应任何列。
        let arrow =
            sort_key
                .filter(|key| *key == self.prefs.sort_key)
                .map(|_| match self.prefs.sort_dir {
                    SortDir::Asc => FluxIcon::ArrowUp,
                    SortDir::Desc => FluxIcon::ArrowDown,
                });
        let on_click = sort_key.map(|_| {
            cx.listener(move |table, event: &ClickEvent, _, cx| {
                // 拖动列头换位后在原列松开也会产生 click，位移过大时不当作排序。
                if let ClickEvent::Mouse(click) = event {
                    let delta = click.up.position - click.down.position;
                    if f32::from(delta.x).abs() + f32::from(delta.y).abs() > HEADER_CLICK_SLOP {
                        return;
                    }
                }
                let delegate = table.delegate_mut();
                if !delegate.toggle_sort(kind) {
                    return;
                }
                delegate.refresh_view();
                // 偏好写回走宿主的防抖持久化；表格实体此刻正被更新，放到帧外调用。
                if let Some(host) = delegate.host.clone() {
                    cx.defer(move |cx| {
                        let Ok(()) = host.update(cx, |view, cx| view.schedule_persist_prefs(cx))
                        else {
                            // 视图已释放，结束这次回调而不再更新状态。
                            return;
                        };
                    });
                }
                cx.notify();
            })
        });
        let theme = active_theme(cx);
        let typography = &theme.tokens().typography;
        let extended = theme.extended();
        let label = div().min_w_0().truncate().child(kind.label(&self.strings));
        let arrow = arrow.map(|icon| Icon::new(icon).size(extended.icon.sm));
        // 每列右缘常驻一条短分隔线，提示此处可拖宽。gpui-component 的拖宽柄
        // 取 `table_row_border`（为去网格线已设透明，且同色还控制行线与填充行，不能改），
        // 仅悬停时显示；这里自绘同位置的线：th 内容右缘向外偏 `CELL_PADDING_X`
        // 恰为单元格右缘，与拖宽柄的 1px 线重合，悬停时被其全高高亮覆盖。
        let divider = div()
            .absolute()
            .top_0()
            .bottom_0()
            .right(px(-CELL_PADDING_X))
            .flex()
            .items_center()
            .child(
                div()
                    .w(extended.stroke.thin)
                    .h(px(COLUMN_DIVIDER_HEIGHT))
                    .bg(extended.colors.hairline),
            );
        h_flex()
            .id(("download-column-header", col_ix))
            .size_full()
            .relative()
            .min_w_0()
            .gap(theme.tokens().spacing.xxs)
            .text_size(typography.xs.size)
            .line_height(typography.xs.line_height)
            .font_weight(FontWeight::MEDIUM)
            .text_color(extended.colors.text_tertiary)
            // 数字列右对齐：箭头放在标签左侧，保持标签右缘与数值对齐。
            .map(|this| {
                if numeric {
                    this.justify_end().children(arrow).child(label)
                } else {
                    this.child(label).children(arrow)
                }
            })
            .when_some(on_click, |this, on_click| {
                this.cursor_pointer().on_click(on_click)
            })
            .child(divider)
            .into_any_element()
    }

    /// 行高按密度覆盖 DataTable 的统一尺寸（DataTable 用 `refine_style` 合并本样式，
    /// 高度以这里为准）；分组头行与任务行等高（uniform_list 要求）。
    fn render_tr(
        &mut self,
        row_ix: usize,
        _window: &mut Window,
        cx: &mut Context<TableState<Self>>,
    ) -> Stateful<Div> {
        let theme = active_theme(cx);
        let row_height = self.prefs.density.row_height(theme.density());
        let Some(VisibleRow::Task(id)) = self.visible.get(row_ix).cloned() else {
            return div().id(("download-group-row", row_ix)).h(row_height);
        };
        let Some((key, drag_visual)) = self.store.row(id).map(|row| {
            let drag_visual = row.has_local_file().then(|| {
                (
                    row.kind,
                    SharedString::from(row.name_fold.clone()),
                    SharedString::from(row.name.clone()),
                )
            });
            (row.key.clone(), drag_visual)
        }) else {
            return div().id(("download-task-row", row_ix)).h(row_height);
        };
        let selected = self.selected_tasks.contains(&key);
        let (accent, radius) = (
            theme.tokens().colors.accent,
            theme.components().task_row_radius,
        );
        // 已完成且文件仍在下载目录的行可按住拖到系统文件管理器 / 桌面。
        let dragged = drag_visual.map(|(kind, name_fold, name)| DraggedTasks {
            anchor: key.clone(),
            kind,
            name_fold,
            name,
            table: cx.weak_entity(),
            host: self.host.clone(),
        });
        // 双击：已完成打开文件，其余打开详情。独立于选择监听注册（普通闭包，不占用表格
        // 实体），这样 DownloadView 处理时可以自由读写表格状态。
        let activate = self.host.clone().map(|host| {
            let key = key.clone();
            move |event: &ClickEvent, window: &mut Window, cx: &mut App| {
                let modifiers = event.modifiers();
                if event.click_count() != 2 || modifiers.secondary() || modifiers.shift {
                    return;
                }
                let key = key.clone();

                let Ok(()) = host.update(cx, |view, cx| view.activate_row(key, window, cx)) else {
                    // 视图已释放，结束这次回调而不再更新状态。
                    return;
                };
            }
        });

        div()
            .id(("download-task-row", row_ix))
            .h(row_height)
            .group(ROW_GROUP)
            .relative()
            // 覆盖 DataTable 自带的满宽悬浮底色，两种状态共用内缩背景。
            .child(div().absolute().inset_0().bg(theme.tokens().colors.surface))
            .child(
                div()
                    .absolute()
                    .top_0()
                    .bottom_0()
                    .left(px(SELECTED_INSET_X))
                    .right(px(SELECTED_INSET_X))
                    .rounded(radius)
                    .when(selected, |this| this.bg(accent))
                    .when(!selected, |this| {
                        this.group_hover(ROW_GROUP, |style| {
                            style.bg(theme.extended().colors.row_hover)
                        })
                    }),
            )
            .on_click(cx.listener(move |table, event: &ClickEvent, _, cx| {
                table
                    .delegate_mut()
                    .select_task(key.clone(), event.modifiers());
                cx.notify();
            }))
            .when_some(activate, |this, activate| this.on_click(activate))
            .when_some(dragged, |this, dragged| {
                this.on_drag(dragged, |dragged, click_offset, _, cx| {
                    dragged.preview(click_offset, cx)
                })
                .external_drag_payload(|dragged: &DraggedTasks, _, cx| dragged.external_payload(cx))
            })
    }

    fn render_td(
        &mut self,
        row_ix: usize,
        col_ix: usize,
        window: &mut Window,
        cx: &mut Context<TableState<Self>>,
    ) -> impl IntoElement {
        let id = match self.visible.get(row_ix) {
            Some(VisibleRow::Task(id)) => *id,
            Some(VisibleRow::GroupHeader {
                key,
                label,
                count,
                collapsed,
            }) => {
                if col_ix == 1 {
                    let (key, label, count, collapsed) =
                        (key.clone(), label.clone(), *count, *collapsed);
                    return self.render_group_header(row_ix, &key, &label, count, collapsed, cx);
                }
                return div().into_any_element();
            }
            None => return div().into_any_element(),
        };
        let Some(task) = self.store.row(id) else {
            return div().into_any_element();
        };
        let task: &DownloadTaskView = &task;

        if col_ix == 0 {
            return self.render_selection_cell(row_ix, task, window, cx);
        }

        let Some(kind) = self.shown_column(col_ix).map(|column| column.kind) else {
            return div().into_any_element();
        };
        let theme = active_theme(cx);
        let muted = theme.tokens().colors.muted_foreground;
        let tertiary = theme.extended().colors.text_tertiary;
        let downloading = task.state == TaskState::Downloading;
        let cell = match kind {
            DownloadColumnKind::FileName => self.render_file_cell(task, cx),
            DownloadColumnKind::Progress => self.render_progress_cell(task, cx),
            DownloadColumnKind::Size if task.size_bytes > 0 => {
                meta_cell(task.size.clone(), muted, true, cx)
            }
            DownloadColumnKind::Status => self.render_status_cell(task, cx),
            DownloadColumnKind::Speed => match task
                .speed_bytes_per_second
                .filter(|speed| downloading && *speed > 0)
            {
                Some(speed) => meta_cell(format!("{}/s", format_bytes(speed)), muted, true, cx),
                None => meta_cell("—", tertiary, true, cx),
            },
            DownloadColumnKind::Eta => match task
                .eta_seconds
                .filter(|seconds| downloading && *seconds <= MAX_ETA_SECS)
            {
                Some(seconds) => meta_cell(self.strings.format_eta(seconds), muted, true, cx),
                None => meta_cell("—", tertiary, true, cx),
            },
            DownloadColumnKind::Size => meta_cell("—", tertiary, true, cx),
            DownloadColumnKind::Created if task.created_at_secs > 0 => meta_cell(
                DownloadStrings::format_datetime(task.created_at_secs),
                muted,
                true,
                cx,
            ),
            DownloadColumnKind::Created => meta_cell("—", tertiary, true, cx),
            DownloadColumnKind::Protocol => meta_cell(task.protocol.label(), muted, false, cx),
            DownloadColumnKind::Source => {
                meta_cell(task.source_site().to_owned(), muted, false, cx)
            }
            DownloadColumnKind::Queue => {
                // 远程任务没有本机队列：这一列显示它所在的目标设备。
                let name = if task.source == TaskSource::Remote {
                    self.device_names
                        .get(&task.to_device)
                        .cloned()
                        .unwrap_or_else(|| task.to_device.clone())
                } else {
                    self.queue_names
                        .iter()
                        .find(|(id, _)| *id == task.queue_id)
                        .map_or_else(|| task.queue_id.clone(), |(_, name)| name.clone())
                };
                meta_cell(name, muted, false, cx)
            }
        };
        if col_ix == self.shown_columns_count()
            && let Some(actions) = self.render_row_actions(task, cx)
        {
            return div()
                .size_full()
                .relative()
                .child(cell)
                .child(actions)
                .into_any_element();
        }
        cell
    }

    /// 右侧留白与选择列内含的左侧留白对称。
    fn render_last_empty_col(
        &mut self,
        _window: &mut Window,
        _cx: &mut Context<TableState<Self>>,
    ) -> impl IntoElement {
        h_flex()
            .w(px(TABLE_TRAILING_GUTTER))
            .h_full()
            .flex_shrink_0()
    }

    fn move_column(
        &mut self,
        col_ix: usize,
        to_ix: usize,
        _window: &mut Window,
        _cx: &mut Context<TableState<Self>>,
    ) {
        self.move_shown_column(col_ix, to_ix);
    }

    /// 右键选中必须在这里完成：gpui-component 在鼠标冒泡阶段先 `window.defer` 排队构建
    /// 菜单，随后行监听才 emit `RightClickedRow`；effect 按 FIFO 执行，订阅者收到事件时
    /// 菜单早已按旧选区构建完毕（无选中 → 空菜单不显示，只剩复选框常显；已选他行 →
    /// 菜单作用于旧选区）。`right_clicked_row` 在派发时同步写入，构建时可靠。
    fn context_menu(
        &mut self,
        row_ix: usize,
        menu: PopupMenu,
        _window: &mut Window,
        cx: &mut Context<TableState<Self>>,
    ) -> PopupMenu {
        let menu = match self.action_context.clone() {
            Some(handle) => menu.action_context(handle),
            None => menu,
        };
        match self.visible.get(row_ix).cloned() {
            Some(VisibleRow::GroupHeader { key, .. }) => self.group_context_menu(&key, menu),
            Some(VisibleRow::Task(_)) => {
                if let Some(key) = self.row_key_at(row_ix) {
                    self.select_task_for_context_menu(key);
                    cx.notify();
                }
                self.task_context_menu(menu)
            }
            None => menu,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum ToolbarCommand {
    Resume,
    Pause,
    PauseAll,
    ResumeAll,
    Delete,
    Open,
    Reveal,
}

/// 选中集合投影（选择条 / 工具栏 / 快捷键）：只统计仍存在于 store 的选中任务。
/// - `count`：选中数量；`any`：是否有选中（删除 / 取消选择）。
/// - `any_local`：含本地任务（在文件夹中显示；远程任务没有本机文件）。
/// - `all_openable`：全部为本地已完成且文件仍在下载目录的任务（打开文件；未完成的产物
///   尚不存在，被删除 / 移走的已找不到）。
/// - `any_active`：含下载中 / 排队（暂停）；`any_resumable`：含暂停 / 失败（继续）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SelectionSummary {
    pub(crate) count: usize,
    pub(crate) any: bool,
    pub(crate) any_local: bool,
    pub(crate) all_openable: bool,
    pub(crate) any_active: bool,
    pub(crate) any_resumable: bool,
}

/// 行悬停操作；除「详情」外执行时映射为 [`ToolbarCommand`]，与工具栏 / 右键菜单同一命令路径。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RowAction {
    Pause,
    Resume,
    /// 失败任务的「继续」，图标换成重试。
    Retry,
    Open,
    Reveal,
    /// 在停靠面板中查看详情（与右键「详情」同一入口）。
    Detail,
}

impl RowAction {
    /// 对应的工具栏命令；「详情」不是任务命令，返回 `None`。
    fn command(self) -> Option<ToolbarCommand> {
        match self {
            Self::Pause => Some(ToolbarCommand::Pause),
            Self::Resume | Self::Retry => Some(ToolbarCommand::Resume),
            Self::Open => Some(ToolbarCommand::Open),
            Self::Reveal => Some(ToolbarCommand::Reveal),
            Self::Detail => None,
        }
    }

    /// 远程任务上对应的云端动作（打开 / 显示 / 详情没有远程对应）。
    fn remote_action(self) -> Option<RemoteCommandAction> {
        match self {
            Self::Pause => Some(RemoteCommandAction::Pause),
            Self::Resume | Self::Retry => Some(RemoteCommandAction::Resume),
            Self::Open | Self::Reveal | Self::Detail => None,
        }
    }

    fn id(self) -> &'static str {
        match self {
            Self::Pause => "download-row-action-pause",
            Self::Resume => "download-row-action-resume",
            Self::Retry => "download-row-action-retry",
            Self::Open => "download-row-action-open",
            Self::Reveal => "download-row-action-reveal",
            Self::Detail => "download-row-action-detail",
        }
    }
}

/// 行悬停操作集合：下载中 / 排队 → 暂停；暂停 → 继续；失败 → 重试；完成 → 打开
/// 文件（仅本地，且文件仍在下载目录）；本地任务再加「在文件夹中显示」，`with_detail`
/// 时最后加「详情」（远程任务没有本机详情）。远程任务只给可用操作：已结束的远程任务
/// （失败 / 取消 / 完成）不能「继续」——云端只接受暂停中任务的继续。
pub(crate) fn row_actions(
    state: TaskState,
    is_local: bool,
    file_missing: bool,
    with_detail: bool,
) -> impl Iterator<Item = RowAction> {
    let primary = match state {
        TaskState::Downloading | TaskState::Pending => Some(RowAction::Pause),
        TaskState::Paused => Some(RowAction::Resume),
        TaskState::Failed => is_local.then_some(RowAction::Retry),
        TaskState::Completed => (is_local && !file_missing).then_some(RowAction::Open),
    };
    primary
        .into_iter()
        .chain(is_local.then_some(RowAction::Reveal))
        .chain((is_local && with_detail).then_some(RowAction::Detail))
}

/// 单任务命令：本地任务走 daemon；远程任务只有暂停 / 继续 / 删除经 `agent.remote.command`
/// 转发，且仅限云端状态允许该动作的任务（状态未知的任务不控制）。打开 / 显示没有本机
/// 文件、全局命令不属于单任务，返回 `None`。
fn task_command(row: &DownloadTaskView, action: ToolbarCommand) -> Option<DownloadsCommand> {
    let task_id = row.key.task_id().to_owned();
    if !row.key.is_local() {
        let remote_action = match action {
            ToolbarCommand::Resume => RemoteCommandAction::Resume,
            ToolbarCommand::Pause => RemoteCommandAction::Pause,
            ToolbarCommand::Delete => RemoteCommandAction::Delete,
            ToolbarCommand::Open
            | ToolbarCommand::Reveal
            | ToolbarCommand::PauseAll
            | ToolbarCommand::ResumeAll => return None,
        };
        return row
            .remote_can(remote_action)
            .then_some(DownloadsCommand::RemoteCommand(RemoteCommandParams {
                task_id,
                action: remote_action,
                command_id: None,
                delete_files: false,
            }));
    }
    Some(match action {
        ToolbarCommand::Resume => DownloadsCommand::Resume { task_id },
        ToolbarCommand::Pause => DownloadsCommand::Pause { task_id },
        ToolbarCommand::Delete => DownloadsCommand::Delete {
            task_id,
            delete_files: false,
        },
        ToolbarCommand::Open => DownloadsCommand::OpenTask { task_id },
        ToolbarCommand::Reveal => DownloadsCommand::RevealTask { task_id },
        ToolbarCommand::PauseAll | ToolbarCommand::ResumeAll => return None,
    })
}

/// 进度条颜色：下载中 `progressFill`、失败 `statusFailed`、暂停为 `statusPaused` 的 40%、
/// 排队 / 完成取对应状态色。主表格与详情窗口共用。
pub(crate) fn progress_bar_color(state: TaskState, cx: &App) -> Hsla {
    let colors = &active_theme(cx).extended().colors;
    match state {
        TaskState::Downloading => colors.progress_fill,
        TaskState::Paused => colors.status_paused.opacity(PAUSED_BAR_ALPHA),
        TaskState::Failed => colors.status_failed,
        TaskState::Pending => colors.status_queued,
        TaskState::Completed => colors.status_completed,
    }
}

/// 进度轨道颜色（`progressTrack`）。主表格与详情窗口共用。
pub(crate) fn progress_track_color(cx: &App) -> Hsla {
    active_theme(cx).extended().colors.progress_track
}

/// 状态文字色（`status*`）：默认只有下载中（primary）与失败（destructive）着色，暂停为
/// 二级文字，排队 / 完成退为三级文字。主表格与详情窗口共用。
pub(crate) fn status_color(state: TaskState, cx: &App) -> Hsla {
    let colors = &active_theme(cx).extended().colors;
    match state {
        TaskState::Downloading => colors.status_downloading,
        TaskState::Failed => colors.status_failed,
        TaskState::Paused => colors.status_paused,
        TaskState::Pending => colors.status_queued,
        TaskState::Completed => colors.status_completed,
    }
}

/// 行级状态色：已完成但文件已不在下载目录时取 `warning`，其余同 [`status_color`]。
pub(crate) fn task_status_color(task: &DownloadTaskView, cx: &App) -> Hsla {
    if task.is_file_missing() {
        active_theme(cx).extended().colors.warning
    } else {
        status_color(task.state, cx)
    }
}

/// 已下 / 总量；总量未知时只给已下。
fn bytes_progress(task: &DownloadTaskView) -> String {
    if task.size_bytes > 0 {
        format!("{} / {}", format_bytes(task.downloaded_bytes), task.size)
    } else {
        format_bytes(task.downloaded_bytes)
    }
}

/// 整数百分比（向下取整，未完成不会显示 100%）；介于 0 与 1% 之间显示 `<1%`。
fn percent_label(progress: f32) -> SharedString {
    let percent = progress.clamp(0., 1.) * 100.;
    if percent > 0. && percent < 1. {
        SharedString::from("<1%")
    } else {
        SharedString::from(format!("{}%", percent.floor() as u32))
    }
}

/// 单行元信息单元格（xs）；数字列右对齐并用等宽数字。
fn meta_cell(text: impl Into<SharedString>, color: Hsla, numeric: bool, cx: &App) -> AnyElement {
    let typography = &active_theme(cx).tokens().typography;
    h_flex()
        .size_full()
        .min_w_0()
        .when(numeric, |this| {
            this.justify_end().font_features(tabular_numbers())
        })
        .text_size(typography.xs.size)
        .line_height(typography.xs.line_height)
        .text_color(color)
        .child(div().min_w_0().truncate().child(text.into()))
        .into_any_element()
}

/// 下载任务表（主窗口与任务组详情共用）：表头高取 `density.control`、行高按密度、表格底色 surface。
///
/// 文件名列吸收剩余宽度：prepaint 测得容器宽后只做比较，确需更新时用
/// `window.defer` 在帧外写入代理并 `refresh`（不在 prepaint 里同步更新实体），
/// 写入后期望值与已应用值一致，下一帧不再触发，避免逐帧重算或抖动。
///
/// 行顺序稳定：表格内指针活动推迟内容变化引起的重排；prepaint 发现有被推迟的重排
/// 时安排补偿定时器（[`arm_reorder_timer`]）。
pub(crate) fn render_download_table(
    id: &'static str,
    table_state: &Entity<TableState<DownloadTableDelegate>>,
    cx: &App,
) -> Stateful<Div> {
    let observed = table_state.clone();
    div()
        .id(id)
        .relative()
        .flex_1()
        .min_w_0()
        .min_h_0()
        .overflow_hidden()
        .bg(active_theme(cx).tokens().colors.surface)
        .on_mouse_move(note_pointer::<MouseMoveEvent>(table_state, cx))
        .on_scroll_wheel(note_pointer::<ScrollWheelEvent>(table_state, cx))
        .capture_any_mouse_down(note_pointer::<MouseDownEvent>(table_state, cx))
        .capture_any_mouse_down(clear_context_row_on_left_press(table_state))
        .child(
            div().absolute().inset_0().child(
                DataTable::new(table_state)
                    .with_size(Size::Size(table_header_height(cx)))
                    .stripe(false)
                    .bordered(false)
                    .scrollbar_visible(true, true),
            ),
        )
        .on_prepaint(move |bounds, window, cx| {
            let width = f32::from(bounds.size.width);
            let text_scale = f32::from(active_theme(cx).text_extent(1.));
            let delegate = observed.read(cx).delegate();
            let sync_width = delegate.viewport_needs_sync(width);
            let rescale = delegate.text_scale_stale(text_scale);
            let arm_timer = delegate.needs_reorder_timer();
            if !sync_width && !rescale && !arm_timer {
                return;
            }
            window.defer(cx, move |_, cx| {
                observed.update(cx, |table, cx| {
                    // 界面缩放 / 字号变化后列的最小宽、默认宽随之变化，重建列。
                    let resized = sync_width && table.delegate_mut().set_viewport_width(width);
                    if resized || rescale {
                        table.refresh(cx);
                    }
                    if arm_timer {
                        arm_reorder_timer(table, cx);
                    }
                });
            });
        })
}

/// 表格指针事件监听：只记录活动时刻，不写表格 entity、不触发重绘。
fn note_pointer<E: 'static>(
    table_state: &Entity<TableState<DownloadTableDelegate>>,
    cx: &App,
) -> impl Fn(&E, &mut Window, &mut App) + 'static {
    let hold = table_state.read(cx).delegate().interaction_hold();
    move |_, _, _| hold.note(Instant::now())
}

/// 表格内左键按下即清除右键高亮行。
///
/// 两张表都 `row_selectable(false)`，选择由委托自管，DataTable 内置的「左键点行清除
/// `right_clicked_row`」分支因此不会执行；它自带的 `on_mouse_down_out` 又只管表格外的
/// 点击。不补这一步，右键过的行边框会一直残留（改选别的任务也不消失），
/// [`arm_reorder_timer`] 也会把它当作菜单仍开着而无限顺延重排。
///
/// 捕获阶段注册：勾选框 / 行内按钮在冒泡阶段 `stop_propagation` 也拦不住。只认左键，
/// 右键另一行由 DataTable 自己改写高亮；菜单浮层 `occlude`，点菜单项不会走到这里，
/// 而菜单是右键当帧 `window.defer` 构建的，此时清除不影响菜单内容。
fn clear_context_row_on_left_press(
    table_state: &Entity<TableState<DownloadTableDelegate>>,
) -> impl Fn(&MouseDownEvent, &mut Window, &mut App) + 'static {
    let table_state = table_state.clone();
    move |event, _, cx| {
        if event.button != MouseButton::Left {
            return;
        }
        // 先读后写：没有高亮行时不写表格，免得每次左键都让 gpui-fast 把表格记为已变化。
        if table_state.read(cx).right_clicked_row().is_none() {
            return;
        }
        table_state.update(cx, |table, cx| table.set_right_clicked_row(None, cx));
    }
}

/// 跨过本地零点时刷新按日期分组的表格；表格释放后循环随 `update` 失败结束。
///
/// 日期分桶只在重算可见行时读取时钟，没有任务事件的夜里不会自己换天。
pub(crate) fn spawn_midnight_refresh(
    table_state: &Entity<TableState<DownloadTableDelegate>>,
    cx: &mut App,
) {
    let table = table_state.downgrade();
    cx.spawn(async move |cx| {
        loop {
            let delay = until_next_local_day(chrono::Local::now());
            cx.background_executor().timer(delay).await;
            let Ok(()) = table.update(cx, |table, cx| {
                if table.delegate_mut().refresh_for_new_day() {
                    cx.notify();
                }
            }) else {
                // 表格已释放，结束跨天刷新。
                return;
            };
        }
    })
    .detach();
}

/// 距下一个本地日期开始的时长（多留 1 秒确保已换天）；时区换算失败时 1 分钟后复查。
fn until_next_local_day(now: chrono::DateTime<chrono::Local>) -> Duration {
    const RECHECK: Duration = Duration::from_secs(60);
    let Some(next_day) = now.date_naive().succ_opt() else {
        return RECHECK;
    };
    next_day
        .and_hms_opt(0, 0, 1)
        .and_then(|at| at.and_local_timezone(chrono::Local).earliest())
        .and_then(|at| (at - now).to_std().ok())
        .unwrap_or(RECHECK)
}

/// 为被推迟的重排安排定时器。到期时右键菜单仍开着（高亮行按下标定位，重排会让高亮
/// 落到别的任务上）或指针又活动过则顺延；否则强制按最新排序重排一次。
fn arm_reorder_timer(
    table: &mut TableState<DownloadTableDelegate>,
    cx: &mut Context<TableState<DownloadTableDelegate>>,
) {
    let delegate = table.delegate_mut();
    if delegate.reorder_timer.is_some() {
        return;
    }
    let Some(deadline) = delegate.reorder_deadline() else {
        return;
    };
    let delay = deadline.saturating_duration_since(Instant::now());
    delegate.reorder_timer = Some(cx.spawn(async move |this, cx| {
        cx.background_executor().timer(delay).await;

        let Ok(()) = this.update(cx, |table, cx| {
            let menu_open = table.right_clicked_row().is_some();
            let now = Instant::now();
            let delegate = table.delegate_mut();
            delegate.reorder_timer = None;
            if menu_open {
                delegate.row_order.note_interaction(now);
            }
            match delegate.reorder_deadline() {
                Some(deadline) if deadline > now => arm_reorder_timer(table, cx),
                Some(_) => {
                    delegate.view_dirty = true;
                    if delegate.refresh_view_at(now) {
                        cx.notify();
                    }
                }
                None => {}
            }
        }) else {
            // 视图已释放，结束这次回调而不再更新状态。
            return;
        };
    }));
}

impl DownloadView {
    /// 选中集合 → 命令列表（远程任务走 `agent.remote.command`）。
    pub(crate) fn commands_for_selection(
        &self,
        action: ToolbarCommand,
        cx: &Context<Self>,
    ) -> Vec<DownloadsCommand> {
        let store = self.controller.store();
        let commands_for = |keys: Vec<RowKey>| -> Vec<DownloadsCommand> {
            keys.iter()
                .filter_map(|key| {
                    let row = store.get(key)?;
                    task_command(&row, action)
                })
                .collect()
        };
        match action {
            ToolbarCommand::PauseAll => vec![DownloadsCommand::PauseAll],
            ToolbarCommand::ResumeAll => vec![DownloadsCommand::ResumeAll],
            ToolbarCommand::Open => commands_for(
                self.table_state
                    .read(cx)
                    .delegate()
                    .openable_selected_keys(),
            ),
            _ => commands_for(self.table_state.read(cx).delegate().selected_keys()),
        }
    }

    /// 「删除」命令：本地走 daemon（`delete_files` 决定是否删文件）；远程任务经
    /// `agent.remote.command`（`delete_files` 同样透传给目标设备），状态未知的任务不删。
    pub(crate) fn delete_commands(
        &self,
        keys: &[RowKey],
        delete_files: bool,
    ) -> Vec<DownloadsCommand> {
        let store = self.controller.store();
        keys.iter()
            .filter_map(|key| {
                if key.is_local() {
                    return Some(DownloadsCommand::Delete {
                        task_id: key.task_id().to_owned(),
                        delete_files,
                    });
                }
                let row = store.get(key)?;
                row.remote_can(RemoteCommandAction::Delete).then(|| {
                    DownloadsCommand::RemoteCommand(RemoteCommandParams {
                        task_id: key.task_id().to_owned(),
                        action: RemoteCommandAction::Delete,
                        command_id: None,
                        delete_files,
                    })
                })
            })
            .collect()
    }

    pub(crate) fn execute_toolbar(&mut self, action: ToolbarCommand, cx: &mut Context<Self>) {
        let commands = self.commands_for_selection(action, cx);
        self.execute_commands(commands, cx);
    }

    /// 执行一批命令。同类多任务的暂停 / 继续 / 删除先合并成批量 RPC
    /// （见 [`coalesce_commands`]），其余命令按 [`MAX_IN_FLIGHT`] 限制并发；幂等命令被
    /// agent 以可重试的 `Unavailable` 拒绝时退避重试。任一失败在页面横幅提示（按错误
    /// `reason` 给出原因，同一批里先失败的错误一直保留，后完成的成功不会冲掉它）。
    /// 只含一条单任务「继续」/「重新下载」时，成功后通知宿主这是一次交互式开始
    /// （批量选择不逐个弹进度窗口）。
    pub(crate) fn execute_commands(
        &mut self,
        commands: Vec<DownloadsCommand>,
        cx: &mut Context<Self>,
    ) {
        let interactive_start = match commands.as_slice() {
            [DownloadsCommand::Resume { task_id }] => Some(Some(task_id.clone())),
            [DownloadsCommand::Redownload(request, _)] if !request.start_paused => Some(None),
            _ => None,
        };
        let queue = Rc::new(RefCell::new(VecDeque::from(coalesce_commands(commands))));
        let batch = Rc::new(RefCell::new(DispatchSummary::default()));
        let workers = queue.borrow().len().min(MAX_IN_FLIGHT);
        for _ in 0..workers {
            let queue = Rc::clone(&queue);
            let batch = Rc::clone(&batch);
            let interactive_start = interactive_start.clone();
            cx.spawn(async move |this, cx| {
                loop {
                    let Some(mut command) = queue.borrow_mut().pop_front() else {
                        break;
                    };
                    // 打开失败多半是文件已被删除 / 移走：立即重扫，让行上的丢失标记跟上磁盘现状。
                    let rescan_on_failure = matches!(command, DownloadsCommand::OpenTask { .. });
                    let mut retries = 0_u32;
                    let result = loop {
                        let replay = retry_copy(&command);
                        let Ok((future, stale)) = this.update(cx, |this, _| {
                            (this.controller.execute(command), this.controller.is_stale())
                        }) else {
                            return;
                        };
                        let result = future.await;
                        let wait = match (&result, replay) {
                            (Err(error), Some(replay)) if !stale => {
                                retry_delay(error, retries).map(|delay| (delay, replay))
                            }
                            _ => None,
                        };
                        let Some((delay, replay)) = wait else {
                            break result;
                        };
                        cx.background_executor().timer(delay).await;
                        command = replay;
                        retries += 1;
                    };
                    batch.borrow_mut().record(&result);
                    let interactive_start = interactive_start.clone();
                    let batch = Rc::clone(&batch);

                    let Ok(()) = this.update(cx, |this, cx| {
                        if rescan_on_failure && result.is_err() {
                            this.rescan_files_now(cx);
                        }
                        this.last_error = batch.borrow().first_error.as_ref().map(|error| {
                            SharedString::from(error_text(this.translator.read(cx), error))
                        });
                        if let (Ok(result), Some(resumed)) = (&result, interactive_start) {
                            // 继续：原任务 ID；重新下载：响应里的新任务 ID。
                            let started =
                                resumed.map_or_else(|| result.created_task_ids(), |id| vec![id]);
                            this.notify_user_started(&started, cx);
                        }
                        cx.notify();
                    }) else {
                        // 视图已释放，结束这次回调而不再更新状态。
                        return;
                    };
                }
            })
            .detach();
        }
    }

    pub(crate) fn render_table(&self, cx: &mut Context<Self>) -> Stateful<Div> {
        render_download_table("download-task-table-container", &self.table_state, cx)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        rc::Rc,
        time::{Duration, Instant},
    };

    use fluxdown_ui_i18n::{I18nCatalog, I18nError};
    use gpui::Modifiers;

    use super::{
        DownloadColumnKind, DownloadTableDelegate, DownloadsCommand, RowAction, SelectionSummary,
        TableFilter, ToolbarCommand, VisibleRow, group_by_key, percent_label, row_actions,
        task_command,
    };
    use crate::{
        model::{
            CategoryIndex, DownloadFilter, DownloadStatusFilter, DownloadTaskView, RowKey,
            TaskSource, TaskState, TaskStore,
            view_prefs::{SortDir, ViewGroupBy, ViewPrefs, ViewSortKey},
        },
        strings::DownloadStrings,
    };

    fn delegate(statuses: &[i32]) -> Result<DownloadTableDelegate, I18nError> {
        delegate_with_missing(statuses, &[])
    }

    /// `missing` 中的下标对应任务带 `fileMissing: true`（文件跟踪判定产物已不在磁盘）。
    fn delegate_with_missing(
        statuses: &[i32],
        missing: &[usize],
    ) -> Result<DownloadTableDelegate, I18nError> {
        let catalog = std::sync::Arc::new(I18nCatalog::load_embedded()?);
        let strings = DownloadStrings::from_translator(&catalog.translator("en"));
        let store = Rc::new(TaskStore::default());
        let rows = statuses
            .iter()
            .enumerate()
            .map(|(index, status)| {
                let task =
                    serde_json::from_value::<fluxdown_protocol::TaskDto>(serde_json::json!({
                        "taskId": format!("t{index}"),
                        "url": "https://example.com/file",
                        "fileName": format!("f{index}.bin"),
                        "saveDir": "/tmp",
                        "status": status,
                        "downloadedBytes": 0,
                        "totalBytes": 1,
                        "errorMessage": "",
                        "createdAt": format!("{}", 100 - index),
                        "proxyUrl": "",
                        "queueId": "main",
                        "checksum": "",
                        "fileMissing": missing.contains(&index)
                    }))
                    .expect("task");
                DownloadTaskView::local(&task, None, false)
            })
            .collect();
        store.replace_local(rows);
        let mut delegate = DownloadTableDelegate::new(strings, store);
        delegate.refresh_view();
        Ok(delegate)
    }

    #[test]
    fn context_menu_selection_preserves_selected_rows_and_replaces_unselected_rows()
    -> Result<(), I18nError> {
        let mut delegate = delegate(&[2, 2, 2])?;
        delegate
            .selected_tasks
            .extend([RowKey::Local("t0".into()), RowKey::Local("t1".into())]);

        delegate.select_task_for_context_menu(RowKey::Local("t1".into()));
        assert_eq!(
            delegate.selected_tasks,
            HashSet::from([RowKey::Local("t0".into()), RowKey::Local("t1".into())])
        );

        delegate.select_task_for_context_menu(RowKey::Local("t2".into()));
        assert_eq!(
            delegate.selected_tasks,
            HashSet::from([RowKey::Local("t2".into())])
        );
        Ok(())
    }

    #[test]
    fn selection_summary_counts_every_live_selected_task() -> Result<(), I18nError> {
        // t0 已暂停、t1 下载中。
        let mut delegate = delegate(&[2, 1])?;
        assert_eq!(delegate.selection_summary(), SelectionSummary::default());

        // 任务已被删除但选中集合仍残留其 key：不计入。
        delegate.selected_tasks.insert(RowKey::Local("gone".into()));
        assert_eq!(delegate.selection_summary(), SelectionSummary::default());

        delegate.selected_tasks.insert(RowKey::Local("t0".into()));
        assert_eq!(
            delegate.selection_summary(),
            SelectionSummary {
                count: 1,
                any: true,
                any_local: true,
                all_openable: false,
                any_active: false,
                any_resumable: true,
            }
        );

        // 遇到本地任务后仍需继续遍历，否则后续下载中任务的可暂停性会丢失。
        delegate.selected_tasks.insert(RowKey::Local("t1".into()));
        assert_eq!(
            delegate.selection_summary(),
            SelectionSummary {
                count: 2,
                any: true,
                any_local: true,
                all_openable: false,
                any_active: true,
                any_resumable: true,
            }
        );
        Ok(())
    }

    #[test]
    fn open_file_requires_every_selected_task_completed() -> Result<(), I18nError> {
        // t0 已完成、t1 已暂停（磁盘上只有 `.fdownloading`）。
        let mut delegate = delegate(&[3, 2])?;
        delegate.selected_tasks.insert(RowKey::Local("t0".into()));
        assert!(delegate.selection_summary().all_openable);
        assert_eq!(
            delegate.openable_selected_keys(),
            [RowKey::Local("t0".into())]
        );

        delegate.selected_tasks.insert(RowKey::Local("t1".into()));
        assert!(!delegate.selection_summary().all_openable);
        assert_eq!(
            delegate.openable_selected_keys(),
            [RowKey::Local("t0".into())]
        );
        Ok(())
    }

    #[test]
    fn missing_files_are_neither_openable_nor_draggable() -> Result<(), I18nError> {
        // t0 已完成、t1 已完成但文件已被删除 / 移走、t2 已暂停。
        let mut delegate = delegate_with_missing(&[3, 3, 2], &[1])?;
        delegate.selected_tasks.insert(RowKey::Local("t1".into()));
        assert!(!delegate.selection_summary().all_openable);
        assert!(delegate.openable_selected_keys().is_empty());
        assert!(delegate.drag_paths(&RowKey::Local("t1".into())).is_empty());

        delegate.selected_tasks.insert(RowKey::Local("t0".into()));
        assert_eq!(
            delegate.openable_selected_keys(),
            [RowKey::Local("t0".into())]
        );
        Ok(())
    }

    #[test]
    fn drag_carries_selection_only_when_anchor_is_selected() -> Result<(), I18nError> {
        // t0 / t1 / t3 已完成、t2 已暂停；t3 文件已丢失。
        let mut delegate = delegate_with_missing(&[3, 3, 2, 3], &[3])?;
        let path = |name: &str| std::path::Path::new("/tmp").join(name);

        // 未选中时拖任意行：只拖该行。
        assert_eq!(
            delegate.drag_paths(&RowKey::Local("t1".into())),
            [path("f1.bin")]
        );

        // 从选区内拖起：带上整个选区里仍可拖出的文件（跳过未完成与已丢失）。
        delegate.select_all_tasks();
        assert_eq!(
            delegate.drag_paths(&RowKey::Local("t1".into())),
            [path("f0.bin"), path("f1.bin")]
        );

        // 从选区外的行拖起：选区不参与。
        delegate.selected_tasks.clear();
        delegate.selected_tasks.insert(RowKey::Local("t0".into()));
        assert_eq!(
            delegate.drag_paths(&RowKey::Local("t1".into())),
            [path("f1.bin")]
        );
        Ok(())
    }

    #[test]
    fn selection_is_pruned_to_tasks_left_in_view() -> Result<(), I18nError> {
        let mut delegate = delegate(&[2, 1, 2])?;
        delegate.select_all_tasks();
        assert_eq!(delegate.selection_summary().count, 3);

        // 搜索收窄：被筛掉的任务移出选中集，汇总与批量动作只看剩余可见任务。
        delegate.set_query("f1.bin");
        delegate.refresh_view();
        assert_eq!(delegate.selected_keys(), vec![RowKey::Local("t1".into())]);
        assert_eq!(delegate.selection_summary().count, 1);

        // 列表为空时不得再报告「已选 N 项」。
        delegate.set_query("no-such-task");
        delegate.refresh_view();
        assert_eq!(delegate.selection_summary(), SelectionSummary::default());

        // 清空搜索不会让已移出的任务「复活」为选中。
        delegate.set_query("");
        delegate.refresh_view();
        assert!(delegate.selected_keys().is_empty());
        Ok(())
    }

    #[test]
    fn collapsing_a_group_keeps_its_selected_members() -> Result<(), I18nError> {
        let mut delegate = delegate(&[1, 3])?;
        delegate.set_prefs(ViewPrefs {
            group_by: ViewGroupBy::Status,
            ..ViewPrefs::default()
        });
        delegate.refresh_view();
        delegate.select_all_tasks();
        let key = delegate
            .group_key_at(0)
            .map(str::to_owned)
            .unwrap_or_default();
        delegate.toggle_group_collapsed(&key);
        delegate.refresh_view();
        // 两个分组各 1 项，折叠首组后只剩 2 个组头 + 1 行任务。
        assert_eq!(delegate.visible.len(), 3);
        assert_eq!(delegate.selection_summary().count, 2);
        Ok(())
    }

    #[test]
    fn grouping_inserts_headers_and_collapsing_hides_members() -> Result<(), I18nError> {
        let mut delegate = delegate(&[1, 3, 1, 3])?;
        let prefs = ViewPrefs {
            group_by: ViewGroupBy::Status,
            ..ViewPrefs::default()
        };
        delegate.set_prefs(prefs);
        delegate.refresh_view();
        assert_eq!(delegate.visible.len(), 6);
        assert!(matches!(
            delegate.visible[0],
            VisibleRow::GroupHeader {
                count: 2,
                collapsed: false,
                ..
            }
        ));

        let key = delegate
            .group_key_at(0)
            .map(str::to_owned)
            .expect("group key");
        delegate.toggle_group_collapsed(&key);
        delegate.refresh_view();
        assert_eq!(delegate.visible.len(), 4);
        assert!(matches!(
            delegate.visible[0],
            VisibleRow::GroupHeader {
                collapsed: true,
                ..
            }
        ));
        Ok(())
    }

    #[test]
    fn group_by_key_matches_linear_bucketing() {
        // 交错重复的 key：桶顺序为首次出现顺序，成员保持原相对顺序。
        let keys = ["b", "a", "b", "c", "a", "d", "b", "e", "c", "a"];
        let items: Vec<(String, usize)> = keys
            .iter()
            .enumerate()
            .map(|(index, key)| ((*key).to_owned(), index))
            .collect();
        let mut expected: Vec<(String, Vec<usize>)> = Vec::new();
        for (key, value) in items.iter().cloned() {
            match expected.iter_mut().find(|(existing, _)| *existing == key) {
                Some((_, members)) => members.push(value),
                None => expected.push((key, vec![value])),
            }
        }
        assert_eq!(group_by_key(items, |key: &String| key.as_str()), expected);
    }

    #[test]
    fn search_matches_url_and_site_case_insensitively() -> Result<(), I18nError> {
        // 文件名（f0.bin / f1.bin）不含查询词，命中只能来自链接 / 站点。
        let mut delegate = delegate(&[1, 3])?;
        delegate.set_query("EXAMPLE.com/FILE");
        delegate.refresh_view();
        assert_eq!(delegate.visible.len(), 2);
        delegate.set_query("Example.COM");
        delegate.refresh_view();
        assert_eq!(delegate.visible.len(), 2);
        delegate.set_query("other.org");
        delegate.refresh_view();
        assert!(delegate.visible.is_empty());
        Ok(())
    }

    #[test]
    fn shift_range_selection_skips_group_headers() -> Result<(), I18nError> {
        let mut delegate = delegate(&[1, 3, 1, 3])?;
        let prefs = ViewPrefs {
            group_by: ViewGroupBy::Status,
            ..ViewPrefs::default()
        };
        delegate.set_prefs(prefs);
        delegate.refresh_view();
        let first = delegate.row_key_at(1).expect("first task");
        let last = delegate.row_key_at(5).expect("last task");
        delegate.select_task(first, Modifiers::default());
        delegate.select_task(
            last,
            Modifiers {
                shift: true,
                ..Modifiers::default()
            },
        );
        assert_eq!(delegate.selected_tasks.len(), 4);
        Ok(())
    }

    fn width_of(delegate: &DownloadTableDelegate, kind: DownloadColumnKind) -> Option<f32> {
        delegate
            .columns
            .iter()
            .find(|column| column.kind == kind)
            .map(|column| column.width)
    }

    #[test]
    fn relaxed_density_folds_metric_columns_into_file_name() -> Result<(), I18nError> {
        let mut delegate = delegate(&[1])?;
        assert!(delegate.set_column_visible(DownloadColumnKind::Speed, true));
        assert!(delegate.set_column_visible(DownloadColumnKind::FileName, false));
        let shown = |delegate: &DownloadTableDelegate| -> Vec<DownloadColumnKind> {
            (1..=delegate.shown_columns_count())
                .filter_map(|ix| delegate.shown_column(ix).map(|column| column.kind))
                .collect()
        };
        let comfortable = shown(&delegate);
        assert!(comfortable.contains(&DownloadColumnKind::Speed));
        assert!(!comfortable.contains(&DownloadColumnKind::FileName));

        // 宽松：主列恒显示，大小 / 进度 / 速度 / 剩余时间不单独成列，其余列照偏好。
        delegate.prefs_mut().density = crate::model::view_prefs::ViewDensity::Relaxed;
        assert_eq!(
            shown(&delegate),
            vec![
                DownloadColumnKind::FileName,
                DownloadColumnKind::Status,
                DownloadColumnKind::Created
            ]
        );
        // 列偏好不变：切回舒适密度原样恢复。
        delegate.prefs_mut().density = crate::model::view_prefs::ViewDensity::Comfortable;
        assert_eq!(shown(&delegate), comfortable);
        Ok(())
    }

    #[test]
    fn sync_column_widths_maps_shown_columns_and_clamps() -> Result<(), I18nError> {
        let mut delegate = delegate(&[1])?;
        let _ = delegate.set_viewport_width(1400.);
        let applied = delegate.apply_file_name_width();
        let shown: Vec<DownloadColumnKind> = delegate
            .columns
            .iter()
            .filter(|column| column.visible)
            .map(|column| column.kind)
            .collect();
        let widths_with = |file_name: f32| -> Vec<gpui::Pixels> {
            // 首项是勾选列；大小列拖宽、状态列拖到下限以下。
            std::iter::once(gpui::px(36.))
                .chain(shown.iter().map(|kind| match kind {
                    DownloadColumnKind::Size => gpui::px(180.),
                    DownloadColumnKind::Status => gpui::px(10.),
                    DownloadColumnKind::FileName => gpui::px(file_name),
                    other => gpui::px(other.default_width()),
                }))
                .collect()
        };
        // 文件名列宽等于已应用的回算值：没被拖，保持自适应。
        let widths = widths_with(applied);
        assert!(delegate.sync_column_widths(&widths));
        assert_eq!(width_of(&delegate, DownloadColumnKind::Size), Some(180.));
        assert_eq!(
            width_of(&delegate, DownloadColumnKind::Status),
            Some(DownloadColumnKind::Status.min_width())
        );
        assert_eq!(delegate.prefs.file_name_width, None);
        // 隐藏列不参与映射，宽度不变。
        assert_eq!(
            width_of(&delegate, DownloadColumnKind::Speed),
            Some(DownloadColumnKind::Speed.default_width())
        );
        assert!(!delegate.sync_column_widths(&widths));

        // 文件名列被拖（宽度偏离回算值）→ 固定，不再随容器变化；过窄钳到下限。
        assert!(delegate.sync_column_widths(&widths_with(applied + 120.)));
        assert_eq!(delegate.prefs.file_name_width, Some(applied + 120.));
        assert_eq!(delegate.file_name_width_for(900.), applied + 120.);
        assert!(!delegate.viewport_needs_sync(1400.));
        assert!(delegate.sync_column_widths(&widths_with(10.)));
        assert_eq!(
            delegate.file_name_width_for(1400.),
            DownloadColumnKind::FileName.min_width()
        );

        // 重置列恢复自适应。
        delegate.reset_columns();
        assert_eq!(delegate.prefs.file_name_width, None);
        Ok(())
    }

    #[test]
    fn file_name_absorbs_remaining_width_and_respects_min() -> Result<(), I18nError> {
        let mut delegate = delegate(&[1])?;
        let wide = delegate.file_name_width_for(1400.);
        let narrower = delegate.file_name_width_for(1300.);
        assert!((wide - narrower - 100.).abs() < 0.01);

        // 其他列拖宽 50 → 文件名列让出 50。
        let size = width_of(&delegate, DownloadColumnKind::Size).unwrap_or_default();
        if let Some(column) = delegate
            .columns
            .iter_mut()
            .find(|column| column.kind == DownloadColumnKind::Size)
        {
            column.width = size + 50.;
        }
        assert!((wide - delegate.file_name_width_for(1400.) - 50.).abs() < 0.01);

        // 容器过窄时不低于最小宽度（超出部分走横向滚动）。
        assert_eq!(
            delegate.file_name_width_for(300.),
            DownloadColumnKind::FileName.min_width()
        );

        // 容器宽写入后一次 refresh 即收敛：应用新宽度后不再报告需要同步。
        assert!(delegate.set_viewport_width(1400.));
        let _ = delegate.apply_file_name_width();
        assert!(!delegate.viewport_needs_sync(1400.));
        Ok(())
    }

    fn paired_delegates(
        statuses: &[i32],
    ) -> Result<(DownloadTableDelegate, DownloadTableDelegate), I18nError> {
        let seed = delegate(statuses)?;
        let fast = DownloadTableDelegate::new(seed.strings.clone(), Rc::clone(&seed.store));
        let mut full = DownloadTableDelegate::new(seed.strings.clone(), Rc::clone(&seed.store));
        full.force_full_sort = true;
        Ok((fast, full))
    }

    fn refresh_pair(
        fast: &mut DownloadTableDelegate,
        full: &mut DownloadTableDelegate,
        now: Instant,
    ) {
        assert_eq!(fast.refresh_view_at(now), full.refresh_view_at(now));
        assert_eq!(fast.visible, full.visible);
        assert_eq!(fast.selected_tasks, full.selected_tasks);
        assert_eq!(fast.selection_anchor, full.selection_anchor);
        assert_eq!(fast.reorder_deadline(), full.reorder_deadline());
    }

    #[test]
    fn unchanged_sort_values_match_full_sort_across_event_sequences() -> Result<(), I18nError> {
        let sort_keys = [
            ViewSortKey::Smart,
            ViewSortKey::Created,
            ViewSortKey::Name,
            ViewSortKey::Size,
            ViewSortKey::Progress,
            ViewSortKey::Speed,
            ViewSortKey::Status,
        ];
        let groupings = [
            ViewGroupBy::None,
            ViewGroupBy::Status,
            ViewGroupBy::Date,
            ViewGroupBy::Type,
            ViewGroupBy::Queue,
            ViewGroupBy::Site,
            ViewGroupBy::Group,
        ];
        for sort_key in sort_keys {
            for sort_dir in [SortDir::Asc, SortDir::Desc] {
                for group_by in groupings {
                    let (mut fast, mut full) = paired_delegates(&[1, 1, 0, 3, 2, 4])?;
                    let template = fast.store.local()[0].clone();
                    let mut remote = template.clone();
                    remote.key = RowKey::Remote("remote".into());
                    remote.source = TaskSource::Remote;
                    remote.to_device = "device".into();
                    fast.store.replace_remote(vec![remote]);
                    let categories = Rc::new(CategoryIndex::from_dtos(
                        fluxdown_protocol::CustomCategoryDto::builtin_defaults(),
                    ));
                    fast.set_categories(Rc::clone(&categories));
                    full.set_categories(categories);
                    let prefs = ViewPrefs {
                        sort_key,
                        sort_dir,
                        group_by,
                        ..ViewPrefs::default()
                    };
                    fast.set_prefs(prefs.clone());
                    full.set_prefs(prefs);
                    let start = Instant::now();
                    refresh_pair(&mut fast, &mut full, start);
                    fast.select_all_tasks();
                    full.select_all_tasks();
                    let mut random = 0xa076_1d64_78bd_642f_u64;
                    for step in 1..=192 {
                        random ^= random << 13;
                        random ^= random >> 7;
                        random ^= random << 17;
                        let now = start + Duration::from_millis(step * 137);
                        if let Some(deadline) = fast.reorder_deadline().filter(|at| *at <= now) {
                            fast.view_dirty = true;
                            full.view_dirty = true;
                            refresh_pair(&mut fast, &mut full, deadline);
                        }
                        let count = fast.store.local().len();
                        let ix = (random >> 32) as usize % count;
                        let mut row = fast.store.local()[ix].clone();
                        match random % 16 {
                            0 => {
                                row.downloaded_bytes += 1;
                                row.progress = (random % 101) as f32 / 100.;
                                fast.store.set_local(ix, row);
                            }
                            1 => {
                                row.speed_bytes_per_second = Some(random % 500);
                                fast.store.set_local(ix, row);
                            }
                            2 => {
                                row.size_bytes = random % 200;
                                fast.store.set_local(ix, row);
                            }
                            3 => {
                                row.state = if row.state == TaskState::Completed {
                                    TaskState::Downloading
                                } else {
                                    TaskState::Completed
                                };
                                fast.store.set_local(ix, row);
                            }
                            4 => {
                                row.boosted = !row.boosted;
                                row.preparing = !row.preparing;
                                row.queue_position = (random % 5) as u32;
                                fast.store.set_local(ix, row);
                            }
                            5 => {
                                row.queue_order += 1;
                                fast.store.set_local(ix, row);
                            }
                            6 => {
                                row.name = format!("episode{}.zip", random % 12);
                                row.name_fold = row.name.to_lowercase();
                                row.file_extension = "zip".into();
                                fast.store.set_local(ix, row);
                            }
                            7 => {
                                row.queue_id = format!("queue{}", random % 3);
                                row.group_id = format!("group{}", random % 2);
                                row.referrer = format!("https://site{}.test/page", random % 4);
                                fast.store.set_local(ix, row);
                            }
                            8 => {
                                let query = if fast.query.is_empty() { "episode" } else { "" };
                                fast.set_query(query);
                                full.set_query(query);
                            }
                            9 => {
                                if let Some(key) = fast.visible.iter().find_map(|row| match row {
                                    VisibleRow::GroupHeader { key, .. } => Some(key.clone()),
                                    VisibleRow::Task(_) => None,
                                }) {
                                    fast.toggle_group_collapsed(&key);
                                    full.toggle_group_collapsed(&key);
                                }
                            }
                            10 => {
                                let mut rows = fast.store.local().to_vec();
                                rows[ix].progress = (random % 101) as f32 / 100.;
                                rows[ix].speed_bytes_per_second = Some(random % 500);
                                fast.store.replace_local(rows);
                                let rows = fast.store.remote().to_vec();
                                fast.store.replace_remote(rows);
                            }
                            11 => {
                                if count > 3 {
                                    fast.store.swap_remove_local(ix);
                                } else {
                                    let mut added = template.clone();
                                    added.key = RowKey::Local(format!("added{step}"));
                                    fast.store.push_local(added);
                                }
                            }
                            12 => {
                                fast.row_order.note_interaction(now);
                                full.row_order.note_interaction(now);
                            }
                            13 => {
                                row.created_at_secs += 1;
                                fast.store.set_local(ix, row);
                            }
                            14 => {
                                let filter = match random >> 8 & 3 {
                                    0 => TableFilter::Download(DownloadFilter::ALL),
                                    1 => TableFilter::Download(DownloadFilter::status(
                                        DownloadStatusFilter::Incomplete,
                                    )),
                                    2 => TableFilter::Queue("main".into()),
                                    _ => TableFilter::Device("device".into()),
                                };
                                fast.set_filter(filter.clone());
                                full.set_filter(filter);
                            }
                            _ => {
                                row.runtime_connected = !row.runtime_connected;
                                row.eta_seconds = Some(random % 600);
                                fast.store.set_local(ix, row);
                            }
                        }
                        refresh_pair(&mut fast, &mut full, now);
                        if step % 19 == 0
                            && let Some(key) = fast.visible_task_keys().first().cloned()
                        {
                            fast.select_task(key.clone(), Modifiers::default());
                            full.select_task(key, Modifiers::default());
                        }
                    }
                    assert!(fast.full_sort_count < full.full_sort_count);
                }
            }
        }
        Ok(())
    }

    #[test]
    fn cached_sorted_input_preserves_deferred_reorder_and_live_clock() -> Result<(), I18nError> {
        for sort_key in [ViewSortKey::Speed, ViewSortKey::Progress] {
            let (mut fast, mut full) = paired_delegates(&[1, 1])?;
            let prefs = ViewPrefs {
                sort_key,
                ..ViewPrefs::default()
            };
            fast.set_prefs(prefs.clone());
            full.set_prefs(prefs);
            let now = Instant::now();
            refresh_pair(&mut fast, &mut full, now);
            let previous = fast.visible.clone();
            let mut row = fast.store.local()[1].clone();
            row.speed_bytes_per_second = Some(100);
            row.progress = 0.8;
            fast.store.set_local(1, row);
            refresh_pair(&mut fast, &mut full, now + Duration::from_millis(100));
            assert_eq!(fast.visible, previous);
            let deadline = fast.reorder_deadline().expect("live sort deferred");
            let sorts = fast.full_sort_count;
            let mut row = fast.store.local()[1].clone();
            row.eta_seconds = Some(3);
            fast.store.set_local(1, row);
            refresh_pair(&mut fast, &mut full, now + Duration::from_millis(200));
            assert_eq!(fast.full_sort_count, sorts);
            assert_eq!(fast.reorder_deadline(), Some(deadline));

            let mut row = fast.store.local()[1].clone();
            row.eta_seconds = Some(2);
            fast.store.set_local(1, row);
            refresh_pair(&mut fast, &mut full, deadline);
            assert_eq!(fast.full_sort_count, sorts);
            assert_eq!(
                fast.visible_task_keys(),
                [RowKey::Local("t1".into()), RowKey::Local("t0".into())]
            );
            assert_eq!(fast.reorder_deadline(), None);

            let mut row = fast.store.local()[0].clone();
            row.speed_bytes_per_second = Some(200);
            row.progress = 0.9;
            fast.store.set_local(0, row);
            refresh_pair(&mut fast, &mut full, deadline + Duration::from_millis(100));
            assert_eq!(
                fast.visible_task_keys(),
                [RowKey::Local("t1".into()), RowKey::Local("t0".into())]
            );
            assert_eq!(
                fast.reorder_deadline(),
                Some(deadline + Duration::from_secs(2))
            );
        }
        Ok(())
    }

    #[test]
    fn changed_sort_keys_and_tie_breakers_take_full_path() -> Result<(), I18nError> {
        for sort_key in [
            ViewSortKey::Smart,
            ViewSortKey::Created,
            ViewSortKey::Name,
            ViewSortKey::Size,
            ViewSortKey::Progress,
            ViewSortKey::Speed,
            ViewSortKey::Status,
        ] {
            let (mut fast, mut full) = paired_delegates(&[1, 1])?;
            let prefs = ViewPrefs {
                sort_key,
                ..ViewPrefs::default()
            };
            fast.set_prefs(prefs.clone());
            full.set_prefs(prefs);
            let now = Instant::now();
            refresh_pair(&mut fast, &mut full, now);
            let sorts = fast.full_sort_count;
            let mut row = fast.store.local()[0].clone();
            row.eta_seconds = Some(10);
            fast.store.set_local(0, row);
            refresh_pair(&mut fast, &mut full, now + Duration::from_millis(10));
            assert_eq!(fast.full_sort_count, sorts);
            let mut row = fast.store.local()[0].clone();
            match sort_key {
                ViewSortKey::Smart => row.boosted = true,
                ViewSortKey::Created => row.created_at_secs += 1,
                ViewSortKey::Name => {
                    row.name = "a.bin".into();
                    row.name_fold = "a.bin".into();
                }
                ViewSortKey::Size => row.size_bytes += 1,
                ViewSortKey::Progress => row.progress = 0.5,
                ViewSortKey::Speed => row.speed_bytes_per_second = Some(10),
                ViewSortKey::Status => row.state = TaskState::Completed,
            }
            fast.store.set_local(0, row);
            refresh_pair(&mut fast, &mut full, now + Duration::from_millis(20));
            assert_eq!(fast.full_sort_count, sorts + 1);
            let mut row = fast.store.local()[0].clone();
            row.queue_order += 1;
            fast.store.set_local(0, row);
            refresh_pair(&mut fast, &mut full, now + Duration::from_millis(30));
            assert_eq!(fast.full_sort_count, sorts + 2);
        }
        Ok(())
    }

    #[test]
    fn snapshot_and_view_changes_invalidate_cached_sort() -> Result<(), I18nError> {
        let (mut fast, mut full) = paired_delegates(&[1, 1, 3])?;
        let now = Instant::now();
        fast.set_prefs(ViewPrefs::default());
        full.set_prefs(ViewPrefs::default());
        refresh_pair(&mut fast, &mut full, now);
        let mut expected = fast.full_sort_count;
        let rows = fast.store.local().to_vec();
        let structure = fast.store.structure_generation();
        fast.store.replace_local(rows);
        refresh_pair(&mut fast, &mut full, now + Duration::from_millis(10));
        expected += 1;
        assert_eq!(fast.full_sort_count, expected);
        assert_eq!(fast.store.structure_generation(), structure);
        fast.store.replace_remote(Vec::new());
        refresh_pair(&mut fast, &mut full, now + Duration::from_millis(20));
        expected += 1;
        assert_eq!(fast.full_sort_count, expected);
        fast.prefs_mut().sort_key = ViewSortKey::Name;
        full.prefs_mut().sort_key = ViewSortKey::Name;
        refresh_pair(&mut fast, &mut full, now + Duration::from_millis(30));
        expected += 1;
        assert_eq!(fast.full_sort_count, expected);
        let categories = Rc::new(CategoryIndex::default());
        fast.set_categories(Rc::clone(&categories));
        full.set_categories(categories);
        refresh_pair(&mut fast, &mut full, now + Duration::from_millis(40));
        expected += 1;
        assert_eq!(fast.full_sort_count, expected);
        fast.select_all_tasks();
        full.select_all_tasks();
        fast.set_query("f0");
        full.set_query("f0");
        refresh_pair(&mut fast, &mut full, now + Duration::from_millis(50));
        expected += 1;
        assert_eq!(fast.full_sort_count, expected);
        assert_eq!(fast.selected_keys(), [RowKey::Local("t0".into())]);
        let filter = TableFilter::Download(DownloadFilter::status(DownloadStatusFilter::Completed));
        fast.set_filter(filter.clone());
        full.set_filter(filter);
        refresh_pair(&mut fast, &mut full, now + Duration::from_millis(60));
        expected += 1;
        assert_eq!(fast.full_sort_count, expected);
        assert!(fast.selected_tasks.is_empty());
        Ok(())
    }

    #[test]
    fn smart_sort_tracks_effective_tiers_and_queue_slots() -> Result<(), I18nError> {
        let (mut fast, mut full) = paired_delegates(&[1, 0])?;
        let now = Instant::now();
        fast.set_prefs(ViewPrefs::default());
        full.set_prefs(ViewPrefs::default());
        refresh_pair(&mut fast, &mut full, now);
        let sorts = fast.full_sort_count;
        let mut active = fast.store.local()[0].clone();
        active.queue_position = 9;
        active.preparing = true;
        fast.store.set_local(0, active);
        refresh_pair(&mut fast, &mut full, now + Duration::from_millis(10));
        assert_eq!(fast.full_sort_count, sorts);

        let mut queued = fast.store.local()[1].clone();
        queued.queue_position = 1;
        fast.store.set_local(1, queued);
        refresh_pair(&mut fast, &mut full, now + Duration::from_millis(20));
        assert_eq!(fast.full_sort_count, sorts + 1);
        let mut queued = fast.store.local()[1].clone();
        queued.preparing = true;
        fast.store.set_local(1, queued);
        refresh_pair(&mut fast, &mut full, now + Duration::from_millis(30));
        assert_eq!(fast.full_sort_count, sorts + 2);
        let mut preparing = fast.store.local()[1].clone();
        preparing.queue_position = 3;
        fast.store.set_local(1, preparing);
        refresh_pair(&mut fast, &mut full, now + Duration::from_millis(40));
        assert_eq!(fast.full_sort_count, sorts + 2);
        let mut preparing = fast.store.local()[1].clone();
        preparing.boosted = true;
        fast.store.set_local(1, preparing);
        refresh_pair(&mut fast, &mut full, now + Duration::from_millis(50));
        assert_eq!(fast.full_sort_count, sorts + 3);
        Ok(())
    }

    #[test]
    fn unordered_progress_values_conservatively_take_full_path() -> Result<(), I18nError> {
        let (mut fast, mut full) = paired_delegates(&[1, 1])?;
        let prefs = ViewPrefs {
            sort_key: ViewSortKey::Progress,
            ..ViewPrefs::default()
        };
        fast.set_prefs(prefs.clone());
        full.set_prefs(prefs);
        let now = Instant::now();
        refresh_pair(&mut fast, &mut full, now);
        let sorts = fast.full_sort_count;
        for (step, progress) in [f32::NAN, f32::NAN, 0.5].into_iter().enumerate() {
            let mut row = fast.store.local()[0].clone();
            row.progress = progress;
            fast.store.set_local(0, row);
            refresh_pair(
                &mut fast,
                &mut full,
                now + Duration::from_millis((step as u64 + 1) * 10),
            );
            assert_eq!(fast.full_sort_count, sorts + step + 1);
        }
        Ok(())
    }

    #[test]
    fn state_change_under_pointer_keeps_row_order_until_hold_ends() -> Result<(), I18nError> {
        // 两个下载中任务：活跃档按添加正序，t1（created 99）在 t0（created 100）之前。
        let mut delegate = delegate(&[1, 1])?;
        let t0 = RowKey::Local("t0".into());
        let t1 = RowKey::Local("t1".into());
        assert_eq!(delegate.visible_task_keys(), [t1.clone(), t0.clone()]);

        let now = Instant::now();
        delegate.row_order.note_interaction(now);
        // t1 下载完成，按智能排序应沉到历史档；指针仍在表格上，行不动。
        let mut done = delegate.store.local()[1].clone();
        done.state = TaskState::Completed;
        delegate.store.set_local(1, done);
        assert!(delegate.refresh_view_at(now + Duration::from_millis(100)));
        assert_eq!(delegate.visible_task_keys(), [t1.clone(), t0.clone()]);

        // 保持期结束，补偿定时器强制重排。
        let deadline = delegate.reorder_deadline().expect("reorder deferred");
        delegate.view_dirty = true;
        assert!(delegate.refresh_view_at(deadline));
        assert_eq!(delegate.visible_task_keys(), [t0, t1]);
        assert_eq!(delegate.reorder_deadline(), None);
        Ok(())
    }

    #[test]
    fn header_click_cycles_default_dir_reverse_then_reset() -> Result<(), I18nError> {
        let mut delegate = delegate(&[1])?;
        assert_eq!(delegate.prefs.sort_key, ViewSortKey::Smart);

        assert!(delegate.toggle_sort(DownloadColumnKind::FileName));
        assert_eq!(
            (delegate.prefs.sort_key, delegate.prefs.sort_dir),
            (ViewSortKey::Name, SortDir::Asc)
        );
        assert!(delegate.toggle_sort(DownloadColumnKind::FileName));
        assert_eq!(delegate.prefs.sort_dir, SortDir::Desc);
        // 第三次点击恢复默认排序。
        assert!(delegate.toggle_sort(DownloadColumnKind::FileName));
        assert_eq!(delegate.prefs.sort_key, ViewSortKey::Smart);

        // 切到别的列从该列默认方向开始，即使上一列停在反方向。
        assert!(delegate.toggle_sort(DownloadColumnKind::Size));
        assert!(delegate.toggle_sort(DownloadColumnKind::Size));
        assert_eq!(
            (delegate.prefs.sort_key, delegate.prefs.sort_dir),
            (ViewSortKey::Size, SortDir::Asc)
        );
        assert!(delegate.toggle_sort(DownloadColumnKind::Status));
        assert_eq!(
            (delegate.prefs.sort_key, delegate.prefs.sort_dir),
            (ViewSortKey::Status, SortDir::Desc)
        );
        assert!(delegate.toggle_sort(DownloadColumnKind::Status));
        assert!(delegate.toggle_sort(DownloadColumnKind::Status));
        assert_eq!(delegate.prefs.sort_key, ViewSortKey::Smart);

        assert!(!delegate.toggle_sort(DownloadColumnKind::Protocol));
        Ok(())
    }

    #[test]
    fn row_actions_offer_only_available_operations() {
        let actions = |state, local| row_actions(state, local, false, false).collect::<Vec<_>>();
        assert_eq!(
            actions(TaskState::Downloading, true),
            [RowAction::Pause, RowAction::Reveal]
        );
        assert_eq!(actions(TaskState::Pending, false), [RowAction::Pause]);
        assert_eq!(
            actions(TaskState::Failed, true),
            [RowAction::Retry, RowAction::Reveal]
        );
        assert_eq!(
            actions(TaskState::Completed, true),
            [RowAction::Open, RowAction::Reveal]
        );
        // 远程已完成任务没有本机文件：无任何行操作；远程失败 / 取消的任务不能「继续」。
        assert!(actions(TaskState::Completed, false).is_empty());
        assert!(actions(TaskState::Failed, false).is_empty());
        // 文件已被删除 / 移走的已完成任务：不给「打开」，仍可「在文件夹中显示」。
        assert_eq!(
            row_actions(TaskState::Completed, true, true, false).collect::<Vec<_>>(),
            [RowAction::Reveal]
        );
        // 「详情」排在最后，只给本地任务（远程任务没有本机详情）。
        assert_eq!(
            row_actions(TaskState::Paused, true, false, true).collect::<Vec<_>>(),
            [RowAction::Resume, RowAction::Reveal, RowAction::Detail]
        );
        assert_eq!(
            row_actions(TaskState::Paused, false, false, true).collect::<Vec<_>>(),
            [RowAction::Resume]
        );
    }

    #[test]
    fn remote_task_commands_carry_typed_params_and_skip_uncontrollable_rows() {
        use fluxdown_protocol::{RemoteCommandAction, RemoteTaskDto};
        let row = |status: &str| {
            let dto = serde_json::from_value::<RemoteTaskDto>(serde_json::json!({
                "id": "r1", "toDevice": "dev-b", "url": "https://example.com/a", "status": status
            }))
            .expect("remote task");
            DownloadTaskView::remote(&dto)
        };
        let Some(DownloadsCommand::RemoteCommand(params)) =
            task_command(&row("paused"), ToolbarCommand::Resume)
        else {
            panic!("paused remote task must resume through remote.command");
        };
        assert_eq!(params.task_id, "r1");
        assert_eq!(params.action, RemoteCommandAction::Resume);
        assert!(!params.delete_files);
        // 状态不允许 / 未知 / 无远程对应的动作：不发命令。
        assert!(task_command(&row("completed"), ToolbarCommand::Pause).is_none());
        assert!(task_command(&row("paused"), ToolbarCommand::Pause).is_none());
        assert!(task_command(&row("brandNewState"), ToolbarCommand::Delete).is_none());
        assert!(task_command(&row("downloading"), ToolbarCommand::Reveal).is_none());
        assert!(matches!(
            task_command(&row("completed"), ToolbarCommand::Delete),
            Some(DownloadsCommand::RemoteCommand(params))
                if params.action == RemoteCommandAction::Delete
        ));
    }

    #[test]
    fn percent_label_floors_and_marks_sub_percent_progress() {
        assert_eq!(percent_label(0.).as_ref(), "0%");
        assert_eq!(percent_label(0.004).as_ref(), "<1%");
        assert_eq!(percent_label(0.01).as_ref(), "1%");
        assert_eq!(percent_label(0.999).as_ref(), "99%");
        assert_eq!(percent_label(1.).as_ref(), "100%");
    }
}

#[cfg(test)]
mod context_menu_tests {
    use super::{MenuEntry, PLUGIN_ERROR_PREFIX, TaskMenuFacts, context_menu_items};
    use crate::model::TaskState;

    fn task(is_local: bool, state: TaskState) -> TaskMenuFacts {
        TaskMenuFacts {
            is_local,
            controllable: true,
            state,
            boosted: false,
            is_torrent_sentinel: false,
            is_plugin_retry_error: false,
            file_missing: false,
        }
    }

    #[test]
    fn empty_selection_has_no_menu_items() {
        assert!(context_menu_items(&[]).is_empty());
    }

    #[test]
    fn uncontrollable_remote_task_offers_no_control_entries() {
        let mut unknown = task(false, TaskState::Pending);
        unknown.controllable = false;
        let items = context_menu_items(&[unknown]);
        assert!(items.contains(&MenuEntry::CopyUrl));
        for entry in [
            MenuEntry::Pause,
            MenuEntry::Resume,
            MenuEntry::Delete,
            MenuEntry::DeleteWithFiles,
        ] {
            assert!(!items.contains(&entry), "{entry:?}");
        }
        // 与可控任务混选时，控制项仍可用（不可控的行在命令层被跳过）。
        let mut unknown = task(false, TaskState::Pending);
        unknown.controllable = false;
        let mixed = context_menu_items(&[unknown, task(false, TaskState::Paused)]);
        assert!(mixed.contains(&MenuEntry::Delete));
    }

    #[test]
    fn single_local_completed_task_shows_full_menu() {
        let items = context_menu_items(&[task(true, TaskState::Completed)]);
        assert_eq!(
            items,
            vec![
                MenuEntry::Boost,
                MenuEntry::OpenFile,
                MenuEntry::OpenFolder,
                MenuEntry::Rename,
                MenuEntry::Redownload,
                MenuEntry::CopyUrl,
                MenuEntry::MoveToQueue,
                MenuEntry::Separator,
                MenuEntry::Delete,
                MenuEntry::DeleteWithFiles,
                MenuEntry::ShowDetail,
            ]
        );
    }

    #[test]
    fn multi_select_only_keeps_items_valid_for_every_selected_task() {
        // 一个本地已完成 + 一个远程下载中：单选专属项（重命名/加速/忽略插件重试）
        // 消失；要求全体本地的项（打开文件/目录/移动队列/重下载）因远程任务
        // 不满足而消失；resume 因两者都不满足「非完成非下载中」而消失；pause
        // 因远程任务处于下载中而显示（存在语义）；复制链接/删除类始终显示；
        // 「详情」因存在本地任务而显示。
        let selection = [
            task(true, TaskState::Completed),
            task(false, TaskState::Downloading),
        ];
        let items = context_menu_items(&selection);
        assert_eq!(
            items,
            vec![
                MenuEntry::Pause,
                MenuEntry::CopyUrl,
                MenuEntry::Separator,
                MenuEntry::Delete,
                MenuEntry::DeleteWithFiles,
                MenuEntry::ShowDetail,
            ]
        );
    }

    #[test]
    fn multi_select_paused_and_failed_shows_resume_but_not_pause() {
        let selection = [task(true, TaskState::Paused), task(true, TaskState::Failed)];
        let items = context_menu_items(&selection);
        assert!(items.contains(&MenuEntry::Resume));
        assert!(!items.contains(&MenuEntry::Pause));
    }

    #[test]
    fn ignore_plugin_retry_only_for_single_failed_plugin_task() {
        let mut plugin_failure = task(true, TaskState::Failed);
        plugin_failure.is_plugin_retry_error = true;
        assert!(context_menu_items(&[plugin_failure]).contains(&MenuEntry::IgnorePluginRetry));

        // 双选即使都满足条件也不显示（单选专属）。
        assert!(
            !context_menu_items(&[plugin_failure, plugin_failure])
                .contains(&MenuEntry::IgnorePluginRetry)
        );

        // 失败但错误消息不带插件前缀不显示。
        let plain_failure = task(true, TaskState::Failed);
        assert!(!context_menu_items(&[plain_failure]).contains(&MenuEntry::IgnorePluginRetry));
        assert_eq!(PLUGIN_ERROR_PREFIX, "[插件]");
    }

    #[test]
    fn boost_toggles_label_by_boosted_state() {
        let mut boosted = task(true, TaskState::Downloading);
        boosted.boosted = true;
        assert!(context_menu_items(&[boosted]).contains(&MenuEntry::CancelBoost));

        let not_boosted = task(true, TaskState::Downloading);
        assert!(context_menu_items(&[not_boosted]).contains(&MenuEntry::Boost));
    }

    #[test]
    fn missing_file_hides_open_file_but_keeps_folder_and_redownload() {
        let mut missing = task(true, TaskState::Completed);
        missing.file_missing = true;
        let items = context_menu_items(&[missing]);
        assert!(!items.contains(&MenuEntry::OpenFile));
        assert!(items.contains(&MenuEntry::OpenFolder));
        assert!(items.contains(&MenuEntry::Redownload));
        // 与可打开的任务混选：「打开文件」要求全体成立。
        assert!(
            !context_menu_items(&[task(true, TaskState::Completed), missing])
                .contains(&MenuEntry::OpenFile)
        );
    }

    #[test]
    fn torrent_sentinel_and_remote_tasks_cannot_redownload() {
        let mut sentinel = task(true, TaskState::Completed);
        sentinel.is_torrent_sentinel = true;
        assert!(!context_menu_items(&[sentinel]).contains(&MenuEntry::Redownload));

        let remote = task(false, TaskState::Completed);
        assert!(!context_menu_items(&[remote]).contains(&MenuEntry::Redownload));
    }
}

#[cfg(test)]
mod midnight_tests {
    use super::until_next_local_day;
    use chrono::{Local, TimeZone};

    /// 跨天定时器必须恰好落在下一个本地日期，不提前（同一天重算无效）也不晚一整天。
    #[test]
    fn next_day_timer_lands_just_after_local_midnight() {
        for (h, m, s) in [(0, 0, 0), (12, 30, 0), (23, 59, 59)] {
            let Some(now) = Local.with_ymd_and_hms(2026, 3, 14, h, m, s).earliest() else {
                continue;
            };
            let delay = until_next_local_day(now);
            let Ok(delay) = chrono::Duration::from_std(delay) else {
                panic!("delay out of range");
            };
            let fired = now + delay;
            assert_eq!(
                fired.date_naive(),
                now.date_naive().succ_opt().unwrap_or_default()
            );
            assert!(delay <= chrono::Duration::hours(25), "{h}:{m}:{s}");
        }
    }
}
