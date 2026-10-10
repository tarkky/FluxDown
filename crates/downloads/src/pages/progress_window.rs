//! 独立下载进度窗口：用户开始的下载一个任务一个窗口，下载中显示进度视图，完成后同一窗口
//! 切换为「下载完成」视图。
//!
//! 由 app 经 `session::attach` 驱动（[`SessionConsumer`] 三个同名 `pub fn`），自带私有
//! [`DownloadsController`] 维护任务状态。窗口尺寸由宿主恢复并交给用户调整，内容区可滚动，
//! 操作栏固定在底部；进度刷新和完成视图切换不改变窗口边界。
//! 开窗与关窗策略由宿主决定（见 [`crate::ProgressWindowTracker`]），本视图只在任务被删除、
//! 用户点「停止」、确认删除任务或打开文件 / 文件夹后发出 [`ProgressWindowEvent::Close`]。

use std::sync::Arc;

use fluxdown_protocol::{AgentEvent, AgentSnapshot, DaemonEvent, ServiceEvent, TaskRuntimeDto};
use fluxdown_ui_components::{
    ControlExt as _, FluxIcon, IconControlExt as _, card, check_row, tabular_numbers,
};
use fluxdown_ui_i18n::Translator;
use fluxdown_ui_theme::active_theme;
use gpui::{
    AnyElement, App, Context, Entity, EventEmitter, FontWeight, InteractiveElement as _,
    IntoElement, ParentElement, SharedString, StatefulInteractiveElement as _, Styled, Window, div,
    prelude::FluentBuilder as _, px,
};
use gpui_component::{
    Disableable as _, Icon, WindowExt as _,
    button::{Button, ButtonVariants as _},
    h_flex, v_flex,
};

use crate::{
    components::{
        file_icon::task_file_icon,
        segment_progress::render_segment_progress,
        task_table::{progress_bar_color, progress_track_color, status_color},
    },
    controller::{DownloadsCommand, DownloadsController, DownloadsPort},
    model::{DownloadTaskView, RowKey, TaskProtocol, TaskState, format_bytes},
    strings::DownloadStrings,
};

/// 窗口最小宽度（逻辑像素），进度与完成视图共用。
pub const PROGRESS_WINDOW_WIDTH: f32 = 480.;
/// 窗口默认及最小高度（逻辑像素）。
pub const PROGRESS_WINDOW_INITIAL_HEIGHT: f32 = 340.;

/// 进度条高度：比任务表更醒目。
const BAR_HEIGHT: f32 = 8.;
/// 文件图标位边长：图标在此位置内居中。
const ICON_TILE: f32 = 40.;
/// 文件图标边长（系统图标自带留白，四周再留 4px）。
const FILE_ICON: f32 = 32.;
/// 完成视图的状态角标边长。
const BADGE: f32 = 18.;
/// 分段列表最大高度，超出后内部滚动。
const PARTS_MAX_HEIGHT: f32 = 168.;
/// 错误信息最大高度，超出后内部滚动（多行 `Caused by:` 链完整可读，窗口不会无限增高）。
const ERROR_MAX_HEIGHT: f32 = 120.;
/// 分段列表行高。
const PART_ROW_HEIGHT: f32 = 24.;
/// 分段序号列宽。
const PART_INDEX_WIDTH: f32 = 36.;
/// 信息卡键值行高与标签列宽（标签为「路径」「地址」这类短词）。
const INFO_ROW_HEIGHT: f32 = 22.;
const INFO_LABEL_WIDTH: f32 = 44.;
/// 超过此值的剩余时间不可信，不显示。
const MAX_ETA_SECS: u64 = 86_400;

/// 视图对宿主的事件。
pub enum ProgressWindowEvent {
    /// 关闭承载窗口（任务已删除 / 用户停止 / 用户删除）。
    Close,
    /// 已把文件 / 所在文件夹交给系统打开：宿主应关窗，但要等打开的程序接管前台后再关，
    /// 否则关闭本应用的前台窗口会让系统把主窗口提到前面。
    HandedOff,
    /// 用户切换全局「完成后显示完成窗口」设置。
    ShowCompletionChanged(bool),
}

/// 命令成功后的窗口去留。
#[derive(Clone, Copy)]
enum AfterSuccess {
    /// 立即关窗（停止 / 删除）。
    Close,
    /// 交给系统程序后关窗（打开文件 / 文件夹）。
    HandOff,
}

pub struct ProgressWindowView {
    task_id: String,
    controller: DownloadsController,
    port: Arc<dyn DownloadsPort>,
    translator: Entity<Translator>,
    strings: DownloadStrings,
    runtime: Option<TaskRuntimeDto>,
    show_completion: bool,
    parts_expanded: bool,
    closed: bool,
    /// 删除确认框里「同时删除已下载的文件」的勾选状态（每次打开确认框重置为不删）。
    delete_files: bool,
    last_error: Option<SharedString>,
}

impl EventEmitter<ProgressWindowEvent> for ProgressWindowView {}

impl ProgressWindowView {
    /// `show_completion` 为宿主从共享设置读取的当前值。
    pub fn new(
        translator: Entity<Translator>,
        task_id: String,
        port: Arc<dyn DownloadsPort>,
        show_completion: bool,
        cx: &mut Context<Self>,
    ) -> Self {
        let strings = DownloadStrings::from_translator(translator.read(cx));
        cx.observe(&translator, |this, translator, cx| {
            this.strings = DownloadStrings::from_translator(translator.read(cx));
            cx.notify();
        })
        .detach();
        Self {
            task_id,
            controller: DownloadsController::new(Arc::clone(&port)),
            port,
            translator,
            strings,
            runtime: None,
            show_completion,
            parts_expanded: false,
            closed: false,
            delete_files: false,
            last_error: None,
        }
    }

    #[must_use]
    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    /// 窗口标题：下载中带百分比（任务栏 / 窗口切换器里可直接看进度）。
    #[must_use]
    pub fn title(&self) -> Option<String> {
        let row = self.row()?;
        if row.name.is_empty() {
            return None;
        }
        Some(
            if row.state == TaskState::Completed || row.size_bytes == 0 {
                row.name.clone()
            } else {
                format!("{} {}", row.progress_label, row.name)
            },
        )
    }

    pub fn set_show_completion(&mut self, value: bool, cx: &mut Context<Self>) {
        if self.show_completion != value {
            self.show_completion = value;
            cx.notify();
        }
    }

    fn row(&self) -> Option<std::cell::Ref<'_, DownloadTaskView>> {
        self.controller
            .store()
            .get(&RowKey::Local(self.task_id.clone()))
    }

    fn t(&self, cx: &App, key: &str) -> SharedString {
        SharedString::from(self.translator.read(cx).text(key).to_owned())
    }

    fn close(&mut self, cx: &mut Context<Self>) {
        if !self.closed {
            self.closed = true;
            cx.emit(ProgressWindowEvent::Close);
        }
    }

    fn refresh(&mut self, cx: &mut Context<Self>) {
        self.runtime = self.controller.task_runtime(&self.task_id).cloned();
        if self.row().is_none() {
            self.close(cx);
            return;
        }
        cx.notify();
    }

    // ---- SessionConsumer（app 经 `attach` 驱动）----

    pub fn replace_snapshot(&mut self, snapshot: &AgentSnapshot, cx: &mut Context<Self>) {
        self.controller.replace_snapshot(snapshot);
        self.last_error = (!snapshot.daemon_connected).then(|| self.strings.disconnected.clone());
        if !snapshot.daemon_connected {
            // 服务未就绪的空快照不代表任务已删除；等 daemon 已连接的快照再判定。
            self.runtime = None;
            cx.notify();
            return;
        }
        self.refresh(cx);
    }

    pub fn apply_event(&mut self, event: &ServiceEvent, cx: &mut Context<Self>) {
        if let ServiceEvent::Agent(AgentEvent::Daemon(DaemonEvent::TaskDeleted { task_id })) = event
            && *task_id == self.task_id
        {
            self.close(cx);
            return;
        }
        if let ServiceEvent::Agent(AgentEvent::DaemonConnectionChanged(connected)) = event {
            self.last_error = (!connected).then(|| self.strings.disconnected.clone());
        }
        if self.controller.apply_event(event) {
            self.refresh(cx);
        }
    }

    pub fn mark_stale(&mut self, cx: &mut Context<Self>) {
        self.controller.mark_stale();
        self.runtime = None;
        self.last_error = Some(self.strings.disconnected.clone());
        cx.notify();
    }

    // ---- 动作 ----

    /// 执行命令；成功后按 `then` 发出关窗类事件（`None` = 保持窗口）。
    fn run(
        &mut self,
        command: DownloadsCommand,
        then: Option<AfterSuccess>,
        cx: &mut Context<Self>,
    ) {
        let future = self.port.execute(command);
        cx.spawn(async move |this, cx| {
            let failed = future.await.is_err();

            let Ok(()) = this.update(cx, |this, cx| {
                if failed {
                    this.last_error = Some(this.strings.action_failed.clone());
                    cx.notify();
                    return;
                }
                match then {
                    Some(AfterSuccess::Close) => this.close(cx),
                    Some(AfterSuccess::HandOff) if !this.closed => {
                        this.closed = true;
                        cx.emit(ProgressWindowEvent::HandedOff);
                    }
                    Some(AfterSuccess::HandOff) | None => {}
                }
            }) else {
                // 视图已释放，结束回调而不再更新状态。
                return;
            };
        })
        .detach();
    }

    fn toggle_pause(&mut self, cx: &mut Context<Self>) {
        let Some(active) = self
            .row()
            .map(|row| matches!(row.state, TaskState::Downloading | TaskState::Pending))
        else {
            return;
        };
        let task_id = self.task_id.clone();
        let command = if active {
            DownloadsCommand::Pause { task_id }
        } else {
            DownloadsCommand::Resume { task_id }
        };
        self.run(command, None, cx);
    }

    /// 停止：暂停任务（保留进度，可稍后继续）并关窗。
    fn stop(&mut self, cx: &mut Context<Self>) {
        let task_id = self.task_id.clone();
        self.run(
            DownloadsCommand::Pause { task_id },
            Some(AfterSuccess::Close),
            cx,
        );
    }

    /// 删除：二次确认，由用户勾选是否同时删除已下载的文件（默认保留）。
    fn confirm_delete(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(name) = self.row().map(|row| row.name.clone()) else {
            return;
        };
        self.delete_files = false;
        let title = self.strings.delete_task.clone();
        let ok_label = self.strings.delete.clone();
        let cancel_label = self.strings.cancel.clone();
        let files_label = self.t(cx, "progressWindowDeleteFiles");
        let this = cx.weak_entity();
        window.open_alert_dialog(cx, move |dialog, _, cx| {
            // 确认框每帧重建：勾选状态从视图读取，描述随勾选切换。
            let delete_files = this
                .upgrade()
                .is_some_and(|view| view.read(cx).delete_files);
            let description = this.upgrade().map_or_else(SharedString::default, |view| {
                view.read(cx)
                    .strings
                    .delete_task_description(&name, delete_files)
            });
            let toggle = this.clone();
            let submit = this.clone();
            dialog
                .title(fluxdown_ui_components::dialog_title(title.clone(), cx))
                .description(description)
                .child(check_row(
                    "progress-delete-files",
                    delete_files,
                    files_label.clone(),
                    move |value, window, cx| {
                        let Ok(()) = toggle.update(cx, |this, _| this.delete_files = value) else {
                            // 视图已释放，确认框随窗口关闭。
                            return;
                        };
                        window.refresh();
                    },
                    cx,
                ))
                .footer(fluxdown_ui_components::dialog_footer(
                    Some(cancel_label.clone()),
                    ok_label.clone(),
                    fluxdown_ui_components::DialogIntent::Destructive,
                    cx,
                ))
                .on_ok(move |_, _, cx| {
                    // 视图释放后没有提交命令，不能让确认框报告成功。
                    submit
                        .update(cx, |this, cx| {
                            let command = DownloadsCommand::Delete {
                                task_id: this.task_id.clone(),
                                delete_files: this.delete_files,
                            };
                            this.run(command, Some(AfterSuccess::Close), cx);
                        })
                        .is_ok()
                })
        });
    }

    fn render_delete_button(&self, cx: &mut Context<Self>) -> Button {
        Button::new("progress-delete")
            .outline()
            .control_icon(cx)
            .icon(FluxIcon::Trash2)
            .tooltip(self.strings.delete_task.clone())
            .on_click(cx.listener(|this, _, window, cx| this.confirm_delete(window, cx)))
    }

    fn open_file(&mut self, cx: &mut Context<Self>) {
        let task_id = self.task_id.clone();
        self.run(
            DownloadsCommand::OpenTask { task_id },
            Some(AfterSuccess::HandOff),
            cx,
        );
    }

    fn reveal(&mut self, cx: &mut Context<Self>) {
        let task_id = self.task_id.clone();
        self.run(
            DownloadsCommand::RevealTask { task_id },
            Some(AfterSuccess::HandOff),
            cx,
        );
    }

    fn toggle_show_completion(&mut self, value: bool, cx: &mut Context<Self>) {
        cx.emit(ProgressWindowEvent::ShowCompletionChanged(value));
    }

    // ---- 渲染 ----

    /// 文件图标位（按所选图标包；系统图标请求中留空，避免先闪一下回退图标）。
    fn render_icon_tile(row: &DownloadTaskView, window: &mut Window, cx: &mut App) -> AnyElement {
        div()
            .flex_none()
            .size(px(ICON_TILE))
            .flex()
            .items_center()
            .justify_center()
            .child(task_file_icon(row, px(FILE_ICON), window, cx))
            .into_any_element()
    }

    fn render_title(name: SharedString, cx: &App) -> gpui::Div {
        let title = active_theme(cx).extended().title;
        div()
            .min_w_0()
            .truncate()
            .text_size(title.size)
            .line_height(title.line_height)
            .font_weight(title.weight)
            .child(name)
    }

    fn render_progress(
        &self,
        row: &DownloadTaskView,
        icon: AnyElement,
        window: &Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let theme = active_theme(cx);
        let tokens = theme.tokens().clone();
        let extended = theme.extended().clone();
        let downloading = row.state == TaskState::Downloading;
        let is_bt = row.protocol == TaskProtocol::Bt;

        let meta = SharedString::from(Self::progress_meta(row));
        let speed = row
            .speed_bytes_per_second
            .filter(|speed| downloading && *speed > 0)
            .map(|speed| format!("{}/s", format_bytes(speed)));
        let eta = row
            .eta_seconds
            .filter(|seconds| downloading && *seconds <= MAX_ETA_SECS)
            .map(|seconds| self.strings.format_eta(seconds));
        let active_transfers = row.active_transfers().filter(|_| downloading).map(|count| {
            self.runtime
                .as_ref()
                .and_then(|runtime| runtime.parallelism_limit)
                .map_or_else(|| count.to_string(), |limit| format!("{count} / {limit}"))
        });
        let peers = self
            .runtime
            .as_ref()
            .filter(|_| downloading && row.runtime_connected && is_bt)
            .and_then(|runtime| runtime.connected_peers);
        let segments = self
            .runtime
            .as_ref()
            .filter(|runtime| !is_bt && runtime.segments.len() > 1)
            .map(|runtime| runtime.segments.len());
        let bar_width =
            (f32::from(window.viewport_size().width) - 2. * f32::from(tokens.spacing.lg)).max(0.);

        let stat = |label: SharedString, value: SharedString, emphasize: bool| {
            v_flex()
                .flex_1()
                .min_w_0()
                .gap(tokens.spacing.xxs)
                .child(
                    div()
                        .text_size(extended.caption.size)
                        .line_height(extended.caption.line_height)
                        .text_color(extended.colors.text_tertiary)
                        .child(label),
                )
                .child(
                    div()
                        .truncate()
                        .text_size(tokens.typography.sm.size)
                        .line_height(tokens.typography.sm.line_height)
                        .font_weight(FontWeight::MEDIUM)
                        .font_features(tabular_numbers())
                        .text_color(if emphasize {
                            tokens.colors.primary
                        } else {
                            tokens.colors.foreground
                        })
                        .child(value),
                )
        };
        let dash = SharedString::from("—");
        let connections = if is_bt {
            (
                self.t(cx, "detailConnectedPeers"),
                peers.map_or_else(|| dash.clone(), |count| count.to_string().into()),
            )
        } else {
            (
                self.t(cx, "detailActiveTransfers"),
                active_transfers.map_or_else(|| dash.clone(), SharedString::from),
            )
        };

        v_flex()
            .child(
                h_flex()
                    .gap(tokens.spacing.md)
                    .px(tokens.spacing.lg)
                    .pt(tokens.spacing.lg)
                    .child(icon)
                    .child(
                        v_flex()
                            .flex_1()
                            .min_w_0()
                            .gap(tokens.spacing.xxs)
                            .child(Self::render_title(SharedString::from(row.name.clone()), cx))
                            .child(
                                h_flex()
                                    .gap(tokens.spacing.xs)
                                    .text_size(tokens.typography.xs.size)
                                    .line_height(tokens.typography.xs.line_height)
                                    .font_features(tabular_numbers())
                                    .child(
                                        div()
                                            .flex_none()
                                            .font_weight(FontWeight::MEDIUM)
                                            .text_color(status_color(row.state, cx))
                                            .child(self.strings.state_label(row.state)),
                                    )
                                    .child(
                                        div()
                                            .min_w_0()
                                            .truncate()
                                            .text_color(extended.colors.text_tertiary)
                                            .child(meta),
                                    ),
                            ),
                    ),
            )
            .child(
                v_flex()
                    .gap(tokens.spacing.xs)
                    .px(tokens.spacing.lg)
                    .pt(tokens.spacing.lg)
                    .child(div().rounded(tokens.radius.sm).overflow_hidden().child(
                        render_segment_progress(
                            self.runtime.as_ref(),
                            row.progress,
                            bar_width,
                            px(BAR_HEIGHT),
                            active_theme(cx).components().progress_radius,
                            progress_bar_color(row.state, cx),
                            progress_track_color(cx),
                        ),
                    ))
                    .child(
                        h_flex()
                            .justify_between()
                            .text_size(tokens.typography.xs.size)
                            .line_height(tokens.typography.xs.line_height)
                            .font_features(tabular_numbers())
                            .text_color(tokens.colors.muted_foreground)
                            .child(
                                div()
                                    .font_weight(FontWeight::MEDIUM)
                                    .text_color(tokens.colors.foreground)
                                    .child(SharedString::from(row.progress_label.clone())),
                            )
                            .when_some(segments, |this, count| {
                                let count = count.to_string();
                                this.child(
                                    div().child(SharedString::from(
                                        self.translator
                                            .read(cx)
                                            .text_with("progressWindowSegments", &[("n", &count)]),
                                    )),
                                )
                            }),
                    ),
            )
            .child(
                div().px(tokens.spacing.lg).pt(tokens.spacing.md).child(
                    card(cx)
                        .px(tokens.spacing.md)
                        .py(tokens.spacing.sm)
                        .child(
                            h_flex()
                                .gap(tokens.spacing.md)
                                .pb(tokens.spacing.sm)
                                .child(stat(
                                    self.t(cx, "infoSpeed"),
                                    speed.map_or_else(|| dash.clone(), SharedString::from),
                                    downloading,
                                ))
                                .child(stat(
                                    self.t(cx, "infoRemaining"),
                                    eta.unwrap_or_else(|| dash.clone()),
                                    false,
                                ))
                                .child(stat(connections.0, connections.1, false)),
                        )
                        .child(div().h(extended.stroke.thin).bg(extended.colors.hairline))
                        .child(
                            v_flex()
                                .pt(tokens.spacing.xs)
                                .child(Self::info_row(
                                    self.t(cx, "infoPath"),
                                    SharedString::from(row.save_dir.clone()),
                                    cx,
                                ))
                                .child(Self::info_row(
                                    self.t(cx, "infoUrl"),
                                    SharedString::from(row.share_url().to_owned()),
                                    cx,
                                )),
                        ),
                ),
            )
            .when(segments.is_some(), |this| this.child(self.render_parts(cx)))
            .when(
                row.state == TaskState::Failed && !row.error_message.is_empty(),
                |this| {
                    this.child(Self::render_error(
                        0,
                        SharedString::from(row.error_message.clone()),
                        cx,
                    ))
                },
            )
            .when_some(self.last_error.clone(), |this, error| {
                this.child(Self::render_error(1, error, cx))
            })
            .into_any_element()
    }

    /// 信息卡的单行键值：窄标签列 + 单行截断的值（路径 / 链接）。
    fn info_row(label: SharedString, value: SharedString, cx: &App) -> gpui::Div {
        let theme = active_theme(cx);
        let tokens = theme.tokens();
        h_flex()
            .gap(tokens.spacing.sm)
            .min_h(px(INFO_ROW_HEIGHT))
            .text_size(tokens.typography.xs.size)
            .line_height(tokens.typography.xs.line_height)
            .child(
                div()
                    .flex_none()
                    .w(theme.text_extent(INFO_LABEL_WIDTH))
                    .text_color(theme.extended().colors.text_tertiary)
                    .child(label),
            )
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .truncate()
                    .text_color(tokens.colors.muted_foreground)
                    .child(value),
            )
    }

    /// 进度视图标题下的元信息行：协议 · 已下载 / 总大小。
    fn progress_meta(row: &DownloadTaskView) -> String {
        let size_label = if row.size_bytes > 0 {
            format!("{} / {}", format_bytes(row.downloaded_bytes), row.size)
        } else {
            format_bytes(row.downloaded_bytes)
        };
        format!("{} · {size_label}", row.protocol.label())
    }

    /// 完成视图的元信息行：大小 · 耗时（耗时未知时只有大小）。
    fn completed_meta(&self, row: &DownloadTaskView, cx: &App) -> String {
        let size = if row.size_bytes > 0 {
            row.size.clone()
        } else {
            format_bytes(row.downloaded_bytes)
        };
        let elapsed = row.completed_at_secs - row.created_at_secs;
        if elapsed > 0 {
            format!(
                "{size} · {} {}",
                self.t(cx, "infoDuration"),
                format_elapsed(elapsed as u64)
            )
        } else {
            size
        }
    }

    /// 错误信息：完整换行显示（含多行 `Caused by:` 链），超过 [`ERROR_MAX_HEIGHT`] 内部滚动。
    /// `slot` 区分同一视图里并存的多条错误（元素 id 不重复）。
    fn render_error(slot: usize, message: SharedString, cx: &App) -> gpui::Div {
        let theme = active_theme(cx);
        let tokens = theme.tokens();
        h_flex()
            .items_start()
            .gap(tokens.spacing.xs)
            .px(tokens.spacing.lg)
            .pt(tokens.spacing.sm)
            .text_size(tokens.typography.xs.size)
            .line_height(tokens.typography.xs.line_height)
            .text_color(tokens.colors.destructive)
            .child(
                div()
                    .flex_none()
                    .h(tokens.typography.xs.line_height)
                    .flex()
                    .items_center()
                    .child(Icon::new(FluxIcon::CircleAlert).size(theme.extended().icon.sm)),
            )
            .child(
                div()
                    .id(("progress-error", slot))
                    .min_w_0()
                    .flex_1()
                    .max_h(px(ERROR_MAX_HEIGHT))
                    .overflow_y_scroll()
                    .child(message),
            )
    }

    /// 分段列表：折叠开关 + 每段状态 / 已下载 / 大小 / 行内进度。
    fn render_parts(&self, cx: &mut Context<Self>) -> AnyElement {
        let theme = active_theme(cx);
        let tokens = theme.tokens().clone();
        let extended = theme.extended().clone();
        let expanded = self.parts_expanded;
        let label = if expanded {
            self.t(cx, "progressWindowPartsHide")
        } else {
            self.t(cx, "progressWindowPartsShow")
        };
        let toggle = h_flex()
            .id("progress-parts-toggle")
            .flex_none()
            .gap(tokens.spacing.xxs)
            .px(tokens.spacing.xs)
            .min_h(px(24.))
            .rounded(tokens.radius.md)
            .cursor_pointer()
            .text_size(tokens.typography.xs.size)
            .line_height(tokens.typography.xs.line_height)
            .text_color(tokens.colors.muted_foreground)
            .hover(move |style| style.bg(extended.colors.row_hover))
            .on_click(cx.listener(|this, _, _, cx| {
                this.parts_expanded = !this.parts_expanded;
                cx.notify();
            }))
            .child(
                Icon::new(if expanded {
                    FluxIcon::ChevronDown
                } else {
                    FluxIcon::ChevronRight
                })
                .size(extended.icon.sm),
            )
            .child(label);

        let mut section = v_flex()
            .px(tokens.spacing.lg)
            .pt(tokens.spacing.sm)
            .gap(tokens.spacing.xs)
            .child(h_flex().child(toggle));
        if expanded {
            section = section.child(self.render_parts_table(cx));
        }
        section.into_any_element()
    }

    fn render_parts_table(&self, cx: &App) -> AnyElement {
        let theme = active_theme(cx);
        let tokens = theme.tokens().clone();
        let extended = theme.extended().clone();
        let running = self
            .row()
            .is_some_and(|row| row.state == TaskState::Downloading);
        let mut segments = self
            .runtime
            .as_ref()
            .map(|runtime| runtime.segments.clone())
            .unwrap_or_default();
        segments.sort_by_key(|segment| segment.index);

        let index_width = theme.text_extent(PART_INDEX_WIDTH);
        let total_width = theme.text_extent(72.);
        let columns =
            |index: AnyElement, status: AnyElement, done: AnyElement, total: AnyElement| {
                h_flex()
                    .gap(tokens.spacing.sm)
                    .child(div().flex_none().w(index_width).child(index))
                    .child(div().flex_1().min_w_0().child(status))
                    .child(div().flex_1().min_w_0().child(done))
                    .child(div().w(total_width).flex_none().text_right().child(total))
            };
        let header_text = |text: SharedString| {
            div()
                .text_size(extended.caption.size)
                .line_height(extended.caption.line_height)
                .text_color(extended.colors.text_tertiary)
                .child(text)
                .into_any_element()
        };

        let rows = segments.into_iter().map(|segment| {
            let size = (segment.end_byte - segment.start_byte + 1).max(0) as u64;
            let downloaded = segment.downloaded_bytes.max(0) as u64;
            let fraction = if size > 0 {
                (downloaded as f32 / size as f32).clamp(0., 1.)
            } else {
                0.
            };
            let status_key = if size > 0 && downloaded >= size {
                "statusCompleted"
            } else if !running {
                "statusPaused"
            } else if segment.active == Some(true) {
                "statusDownloading"
            } else {
                "progressWindowPartWaiting"
            };
            let active = segment.active == Some(true) && running;
            let cell = |text: SharedString| {
                div()
                    .truncate()
                    .text_size(tokens.typography.xs.size)
                    .line_height(tokens.typography.xs.line_height)
                    .font_features(tabular_numbers())
                    .text_color(tokens.colors.muted_foreground)
                    .child(text)
                    .into_any_element()
            };
            div()
                .min_h(px(PART_ROW_HEIGHT))
                .flex()
                .items_center()
                .px(tokens.spacing.md)
                .child(columns(
                    cell(SharedString::from(format!("#{}", segment.index + 1))),
                    div()
                        .truncate()
                        .text_size(tokens.typography.xs.size)
                        .line_height(tokens.typography.xs.line_height)
                        .text_color(if active {
                            tokens.colors.primary
                        } else {
                            tokens.colors.muted_foreground
                        })
                        .child(self.t(cx, status_key))
                        .into_any_element(),
                    h_flex()
                        .gap(tokens.spacing.sm)
                        .child(
                            div()
                                .flex_none()
                                .child(cell(SharedString::from(format_bytes(downloaded)))),
                        )
                        .child(
                            div()
                                .flex_1()
                                .h(px(3.))
                                .rounded(px(1.5))
                                .bg(progress_track_color(cx))
                                .child(
                                    div()
                                        .h_full()
                                        .w(gpui::relative(fraction))
                                        .rounded(px(1.5))
                                        .bg(if active {
                                            tokens.colors.primary
                                        } else {
                                            extended.colors.text_tertiary
                                        }),
                                ),
                        )
                        .into_any_element(),
                    cell(SharedString::from(format_bytes(size))),
                ))
        });

        card(cx)
            .child(
                div()
                    .px(tokens.spacing.md)
                    .py(tokens.spacing.xs)
                    .border_b(extended.stroke.thin)
                    .border_color(extended.colors.hairline)
                    .child(columns(
                        header_text(SharedString::from("#")),
                        header_text(self.t(cx, "infoStatus")),
                        header_text(self.t(cx, "infoDownloaded")),
                        header_text(self.t(cx, "infoSize")),
                    )),
            )
            .child(
                div()
                    .id("progress-parts-list")
                    .max_h(px(PARTS_MAX_HEIGHT))
                    .overflow_y_scroll()
                    .py(tokens.spacing.xxs)
                    .children(rows),
            )
            .into_any_element()
    }

    fn render_progress_footer(&self, active: bool, cx: &mut Context<Self>) -> AnyElement {
        let theme = active_theme(cx);
        let tokens = theme.tokens().clone();
        let this = cx.weak_entity();
        Self::footer(cx)
            .child(
                div().flex_1().min_w_0().child(check_row(
                    "progress-show-completion",
                    self.show_completion,
                    div()
                        .truncate()
                        .text_size(tokens.typography.xs.size)
                        .line_height(tokens.typography.xs.line_height)
                        .text_color(tokens.colors.muted_foreground)
                        .child(self.t(cx, "progressWindowShowCompletion")),
                    move |value, _, cx| {
                        let Ok(()) =
                            this.update(cx, |this, cx| this.toggle_show_completion(value, cx))
                        else {
                            // 视图已释放，结束回调而不再更新状态。
                            return;
                        };
                    },
                    cx,
                )),
            )
            .child(self.render_delete_button(cx))
            .child(
                Button::new("progress-stop")
                    .outline()
                    .control(cx)
                    .label(self.t(cx, "progressWindowStop"))
                    .on_click(cx.listener(|this, _, _, cx| this.stop(cx))),
            )
            .child(
                Button::new("progress-toggle-pause")
                    .primary()
                    .control(cx)
                    .icon(if active {
                        FluxIcon::Pause
                    } else {
                        FluxIcon::Play
                    })
                    .label(if active {
                        self.strings.pause.clone()
                    } else {
                        self.strings.resume.clone()
                    })
                    .on_click(cx.listener(|this, _, _, cx| this.toggle_pause(cx))),
            )
            .into_any_element()
    }

    fn footer(cx: &App) -> gpui::Div {
        let theme = active_theme(cx);
        let tokens = theme.tokens();
        h_flex()
            .flex_none()
            .mt(tokens.spacing.lg)
            .gap(tokens.spacing.sm)
            .px(tokens.spacing.lg)
            .py(tokens.spacing.md)
            .border_t(theme.extended().stroke.thin)
            .border_color(theme.extended().colors.hairline)
            .bg(theme.extended().colors.chrome)
    }

    fn render_completed(
        &self,
        row: &DownloadTaskView,
        icon: AnyElement,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let theme = active_theme(cx);
        let tokens = theme.tokens().clone();
        let extended = theme.extended().clone();
        let missing = row.file_missing;
        let meta = self.completed_meta(row, cx);
        let (badge_icon, badge_color, headline) = if missing {
            (
                FluxIcon::CircleAlert,
                extended.colors.warning,
                self.t(cx, "statusFileMissing"),
            )
        } else {
            (
                FluxIcon::Check,
                extended.colors.success,
                self.t(cx, "downloadCompleted"),
            )
        };

        v_flex()
            .child(
                h_flex()
                    .items_start()
                    .gap(tokens.spacing.md)
                    .px(tokens.spacing.lg)
                    .pt(tokens.spacing.lg)
                    .child(
                        div().relative().flex_none().child(icon).child(
                            div()
                                .absolute()
                                .right(px(-BADGE / 4.))
                                .bottom(px(-BADGE / 4.))
                                .size(px(BADGE))
                                .flex()
                                .items_center()
                                .justify_center()
                                .rounded_full()
                                .border(extended.stroke.strong)
                                .border_color(tokens.colors.surface)
                                .bg(badge_color)
                                .child(
                                    Icon::new(badge_icon)
                                        .size(px(10.))
                                        .text_color(tokens.colors.surface),
                                ),
                        ),
                    )
                    .child(
                        v_flex()
                            .flex_1()
                            .min_w_0()
                            .gap(tokens.spacing.xxs)
                            .child(
                                div()
                                    .text_size(tokens.typography.xs.size)
                                    .line_height(tokens.typography.xs.line_height)
                                    .font_weight(FontWeight::MEDIUM)
                                    .text_color(badge_color)
                                    .child(headline),
                            )
                            .child(Self::render_title(SharedString::from(row.name.clone()), cx))
                            .child(
                                div()
                                    .text_size(tokens.typography.xs.size)
                                    .line_height(tokens.typography.xs.line_height)
                                    .font_features(tabular_numbers())
                                    .text_color(tokens.colors.muted_foreground)
                                    .child(SharedString::from(meta)),
                            )
                            .child(
                                div()
                                    .truncate()
                                    .text_size(tokens.typography.xs.size)
                                    .line_height(tokens.typography.xs.line_height)
                                    .text_color(extended.colors.text_tertiary)
                                    .child(SharedString::from(row.save_dir.clone())),
                            ),
                    ),
            )
            .when_some(self.last_error.clone(), |this, error| {
                this.child(Self::render_error(1, error, cx))
            })
            .into_any_element()
    }

    fn render_completed_footer(&self, missing: bool, cx: &mut Context<Self>) -> AnyElement {
        Self::footer(cx)
            .justify_end()
            .child(div().flex_1().child(self.render_delete_button(cx)))
            .child(
                Button::new("progress-open-folder")
                    .outline()
                    .control(cx)
                    .icon(FluxIcon::FolderOpen)
                    .label(self.strings.open_folder.clone())
                    .on_click(cx.listener(|this, _, _, cx| this.reveal(cx))),
            )
            .child(
                Button::new("progress-open-file")
                    .primary()
                    .control(cx)
                    .icon(FluxIcon::File)
                    .label(self.strings.open_file.clone())
                    .disabled(missing)
                    .on_click(cx.listener(|this, _, _, cx| this.open_file(cx))),
            )
            .into_any_element()
    }
}

/// 耗时：`m:ss`，超过一小时 `h:mm:ss`。
fn format_elapsed(seconds: u64) -> String {
    let (hours, minutes, secs) = (seconds / 3600, seconds / 60 % 60, seconds % 60);
    if hours > 0 {
        format!("{hours}:{minutes:02}:{secs:02}")
    } else {
        format!("{minutes}:{secs:02}")
    }
}

impl gpui::Render for ProgressWindowView {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let surface = active_theme(cx).tokens().colors.surface;
        let (content, footer) = match self.row() {
            Some(row) if row.state == TaskState::Completed => {
                let row = row.clone();
                let icon = Self::render_icon_tile(&row, window, cx);
                (
                    self.render_completed(&row, icon, cx),
                    self.render_completed_footer(row.file_missing, cx),
                )
            }
            Some(row) => {
                let row = row.clone();
                let icon = Self::render_icon_tile(&row, window, cx);
                let active = matches!(row.state, TaskState::Downloading | TaskState::Pending);
                (
                    self.render_progress(&row, icon, window, cx),
                    self.render_progress_footer(active, cx),
                )
            }
            None => (div().into_any_element(), div().into_any_element()),
        };
        v_flex()
            .size_full()
            .bg(surface)
            .child(
                div()
                    .id("progress-content")
                    .flex_1()
                    .min_h_0()
                    .w_full()
                    .overflow_y_scroll()
                    .child(content),
            )
            .child(footer)
    }
}

#[cfg(test)]
mod tests {
    use super::format_elapsed;

    #[test]
    fn elapsed_switches_to_hours_past_sixty_minutes() {
        assert_eq!(format_elapsed(59), "0:59");
        assert_eq!(format_elapsed(201), "3:21");
        assert_eq!(format_elapsed(3_725), "1:02:05");
    }
}
