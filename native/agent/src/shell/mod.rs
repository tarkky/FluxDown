//! agent 系统外壳：托盘可用性、关闭全部 UI 后的驻留策略、按需拉起桌面程序。
//!
//! 驻留语义（`close_to_tray` 偏好，默认开启）：
//! - 托盘可用且开启：关闭全部 UI 只退出界面，agent + daemon 继续运行，托盘常驻；
//! - 托盘不可用或关闭：后台随界面一起退出（界面显式调用 `system.shutdown`；界面异常退出时
//!   由这里的闲置检测兜底）；
//! - headless 构建（未启用 `desktop` feature）没有托盘，后台恒驻留。
//!
//! 托盘本身跑在宿主线程（[`host`]，`desktop` feature），这里只通过 [`TrayPort`] 推送
//! [`TrayModel`] 并消费 [`TrayAction`]。

#[cfg(feature = "desktop")]
pub mod host;
mod prompt_launch;
#[cfg(feature = "desktop")]
mod tray_model;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use fluxdown_protocol::{
    AgentEvent, AgentPreferencesDto, AgentSnapshot, DaemonEvent, ServiceEvent, ShellStatusDto,
    TrayUnavailableReason, method,
};
use serde_json::Value;
use tokio::sync::{Notify, broadcast, mpsc};
use tokio_util::sync::CancellationToken;

use crate::daemon_client::DaemonClient;
use crate::event_hub::AgentEventHub;
use crate::lifecycle::Lifecycle;
use crate::power::PowerService;
use prompt_launch::UiLaunches;

/// 非驻留模式下没有 UI、也没有任何待处理工作多久后完全退出。
const IDLE_EXIT_GRACE: Duration = Duration::from_secs(15);
const CLOSE_TO_TRAY_KEY: &str = "close_to_tray";
const START_MINIMIZED_TO_TRAY_KEY: &str = "start_minimized_to_tray";
/// 官方 UI 的进度窗口开关（设备本地偏好，缺省开启）。
const PROGRESS_WINDOW_PREF: &str = "desktop.progress_window";

/// 托盘可用性，在 runtime 启动前由宿主确定，进程生命周期内不变。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TrayAvailability {
    Available,
    Unavailable(TrayUnavailableReason),
}

/// 托盘 / 系统事件回流到 runtime 的动作。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TrayAction {
    ShowWindow,
    ShowSettings,
    PauseAll,
    ResumeAll,
    CancelShutdown,
    Quit,
    /// 系统把链接 / `.torrent` 交给了 agent（macOS 上仅当 agent 被 Launch Services 视为
    /// 处理程序时出现，如未打包的开发布局；打包后这些事件由外层桌面程序接收）。
    OpenUrls(Vec<String>),
    /// 系统注销 / 关机：执行完全退出。
    SessionEnd,
}

/// 托盘的完整展示状态；宿主按值比较后只应用变化部分。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TrayModel {
    pub visible: bool,
    pub show_window: String,
    pub settings: String,
    pub pause_all: String,
    pub resume_all: String,
    /// `Some` 时显示「取消完成后关机」菜单项。
    pub cancel_shutdown: Option<String>,
    pub quit: String,
    pub tooltip: String,
}

/// runtime → 托盘宿主线程的推送端口。
pub trait TrayPort: Send + Sync {
    fn apply(&self, model: &TrayModel);
}

/// 宿主交给 runtime 的外壳能力。
pub struct ShellHost {
    pub availability: TrayAvailability,
    pub port: Option<Arc<dyn TrayPort>>,
    pub actions: Option<mpsc::UnboundedReceiver<TrayAction>>,
    /// 由开机自启拉起（`--autostart`）。
    pub autostart: bool,
}

impl ShellHost {
    /// 没有托盘宿主线程（headless 构建或无图形会话）。
    #[must_use]
    pub fn without_tray(reason: TrayUnavailableReason, autostart: bool) -> Self {
        Self {
            availability: TrayAvailability::Unavailable(reason),
            port: None,
            actions: None,
            autostart,
        }
    }
}

/// 由托盘可用性与偏好推导外壳状态（快照首帧与后续变化共用）。
#[must_use]
pub fn shell_status(
    availability: TrayAvailability,
    preferences: &AgentPreferencesDto,
) -> ShellStatusDto {
    let close_to_tray = preference_bool(preferences, CLOSE_TO_TRAY_KEY, true);
    match availability {
        TrayAvailability::Available => ShellStatusDto {
            tray_available: true,
            tray_unavailable_reason: None,
            resident: close_to_tray,
        },
        TrayAvailability::Unavailable(reason) => ShellStatusDto {
            tray_available: false,
            tray_unavailable_reason: Some(reason),
            resident: reason == TrayUnavailableReason::NotBuilt,
        },
    }
}

fn preference_bool(preferences: &AgentPreferencesDto, key: &str, default: bool) -> bool {
    preferences
        .values
        .get(key)
        .and_then(Value::as_bool)
        .unwrap_or(default)
}

/// gateway / capture / 控制器共享的外壳状态。
pub struct ShellState {
    availability: TrayAvailability,
    ui_clients: AtomicUsize,
    resident: AtomicBool,
    tray_visible: AtomicBool,
    /// 为待确认交互 / 进度窗口拉起的桌面进程记账；回收线程在进程退出时更新。
    launches: Arc<Mutex<UiLaunches>>,
    /// 本 agent 在当前 daemon 连接上是否已订阅选择请求。
    selection_subscribed: tokio::sync::Mutex<bool>,
    daemon: Arc<DaemonClient>,
    events: AgentEventHub,
    changed: Arc<Notify>,
}

impl ShellState {
    #[must_use]
    pub fn new(
        availability: TrayAvailability,
        daemon: Arc<DaemonClient>,
        events: AgentEventHub,
    ) -> Arc<Self> {
        let status = events.inspect(|snapshot| snapshot.shell.clone());
        Arc::new(Self {
            availability,
            ui_clients: AtomicUsize::new(0),
            resident: AtomicBool::new(status.resident),
            tray_visible: AtomicBool::new(status.tray_available && status.resident),
            launches: Arc::new(Mutex::new(UiLaunches::default())),
            selection_subscribed: tokio::sync::Mutex::new(false),
            daemon,
            events,
            changed: Arc::new(Notify::new()),
        })
    }

    #[must_use]
    pub fn resident(&self) -> bool {
        self.resident.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn ui_clients(&self) -> usize {
        self.ui_clients.load(Ordering::Acquire)
    }

    /// 官方 UI（声明 `client.selections`）完成握手。
    pub async fn ui_connected(&self) {
        self.ui_clients.fetch_add(1, Ordering::AcqRel);
        self.reconcile_selection(false).await;
        self.changed.notify_one();
    }

    /// 官方 UI 断开。最后一个 UI 断开后留静默期给重连，之后若仍有待确认交互由控制循环拉起界面。
    pub async fn ui_disconnected(&self) {
        let previous = self
            .ui_clients
            .try_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                Some(count.saturating_sub(1))
            })
            .unwrap_or_else(|_| panic!("UI client decrement always supplies an updated count"));
        if previous <= 1 {
            self.launches().ui_left(Instant::now());
        }
        self.reconcile_selection(false).await;
        self.changed.notify_one();
    }

    /// 有 UI，或托盘驻留可以随时拉起 UI 时，才订阅 daemon 选择请求；否则 daemon 走默认值。
    async fn reconcile_selection(&self, force: bool) {
        let want = self.ui_clients() > 0 || self.tray_visible.load(Ordering::Acquire);
        let mut subscribed = self.selection_subscribed.lock().await;
        if want == *subscribed && !(force && want) {
            return;
        }
        // 未连接时不调用：首次连接前调用会等待 daemon 就绪，而这里在 UI 握手路径上，
        // 等待会拖住首个快照。连上后 `DaemonConnectionChanged(true)` 强制补订。
        if !self.daemon.is_connected() {
            *subscribed = false;
            return;
        }
        let method_name = if want {
            method::DAEMON_SELECTION_SUBSCRIBE
        } else {
            method::DAEMON_SELECTION_UNSUBSCRIBE
        };
        match self
            .daemon
            .call::<Value, Value>(method_name, Some(serde_json::json!({})))
            .await
        {
            Ok(_) => *subscribed = want,
            Err(error) => {
                tracing::debug!(code = ?error.code, method_name, "selection subscription deferred");
                // daemon 未连接时订阅随连接一起丢失：记为未订阅，重连后强制补订。
                *subscribed = false;
            }
        }
    }

    fn launches(&self) -> MutexGuard<'_, UiLaunches> {
        self.launches.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// 待确认交互发生变化：确认拉起重新计数。
    fn prompts_changed(&self) {
        self.launches().prompts_changed();
    }

    /// 电平触发的确认拉起：有待确认交互、没有 UI 连接时，按 [`UiLaunches`] 的约束拉起界面。
    /// 驻留模式只开确认窗口，非驻留模式开主窗口（没有界面就没有人承载接下来的下载）。
    fn reconcile_prompt(&self, now: Instant) {
        if self.ui_clients() > 0 {
            return;
        }
        let tray_visible = self.tray_visible.load(Ordering::Acquire);
        if !self
            .events
            .inspect(|snapshot| needs_prompt_ui(snapshot, tray_visible))
        {
            return;
        }
        {
            let mut launches = self.launches();
            if !launches.prompt_due(now) {
                return;
            }
            launches.begin_prompt(now);
        }
        let args: &[&str] = if self.resident() { &["--capture"] } else { &[] };
        if let Err(error) = self.launch_tracked(args) {
            tracing::warn!(error = %error, "could not launch desktop for pending prompt");
        }
    }

    /// 静默捕获建成单个任务且没有 UI 连接时，拉起界面承载该任务的进度窗口（界面收到
    /// `--progress-task` 后按用户开始处理；已有主实例时经单实例通道转交）。进度窗口偏好关闭
    /// （`desktop.progress_window` 为 false）或已有 UI（它会收到 `CaptureTasksStarted`）时不拉起。
    pub fn launch_for_progress(&self, task_id: &str) {
        let enabled = self.events.inspect(|snapshot| {
            snapshot
                .preferences
                .values
                .get(PROGRESS_WINDOW_PREF)
                .and_then(Value::as_bool)
                .unwrap_or(true)
        });
        if !enabled || self.ui_clients() > 0 {
            return;
        }
        self.launches().begin_progress(Instant::now());
        let mut args = Vec::with_capacity(3);
        if self.resident() {
            args.push("--capture");
        }
        args.extend(["--progress-task", task_id]);
        if let Err(error) = self.launch_tracked(&args) {
            tracing::warn!(error = %error, "could not launch desktop for progress window");
        }
    }

    /// 拉起已在 [`UiLaunches`] 登记的桌面进程；进程退出（或没能启动）时释放登记并唤醒控制循环
    /// 重新判定。
    fn launch_tracked(&self, args: &[&str]) -> Result<(), crate::platform::PlatformError> {
        let launches = Arc::clone(&self.launches);
        let changed = Arc::clone(&self.changed);
        let launched = crate::platform::launch_desktop_observed(args, move || {
            launches
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .finished();
            changed.notify_one();
        });
        if launched.is_err() {
            self.launches().finished();
        }
        launched
    }

    /// 显示（或新开）主窗口。
    pub fn show_window(&self) {
        if let Err(error) = crate::platform::launch_desktop(&[]) {
            tracing::warn!(error = %error, "could not launch desktop");
        }
    }

    /// 偏好变化后重算驻留与托盘可见；有变化时发布 `ShellChanged` 并返回 `true`。
    async fn refresh(&self) -> bool {
        let (current, next) = self.events.inspect(|snapshot| {
            (
                snapshot.shell.clone(),
                shell_status(self.availability, &snapshot.preferences),
            )
        });
        self.resident.store(next.resident, Ordering::Release);
        self.tray_visible
            .store(next.tray_available && next.resident, Ordering::Release);
        if current == next {
            return false;
        }
        self.events.publish(AgentEvent::ShellChanged(next));
        self.reconcile_selection(false).await;
        true
    }
}

/// 外壳控制器依赖的服务。
pub struct ShellServices {
    pub state: Arc<ShellState>,
    pub lifecycle: Arc<Lifecycle>,
    pub power: Arc<PowerService>,
    pub daemon: Arc<DaemonClient>,
    pub events: AgentEventHub,
    pub gateway: Arc<crate::gateway::GatewayService>,
}

/// 外壳控制循环：偏好 → 驻留 / 托盘展示；托盘动作；待确认交互拉起界面；非驻留闲置退出。
pub async fn run_controller(
    services: ShellServices,
    mut host: ShellHost,
    cancel: CancellationToken,
) {
    let ShellServices {
        state,
        lifecycle,
        power,
        daemon,
        events,
        gateway,
    } = services;
    #[cfg(feature = "desktop")]
    let mut tray = tray_model::TrayPresenter::new(host.port.take());
    #[cfg(not(feature = "desktop"))]
    let _ = host.port.take();
    let mut actions = host.actions.take();
    let (mut receiver, _) = events.subscribe_and_snapshot();

    state.refresh().await;
    state.reconcile_selection(true).await;
    if host.autostart {
        start_from_autostart(&state, &events);
    }
    #[cfg(feature = "desktop")]
    tray.update(&events);

    let mut idle_since: Option<Instant> = None;
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            frame = receiver.recv() => match frame {
                Ok(frame) => handle_frame(&state, &frame.event).await,
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    // 断档期间可能错过待确认交互的变化：按变化处理，确认拉起重新计数。
                    state.prompts_changed();
                    state.reconcile_selection(true).await;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            },
            action = receive_action(&mut actions), if actions.is_some() => match action {
                Some(action) => {
                    handle_action(action, &state, &lifecycle, &power, &daemon, &gateway).await;
                }
                None => actions = None,
            },
            _ = state.changed.notified() => {}
            _ = tick.tick() => {
                if should_idle_exit(&state, &events, &mut idle_since, Instant::now()) {
                    tracing::info!("no UI and no pending work outside tray residency: quitting");
                    lifecycle.request_quit();
                }
            }
        }
        state.refresh().await;
        // 完全退出流程中不再拉起界面：桌面进程会把刚关停的后台重新拉起来。
        if !lifecycle.quit_requested() {
            state.reconcile_prompt(Instant::now());
        }
        #[cfg(feature = "desktop")]
        tray.update(&events);
    }
}

async fn receive_action(
    actions: &mut Option<mpsc::UnboundedReceiver<TrayAction>>,
) -> Option<TrayAction> {
    match actions {
        Some(actions) => actions.recv().await,
        None => std::future::pending().await,
    }
}

async fn handle_frame(state: &ShellState, event: &ServiceEvent) {
    let ServiceEvent::Agent(event) = event else {
        return;
    };
    match event {
        AgentEvent::PendingCapturesChanged(_)
        | AgentEvent::Daemon(
            DaemonEvent::SelectionPending(_) | DaemonEvent::SelectionResolved { .. },
        ) => state.prompts_changed(),
        // daemon 重连后选择订阅随旧连接丢失，必须补订；替换的快照可能带来新的选择请求。
        AgentEvent::DaemonSnapshotReplaced(_) => {
            state.prompts_changed();
            state.reconcile_selection(true).await;
        }
        AgentEvent::DaemonConnectionChanged(true) => {
            state.reconcile_selection(true).await;
        }
        _ => {}
    }
}

async fn handle_action(
    action: TrayAction,
    state: &ShellState,
    lifecycle: &Arc<Lifecycle>,
    power: &PowerService,
    daemon: &DaemonClient,
    gateway: &crate::gateway::GatewayService,
) {
    match action {
        TrayAction::ShowWindow => state.show_window(),
        TrayAction::ShowSettings => {
            if let Err(error) = crate::platform::launch_desktop(&["--settings"]) {
                tracing::warn!(error = %error, "could not launch desktop settings");
            }
        }
        TrayAction::PauseAll => call_daemon(daemon, method::DAEMON_TASK_PAUSE_ALL).await,
        TrayAction::ResumeAll => call_daemon(daemon, method::DAEMON_TASK_RESUME_ALL).await,
        TrayAction::CancelShutdown => power.disarm(),
        TrayAction::Quit | TrayAction::SessionEnd => lifecycle.request_quit(),
        TrayAction::OpenUrls(urls) => {
            submit_opened_urls(gateway, urls).await;
            if !state.resident() && state.ui_clients() == 0 {
                state.show_window();
            }
        }
    }
}

async fn call_daemon(daemon: &DaemonClient, method_name: &str) {
    if let Err(error) = daemon.call::<Value, Value>(method_name, None).await {
        tracing::warn!(code = ?error.code, method_name, "tray daemon action failed");
    }
}

/// 与桌面进程处理系统交来的链接一致：`.torrent` 文件与可捕获链接都静默建任务，并声明来源
/// 关联（[`OpenAssociation`]），已被用户关闭的关联由网关拦截。
///
/// [`OpenAssociation`]: fluxdown_protocol::capture_link::OpenAssociation
async fn submit_opened_urls(gateway: &crate::gateway::GatewayService, urls: Vec<String>) {
    use fluxdown_protocol::capture_link::{
        OpenAssociation, deep_link_file_name, is_capture_url, normalize_capture_url,
        torrent_file_path,
    };
    for url in urls {
        let result = if let Some(path) = torrent_file_path(&url) {
            gateway
                .dispatch_local(
                    method::AGENT_CAPTURE_SUBMIT_TORRENT_FILE,
                    serde_json::json!({
                        "path": path.display().to_string(),
                        "silent": true,
                        "association": OpenAssociation::Torrent,
                    }),
                )
                .await
        } else if is_capture_url(&url) {
            gateway
                .dispatch_local(
                    method::AGENT_CAPTURE_SUBMIT,
                    serde_json::json!({
                        "request": {
                            "url": normalize_capture_url(&url),
                            "filename": deep_link_file_name(&url).unwrap_or_default(),
                        },
                        "silent": true,
                        "association": OpenAssociation::of_url(&url),
                    }),
                )
                .await
        } else {
            continue;
        };
        if let Err(error) = result {
            tracing::warn!(code = ?error.code, "could not submit opened link");
        }
    }
}

/// 开机自启：托盘可见且偏好「启动时最小化到托盘」时只驻留托盘，否则拉起最小化的主窗口。
fn start_from_autostart(state: &ShellState, events: &AgentEventHub) {
    let tray_only = state.tray_visible.load(Ordering::Acquire)
        && events.inspect(|snapshot| {
            preference_bool(&snapshot.preferences, START_MINIMIZED_TO_TRAY_KEY, false)
        });
    if tray_only {
        return;
    }
    if let Err(error) = crate::platform::launch_desktop(&["--minimized"]) {
        tracing::warn!(error = %error, "could not launch desktop at login");
    }
}

/// 非驻留（托盘构建但托盘不可用或已关闭 `close_to_tray`）时，没有 UI 且没有待处理工作
/// 持续 [`IDLE_EXIT_GRACE`] 即完全退出；有下载 / 捕获 / 选择 / 关机计划时一直等待。
fn should_idle_exit(
    state: &ShellState,
    events: &AgentEventHub,
    idle_since: &mut Option<Instant>,
    now: Instant,
) -> bool {
    let eligible = state.availability
        != TrayAvailability::Unavailable(TrayUnavailableReason::NotBuilt)
        && !state.resident()
        && state.ui_clients() == 0
        && events.inspect(snapshot_is_idle);
    if !eligible {
        *idle_since = None;
        return false;
    }
    let since = *idle_since.get_or_insert(now);
    now.saturating_duration_since(since) >= IDLE_EXIT_GRACE
}

fn snapshot_is_idle(snapshot: &AgentSnapshot) -> bool {
    let stats = &snapshot.daemon.runtime_stats;
    // 自动重试间隙的任务在任务表里是失败状态，只在 `retry_pending_tasks` 里可见。
    stats.active_tasks == 0
        && stats.pending_tasks == 0
        && stats.retry_pending_tasks == 0
        && snapshot.pending_captures.is_empty()
        && snapshot.daemon.pending_selections.is_empty()
        && snapshot.power.armed_delay_secs.is_none()
}

/// 是否有必须由界面承载的待确认交互：外部捕获恒需要；引擎选择只在托盘驻留、由 agent 代为订阅
/// 时才需要拉起界面（非驻留且无 UI 时 agent 不订阅，daemon 直接走默认值）。
fn needs_prompt_ui(snapshot: &AgentSnapshot, tray_visible: bool) -> bool {
    !snapshot.pending_captures.is_empty()
        || (tray_visible && !snapshot.daemon.pending_selections.is_empty())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn preferences(values: &[(&str, Value)]) -> AgentPreferencesDto {
        AgentPreferencesDto {
            revision: 1,
            values: values
                .iter()
                .map(|(key, value)| ((*key).to_owned(), value.clone()))
                .collect::<BTreeMap<_, _>>(),
        }
    }

    #[test]
    fn available_tray_follows_close_to_tray_defaulting_to_resident() {
        let status = shell_status(TrayAvailability::Available, &preferences(&[]));
        assert!(status.tray_available && status.resident);
        let status = shell_status(
            TrayAvailability::Available,
            &preferences(&[(CLOSE_TO_TRAY_KEY, Value::Bool(false))]),
        );
        assert!(status.tray_available && !status.resident);
    }

    #[test]
    fn unavailable_tray_quits_with_ui_except_headless_builds() {
        let on = preferences(&[(CLOSE_TO_TRAY_KEY, Value::Bool(true))]);
        let no_host = shell_status(
            TrayAvailability::Unavailable(TrayUnavailableReason::NoHost),
            &on,
        );
        assert!(!no_host.resident);
        assert_eq!(
            no_host.tray_unavailable_reason,
            Some(TrayUnavailableReason::NoHost)
        );
        let headless = shell_status(
            TrayAvailability::Unavailable(TrayUnavailableReason::NotBuilt),
            &preferences(&[(CLOSE_TO_TRAY_KEY, Value::Bool(false))]),
        );
        assert!(
            headless.resident,
            "headless agents stay resident regardless of prefs"
        );
    }

    #[test]
    fn idle_requires_no_tasks_captures_selections_or_shutdown_plan() {
        let mut snapshot = AgentSnapshot::default();
        assert!(snapshot_is_idle(&snapshot));
        snapshot.daemon.runtime_stats.pending_tasks = 1;
        assert!(!snapshot_is_idle(&snapshot));
        snapshot.daemon.runtime_stats.pending_tasks = 0;
        snapshot.daemon.runtime_stats.retry_pending_tasks = 1;
        assert!(!snapshot_is_idle(&snapshot));
        snapshot.daemon.runtime_stats.retry_pending_tasks = 0;
        snapshot.power.armed_delay_secs = Some(0);
        assert!(!snapshot_is_idle(&snapshot));
    }

    #[test]
    fn captures_always_need_ui_but_selections_only_with_visible_tray() {
        let mut snapshot = AgentSnapshot::default();
        assert!(!needs_prompt_ui(&snapshot, true));
        snapshot.daemon.pending_selections.push(
            serde_json::from_value(serde_json::json!({
                "requestId": "selection-1",
                "taskId": "task-1",
                "kind": { "type": "hls", "options": [] },
                "defaultChoice": { "kind": "hls", "index": 0 },
                "deadlineUnixMs": 0,
            }))
            .expect("selection request"),
        );
        assert!(needs_prompt_ui(&snapshot, true));
        assert!(!needs_prompt_ui(&snapshot, false));
        snapshot.daemon.pending_selections.clear();
        snapshot.pending_captures.push(
            serde_json::from_value(serde_json::json!({
                "transactionId": "capture-1",
                "url": "https://example.com/a.zip",
                "createdAtUnixMs": 0,
            }))
            .expect("pending capture"),
        );
        assert!(needs_prompt_ui(&snapshot, false));
    }

    #[tokio::test]
    async fn idle_exit_waits_for_grace_and_resets_on_ui() {
        let events = AgentEventHub::new(AgentSnapshot {
            shell: shell_status(
                TrayAvailability::Unavailable(TrayUnavailableReason::NoHost),
                &AgentPreferencesDto::default(),
            ),
            ..AgentSnapshot::default()
        });
        let state = ShellState::new(
            TrayAvailability::Unavailable(TrayUnavailableReason::NoHost),
            Arc::new(DaemonClient::disconnected()),
            events.clone(),
        );
        let start = Instant::now();
        let mut idle_since = None;
        assert!(!should_idle_exit(&state, &events, &mut idle_since, start));
        assert!(!should_idle_exit(
            &state,
            &events,
            &mut idle_since,
            start + IDLE_EXIT_GRACE - Duration::from_secs(1)
        ));
        state.ui_connected().await;
        assert!(!should_idle_exit(
            &state,
            &events,
            &mut idle_since,
            start + IDLE_EXIT_GRACE
        ));
        state.ui_disconnected().await;
        let reconnect = start + IDLE_EXIT_GRACE;
        assert!(!should_idle_exit(
            &state,
            &events,
            &mut idle_since,
            reconnect
        ));
        assert!(should_idle_exit(
            &state,
            &events,
            &mut idle_since,
            reconnect + IDLE_EXIT_GRACE
        ));
    }

    #[tokio::test]
    async fn resident_and_headless_agents_never_idle_exit() {
        for availability in [
            TrayAvailability::Available,
            TrayAvailability::Unavailable(TrayUnavailableReason::NotBuilt),
        ] {
            let events = AgentEventHub::new(AgentSnapshot {
                shell: shell_status(availability, &AgentPreferencesDto::default()),
                ..AgentSnapshot::default()
            });
            let state = ShellState::new(
                availability,
                Arc::new(DaemonClient::disconnected()),
                events.clone(),
            );
            let mut idle_since = None;
            let start = Instant::now();
            should_idle_exit(&state, &events, &mut idle_since, start);
            assert!(!should_idle_exit(
                &state,
                &events,
                &mut idle_since,
                start + IDLE_EXIT_GRACE * 4
            ));
        }
    }
}
