//! 单线程 daemon actor：独占引擎、串行领域写入并排空所有管理器回流通道。

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use base64::Engine as _;
use fluxdown_engine::Engine;
use fluxdown_engine::download_manager::{
    BT_DISABLED_MESSAGE, CreateGroupSpec, FileExistsBehavior, NewTaskSpec, TaskDone,
};
#[cfg(feature = "plugins")]
use fluxdown_engine::download_manager::{ResolveOutcome, ResolvePreviewOutcome};
use fluxdown_engine::rss::RssValidateOutcome;
use fluxdown_engine::rss::model::RssSourceInfo;
use fluxdown_protocol::CreateTaskRequest;
use tokio::sync::{mpsc, oneshot};
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::selection::DaemonSelection;

/// 生产命令队列容量。
pub const COMMAND_CAPACITY: usize = 64;

/// actor 领域操作。
pub enum ActorOperation {
    CreateTask {
        request: Box<CreateTaskRequest>,
        torrent_file_bytes: Vec<u8>,
        hint_file_size: i64,
        unattended: bool,
    },
    PauseTask {
        task_id: String,
    },
    ResumeTask {
        task_id: String,
    },
    RenameTask {
        task_id: String,
        file_name: String,
    },
    ChangeTaskUrl {
        task_id: String,
        url: String,
    },
    DeleteTask {
        task_id: String,
        delete_files: bool,
    },
    PauseAll,
    ResumeAll,
    RescanFiles,
    PauseTasks {
        task_ids: Vec<String>,
    },
    ResumeTasks {
        task_ids: Vec<String>,
    },
    DeleteTasks {
        task_ids: Vec<String>,
        delete_files: bool,
    },
    SetTaskSeedLimits {
        task_id: String,
        ratio_limit_milli: i64,
        post_ratio_limit_milli: i64,
        seed_time_limit_minutes: i64,
        inactive_time_limit_minutes: i64,
        upload_limit_bps: i64,
    },
    CreateQueue {
        name: String,
        speed_limit_kbps: i64,
        upload_limit_kbps: i64,
        max_concurrent: i32,
        default_save_dir: String,
        default_segments: i32,
        default_user_agent: String,
    },
    UpdateQueue {
        queue_id: String,
        name: String,
        speed_limit_kbps: i64,
        upload_limit_kbps: i64,
        max_concurrent: i32,
        default_save_dir: String,
        default_segments: i32,
        default_user_agent: String,
    },
    DeleteQueue {
        queue_id: String,
    },
    StartQueue {
        queue_id: String,
    },
    StopQueue {
        queue_id: String,
    },
    SetQueueSchedule {
        queue_id: String,
        enabled: bool,
        start_time: String,
        stop_time: String,
        days: i32,
    },
    ReorderQueue {
        queue_id: String,
        task_ids: Vec<String>,
    },
    MoveToQueue {
        task_id: String,
        queue_id: String,
    },
    Boost {
        task_id: String,
    },
    TestProxy {
        proxy_type: String,
        host: String,
        port: String,
        username: String,
        password: String,
    },
    PatchConfig {
        expected_revision: u64,
        values: BTreeMap<String, String>,
    },
    ClearConnPolicy,
    SiteAuthDelete {
        site: String,
    },
    SiteAuthSave {
        site: String,
        user: String,
        pass: String,
    },
    SiteAuthClear,
    CdnReportsPeek,
    CdnReportsAck {
        batch_id: String,
    },
    CdnConfigApply {
        values: BTreeMap<String, String>,
    },
    RefreshTrackerSubscription,
    RefreshEd2kServerSubscription,
    RefreshEd2kNodes,
    MigrationLinkExport,
    MigrationLinkAck {
        revision: u64,
    },
    MigrationGatewayExport,
    MigrationGatewayAck {
        revision: u64,
    },
    CreateGroup {
        spec: Box<CreateGroupSpec>,
    },
    PauseGroup {
        group_id: String,
    },
    ResumeGroup {
        group_id: String,
    },
    DeleteGroup {
        group_id: String,
        delete_files: bool,
    },
    #[cfg(feature = "plugins")]
    ResolvePreview {
        url: String,
        cookies: String,
        referrer: String,
        user_agent: String,
        extra_headers: HashMap<String, String>,
    },
    RssCreate {
        source: Box<RssSourceInfo>,
    },
    RssUpdate {
        source: Box<RssSourceInfo>,
    },
    RssDelete {
        source_id: String,
    },
    RssRefresh {
        source_id: String,
    },
    RssItemAction {
        source_id: String,
        guid: String,
        action: String,
    },
    /// 订阅列表：重读库（未读计数）并叠加内存运行态（上次检查时间等只在内存前进）。
    RssListSources,
    RssValidate {
        url: String,
        cookies: String,
        user_agent: String,
        proxy_url: String,
    },
    /// 剪贴板识别用的只读 HEAD 探测（off-actor）。
    LinkProbe {
        url: String,
    },
    WebhookDeliveries,
    WebhookClear,
    WebhookSimulate,
    WebhookTest {
        endpoint_json: String,
    },
}

/// actor 操作结果。
pub enum ActorResult {
    Unit,
    Created(String),
    Boolean(bool),
    ProxyLatency(i64),
    Config(fluxdown_protocol::DaemonConfigSnapshot),
    ConnPolicy(fluxdown_protocol::ConnPolicySummaryDto),
    SiteAuth(Vec<fluxdown_protocol::SiteAuthEntryDto>),
    SiteAuthEntry(fluxdown_protocol::SiteAuthEntryDto),
    CdnLease(Option<fluxdown_protocol::CdnReportLeaseDto>),
    TrackerRefresh(fluxdown_protocol::TrackerSubRefreshResponse),
    Ed2kRefresh(fluxdown_protocol::Ed2kServerSubRefreshResponse),
    LinkMigration(fluxdown_protocol::LinkMigrationExport),
    GatewayMigration(fluxdown_protocol::GatewayMigrationExport),
    #[cfg(feature = "plugins")]
    ResolvePreview(ResolvePreviewOutcome),
    RssValidation(Box<RssValidateOutcome>),
    LinkProbe(fluxdown_protocol::LinkProbeResult),
    RssSources(Vec<RssSourceInfo>),
    WebhookDeliveries(Vec<fluxdown_engine::webhook::WebhookDelivery>),
    WebhookSimulation(usize),
    WebhookTest(Box<fluxdown_engine::webhook::WebhookDelivery>),
}

/// actor 领域错误。
#[derive(Debug, thiserror::Error)]
pub enum ActorError {
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("operation failed: {0}")]
    Operation(String),
    #[error("config revision conflict; current revision is {current}")]
    RevisionConflict { current: u64 },
    #[error("resource not found")]
    NotFound,
}

/// daemon actor 命令。
pub enum DaemonCommand {
    Execute {
        operation: ActorOperation,
        ack: oneshot::Sender<Result<ActorResult, ActorError>>,
    },
    Shutdown {
        ack: oneshot::Sender<()>,
    },
}

/// 插件解析与重试后台任务合流后的 actor 事件。
pub enum PluginEvent {
    #[cfg(feature = "plugins")]
    Resolve(Box<ResolveOutcome>),
    #[cfg(feature = "plugins")]
    Retry { task_id: String, delay_ms: u64 },
}

/// off-actor 网络抓取回流，数据库提交只在 actor 内执行。
pub enum MaintenanceEvent {
    Tracker {
        outcome: fluxdown_engine::tracker_subscription::FetchOutcome,
        ack: oneshot::Sender<Result<ActorResult, ActorError>>,
    },
    Ed2k {
        outcome: fluxdown_engine::ed2k::server_subscription::ServerFetchOutcome,
        ack: oneshot::Sender<Result<ActorResult, ActorError>>,
    },
    Ed2kNodes {
        outcome: Result<Vec<u8>, String>,
        ack: oneshot::Sender<Result<ActorResult, ActorError>>,
    },
}

/// 从 `DownloadManager` 一次性取出的全部宿主回流接收端。
pub struct EngineReceivers {
    pub done: mpsc::Receiver<TaskDone>,
    pub retry: mpsc::Receiver<String>,
    pub plugin: mpsc::UnboundedReceiver<PluginEvent>,
    pub missing_cleanup: mpsc::Receiver<Vec<String>>,
}

/// daemon actor 命令入口。
#[derive(Clone)]
pub struct DaemonActorHandle {
    commands: mpsc::Sender<DaemonCommand>,
}

impl DaemonActorHandle {
    /// 执行领域操作；命令发送或回执丢失统一视为 unavailable。
    pub async fn execute(&self, operation: ActorOperation) -> Result<ActorResult, ActorCallError> {
        let (ack, response) = oneshot::channel();
        self.commands
            .send(DaemonCommand::Execute { operation, ack })
            .await
            .map_err(|_| ActorCallError::Unavailable)?;
        response
            .await
            .map_err(|_| ActorCallError::Unavailable)?
            .map_err(ActorCallError::Operation)
    }

    /// 请求 actor 完成关闭收尾。
    pub async fn shutdown(&self) -> Result<(), ActorCallError> {
        let (ack, response) = oneshot::channel();
        self.commands
            .send(DaemonCommand::Shutdown { ack })
            .await
            .map_err(|_| ActorCallError::Unavailable)?;
        response.await.map_err(|_| ActorCallError::Unavailable)
    }
}

#[cfg(test)]
impl DaemonActorHandle {
    pub(crate) fn disconnected() -> Self {
        let (commands, receiver) = mpsc::channel(1);
        drop(receiver);
        Self { commands }
    }
}

/// actor 调用失败。
#[derive(Debug, thiserror::Error)]
pub enum ActorCallError {
    #[error("daemon actor is unavailable")]
    Unavailable,
    #[error(transparent)]
    Operation(ActorError),
}

/// 启动独占引擎的 actor。
pub fn spawn_actor(
    engine: Engine,
    receivers: EngineReceivers,
    selections: DaemonSelection,
    events: crate::event_hub::DaemonEventHub,
    cancel: CancellationToken,
) -> (DaemonActorHandle, tokio::task::JoinHandle<()>) {
    let (commands, command_rx) = mpsc::channel(COMMAND_CAPACITY);
    let (maintenance_tx, maintenance_rx) = mpsc::unbounded_channel();
    let handle = DaemonActorHandle { commands };
    selections.spawn_task_watcher(cancel.clone());
    let task = tokio::spawn(run_actor(
        engine,
        command_rx,
        receivers,
        selections,
        events,
        maintenance_tx,
        maintenance_rx,
        cancel,
    ));
    (handle, task)
}

#[allow(
    clippy::too_many_arguments,
    reason = "single actor loop owns all engine and maintenance receivers"
)]
async fn run_actor(
    mut engine: Engine,
    mut commands: mpsc::Receiver<DaemonCommand>,
    mut receivers: EngineReceivers,
    selections: DaemonSelection,
    events: crate::event_hub::DaemonEventHub,
    maintenance_tx: mpsc::UnboundedSender<MaintenanceEvent>,
    mut maintenance_rx: mpsc::UnboundedReceiver<MaintenanceEvent>,
    cancel: CancellationToken,
) {
    engine.manager.load_queues().await;
    engine.manager.load_and_send_all_tasks().await;
    publish_webhook_history(&engine, &events);
    if let Ok(config) = engine.db.get_all_config().await {
        apply_log_limit(&config);
    }
    let mut rss_events = engine.manager.rss.take_event_rx();

    let mut file_scan = tokio::time::interval(Duration::from_secs(300));
    file_scan.set_missed_tick_behavior(MissedTickBehavior::Delay);
    file_scan.tick().await;
    let mut queue_schedule = tokio::time::interval(Duration::from_secs(20));
    queue_schedule.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut rss_poll = tokio::time::interval(Duration::from_secs(60));
    rss_poll.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut seeding = tokio::time::interval(fluxdown_engine::bt_seeding::SEEDING_EVAL_INTERVAL);
    seeding.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            Some(command) = commands.recv() => {
                match command {
                    DaemonCommand::Execute { operation, ack } => {
                        dispatch_operation(
                            operation,
                            ack,
                            &mut engine,
                            &events,
                            &maintenance_tx,
                        )
                        .await;
                    }
                    DaemonCommand::Shutdown { ack } => {
                        selections.resolve_all_defaults();
                        cancel.cancel();
                        if ack.send(()).is_err() {
                            tracing::debug!("daemon shutdown caller disconnected before acknowledgement");
                        }
                        break;
                    }
                }
            }
            Some(done) = receivers.done.recv() => engine.manager.on_task_done(&done).await,
            Some(task_id) = receivers.retry.recv() => {
                if engine.manager.is_task_in_error(&task_id).await {
                    engine.manager.resume_task_auto(&task_id).await;
                }
            }
            Some(event) = receivers.plugin.recv() => {
                match event {
                    #[cfg(feature = "plugins")]
                    PluginEvent::Resolve(outcome) => engine.manager.on_resolve_ready(*outcome).await,
                    #[cfg(feature = "plugins")]
                    PluginEvent::Retry { task_id, delay_ms } => {
                        engine.manager.plugin_request_retry(&task_id, delay_ms).await;
                    }
                }
            }
            Some(event) = maintenance_rx.recv() => {
                commit_maintenance(event, &mut engine).await;
            }
            Some(ids) = receivers.missing_cleanup.recv() => {
                engine.manager.delete_tasks_batch(&ids, false).await;
                engine.manager.send_tasks_snapshot().await;
            }
            _ = file_scan.tick() => {
                // 完全空闲且 idle_file_scan 关闭时跳过，避免周期性唤醒 NAS 硬盘。
                if engine.manager.should_run_idle_scan() {
                    engine.manager.spawn_file_scan();
                }
            }
            _ = queue_schedule.tick() => engine.manager.tick_queue_schedules().await,
            _ = rss_poll.tick() => engine.manager.tick_rss_sources(),
            event = receive_rss_event(&mut rss_events), if rss_events.is_some() => {
                if let Some(event) = event { engine.manager.on_rss_event(event).await; }
            }
            _ = seeding.tick() => engine.manager.tick_seeding_evaluation().await,
        }
    }

    engine.manager.shutdown().await;
    match tokio::time::timeout(Duration::from_secs(5), engine.flush_task_activity()).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::error!(%error, "daemon task activity flush failed"),
        Err(_) => tracing::error!("daemon task activity flush timed out"),
    }
    selections.resolve_all_defaults();
    commands.close();
    while let Some(command) = commands.recv().await {
        match command {
            DaemonCommand::Execute { ack, .. } => {
                if ack
                    .send(Err(ActorError::Operation(
                        "daemon is shutting down".to_owned(),
                    )))
                    .is_err()
                {
                    tracing::debug!("daemon caller disconnected during shutdown");
                }
            }
            DaemonCommand::Shutdown { ack } => {
                if ack.send(()).is_err() {
                    tracing::debug!("daemon shutdown caller disconnected before acknowledgement");
                }
            }
        }
    }
}

async fn dispatch_operation(
    operation: ActorOperation,
    ack: oneshot::Sender<Result<ActorResult, ActorError>>,
    engine: &mut Engine,
    events: &crate::event_hub::DaemonEventHub,
    maintenance_tx: &mpsc::UnboundedSender<MaintenanceEvent>,
) {
    match operation {
        #[cfg(feature = "plugins")]
        ActorOperation::ResolvePreview {
            url,
            cookies,
            referrer,
            user_agent,
            extra_headers,
        } => {
            let receiver = engine.manager.spawn_resolve_preview(
                url,
                cookies,
                referrer,
                user_agent,
                extra_headers,
            );
            tokio::spawn(async move {
                let result = receiver
                    .await
                    .map(ActorResult::ResolvePreview)
                    .map_err(|_| {
                        ActorError::Operation("resolve preview worker dropped".to_owned())
                    });
                if let Err(result) = ack.send(result) {
                    match result {
                        Err(ActorError::Operation(error)) => {
                            tracing::warn!(%error, "resolve preview failed after caller disconnected");
                        }
                        _ => tracing::debug!("resolve preview caller disconnected before result"),
                    }
                }
            });
        }
        ActorOperation::RefreshTrackerSubscription => {
            let config = match engine.db.get_all_config().await {
                Ok(config) => config,
                Err(error) => {
                    if ack
                        .send(Err(ActorError::Operation(format!("{error:#}"))))
                        .is_err()
                    {
                        tracing::warn!(%error, "tracker refresh config read failed after caller disconnected");
                    }
                    return;
                }
            };
            let urls = config
                .get("bt_tracker_sub_urls")
                .cloned()
                .unwrap_or_else(fluxdown_engine::tracker_subscription::default_subscription_urls);
            let maintenance_tx = maintenance_tx.clone();
            tokio::spawn(async move {
                let outcome =
                    fluxdown_engine::tracker_subscription::fetch_subscriptions(&urls).await;
                if maintenance_tx
                    .send(MaintenanceEvent::Tracker { outcome, ack })
                    .is_err()
                {
                    // Actor 已关闭，未提交的结果丢弃并关闭回执，等待者收到 Unavailable。
                    tracing::debug!("daemon actor closed before tracker refresh commit");
                }
            });
        }
        ActorOperation::RefreshEd2kServerSubscription => {
            let config = match engine.db.get_all_config().await {
                Ok(config) => config,
                Err(error) => {
                    if ack
                        .send(Err(ActorError::Operation(format!("{error:#}"))))
                        .is_err()
                    {
                        tracing::warn!(%error, "ED2K refresh config read failed after caller disconnected");
                    }
                    return;
                }
            };
            let urls = config.get("ed2k_server_sub_urls").cloned().unwrap_or_else(
                fluxdown_engine::ed2k::server_subscription::default_server_met_urls,
            );
            let maintenance_tx = maintenance_tx.clone();
            tokio::spawn(async move {
                let outcome =
                    fluxdown_engine::ed2k::server_subscription::fetch_server_subscriptions(&urls)
                        .await;
                if maintenance_tx
                    .send(MaintenanceEvent::Ed2k { outcome, ack })
                    .is_err()
                {
                    // Actor 已关闭，未提交的结果丢弃并关闭回执，等待者收到 Unavailable。
                    tracing::debug!("daemon actor closed before ED2K refresh commit");
                }
            });
        }
        ActorOperation::RefreshEd2kNodes => {
            let url = match engine.db.get_config("ed2k_nodes_dat_url").await {
                Ok(Some(url)) if !url.trim().is_empty() => url,
                Ok(_) => {
                    if ack.send(Ok(ActorResult::Unit)).is_err() {
                        tracing::debug!("ED2K nodes caller disconnected before acknowledgement");
                    }
                    return;
                }
                Err(error) => {
                    if ack
                        .send(Err(ActorError::Operation(format!("{error:#}"))))
                        .is_err()
                    {
                        tracing::warn!(%error, "ED2K nodes config read failed after caller disconnected");
                    }
                    return;
                }
            };
            let maintenance_tx = maintenance_tx.clone();
            tokio::spawn(async move {
                let outcome = fluxdown_engine::ed2k::kad::fetch_nodes_dat(&url).await;
                if maintenance_tx
                    .send(MaintenanceEvent::Ed2kNodes { outcome, ack })
                    .is_err()
                {
                    // Actor 已关闭，未提交的结果丢弃并关闭回执，等待者收到 Unavailable。
                    tracing::debug!("daemon actor closed before ED2K nodes commit");
                }
            });
        }
        ActorOperation::RssValidate {
            url,
            cookies,
            user_agent,
            proxy_url,
        } => {
            let future = engine
                .manager
                .rss_validate_future(url, cookies, user_agent, proxy_url);
            tokio::spawn(async move {
                if ack
                    .send(Ok(ActorResult::RssValidation(Box::new(future.await))))
                    .is_err()
                {
                    tracing::debug!("RSS validation caller disconnected before result");
                }
            });
        }
        ActorOperation::LinkProbe { url } => {
            let future = engine.manager.link_probe_future(url);
            tokio::spawn(async move {
                let result = link_probe_to_dto(future.await);
                if ack.send(Ok(ActorResult::LinkProbe(result))).is_err() {
                    tracing::debug!("link probe caller disconnected before result");
                }
            });
        }
        ActorOperation::TestProxy {
            proxy_type,
            host,
            port,
            username,
            password,
        } => {
            // 连通性测试最长数十秒且不依赖引擎状态，不能占用 actor 循环。
            tokio::spawn(async move {
                let result = fluxdown_engine::proxy_config::test_proxy_connection(
                    &proxy_type,
                    &host,
                    &port,
                    &username,
                    &password,
                )
                .await
                .map(ActorResult::ProxyLatency)
                .map_err(|error| ActorError::Operation(format!("{error:#}")));
                if let Err(result) = ack.send(result) {
                    match result {
                        Err(ActorError::Operation(error)) => {
                            tracing::warn!(%error, "proxy test failed after caller disconnected");
                        }
                        _ => tracing::debug!("proxy test caller disconnected before result"),
                    }
                }
            });
        }
        ActorOperation::WebhookTest { endpoint_json } => {
            let dispatcher = engine.manager.webhook();
            tokio::spawn(async move {
                let result =
                    serde_json::from_str::<fluxdown_engine::webhook::EndpointSpec>(&endpoint_json)
                        .map_err(|error| ActorError::InvalidArgument(error.to_string()));
                let result = match result {
                    Ok(spec) => Ok(ActorResult::WebhookTest(Box::new(
                        dispatcher.test_endpoint(spec).await,
                    ))),
                    Err(error) => Err(error),
                };
                if let Err(result) = ack.send(result) {
                    match result {
                        Err(ActorError::Operation(error)) => {
                            tracing::warn!(%error, "webhook test failed after caller disconnected");
                        }
                        _ => tracing::debug!("webhook test caller disconnected before result"),
                    }
                }
            });
        }
        operation => {
            let result = execute_operation(operation, engine, events).await;
            if let Err(result) = ack.send(result) {
                match result {
                    Err(ActorError::Operation(error)) => {
                        tracing::warn!(%error, "daemon operation failed after caller disconnected");
                    }
                    _ => tracing::debug!("daemon operation caller disconnected before result"),
                }
            }
        }
    }
}

async fn execute_operation(
    operation: ActorOperation,
    engine: &mut Engine,
    events: &crate::event_hub::DaemonEventHub,
) -> Result<ActorResult, ActorError> {
    match operation {
        ActorOperation::CreateTask {
            request,
            mut torrent_file_bytes,
            hint_file_size,
            unattended,
        } => {
            let request = *request;
            if torrent_file_bytes.is_empty() {
                torrent_file_bytes = decode_torrent_b64(request.torrent_b64.as_deref())?;
            }
            // BT 被禁用：在落库前回报明确原因，而不是让 create_task 的无原因 `None`
            // 变成误导性的「failed to persist task」。
            if engine
                .manager
                .rejects_new_bt_task(&request.url, &torrent_file_bytes)
            {
                return Err(ActorError::InvalidArgument(BT_DISABLED_MESSAGE.to_owned()));
            }
            let mut save_dir = request.save_dir;
            if save_dir.trim().is_empty() {
                save_dir = engine
                    .db
                    .get_config("default_save_dir")
                    .await
                    .map_err(|error| ActorError::Operation(format!("{error:#}")))?
                    .unwrap_or_default();
            }
            if save_dir.trim().is_empty() {
                save_dir = fluxdown_engine::user_dirs::download_dir_or_cwd();
            }
            let queue_id = resolve_queue_id(&engine.db, request.queue_id).await?;
            let task_id = engine
                .manager
                .create_task(NewTaskSpec {
                    url: request.url,
                    save_dir,
                    file_name: request.file_name,
                    segments: request.segments,
                    cookies: request.cookies,
                    referrer: request.referrer,
                    hint_file_size,
                    torrent_file_bytes,
                    proxy_url: request.proxy_url,
                    user_agent: request.user_agent,
                    queue_id,
                    checksum: request.checksum,
                    ignore_tls_errors: request.ignore_tls_errors,
                    extra_headers: request.headers.unwrap_or_default(),
                    method: request.method,
                    body: request
                        .body
                        .map(fluxdown_engine_protocol::request_body_to_engine),
                    audio_url: request.audio_url,
                    start_paused: request.start_paused,
                    http_user: request.http_user,
                    http_password: request.http_password,
                    save_site_auth: request.save_site_auth,
                    unattended_selection: unattended,
                    ..Default::default()
                })
                .await
                .ok_or_else(|| ActorError::Operation("failed to persist task".to_owned()))?;
            engine.manager.send_tasks_snapshot().await;
            return Ok(ActorResult::Created(task_id));
        }
        ActorOperation::PauseTask { task_id } => engine.manager.pause_task(&task_id).await,
        ActorOperation::ResumeTask { task_id } => {
            // BT 被禁用时恢复 BT 任务会被引擎静默拒绝（任务保持原状）；
            // 在此显式回报原因，客户端才不会收到无信息的 `ok`。
            let rejected = engine
                .manager
                .bt_disabled_resume_rejections(std::slice::from_ref(&task_id))
                .await
                .map_err(|error| ActorError::Operation(format!("{error:#}")))?;
            if !rejected.is_empty() {
                return Err(ActorError::InvalidArgument(BT_DISABLED_MESSAGE.to_owned()));
            }
            engine.manager.resume_task(&task_id).await;
        }
        ActorOperation::RenameTask { task_id, file_name } => engine
            .manager
            .rename_task(&task_id, &file_name)
            .await
            .map_err(ActorError::Operation)?,
        ActorOperation::ChangeTaskUrl { task_id, url } => engine
            .manager
            .change_task_url(&task_id, &url)
            .await
            .map_err(ActorError::Operation)?,
        ActorOperation::DeleteTask {
            task_id,
            delete_files,
        } => {
            engine.manager.delete_task(&task_id, delete_files).await;
            engine.manager.send_tasks_snapshot().await;
        }
        ActorOperation::PauseAll => {
            let ids = task_ids_by_status(&engine.db, &[0, 1, 5]).await?;
            engine.manager.batch_pause_all(&ids).await;
        }
        ActorOperation::PauseTasks { task_ids } => {
            let ids = existing_task_ids(&engine.db, task_ids).await?;
            engine.manager.batch_pause(&ids).await;
        }
        ActorOperation::ResumeTasks { task_ids } => {
            let ids = existing_task_ids(&engine.db, task_ids).await?;
            // 批量语义：BT 禁用时 BT 任务被跳过、其余照常恢复；但整批都被拒
            // 时必须回报原因，否则用户只看到「成功」而什么都没发生。
            let rejected = engine
                .manager
                .bt_disabled_resume_rejections(&ids)
                .await
                .map_err(|error| ActorError::Operation(format!("{error:#}")))?;
            if !ids.is_empty() && rejected.len() == ids.len() {
                return Err(ActorError::InvalidArgument(BT_DISABLED_MESSAGE.to_owned()));
            }
            engine.manager.batch_resume(&ids).await;
        }
        ActorOperation::DeleteTasks {
            task_ids,
            delete_files,
        } => {
            let ids = existing_task_ids(&engine.db, task_ids).await?;
            if !ids.is_empty() {
                engine.manager.delete_tasks_batch(&ids, delete_files).await;
                engine.manager.send_tasks_snapshot().await;
            }
        }
        ActorOperation::ResumeAll => {
            engine.manager.resume_all_eligible().await;
        }
        ActorOperation::RescanFiles => engine.manager.spawn_file_scan(),
        ActorOperation::SetTaskSeedLimits {
            task_id,
            ratio_limit_milli,
            post_ratio_limit_milli,
            seed_time_limit_minutes,
            inactive_time_limit_minutes,
            upload_limit_bps,
        } => {
            engine
                .manager
                .set_task_seed_limits(
                    &task_id,
                    ratio_limit_milli,
                    post_ratio_limit_milli,
                    seed_time_limit_minutes,
                    inactive_time_limit_minutes,
                    upload_limit_bps,
                )
                .await;
        }
        ActorOperation::CreateQueue {
            name,
            speed_limit_kbps,
            upload_limit_kbps,
            max_concurrent,
            default_save_dir,
            default_segments,
            default_user_agent,
        } => {
            engine
                .manager
                .create_queue(
                    name,
                    speed_limit_kbps,
                    upload_limit_kbps,
                    max_concurrent,
                    default_save_dir,
                    default_segments,
                    default_user_agent,
                )
                .await
        }
        ActorOperation::UpdateQueue {
            queue_id,
            name,
            speed_limit_kbps,
            upload_limit_kbps,
            max_concurrent,
            default_save_dir,
            default_segments,
            default_user_agent,
        } => {
            engine
                .manager
                .update_queue(
                    queue_id,
                    name,
                    speed_limit_kbps,
                    upload_limit_kbps,
                    max_concurrent,
                    default_save_dir,
                    default_segments,
                    default_user_agent,
                )
                .await
        }
        ActorOperation::DeleteQueue { queue_id } => engine.manager.delete_queue(queue_id).await,
        ActorOperation::StartQueue { queue_id } => engine.manager.start_queue(queue_id).await,
        ActorOperation::StopQueue { queue_id } => engine.manager.stop_queue(queue_id).await,
        ActorOperation::SetQueueSchedule {
            queue_id,
            enabled,
            start_time,
            stop_time,
            days,
        } => {
            engine
                .manager
                .set_queue_schedule(queue_id, enabled, start_time, stop_time, days)
                .await
        }
        ActorOperation::ReorderQueue { queue_id, task_ids } => {
            engine.manager.reorder_queue_tasks(queue_id, task_ids).await
        }
        ActorOperation::MoveToQueue { task_id, queue_id } => {
            engine.manager.move_task_to_queue(task_id, queue_id).await
        }
        ActorOperation::Boost { task_id } => engine.manager.set_priority_task(task_id).await,
        ActorOperation::PatchConfig {
            expected_revision,
            values,
        } => {
            let snapshot = patch_config(engine, events, expected_revision, values).await?;
            return Ok(ActorResult::Config(snapshot));
        }
        ActorOperation::ClearConnPolicy => {
            return Ok(ActorResult::ConnPolicy(
                clear_conn_policy(engine, events).await?,
            ));
        }
        ActorOperation::SiteAuthDelete { site } => {
            return Ok(ActorResult::SiteAuth(
                delete_site_auth(&engine.db, &site).await?,
            ));
        }
        ActorOperation::SiteAuthSave { site, user, pass } => {
            return Ok(ActorResult::SiteAuthEntry(
                upsert_site_auth(&engine.db, site, user, pass).await?,
            ));
        }
        ActorOperation::SiteAuthClear => {
            return Ok(ActorResult::SiteAuth(
                save_site_auth(&engine.db, &BTreeMap::new()).await?,
            ));
        }
        ActorOperation::CdnReportsPeek => return cdn_reports_peek(engine).await,
        ActorOperation::CdnReportsAck { batch_id } => {
            let acknowledged = engine
                .db
                .ack_cdn_report_lease(&batch_id)
                .await
                .map_err(|error| ActorError::Operation(format!("{error:#}")))?;
            return Ok(ActorResult::Boolean(acknowledged));
        }
        ActorOperation::CdnConfigApply { values } => {
            apply_cdn_config(engine, values).await?;
        }
        ActorOperation::MigrationLinkExport => {
            return Ok(ActorResult::LinkMigration(
                link_migration_export(engine).await?,
            ));
        }
        ActorOperation::MigrationLinkAck { revision } => {
            migration_ack(engine, "daemon_migration_link_acked", revision).await?;
        }
        ActorOperation::MigrationGatewayExport => {
            return Ok(ActorResult::GatewayMigration(
                gateway_migration_export(engine).await?,
            ));
        }
        ActorOperation::MigrationGatewayAck { revision } => {
            migration_ack(engine, "daemon_migration_gateway_acked", revision).await?;
        }
        ActorOperation::CreateGroup { spec } => {
            let mut spec = *spec;
            spec.queue_id = resolve_queue_id(&engine.db, spec.queue_id).await?;
            let group_id = engine
                .manager
                .create_task_group(spec)
                .await
                .ok_or_else(|| ActorError::Operation("failed to persist group".to_owned()))?;
            return Ok(ActorResult::Created(group_id));
        }
        ActorOperation::PauseGroup { group_id } => engine.manager.pause_group(&group_id).await,
        ActorOperation::ResumeGroup { group_id } => engine.manager.resume_group(&group_id).await,
        ActorOperation::DeleteGroup {
            group_id,
            delete_files,
        } => {
            engine.manager.delete_group(&group_id, delete_files).await;
            engine.manager.send_tasks_snapshot().await;
        }
        #[cfg(feature = "plugins")]
        ActorOperation::ResolvePreview { .. } => unreachable!("handled off actor"),
        ActorOperation::RssListSources => {
            engine.manager.rss.load().await;
            return Ok(ActorResult::RssSources(
                engine.manager.rss.sources().to_vec(),
            ));
        }
        ActorOperation::RssCreate { source } => {
            let id = engine
                .manager
                .create_rss_source(*source)
                .await
                .ok_or_else(|| ActorError::InvalidArgument("RSS URL is required".to_owned()))?;
            return Ok(ActorResult::Created(id));
        }
        ActorOperation::RssUpdate { source } => {
            return Ok(ActorResult::Boolean(
                engine.manager.rss.update_source(*source).await,
            ));
        }
        ActorOperation::RssDelete { source_id } => {
            return Ok(ActorResult::Boolean(
                engine.manager.rss.delete_source(&source_id).await,
            ));
        }
        ActorOperation::RssRefresh { source_id } => {
            use fluxdown_engine::rss::RssRefreshOutcome;
            // 已在抓取中同样算成功：结果稍后经 RSS 事件到达，只有订阅不存在才是 NotFound。
            return Ok(ActorResult::Boolean(!matches!(
                engine.manager.refresh_rss_source(&source_id),
                RssRefreshOutcome::NotFound
            )));
        }
        ActorOperation::RssItemAction {
            source_id,
            guid,
            action,
        } => match action.as_str() {
            "download" => engine.manager.download_rss_item(&source_id, &guid).await,
            "ignore" => engine.manager.rss.ignore_item(&source_id, &guid).await,
            "readAll" => engine.manager.rss.mark_all_read(&source_id).await,
            _ => {
                return Err(ActorError::InvalidArgument(
                    "unknown RSS item action".to_owned(),
                ));
            }
        },
        ActorOperation::RefreshTrackerSubscription
        | ActorOperation::RefreshEd2kServerSubscription
        | ActorOperation::RefreshEd2kNodes
        | ActorOperation::RssValidate { .. }
        | ActorOperation::LinkProbe { .. }
        | ActorOperation::WebhookTest { .. }
        | ActorOperation::TestProxy { .. } => unreachable!("handled off actor"),
        ActorOperation::WebhookDeliveries => {
            return Ok(ActorResult::WebhookDeliveries(engine.webhook_deliveries()));
        }
        ActorOperation::WebhookClear => {
            engine.clear_webhook_deliveries().await;
            events.publish(fluxdown_protocol::DaemonEvent::WebhooksCleared);
        }
        ActorOperation::WebhookSimulate => {
            return Ok(ActorResult::WebhookSimulation(
                engine.simulate_webhook_event(),
            ));
        }
    }
    Ok(ActorResult::Unit)
}

async fn link_migration_export(
    engine: &Engine,
) -> Result<fluxdown_protocol::LinkMigrationExport, ActorError> {
    ensure_migration_available(engine, "daemon_migration_link_acked").await?;
    let identity = engine
        .db
        .get_config("link.identity_secret")
        .await
        .map_err(|error| ActorError::Operation(format!("{error:#}")))?
        .map_or(serde_json::Value::Null, serde_json::Value::String);
    let roster = engine
        .db
        .link_load_devices()
        .await
        .map_err(|error| ActorError::Operation(format!("{error:#}")))?
        .into_iter()
        .map(|row| {
            serde_json::json!({
                "fingerprint": row.fingerprint,
                "identityPubB64": base64::engine::general_purpose::STANDARD.encode(row.identity_pub),
                "name": row.name,
                "platform": row.platform,
                "linkSecretB64": base64::engine::general_purpose::STANDARD.encode(row.link_secret),
                "candidatesJson": row.candidates_json,
                "pairedAt": row.paired_at,
                "lastSeenAt": row.last_seen_at,
            })
        })
        .collect();
    Ok(fluxdown_protocol::LinkMigrationExport {
        revision: 1,
        identity,
        roster,
    })
}

async fn gateway_migration_export(
    engine: &Engine,
) -> Result<fluxdown_protocol::GatewayMigrationExport, ActorError> {
    ensure_migration_available(engine, "daemon_migration_gateway_acked").await?;
    let config = engine
        .db
        .get_all_config()
        .await
        .map_err(|error| ActorError::Operation(format!("{error:#}")))?;
    let enabled = |key: &str| config.get(key).is_some_and(|value| value == "true");
    Ok(fluxdown_protocol::GatewayMigrationExport {
        revision: 1,
        takeover_enabled: enabled("local_server_takeover_enabled"),
        jsonrpc_enabled: enabled("local_server_jsonrpc_enabled"),
        api_enabled: enabled("local_server_api_enabled"),
        mcp_enabled: enabled("local_server_mcp_enabled"),
        cors_enabled: enabled("local_server_cors_allow_all"),
        user_token_configured: config
            .get("local_server_token")
            .is_some_and(|value| !value.is_empty()),
        user_token: config
            .get("local_server_token")
            .cloned()
            .unwrap_or_default(),
    })
}

async fn ensure_migration_available(engine: &Engine, marker: &str) -> Result<(), ActorError> {
    let acknowledged = engine
        .db
        .get_config(marker)
        .await
        .map_err(|error| ActorError::Operation(format!("{error:#}")))?
        .is_some_and(|value| value == "1");
    if acknowledged {
        Err(ActorError::NotFound)
    } else {
        Ok(())
    }
}

async fn migration_ack(engine: &Engine, marker: &str, revision: u64) -> Result<(), ActorError> {
    if revision != 1 {
        return Err(ActorError::InvalidArgument(
            "migration revision does not match export".to_owned(),
        ));
    }
    ensure_migration_available(engine, marker).await?;
    engine
        .db
        .set_config(marker, "1")
        .await
        .map_err(|error| ActorError::Operation(format!("{error:#}")))
}

async fn commit_maintenance(event: MaintenanceEvent, engine: &mut Engine) {
    match event {
        MaintenanceEvent::Tracker { outcome, ack } => {
            let result = commit_tracker_refresh(outcome, engine).await;
            if let Err(result) = ack.send(result.map(ActorResult::TrackerRefresh)) {
                match result {
                    Err(ActorError::Operation(error)) => {
                        tracing::warn!(%error, "tracker refresh commit failed after caller disconnected");
                    }
                    _ => {
                        tracing::debug!("tracker refresh commit caller disconnected before result")
                    }
                }
            }
        }
        MaintenanceEvent::Ed2kNodes { outcome, ack } => {
            let result = commit_ed2k_nodes(outcome, engine).await;
            if let Err(result) = ack.send(result) {
                match result {
                    Err(ActorError::Operation(error)) => {
                        tracing::warn!(%error, "ED2K nodes commit failed after caller disconnected");
                    }
                    _ => tracing::debug!("ED2K nodes commit caller disconnected before result"),
                }
            }
        }
        MaintenanceEvent::Ed2k { outcome, ack } => {
            let result = commit_ed2k_refresh(outcome, engine).await;
            if let Err(result) = ack.send(result.map(ActorResult::Ed2kRefresh)) {
                match result {
                    Err(ActorError::Operation(error)) => {
                        tracing::warn!(%error, "ED2K refresh commit failed after caller disconnected");
                    }
                    _ => tracing::debug!("ED2K refresh commit caller disconnected before result"),
                }
            }
        }
    }
}

async fn commit_ed2k_nodes(
    outcome: Result<Vec<u8>, String>,
    engine: &mut Engine,
) -> Result<ActorResult, ActorError> {
    let bytes = outcome.map_err(ActorError::Operation)?;
    let values = BTreeMap::from([
        (
            "ed2k_nodes_dat_cache".to_owned(),
            base64::engine::general_purpose::STANDARD.encode(bytes),
        ),
        (
            "ed2k_nodes_dat_updated_at".to_owned(),
            now_unix_secs().to_string(),
        ),
    ]);
    engine
        .db
        .set_config_batch_atomic(&values)
        .await
        .map_err(|error| ActorError::Operation(format!("{error:#}")))?;
    Ok(ActorResult::Unit)
}

async fn commit_tracker_refresh(
    outcome: fluxdown_engine::tracker_subscription::FetchOutcome,
    engine: &mut Engine,
) -> Result<fluxdown_protocol::TrackerSubRefreshResponse, ActorError> {
    let mut updated_at = engine
        .db
        .get_config("bt_tracker_sub_updated_at")
        .await
        .map_err(|error| ActorError::Operation(format!("{error:#}")))?
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0);
    if outcome.is_success() {
        updated_at = now_unix_secs();
        let values = BTreeMap::from([
            (
                "bt_tracker_sub_cache".to_owned(),
                outcome.trackers.join("\n"),
            ),
            (
                "bt_tracker_sub_updated_at".to_owned(),
                updated_at.to_string(),
            ),
        ]);
        engine
            .db
            .set_config_batch_atomic(&values)
            .await
            .map_err(|error| ActorError::Operation(format!("{error:#}")))?;
        let all = engine
            .db
            .get_all_config()
            .await
            .map_err(|error| ActorError::Operation(format!("{error:#}")))?;
        engine
            .manager
            .set_bt_config(crate::config::bt_config_from_map(&all));
        engine.manager.invalidate_bt_session().await;
    }
    Ok(fluxdown_protocol::TrackerSubRefreshResponse {
        success: outcome.is_success(),
        tracker_count: outcome.trackers.len() as i64,
        ok_sources: outcome.ok_sources as i64,
        total_sources: outcome.total_sources as i64,
        updated_at,
        error: outcome.error,
    })
}

async fn commit_ed2k_refresh(
    outcome: fluxdown_engine::ed2k::server_subscription::ServerFetchOutcome,
    engine: &mut Engine,
) -> Result<fluxdown_protocol::Ed2kServerSubRefreshResponse, ActorError> {
    let mut updated_at = engine
        .db
        .get_config("ed2k_server_sub_updated_at")
        .await
        .map_err(|error| ActorError::Operation(format!("{error:#}")))?
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0);
    if outcome.is_success() {
        updated_at = now_unix_secs();
        let values = BTreeMap::from([
            (
                "ed2k_server_sub_cache".to_owned(),
                outcome.servers.join(","),
            ),
            (
                "ed2k_server_sub_updated_at".to_owned(),
                updated_at.to_string(),
            ),
            (
                "ed2k_server_sub_cache_version".to_owned(),
                fluxdown_engine::ed2k::server_subscription::CACHE_FORMAT_VERSION.to_string(),
            ),
        ]);
        engine
            .db
            .set_config_batch_atomic(&values)
            .await
            .map_err(|error| ActorError::Operation(format!("{error:#}")))?;
    }
    Ok(fluxdown_protocol::Ed2kServerSubRefreshResponse {
        success: outcome.is_success(),
        server_count: outcome.servers.len() as i64,
        ok_sources: outcome.ok_sources as i64,
        total_sources: outcome.total_sources as i64,
        updated_at,
        error: outcome.error,
    })
}

/// 引擎探测结果 → wire DTO（引擎不依赖 protocol，转换放在 daemon 边界）。
fn link_probe_to_dto(
    probe: fluxdown_engine::meta_prober::LinkProbe,
) -> fluxdown_protocol::LinkProbeResult {
    use fluxdown_engine::meta_prober::LinkVerdict;
    use fluxdown_protocol::LinkProbeVerdict;
    fluxdown_protocol::LinkProbeResult {
        verdict: match probe.verdict {
            LinkVerdict::Resource => LinkProbeVerdict::Resource,
            LinkVerdict::NotResource => LinkProbeVerdict::NotResource,
            LinkVerdict::Unknown => LinkProbeVerdict::Unknown,
        },
        final_url: probe.final_url,
        file_name: probe.file_name,
        mime: probe.mime,
        total_bytes: probe.total_bytes,
    }
}

fn now_unix_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs().min(i64::MAX as u64) as i64)
}

async fn cdn_reports_peek(engine: &mut Engine) -> Result<ActorResult, ActorError> {
    if let Some(raw) = engine
        .db
        .peek_cdn_report_lease()
        .await
        .map_err(|error| ActorError::Operation(format!("{error:#}")))?
    {
        let lease = serde_json::from_str::<fluxdown_protocol::CdnReportLeaseDto>(&raw)
            .map_err(|error| ActorError::Operation(format!("{error:#}")))?;
        return Ok(ActorResult::CdnLease(Some(lease)));
    }
    let samples = fluxdown_engine::cdn::telemetry::take_all();
    if samples.is_empty() {
        return Ok(ActorResult::CdnLease(None));
    }
    let sample_values = samples
        .iter()
        .map(serde_json::to_value)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| ActorError::Operation(error.to_string()))?;
    let lease = fluxdown_protocol::CdnReportLeaseDto {
        batch_id: uuid::Uuid::new_v4().to_string(),
        samples: sample_values,
    };
    let lease_json =
        serde_json::to_string(&lease).map_err(|error| ActorError::Operation(error.to_string()))?;
    let stored = match engine.db.lease_cdn_reports(&lease_json).await {
        Ok(stored) => stored,
        Err(error) => {
            fluxdown_engine::cdn::telemetry::restore_front(samples);
            return Err(ActorError::Operation(format!("{error:#}")));
        }
    };
    let stored_lease = serde_json::from_str::<fluxdown_protocol::CdnReportLeaseDto>(&stored)
        .map_err(|error| ActorError::Operation(format!("{error:#}")))?;
    if stored_lease.batch_id != lease.batch_id {
        fluxdown_engine::cdn::telemetry::restore_front(samples);
    }
    Ok(ActorResult::CdnLease(Some(stored_lease)))
}

async fn apply_cdn_config(
    engine: &mut Engine,
    values: BTreeMap<String, String>,
) -> Result<(), ActorError> {
    const ALLOWED: [&str; 3] = [
        "cdn_resolver_endpoints",
        "cdn_ecs_subnets",
        "cdn_hints_base",
    ];
    if let Some(key) = values.keys().find(|key| !ALLOWED.contains(&key.as_str())) {
        return Err(ActorError::InvalidArgument(format!(
            "unsupported CDN config key: {key}"
        )));
    }
    // 只写与当前值不同的键：未变化时不产生任何 DB 写（避免空闲周期 fsync 唤醒硬盘）。
    let current = engine
        .db
        .get_all_config()
        .await
        .map_err(|error| ActorError::Operation(format!("{error:#}")))?;
    let changed: BTreeMap<String, String> = values
        .into_iter()
        .filter(|(key, value)| current.get(key) != Some(value))
        .collect();
    if changed.is_empty() {
        return Ok(());
    }
    let values = changed;
    engine
        .db
        .set_config_batch_atomic(&values)
        .await
        .map_err(|error| ActorError::Operation(format!("{error:#}")))?;
    if let Some(value) = values.get("cdn_resolver_endpoints") {
        engine.manager.set_cdn_resolver_endpoints(value);
    }
    if let Some(value) = values.get("cdn_ecs_subnets") {
        engine.manager.set_cdn_ecs_subnets(value);
    }
    if let Some(value) = values.get("cdn_hints_base") {
        engine.manager.set_cdn_hints_base(value);
    }
    Ok(())
}

async fn patch_config(
    engine: &mut Engine,
    events: &crate::event_hub::DaemonEventHub,
    expected_revision: u64,
    values: BTreeMap<String, String>,
) -> Result<fluxdown_protocol::DaemonConfigSnapshot, ActorError> {
    let mut merged = engine
        .db
        .get_all_config()
        .await
        .map_err(|error| ActorError::Operation(format!("{error:#}")))?;
    merged.extend(
        values
            .iter()
            .map(|(key, value)| (key.clone(), value.clone())),
    );
    if values
        .keys()
        .any(|key| key.starts_with("proxy_") || key == "global_user_agent")
    {
        // 与引擎实际应用时一致地构建 client：非法代理或 UA（HeaderValue 规则）
        // 必须在提交前被拒绝，否则毒值落库后会导致下次启动失败。
        let proxy = fluxdown_engine::proxy_config::ProxyConfig::from_config_map(&merged);
        let user_agent = merged
            .get("global_user_agent")
            .map(String::as_str)
            .unwrap_or_default();
        fluxdown_engine::downloader::build_client(&proxy, user_agent)
            .map_err(|error| ActorError::InvalidArgument(format!("{error:#}")))?;
    }
    let revision = engine
        .db
        .apply_config_patch_atomic(expected_revision, &values)
        .await
        .map_err(|error| match error {
            fluxdown_engine::db::ConfigPatchError::RevisionConflict { current, .. } => {
                ActorError::RevisionConflict { current }
            }
            other => ActorError::Operation(format!("{other:#}")),
        })?;
    // DB 与 revision 已提交：即便在线应用失败，也必须广播已提交快照，
    // 否则事件快照与 DB revision 分叉，后续 patch 会一直冲突。
    let applied = apply_live_config(engine, &merged, values.keys()).await;
    let snapshot = fluxdown_protocol::DaemonConfigSnapshot {
        revision,
        values: crate::config::public_config_values(&merged),
    };
    events.publish(fluxdown_protocol::DaemonEvent::ConfigChanged(
        snapshot.clone(),
    ));
    applied?;
    Ok(snapshot)
}

/// 清空引擎学习的域名连接策略（内存 + 持久化），并以刷新后的公开配置广播
/// `ConfigChanged`（该只读键随快照可见）。用户配置 revision 不变。
async fn clear_conn_policy(
    engine: &mut Engine,
    events: &crate::event_hub::DaemonEventHub,
) -> Result<fluxdown_protocol::ConnPolicySummaryDto, ActorError> {
    engine.manager.clear_domain_conn_caps();
    engine
        .db
        .set_config(DOMAIN_CONN_CAPS_KEY, "")
        .await
        .map_err(|error| ActorError::Operation(format!("{error:#}")))?;
    let mut all = engine
        .db
        .get_all_config()
        .await
        .map_err(|error| ActorError::Operation(format!("{error:#}")))?;
    // 引擎的落盘是 fire-and-forget，快照以刚写入的空值为准。
    all.insert(DOMAIN_CONN_CAPS_KEY.to_owned(), String::new());
    events.publish(fluxdown_protocol::DaemonEvent::ConfigChanged(
        fluxdown_protocol::DaemonConfigSnapshot {
            revision: all
                .get("daemon_config_revision")
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(0),
            values: crate::config::public_config_values(&all),
        },
    ));
    Ok(fluxdown_protocol::ConnPolicySummaryDto { domain_count: 0 })
}

/// 引擎持久化域名连接策略的 config 键（引擎侧 `segment_coordinator` 私有常量）。
const DOMAIN_CONN_CAPS_KEY: &str = "domain_conn_caps";

/// 删除单个站点凭据；站点不存在返回 [`ActorError::NotFound`]。
async fn delete_site_auth(
    db: &fluxdown_engine::db::Db,
    site: &str,
) -> Result<Vec<fluxdown_protocol::SiteAuthEntryDto>, ActorError> {
    let json = db
        .get_config(fluxdown_engine::site_auth::SITE_AUTH_CONFIG_KEY)
        .await
        .map_err(|error| ActorError::Operation(format!("{error:#}")))?
        .unwrap_or_default();
    let mut store = fluxdown_engine::site_auth::parse_store(&json);
    if store.remove(site).is_none() {
        return Err(ActorError::NotFound);
    }
    save_site_auth(db, &store).await
}

/// 写回站点凭据表（与引擎 `apply_site_auth` 同一 `set_config` 路径：该键
/// 不属用户配置目录，不触碰 revision），返回脱敏后的列表。
async fn save_site_auth(
    db: &fluxdown_engine::db::Db,
    store: &BTreeMap<String, fluxdown_engine::site_auth::SiteCredential>,
) -> Result<Vec<fluxdown_protocol::SiteAuthEntryDto>, ActorError> {
    db.set_config(
        fluxdown_engine::site_auth::SITE_AUTH_CONFIG_KEY,
        &fluxdown_engine::site_auth::serialize_store(store),
    )
    .await
    .map_err(|error| ActorError::Operation(format!("{error:#}")))?;
    Ok(site_auth_entries(store))
}

/// 站点凭据表的脱敏投影（只含站点键与用户名，密码永不出 daemon）。
pub fn site_auth_entries(
    store: &BTreeMap<String, fluxdown_engine::site_auth::SiteCredential>,
) -> Vec<fluxdown_protocol::SiteAuthEntryDto> {
    store
        .iter()
        .map(|(site, credential)| fluxdown_protocol::SiteAuthEntryDto {
            site: site.clone(),
            user: credential.user.clone(),
        })
        .collect()
}

/// 站点键归一化：完整 URL 直接取 `site_key`，裸 `host` / `host:port` 补 `https://` 再取；
/// 非 http(s) 或无法解析返回 `None`（旧 headless 宿主 `normalize_site` 的收紧版：带 scheme 的
/// 输入不再套 `https://` 二次解析，避免 `ftp://host` 被误当成站点 `ftp`）。
pub fn normalize_site(input: &str) -> Option<String> {
    let input = input.trim();
    let direct = fluxdown_engine::site_auth::site_key(input);
    if input.contains("://") {
        return direct;
    }
    direct.or_else(|| fluxdown_engine::site_auth::site_key(&format!("https://{input}")))
}

/// 新增或覆盖单站点凭据；`site` 必须已归一化，`user` 必须已去空白且非空。
async fn upsert_site_auth(
    db: &fluxdown_engine::db::Db,
    site: String,
    user: String,
    pass: String,
) -> Result<fluxdown_protocol::SiteAuthEntryDto, ActorError> {
    let json = db
        .get_config(fluxdown_engine::site_auth::SITE_AUTH_CONFIG_KEY)
        .await
        .map_err(|error| ActorError::Operation(format!("{error:#}")))?
        .unwrap_or_default();
    let mut store = fluxdown_engine::site_auth::parse_store(&json);
    store.insert(
        site.clone(),
        fluxdown_engine::site_auth::SiteCredential {
            user: user.clone(),
            pass,
        },
    );
    save_site_auth(db, &store).await?;
    Ok(fluxdown_protocol::SiteAuthEntryDto { site, user })
}

/// 请求未指定队列时套用设置项 `default_queue_id`；默认队列为空或已被删除时保持空串，由引擎归入
/// 内置主队列。
async fn resolve_queue_id(
    db: &fluxdown_engine::db::Db,
    requested: String,
) -> Result<String, ActorError> {
    if !requested.is_empty() {
        return Ok(requested);
    }
    let Some(preferred) = db
        .get_config("default_queue_id")
        .await
        .map_err(|error| ActorError::Operation(format!("{error:#}")))?
        .filter(|value| !value.is_empty())
    else {
        return Ok(requested);
    };
    if fluxdown_engine::model::is_builtin_queue(&preferred) {
        return Ok(preferred);
    }
    let exists = db
        .load_all_queues()
        .await
        .map_err(|error| ActorError::Operation(format!("{error:#}")))?
        .iter()
        .any(|queue| queue.queue_id == preferred);
    Ok(if exists { preferred } else { requested })
}

async fn apply_live_config<'a>(
    engine: &mut Engine,
    all: &HashMap<String, String>,
    keys: impl Iterator<Item = &'a String>,
) -> Result<(), ActorError> {
    let keys = keys.map(String::as_str).collect::<Vec<_>>();
    if keys.contains(&"max_concurrent_tasks")
        && let Some(value) = all
            .get("max_concurrent_tasks")
            .and_then(|value| value.parse::<usize>().ok())
    {
        engine.manager.set_max_concurrent(value).await;
    }
    if keys.contains(&"speed_limit_bytes")
        && let Some(value) = all
            .get("speed_limit_bytes")
            .and_then(|value| value.parse::<u64>().ok())
    {
        engine.manager.set_speed_limit(value);
    }
    if keys.contains(&"upload_limit_bytes")
        && let Some(value) = all
            .get("upload_limit_bytes")
            .and_then(|value| value.parse::<u64>().ok())
    {
        engine.manager.set_upload_speed_limit(value);
    }
    if keys.contains(&"default_save_dir")
        && let Some(value) = all.get("default_save_dir")
    {
        engine.manager.set_default_save_dir(value.clone());
    }
    if keys.contains(&"default_segments")
        && let Some(value) = all
            .get("default_segments")
            .and_then(|value| value.parse::<i32>().ok())
    {
        engine.manager.set_default_segments(value);
    }
    if keys.contains(&"auto_max_connections")
        && let Some(value) = all
            .get("auto_max_connections")
            .and_then(|value| value.parse::<i32>().ok())
    {
        engine.manager.set_auto_max_connections(value);
    }
    if keys.contains(&"cdn_multi_enabled") {
        engine.manager.set_cdn_multi_enabled(
            all.get("cdn_multi_enabled")
                .is_some_and(|value| value == "true"),
        );
    }
    if keys.contains(&"cdn_max_nodes")
        && let Some(value) = all
            .get("cdn_max_nodes")
            .and_then(|value| value.parse::<i32>().ok())
    {
        engine.manager.set_cdn_max_nodes(value);
    }
    if keys.contains(&"multi_nic_enabled") {
        engine.manager.set_multi_nic_enabled(
            all.get("multi_nic_enabled")
                .is_some_and(|value| value == "true"),
        );
    }
    if keys.contains(&"max_auto_retries")
        && let Some(value) = all
            .get("max_auto_retries")
            .and_then(|value| value.parse::<i32>().ok())
    {
        engine.manager.set_max_auto_retries(value);
    }
    if keys.contains(&"auto_retry_delay_secs")
        && let Some(value) = all
            .get("auto_retry_delay_secs")
            .and_then(|value| value.parse::<u64>().ok())
    {
        engine.manager.set_auto_retry_delay_secs(value);
    }
    if keys.contains(&"use_server_time") {
        engine.manager.set_use_server_time(
            all.get("use_server_time")
                .is_some_and(|value| value == "true"),
        );
    }
    if keys.contains(&"file_exists_behavior") {
        engine.manager.set_file_exists_behavior(
            all.get("file_exists_behavior")
                .map(|value| FileExistsBehavior::from_config_str(value))
                .unwrap_or(FileExistsBehavior::Rename),
        );
    }
    if keys.contains(&"file_missing_action") {
        engine.manager.set_missing_file_auto_delete(
            all.get("file_missing_action")
                .is_some_and(|value| value == "delete"),
        );
    }
    if keys.contains(&"idle_file_scan") {
        engine.manager.set_idle_file_scan(
            all.get("idle_file_scan")
                .is_some_and(|value| value == "true" || value == "1"),
        );
    }
    if keys.contains(&"global_user_agent")
        && let Some(value) = all.get("global_user_agent")
    {
        engine
            .manager
            .set_user_agent(value.clone())
            .map_err(|error| ActorError::Operation(format!("{error:#}")))?;
    }
    if keys.iter().any(|key| key.starts_with("proxy_")) {
        engine
            .manager
            .set_proxy_config(fluxdown_engine::proxy_config::ProxyConfig::from_config_map(
                all,
            ))
            .map_err(|error| ActorError::Operation(format!("{error:#}")))?;
    }
    if keys.iter().any(|key| key.starts_with("bt_")) {
        engine
            .manager
            .set_bt_config(crate::config::bt_config_from_map(all));
        if keys.iter().any(|key| {
            matches!(
                *key,
                "bt_enabled"
                    | "bt_enable_dht"
                    | "bt_enable_upnp"
                    | "bt_port_start"
                    | "bt_port_end"
                    | "bt_custom_trackers"
                    | "bt_tracker_sub_enabled"
                    | "bt_tracker_sub_urls"
                    | "bt_mse_mode"
            )
        }) {
            engine.manager.invalidate_bt_session().await;
        }
    }
    if keys.contains(&"webhook.endpoints")
        && let Some(value) = all.get("webhook.endpoints")
    {
        engine.manager.set_webhook_endpoints(value);
    }
    if keys.contains(&"log_max_size_mb") {
        apply_log_limit(all);
    }
    Ok(())
}

fn decode_torrent_b64(value: Option<&str>) -> Result<Vec<u8>, ActorError> {
    match value {
        Some(encoded) if !encoded.is_empty() => base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|error| ActorError::InvalidArgument(format!("invalid torrentB64: {error}"))),
        _ => Ok(Vec::new()),
    }
}

/// 把持久化的日志大小上限（MB，缺省 / 非法 = 目录默认值）应用到引擎 logger。
fn apply_log_limit(config: &HashMap<String, String>) {
    fluxdown_engine::logger::set_max_total_bytes(log_limit_bytes(config));
}

fn log_limit_bytes(config: &HashMap<String, String>) -> u64 {
    const KEY: &str = "log_max_size_mb";
    let default_mb = fluxdown_protocol::daemon_config_default(KEY)
        .parse::<u64>()
        .unwrap_or(10);
    let mb = config
        .get(KEY)
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|mb| (1..=1024).contains(mb))
        .unwrap_or(default_mb);
    mb * 1024 * 1024
}

/// 把引擎持久化的投递历史并入 daemon 投影（合并语义，不覆盖启动后已到达的增量）。
fn publish_webhook_history(engine: &Engine, events: &crate::event_hub::DaemonEventHub) {
    let history: Vec<_> = engine
        .webhook_deliveries()
        .into_iter()
        .map(fluxdown_engine_protocol::webhook_delivery_to_dto)
        .collect();
    if !history.is_empty() {
        events.publish(fluxdown_protocol::DaemonEvent::WebhooksChanged(history));
    }
}

/// 去重并剔除不存在的任务 id（批量接口「未知 id 忽略」）。
async fn existing_task_ids(
    db: &fluxdown_engine::db::Db,
    mut task_ids: Vec<String>,
) -> Result<Vec<String>, ActorError> {
    let mut seen = std::collections::HashSet::new();
    task_ids.retain(|id| seen.insert(id.clone()));
    if task_ids.is_empty() {
        return Ok(task_ids);
    }
    let known: std::collections::HashSet<String> = db
        .load_tasks_by_ids(&task_ids)
        .await
        .map_err(|error| ActorError::Operation(format!("{error:#}")))?
        .into_iter()
        .map(|task| task.task_id)
        .collect();
    task_ids.retain(|id| known.contains(id));
    Ok(task_ids)
}

async fn task_ids_by_status(
    db: &fluxdown_engine::db::Db,
    statuses: &[i32],
) -> Result<Vec<String>, ActorError> {
    Ok(db
        .load_all_tasks()
        .await
        .map_err(|error| ActorError::Operation(format!("{error:#}")))?
        .into_iter()
        .filter(|task| statuses.contains(&task.status))
        .map(|task| task.task_id)
        .collect())
}

async fn receive_rss_event(
    receiver: &mut Option<mpsc::UnboundedReceiver<fluxdown_engine::rss::RssEvent>>,
) -> Option<fluxdown_engine::rss::RssEvent> {
    match receiver {
        Some(receiver) => receiver.recv().await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use fluxdown_engine::site_auth::{SITE_AUTH_CONFIG_KEY, SiteCredential, serialize_store};

    use super::{
        ActorError, ActorOperation, BT_DISABLED_MESSAGE, delete_site_auth, execute_operation,
        resolve_queue_id, save_site_auth,
    };

    async fn open_db() -> (fluxdown_engine::db::Db, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "fluxdown_daemon_actor_{}_{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        tokio::fs::create_dir_all(&dir)
            .await
            .expect("create actor test dir");
        let db = fluxdown_engine::db::Db::open(&dir)
            .await
            .expect("open actor test db");
        (db, dir)
    }

    #[tokio::test]
    async fn default_queue_applies_only_to_unspecified_and_existing_queues() {
        let (db, dir) = open_db().await;
        db.init_default_config("/tmp").await.expect("seed config");
        db.seed_builtin_queues().await.expect("seed queues");

        assert_eq!(
            resolve_queue_id(&db, "explicit".to_owned())
                .await
                .expect("explicit passthrough"),
            "explicit"
        );
        assert_eq!(
            resolve_queue_id(&db, String::new())
                .await
                .expect("builtin default"),
            fluxdown_engine::model::MAIN_QUEUE_ID
        );

        db.insert_queue("custom", "Custom", 0, 0, 0, "", 5, 0, "")
            .await
            .expect("insert queue");
        db.set_config("default_queue_id", "custom")
            .await
            .expect("set default queue");
        assert_eq!(
            resolve_queue_id(&db, String::new())
                .await
                .expect("existing named default"),
            "custom"
        );

        db.set_config("default_queue_id", "deleted")
            .await
            .expect("set stale default");
        assert_eq!(
            resolve_queue_id(&db, String::new())
                .await
                .expect("stale default falls back"),
            "",
            "engine maps '' to the builtin main queue"
        );

        drop(db);
        if let Err(error) = tokio::fs::remove_dir_all(dir).await {
            eprintln!("actor test directory removal failed: {error}");
        }
    }

    #[tokio::test]
    async fn site_auth_delete_and_clear_never_expose_passwords() {
        let (db, dir) = open_db().await;
        let store = BTreeMap::from([
            (
                "https://a.example".to_owned(),
                SiteCredential {
                    user: "alice".to_owned(),
                    pass: "s3cret".to_owned(),
                },
            ),
            (
                "https://b.example:8443".to_owned(),
                SiteCredential {
                    user: "bob".to_owned(),
                    pass: "hunter2".to_owned(),
                },
            ),
        ]);
        db.set_config(SITE_AUTH_CONFIG_KEY, &serialize_store(&store))
            .await
            .expect("seed credentials");

        let after_delete = delete_site_auth(&db, "https://a.example")
            .await
            .expect("delete known site");
        assert_eq!(after_delete.len(), 1);
        assert_eq!(after_delete[0].site, "https://b.example:8443");
        assert_eq!(after_delete[0].user, "bob");
        let json = serde_json::to_string(&after_delete).expect("serialize dto");
        assert!(!json.contains("hunter2"), "password must not leak: {json}");

        assert!(matches!(
            delete_site_auth(&db, "https://a.example").await,
            Err(ActorError::NotFound)
        ));

        let cleared = save_site_auth(&db, &BTreeMap::new())
            .await
            .expect("clear store");
        assert!(cleared.is_empty());
        assert_eq!(
            db.get_config(SITE_AUTH_CONFIG_KEY)
                .await
                .expect("read store")
                .as_deref(),
            Some("{}")
        );

        drop(db);
        if let Err(error) = tokio::fs::remove_dir_all(dir).await {
            eprintln!("actor test directory removal failed: {error}");
        }
    }

    #[test]
    fn normalize_site_accepts_bare_hosts_ports_and_urls_but_rejects_other_schemes() {
        assert_eq!(
            super::normalize_site("Example.COM").as_deref(),
            Some("https://example.com")
        );
        assert_eq!(
            super::normalize_site(" example.com:8443 ").as_deref(),
            Some("https://example.com:8443")
        );
        assert_eq!(
            super::normalize_site("https://example.com:443/a.bin").as_deref(),
            Some("https://example.com")
        );
        assert_eq!(
            super::normalize_site("http://nas.local:8080/a.bin").as_deref(),
            Some("http://nas.local:8080")
        );
        assert_eq!(super::normalize_site("ftp://example.com/a.bin"), None);
        assert_eq!(super::normalize_site(""), None);
    }

    #[tokio::test]
    async fn upsert_site_auth_overwrites_same_site_and_keeps_other_entries() {
        let dir = std::env::temp_dir().join(format!(
            "fluxdown_daemon_site_auth_upsert_{}_{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        tokio::fs::create_dir_all(&dir).await.expect("create dir");
        let db = fluxdown_engine::db::Db::open(&dir).await.expect("open db");

        let first =
            super::upsert_site_auth(&db, "https://a.example".into(), "alice".into(), "p1".into())
                .await
                .expect("insert");
        assert_eq!(
            (first.site.as_str(), first.user.as_str()),
            ("https://a.example", "alice")
        );
        super::upsert_site_auth(&db, "https://b.example".into(), "bob".into(), "p2".into())
            .await
            .expect("insert second");
        super::upsert_site_auth(
            &db,
            "https://a.example".into(),
            "alice2".into(),
            "p3".into(),
        )
        .await
        .expect("overwrite");

        let json = db
            .get_config(SITE_AUTH_CONFIG_KEY)
            .await
            .expect("read store")
            .unwrap_or_default();
        let store = fluxdown_engine::site_auth::parse_store(&json);
        assert_eq!(store.len(), 2);
        assert_eq!(store["https://a.example"].user, "alice2");
        assert_eq!(store["https://a.example"].pass, "p3");
        assert_eq!(store["https://b.example"].pass, "p2");

        drop(db);
        if let Err(error) = tokio::fs::remove_dir_all(dir).await {
            eprintln!("site auth upsert test directory removal failed: {error}");
        }
    }

    #[tokio::test]
    async fn batch_ids_are_deduplicated_and_unknown_ids_dropped() {
        let (db, dir) = open_db().await;
        db.init_default_config("/tmp").await.expect("seed config");
        db.seed_builtin_queues().await.expect("seed queues");
        for id in ["t1", "t2"] {
            db.insert_task(id, "https://example.com/a", id, "/tmp", 1, 0, "", "", "", 2)
                .await
                .expect("insert task");
        }

        let ids =
            super::existing_task_ids(&db, ["t2", "ghost", "t1", "t2"].map(str::to_owned).to_vec())
                .await
                .expect("filter ids");
        assert_eq!(ids, ["t2", "t1"]);
        assert!(
            super::existing_task_ids(&db, Vec::new())
                .await
                .expect("empty list")
                .is_empty()
        );

        drop(db);
        if let Err(error) = tokio::fs::remove_dir_all(dir).await {
            eprintln!("actor batch ids test directory removal failed: {error}");
        }
    }

    /// BT 被禁用时：新建 BT 任务与（整批）恢复 BT 任务必须带着明确原因失败，
    /// 而不是「failed to persist task」或无信息的成功。
    #[tokio::test]
    async fn bt_disabled_create_and_resume_report_the_reason() {
        let dir = std::env::temp_dir().join(format!(
            "fluxdown_daemon_bt_disabled_{}_{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        tokio::fs::create_dir_all(&dir)
            .await
            .expect("create bt disabled test dir");
        let dir_text = dir.to_string_lossy().into_owned();
        let mut engine = fluxdown_engine::Engine::new(
            fluxdown_engine::EngineConfig {
                max_concurrent: 1,
                speed_limit_bps: 0,
                upload_limit_bps: 0,
                default_save_dir: dir_text.clone(),
                app_data_dir: dir_text.clone(),
                bt_config: fluxdown_engine::bt_downloader::BtConfig {
                    enabled: false,
                    ..Default::default()
                },
                proxy_config: fluxdown_engine::proxy_config::ProxyConfig::default(),
                user_agent: String::new(),
                data_dir_override: Some(dir.clone()),
                database_url: None,
            },
            std::sync::Arc::new(fluxdown_engine::NoopSink),
            std::sync::Arc::new(fluxdown_engine::NoopSelection),
        )
        .await
        .expect("construct engine");
        let events =
            crate::event_hub::DaemonEventHub::new(fluxdown_protocol::DaemonSnapshot::default(), 32);
        let magnet = "magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=t";
        let create = |url: &str, torrent: Vec<u8>| ActorOperation::CreateTask {
            request: Box::new(
                serde_json::from_value(serde_json::json!({ "url": url })).expect("create request"),
            ),
            torrent_file_bytes: torrent,
            hint_file_size: 0,
            unattended: false,
        };
        let reason = |result: Result<super::ActorResult, ActorError>| match result {
            Err(ActorError::InvalidArgument(message)) => message,
            Err(other) => panic!("unexpected error: {other}"),
            Ok(_) => panic!("operation must be rejected"),
        };

        // 新建：magnet 与种子字节都带明确原因，且不落库。
        assert_eq!(
            reason(execute_operation(create(magnet, Vec::new()), &mut engine, &events).await),
            BT_DISABLED_MESSAGE
        );
        assert_eq!(
            reason(execute_operation(create("", vec![1, 2, 3]), &mut engine, &events).await),
            BT_DISABLED_MESSAGE
        );
        assert!(
            engine
                .db
                .load_all_tasks()
                .await
                .expect("load tasks")
                .is_empty(),
            "rejected BT creation must not persist a task"
        );

        // 恢复：单任务与「整批都是 BT」的批量均带原因失败，任务保持暂停。
        engine
            .db
            .insert_task("bt-1", magnet, "t", &dir_text, 1, 0, "", "", "", 0)
            .await
            .expect("insert BT task");
        engine
            .db
            .update_task_status("bt-1", 2, "")
            .await
            .expect("pause BT task");
        assert_eq!(
            reason(
                execute_operation(
                    ActorOperation::ResumeTask {
                        task_id: "bt-1".to_owned()
                    },
                    &mut engine,
                    &events
                )
                .await
            ),
            BT_DISABLED_MESSAGE
        );
        assert_eq!(
            reason(
                execute_operation(
                    ActorOperation::ResumeTasks {
                        task_ids: vec!["bt-1".to_owned()]
                    },
                    &mut engine,
                    &events
                )
                .await
            ),
            BT_DISABLED_MESSAGE
        );
        let task = engine
            .db
            .load_task_by_id("bt-1")
            .await
            .expect("load task")
            .expect("task exists");
        assert_eq!(task.status, 2, "rejected resume leaves the task paused");

        drop(engine);
        if let Err(error) = tokio::fs::remove_dir_all(dir).await {
            eprintln!("actor bt disabled test directory removal failed: {error}");
        }
    }

    #[test]
    fn log_limit_falls_back_to_catalog_default_for_missing_or_invalid_values() {
        let limit = |value: Option<&str>| {
            let config = value
                .map(|v| {
                    std::collections::HashMap::from([("log_max_size_mb".to_owned(), v.to_owned())])
                })
                .unwrap_or_default();
            super::log_limit_bytes(&config)
        };
        assert_eq!(limit(Some("64")), 64 * 1024 * 1024);
        assert_eq!(limit(Some(" 1024 ")), 1024 * 1024 * 1024);
        for fallback in [None, Some("0"), Some("1025"), Some("abc"), Some("-3")] {
            assert_eq!(limit(fallback), 10 * 1024 * 1024, "{fallback:?}");
        }
    }
}
