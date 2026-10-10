use std::sync::Arc;

use base64::Engine as _;
use fluxdown_protocol::method;
use fluxdown_ui_downloads::{DownloadsCommand, DownloadsPort, DownloadsResult, PortFuture};
use serde_json::{Value, json};

use crate::agent_client::AgentClient;

pub struct AgentDownloadsPort {
    client: Arc<AgentClient>,
}

impl AgentDownloadsPort {
    #[must_use]
    pub fn new(client: Arc<AgentClient>) -> Self {
        Self { client }
    }
}

fn queue_fields(fields: &fluxdown_ui_downloads::QueueFields) -> Value {
    json!({
        "name": fields.name,
        "speedLimitKbps": fields.speed_limit_kbps,
        "uploadLimitKbps": fields.upload_limit_kbps,
        "maxConcurrent": fields.max_concurrent,
        "defaultSaveDir": fields.default_save_dir,
        "defaultSegments": fields.default_segments,
        "defaultUserAgent": fields.default_user_agent,
    })
}

impl DownloadsPort for AgentDownloadsPort {
    fn execute(&self, command: DownloadsCommand) -> PortFuture<DownloadsResult> {
        let client = self.client.clone();
        Box::pin(async move {
            let (method, params) = match command {
                DownloadsCommand::TaskActivity(query) => {
                    let page = client
                        .call(method::DAEMON_TASK_ACTIVITY, Some(query))
                        .await?;
                    return Ok(DownloadsResult::TaskActivity(page));
                }
                DownloadsCommand::FileIcon(params) => {
                    let icon: fluxdown_protocol::PlatformFileIconDto = client
                        .call(method::AGENT_PLATFORM_FILE_ICON, Some(params))
                        .await?;
                    let png = base64::engine::general_purpose::STANDARD
                        .decode(icon.png)
                        .map_err(|_| internal_error())?;
                    return Ok(DownloadsResult::FileIcon(png));
                }
                DownloadsCommand::Create(params) => {
                    (method::DAEMON_TASK_CREATE, serialize(params)?)
                }
                DownloadsCommand::Pause { task_id } => {
                    (method::DAEMON_TASK_PAUSE, json!({ "taskId": task_id }))
                }
                DownloadsCommand::Resume { task_id } => {
                    (method::DAEMON_TASK_RESUME, json!({ "taskId": task_id }))
                }
                DownloadsCommand::Rename { task_id, file_name } => (
                    method::DAEMON_TASK_RENAME,
                    json!({ "taskId": task_id, "fileName": file_name }),
                ),
                DownloadsCommand::Delete {
                    task_id,
                    delete_files,
                } => (
                    method::DAEMON_TASK_DELETE,
                    json!({ "taskId": task_id, "deleteFiles": delete_files }),
                ),
                DownloadsCommand::PauseMany { task_ids } => (
                    method::DAEMON_TASK_PAUSE_MANY,
                    json!({ "taskIds": task_ids }),
                ),
                DownloadsCommand::ResumeMany { task_ids } => (
                    method::DAEMON_TASK_RESUME_MANY,
                    json!({ "taskIds": task_ids }),
                ),
                DownloadsCommand::DeleteMany {
                    task_ids,
                    delete_files,
                } => (
                    method::DAEMON_TASK_DELETE_MANY,
                    json!({ "taskIds": task_ids, "deleteFiles": delete_files }),
                ),
                DownloadsCommand::PauseAll => (method::DAEMON_TASK_PAUSE_ALL, json!({})),
                DownloadsCommand::ResumeAll => (method::DAEMON_TASK_RESUME_ALL, json!({})),
                DownloadsCommand::IgnorePluginRetry { task_id } => (
                    method::DAEMON_PLUGIN_IGNORE_RETRY,
                    json!({ "taskId": task_id }),
                ),
                DownloadsCommand::ToggleBoost { task_id } => {
                    (method::DAEMON_QUEUE_BOOST, json!({ "id": task_id }))
                }
                DownloadsCommand::Redownload(request, task_id) => {
                    client
                        .call::<Value, Value>(
                            method::DAEMON_TASK_DELETE,
                            Some(json!({ "taskId": task_id, "deleteFiles": true })),
                        )
                        .await?;
                    (
                        method::DAEMON_TASK_CREATE,
                        serialize(fluxdown_protocol::DaemonCreateTaskParams {
                            request: *request,
                            torrent_blob_id: None,
                            unattended: false,
                            hint_file_size: None,
                        })?,
                    )
                }
                DownloadsCommand::MoveToQueue { task_id, queue_id } => (
                    method::DAEMON_QUEUE_MOVE_TASK,
                    json!({ "taskId": task_id, "queueId": queue_id }),
                ),
                DownloadsCommand::SetSeedLimits { task_id, limits } => (
                    method::DAEMON_TASK_SET_SEED_LIMITS,
                    json!({
                        "taskId": task_id,
                        "ratioLimitMilli": limits.ratio_limit_milli,
                        "postRatioLimitMilli": limits.post_ratio_limit_milli,
                        "seedTimeLimitMinutes": limits.seed_time_limit_minutes,
                        "inactiveTimeLimitMinutes": limits.inactive_time_limit_minutes,
                        "uploadLimitBps": limits.upload_limit_bps,
                    }),
                ),
                DownloadsCommand::PatchConfig {
                    values,
                    expected_revision,
                } => (
                    method::DAEMON_CONFIG_PATCH,
                    json!({ "expectedRevision": expected_revision, "values": values }),
                ),
                DownloadsCommand::QueueCreate(fields) => {
                    (method::DAEMON_QUEUE_CREATE, queue_fields(&fields))
                }
                DownloadsCommand::QueueUpdate { queue_id, fields } => {
                    let mut params = queue_fields(&fields);
                    if let Value::Object(map) = &mut params {
                        map.insert("queueId".to_owned(), Value::String(queue_id));
                    }
                    (method::DAEMON_QUEUE_UPDATE, params)
                }
                DownloadsCommand::QueueSchedule {
                    queue_id,
                    enabled,
                    start_time,
                    stop_time,
                    days,
                } => (
                    method::DAEMON_QUEUE_SCHEDULE,
                    json!({
                        "queueId": queue_id,
                        "enabled": enabled,
                        "startTime": start_time,
                        "stopTime": stop_time,
                        "days": days,
                    }),
                ),
                DownloadsCommand::QueueStart { queue_id } => {
                    (method::DAEMON_QUEUE_START, json!({ "queueId": queue_id }))
                }
                DownloadsCommand::QueueStop { queue_id } => {
                    (method::DAEMON_QUEUE_STOP, json!({ "queueId": queue_id }))
                }
                DownloadsCommand::QueueDelete { queue_id } => {
                    (method::DAEMON_QUEUE_DELETE, json!({ "queueId": queue_id }))
                }
                DownloadsCommand::GroupPause { group_id } => {
                    (method::DAEMON_GROUP_PAUSE, json!({ "groupId": group_id }))
                }
                DownloadsCommand::GroupResume { group_id } => {
                    (method::DAEMON_GROUP_RESUME, json!({ "groupId": group_id }))
                }
                DownloadsCommand::GroupDelete {
                    group_id,
                    delete_files,
                } => (
                    method::DAEMON_GROUP_DELETE,
                    json!({ "groupId": group_id, "deleteFiles": delete_files }),
                ),
                DownloadsCommand::ResolveSelection(params) => {
                    (method::DAEMON_SELECTION_RESOLVE, serialize(params)?)
                }
                DownloadsCommand::CaptureResolve(params) => {
                    (method::AGENT_CAPTURE_RESOLVE, serialize(params)?)
                }
                DownloadsCommand::ResolvePreview {
                    request,
                    transaction_id,
                } => {
                    let preview = match transaction_id {
                        Some(transaction_id) => {
                            client
                                .call(
                                    method::AGENT_CAPTURE_PREVIEW,
                                    Some(fluxdown_protocol::CapturePreviewParams {
                                        transaction_id,
                                        request: *request,
                                    }),
                                )
                                .await?
                        }
                        None => {
                            client
                                .call(
                                    method::DAEMON_GROUP_RESOLVE_PREVIEW,
                                    Some(preview_request(*request)),
                                )
                                .await?
                        }
                    };
                    return Ok(DownloadsResult::Preview(preview));
                }
                DownloadsCommand::CreateGroup {
                    mut request,
                    context,
                    transaction_id,
                } => match transaction_id {
                    Some(transaction_id) => (
                        method::AGENT_CAPTURE_CREATE_GROUP,
                        serialize(fluxdown_protocol::CaptureCreateGroupParams {
                            transaction_id,
                            request: *request,
                            context: *context,
                        })?,
                    ),
                    None => {
                        apply_basic_auth(&mut request.extra_headers, &context);
                        let value: Value = client
                            .call(method::DAEMON_GROUP_CREATE, Some(request))
                            .await?;
                        if context.save_site_auth && !context.http_user.is_empty() {
                            let credentials = fluxdown_protocol::SiteAuthSaveRequest {
                                site: context.url,
                                user: context.http_user,
                                pass: context.http_password,
                            };
                            if let Err(error) = client
                                .call::<_, Value>(method::DAEMON_SITE_AUTH_SAVE, Some(credentials))
                                .await
                            {
                                log::warn!(
                                    "group created but HTTP credentials could not be saved: {:?}",
                                    error.code
                                );
                            }
                        }
                        return Ok(DownloadsResult::Value(value));
                    }
                },
                DownloadsCommand::SiteAuthMatch { url } => (
                    method::DAEMON_SITE_AUTH_MATCH,
                    serialize(fluxdown_protocol::SiteAuthMatchParams { url })?,
                ),
                DownloadsCommand::RemoteDispatch(params) => {
                    (method::AGENT_REMOTE_DISPATCH, serialize(params)?)
                }
                DownloadsCommand::LinkDispatch(params) => {
                    (method::AGENT_LINK_DISPATCH, serialize(params)?)
                }
                DownloadsCommand::RemoteCommand(params) => {
                    (method::AGENT_REMOTE_COMMAND, serialize(params)?)
                }
                // Windows 由前台界面进程拉起一次性 agent 打开，系统才会把窗口置前（见 shell_open）。
                #[cfg(windows)]
                DownloadsCommand::OpenTask { task_id } => {
                    crate::shell_open::task(&client, task_id, false).await?;
                    return Ok(DownloadsResult::Unit);
                }
                #[cfg(windows)]
                DownloadsCommand::RevealTask { task_id } => {
                    crate::shell_open::task(&client, task_id, true).await?;
                    return Ok(DownloadsResult::Unit);
                }
                #[cfg(not(windows))]
                DownloadsCommand::OpenTask { task_id } => (
                    method::AGENT_PLATFORM_OPEN_TASK,
                    json!({ "taskId": task_id }),
                ),
                #[cfg(not(windows))]
                DownloadsCommand::RevealTask { task_id } => (
                    method::AGENT_PLATFORM_REVEAL_TASK,
                    json!({ "taskId": task_id }),
                ),
                DownloadsCommand::RescanFiles => (method::DAEMON_TASK_RESCAN, json!({})),
                DownloadsCommand::SubmitTorrentFile {
                    path,
                    silent,
                    save_dir,
                    queue_id,
                    start_paused,
                } => (
                    method::AGENT_CAPTURE_SUBMIT_TORRENT_FILE,
                    torrent_file_params(path, silent, save_dir, queue_id, start_paused),
                ),
                DownloadsCommand::SetLocalPreference { key, value } => (
                    method::AGENT_PREFERENCES_PATCH,
                    json!({ "values": { key: value }, "sync": false }),
                ),
                DownloadsCommand::SetSyncedPreference { key, value } => (
                    method::AGENT_PREFERENCES_PATCH,
                    json!({ "values": { key: value } }),
                ),
            };
            let value: Value = client.call(method, Some(params)).await?;
            Ok(if value == json!({ "ok": true }) {
                DownloadsResult::Unit
            } else {
                DownloadsResult::Value(value)
            })
        })
    }
}

fn serialize<T: serde::Serialize>(value: T) -> Result<Value, fluxdown_protocol::RpcErrorData> {
    serde_json::to_value(value).map_err(|_| internal_error())
}

fn internal_error() -> fluxdown_protocol::RpcErrorData {
    fluxdown_protocol::RpcErrorData::new(fluxdown_protocol::ApplicationErrorCode::Internal, false)
}

fn preview_request(
    mut request: fluxdown_protocol::CreateTaskRequest,
) -> fluxdown_protocol::ResolvePreviewRequest {
    let mut extra_headers = request.headers.take().unwrap_or_default();
    apply_basic_auth(&mut extra_headers, &request);
    fluxdown_protocol::ResolvePreviewRequest {
        url: request.url,
        cookies: request.cookies,
        referrer: request.referrer,
        user_agent: request.user_agent,
        extra_headers,
    }
}

fn apply_basic_auth(
    headers: &mut std::collections::HashMap<String, String>,
    request: &fluxdown_protocol::CreateTaskRequest,
) {
    if !request.http_user.is_empty() {
        headers.retain(|name, _| !name.eq_ignore_ascii_case("authorization"));
        let credentials = base64::engine::general_purpose::STANDARD
            .encode(format!("{}:{}", request.http_user, request.http_password));
        headers.insert("Authorization".to_owned(), format!("Basic {credentials}"));
    }
}

/// `agent.capture.submitTorrentFile` 参数：`saveDir` / `queueId` / `startPaused` 仅在表单入口
/// 携带，缺省时省略，agent 维持原有默认行为。
fn torrent_file_params(
    path: String,
    silent: bool,
    save_dir: Option<String>,
    queue_id: Option<String>,
    start_paused: Option<bool>,
) -> Value {
    let mut params = json!({ "path": path, "silent": silent });
    if let Value::Object(map) = &mut params {
        if let Some(save_dir) = save_dir {
            map.insert("saveDir".to_owned(), Value::String(save_dir));
        }
        if let Some(queue_id) = queue_id {
            map.insert("queueId".to_owned(), Value::String(queue_id));
        }
        if let Some(start_paused) = start_paused {
            map.insert("startPaused".to_owned(), Value::Bool(start_paused));
        }
    }
    params
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::torrent_file_params;

    #[test]
    fn user_initiated_torrent_carries_form_options() {
        let params = torrent_file_params(
            "/tmp/a.torrent".to_owned(),
            false,
            Some("/data".to_owned()),
            Some("q1".to_owned()),
            Some(true),
        );
        assert_eq!(
            params,
            json!({
                "path": "/tmp/a.torrent",
                "silent": false,
                "saveDir": "/data",
                "queueId": "q1",
                "startPaused": true,
            })
        );
    }

    #[test]
    fn torrent_without_form_options_omits_them() {
        let params = torrent_file_params("/tmp/a.torrent".to_owned(), true, None, None, None);
        assert_eq!(params, json!({ "path": "/tmp/a.torrent", "silent": true }));
    }
}
