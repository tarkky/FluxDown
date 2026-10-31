//! FluxCloud `/api/v1` 资源调用面。

use reqwest::Method;
use serde::Serialize;
use serde_json::Value;

use super::{CloudClient, CloudError};

#[derive(Clone)]
pub struct CloudApi {
    client: CloudClient,
    epoch: Option<super::client::RequestEpoch>,
}

impl CloudApi {
    pub(crate) fn at_epoch(&self, epoch: super::client::RequestEpoch) -> Self {
        Self {
            client: self.client.clone(),
            epoch: Some(epoch),
        }
    }

    pub(crate) fn request_epoch(&self) -> super::client::RequestEpoch {
        self.epoch.unwrap_or_else(|| self.client.request_epoch())
    }

    pub(crate) async fn lock_epoch(
        &self,
        epoch: super::client::RequestEpoch,
    ) -> Result<tokio::sync::MutexGuard<'_, crate::state::AgentState>, CloudError> {
        self.client.lock_epoch(epoch).await
    }

    #[must_use]
    pub fn new(client: CloudClient) -> Self {
        Self {
            client,
            epoch: None,
        }
    }

    pub async fn profile(&self) -> Result<Value, CloudError> {
        self.authed(Method::GET, "/api/v1/me", None::<&Value>).await
    }

    pub async fn devices(&self, device_id: &str) -> Result<Value, CloudError> {
        self.authed(
            Method::GET,
            &format!("/api/v1/devices?deviceId={}", encode(device_id)),
            None::<&Value>,
        )
        .await
    }

    /// `PATCH /devices/current`：按令牌里的设备定位本机，上报默认目录 / 路径风格 / 版本。
    /// 旧版云端没有该端点（404）——调用方应忽略。
    pub async fn patch_current_device<P: Serialize>(&self, body: &P) -> Result<Value, CloudError> {
        self.authed(Method::PATCH, "/api/v1/devices/current", Some(body))
            .await
    }

    pub async fn rename_device(&self, id: &str, name: &str) -> Result<Value, CloudError> {
        self.authed(
            Method::PATCH,
            &format!("/api/v1/devices/{}", encode(id)),
            Some(&serde_json::json!({"name": name})),
        )
        .await
    }

    pub async fn delete_device(&self, id: &str) -> Result<Value, CloudError> {
        self.authed(
            Method::DELETE,
            &format!("/api/v1/devices/{}", encode(id)),
            None::<&Value>,
        )
        .await
    }

    /// `POST /devices/{id}/disconnect`：让同账号的另一台在线设备下线、释放在线名额（不删除设备）。
    pub async fn disconnect_device(&self, id: &str) -> Result<Value, CloudError> {
        self.authed(
            Method::POST,
            &format!("/api/v1/devices/{}/disconnect", encode(id)),
            None::<&Value>,
        )
        .await
    }

    /// 按 FluxCloud 时钟估计的当前 unix 毫秒；供配置同步的编辑时间戳使用。
    #[must_use]
    pub fn server_now_ms(&self) -> i64 {
        self.client.server_now_ms()
    }

    pub async fn remote_tasks(&self) -> Result<Value, CloudError> {
        self.authed(Method::GET, "/api/v1/tasks/remote", None::<&Value>)
            .await
    }

    pub async fn dispatch_remote<P: Serialize>(&self, body: &P) -> Result<Value, CloudError> {
        self.authed(Method::POST, "/api/v1/tasks/dispatch", Some(body))
            .await
    }

    pub async fn command_remote<P: Serialize>(
        &self,
        id: &str,
        body: &P,
    ) -> Result<Value, CloudError> {
        self.authed(
            Method::POST,
            &format!("/api/v1/tasks/{}/command", encode(id)),
            Some(body),
        )
        .await
    }

    pub async fn report_remote_status<P: Serialize>(
        &self,
        id: &str,
        body: &P,
    ) -> Result<Value, CloudError> {
        self.authed(
            Method::POST,
            &format!("/api/v1/tasks/{}/status", encode(id)),
            Some(body),
        )
        .await
    }

    pub async fn report_remote_progress<P: Serialize>(
        &self,
        body: &P,
    ) -> Result<Value, CloudError> {
        self.authed(Method::POST, "/api/v1/tasks/progress", Some(body))
            .await
    }

    pub async fn ping_presence(&self) -> Result<Value, CloudError> {
        self.authed(Method::POST, "/api/v1/tasks/presence", None::<&Value>)
            .await
    }

    pub async fn remote_events(&self, device_id: &str) -> Result<reqwest::Response, CloudError> {
        self.client
            .authenticated_stream_epoch(
                &format!("/api/v1/tasks/events?deviceId={}", encode(device_id)),
                self.request_epoch(),
            )
            .await
    }

    /// 任务实时通道（WebSocket）：心跳在连接内收发，取代 `remote_events` + `ping_presence`。
    pub(crate) async fn remote_socket(
        &self,
        device_id: &str,
    ) -> Result<super::CloudSocket, CloudError> {
        self.client
            .authenticated_socket_epoch(
                &format!("/api/v1/tasks/ws?deviceId={}", encode(device_id)),
                self.request_epoch(),
            )
            .await
    }

    pub async fn plans(&self) -> Result<Value, CloudError> {
        self.client
            .public::<Value, Value>(Method::GET, "/api/v1/plans/catalog", None)
            .await
    }

    /// 插件市场收藏概览：公开计数始终拉取；已登录时再拉本人收藏。
    /// 已登录判定读本机凭据；本人收藏请求失败（含 401）按错误返回，不静默降级。
    pub async fn plugin_favorites(
        &self,
    ) -> Result<fluxdown_protocol::PluginFavoritesDto, CloudError> {
        let counts: FavoriteCountsBody = self
            .client
            .public::<Value, _>(Method::GET, "/api/v1/plugins/favorites/counts", None)
            .await?;
        let signed_in = self.client.current_user_id().await.is_some();
        let mine = if signed_in {
            let body: FavoriteMineBody = self
                .client
                .authenticated_epoch::<Value, _>(
                    Method::GET,
                    "/api/v1/plugins/favorites",
                    None,
                    self.request_epoch(),
                )
                .await?;
            body.plugin_ids
        } else {
            Vec::new()
        };
        Ok(fluxdown_protocol::PluginFavoritesDto {
            counts: counts.counts,
            mine,
            signed_in,
        })
    }

    /// 收藏 / 取消收藏单个插件（需登录）。
    pub async fn set_plugin_favorite(
        &self,
        request: fluxdown_protocol::PluginFavoriteSetRequest,
    ) -> Result<fluxdown_protocol::PluginFavoriteResultDto, CloudError> {
        let method = if request.favorited {
            Method::PUT
        } else {
            Method::DELETE
        };
        self.client
            .authenticated_epoch::<Value, _>(
                method,
                &format!("/api/v1/plugins/favorites/{}", encode(&request.plugin_id)),
                None,
                self.request_epoch(),
            )
            .await
    }

    pub async fn create_order<P: Serialize>(&self, body: &P) -> Result<Value, CloudError> {
        self.authed(Method::POST, "/api/v1/orders", Some(body))
            .await
    }

    pub async fn order(&self, order_no: &str) -> Result<Value, CloudError> {
        self.authed(
            Method::GET,
            &format!("/api/v1/orders/{}", encode(order_no)),
            None::<&Value>,
        )
        .await
    }

    pub async fn orders(&self) -> Result<Value, CloudError> {
        self.authed(Method::GET, "/api/v1/orders", None::<&Value>)
            .await
    }

    pub async fn referral(
        &self,
        suffix: &str,
        method: Method,
        body: Option<&Value>,
    ) -> Result<Value, CloudError> {
        self.authed(method, &format!("/api/v1/referral{suffix}"), body)
            .await
    }

    pub async fn referral_codes(&self, page: u32, page_size: u32) -> Result<Value, CloudError> {
        self.referral(
            &format!("/codes?page={page}&pageSize={page_size}"),
            Method::GET,
            None,
        )
        .await
    }

    pub async fn delete_referral_code(&self, id: &str) -> Result<Value, CloudError> {
        self.referral(&format!("/codes/{}", encode(id)), Method::DELETE, None)
            .await
    }

    pub async fn referral_records(
        &self,
        page: u32,
        page_size: u32,
        search: Option<&str>,
    ) -> Result<Value, CloudError> {
        let mut suffix = format!("/records?page={page}&pageSize={page_size}");
        if let Some(search) = search.filter(|value| !value.trim().is_empty()) {
            suffix.push_str("&search=");
            suffix.push_str(&encode(search.trim()));
        }
        self.referral(&suffix, Method::GET, None).await
    }

    pub async fn validate_referral(
        &self,
        code: &str,
        plan_code: &str,
    ) -> Result<Value, CloudError> {
        self.referral(
            &format!(
                "/validate?code={}&planCode={}",
                encode(code),
                encode(plan_code)
            ),
            Method::GET,
            None,
        )
        .await
    }

    pub async fn is_authenticated(&self) -> bool {
        self.client.is_authenticated().await
    }

    pub(crate) fn session_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.client.session_changes()
    }

    pub async fn sync_events(&self, device_id: &str) -> Result<reqwest::Response, CloudError> {
        self.client
            .authenticated_stream_epoch(
                &format!("/api/v1/sync/events?deviceId={}", encode(device_id)),
                self.request_epoch(),
            )
            .await
    }

    pub async fn sync_pull(&self, since: u64, device_id: &str) -> Result<Value, CloudError> {
        self.authed(
            Method::GET,
            &format!(
                "/api/v1/sync/items?since={since}&deviceId={}",
                encode(device_id)
            ),
            None::<&Value>,
        )
        .await
    }

    pub async fn sync_push<P: Serialize>(&self, body: &P) -> Result<Value, CloudError> {
        self.authed(Method::PUT, "/api/v1/sync/items", Some(body))
            .await
    }

    pub async fn cdn_config(&self) -> Result<Value, CloudError> {
        self.authed(Method::GET, "/api/v1/cdn/config", None::<&Value>)
            .await
    }

    pub async fn cdn_report<P: Serialize>(&self, body: &P) -> Result<Value, CloudError> {
        self.authed(Method::POST, "/api/v1/cdn/report", Some(body))
            .await
    }

    /// `POST /bt-guard/evidence`（账号鉴权）：BT 防吸血证据上报。遥测红线例外，唯一调用方是
    /// `bt_guard::evidence`；单批 ≤ 200 条由调用方保证。
    pub(crate) async fn bt_guard_evidence<P: Serialize, R: serde::de::DeserializeOwned>(
        &self,
        body: &P,
    ) -> Result<R, CloudError> {
        self.authed_as(Method::POST, "/api/v1/bt-guard/evidence", Some(body))
            .await
    }

    // ---- 云端推送通知（契约 §3，`/api/v1/notifications/*`）----

    /// `GET /notifications/catalog`：公开接口（匿名），返回管理员启用的渠道种类（固定顺序）。
    pub async fn notify_catalog(
        &self,
    ) -> Result<Vec<fluxdown_protocol::CloudNotifyKindDto>, CloudError> {
        #[derive(serde::Deserialize)]
        struct Catalog {
            #[serde(default)]
            kinds: Vec<fluxdown_protocol::CloudNotifyKindDto>,
        }
        let catalog: Catalog = self
            .client
            .public::<Value, Catalog>(Method::GET, "/api/v1/notifications/catalog", None)
            .await?;
        Ok(catalog.kinds)
    }

    /// `GET /notifications/overview`。
    pub async fn notify_overview(
        &self,
    ) -> Result<fluxdown_protocol::CloudNotifyOverviewDto, CloudError> {
        self.notify_call(Method::GET, "/overview", None::<&Value>)
            .await
    }

    /// `POST /notifications/channels`。
    pub async fn notify_create_channel(
        &self,
        params: &fluxdown_protocol::CloudNotifyChannelCreateParams,
    ) -> Result<fluxdown_protocol::CloudNotifyChannelDto, CloudError> {
        self.notify_call(Method::POST, "/channels", Some(params))
            .await
    }

    /// `PATCH /notifications/channels/{id}`：只提交显式给出的字段（`id` 在路径里）。
    pub async fn notify_update_channel(
        &self,
        params: &fluxdown_protocol::CloudNotifyChannelUpdateParams,
    ) -> Result<fluxdown_protocol::CloudNotifyChannelDto, CloudError> {
        let mut body = serde_json::Map::new();
        if let Some(name) = &params.name {
            body.insert("name".to_owned(), Value::from(name.as_str()));
        }
        if let Some(enabled) = params.enabled {
            body.insert("enabled".to_owned(), Value::from(enabled));
        }
        if let Some(events) = &params.events {
            body.insert("events".to_owned(), Value::from(events.clone()));
        }
        if let Some(addresses) = &params.addresses {
            body.insert("addresses".to_owned(), Value::from(addresses.clone()));
        }
        self.notify_call(
            Method::PATCH,
            &format!("/channels/{}", encode(&params.id)),
            Some(&Value::Object(body)),
        )
        .await
    }

    /// `DELETE /notifications/channels/{id}`。
    pub async fn notify_delete_channel(&self, id: &str) -> Result<(), CloudError> {
        let _: Value = self
            .notify_call(
                Method::DELETE,
                &format!("/channels/{}", encode(id)),
                None::<&Value>,
            )
            .await?;
        Ok(())
    }

    /// `POST /notifications/email/code`：向非账号邮箱发 6 位验证码。
    pub async fn notify_send_email_code(
        &self,
        address: &str,
    ) -> Result<fluxdown_protocol::CloudNotifyEmailCodeResult, CloudError> {
        self.notify_call(
            Method::POST,
            "/email/code",
            Some(&serde_json::json!({ "address": address })),
        )
        .await
    }

    /// `POST /notifications/email/verify`：校验验证码，把地址记为本账号已验证的通知邮箱。
    pub async fn notify_verify_email(&self, address: &str, code: &str) -> Result<(), CloudError> {
        let _: Value = self
            .notify_call(
                Method::POST,
                "/email/verify",
                Some(&serde_json::json!({ "address": address, "code": code })),
            )
            .await?;
        Ok(())
    }

    /// `POST /notifications/channels/{id}/test`（同步发送，不计额度）。
    pub async fn notify_test_channel(
        &self,
        id: &str,
    ) -> Result<fluxdown_protocol::CloudNotifyTestResult, CloudError> {
        self.notify_call(
            Method::POST,
            &format!("/channels/{}/test", encode(id)),
            None::<&Value>,
        )
        .await
    }

    /// `POST /notifications/telegram/bind`。
    pub async fn notify_telegram_bind(
        &self,
    ) -> Result<fluxdown_protocol::CloudNotifyTelegramBindDto, CloudError> {
        self.notify_call(Method::POST, "/telegram/bind", None::<&Value>)
            .await
    }

    /// `GET /notifications/telegram/bind/{code}`。
    pub async fn notify_telegram_bind_status(
        &self,
        code: &str,
    ) -> Result<fluxdown_protocol::CloudNotifyTelegramBindStatusDto, CloudError> {
        self.notify_call(
            Method::GET,
            &format!("/telegram/bind/{}", encode(code)),
            None::<&Value>,
        )
        .await
    }

    /// `GET /notifications/deliveries?limit=&before=`。
    pub async fn notify_deliveries(
        &self,
        params: &fluxdown_protocol::CloudNotifyDeliveriesParams,
    ) -> Result<fluxdown_protocol::CloudNotifyDeliveriesPage, CloudError> {
        let mut query = format!("?limit={}", params.limit.unwrap_or(50).clamp(1, 100));
        if let Some(before) = params.before.as_deref().filter(|value| !value.is_empty()) {
            query.push_str("&before=");
            query.push_str(&encode(before));
        }
        self.notify_call(Method::GET, &format!("/deliveries{query}"), None::<&Value>)
            .await
    }

    /// `POST /notifications/events`：批量上报任务事件（≤ 50 条，由调用方分批）。
    pub async fn notify_report_events<P: Serialize>(
        &self,
        events: &[P],
    ) -> Result<NotifyReportResponse, CloudError> {
        self.notify_call(
            Method::POST,
            "/events",
            Some(&serde_json::json!({ "events": events })),
        )
        .await
    }

    async fn notify_call<P: Serialize, R: serde::de::DeserializeOwned>(
        &self,
        method: Method,
        suffix: &str,
        body: Option<&P>,
    ) -> Result<R, CloudError> {
        let value = self
            .authed(method, &format!("/api/v1/notifications{suffix}"), body)
            .await?;
        serde_json::from_value(value)
            .map_err(|error| CloudError::invalid_response(error.to_string()))
    }

    pub async fn profile_call<P: Serialize>(
        &self,
        method: Method,
        suffix: &str,
        body: Option<&P>,
    ) -> Result<Value, CloudError> {
        self.authed(method, &format!("/api/v1/me{suffix}"), body)
            .await
    }

    pub(crate) async fn persist_profile(
        &self,
        epoch: super::client::RequestEpoch,
        value: Value,
    ) -> Result<fluxdown_protocol::AgentSessionDto, CloudError> {
        let profile = serde_json::from_value::<fluxdown_protocol::CloudProfile>(value)
            .map_err(|error| CloudError::invalid_response(error.to_string()))?;
        self.client.persist_profile(epoch, profile).await
    }

    pub async fn clear_session(&self) -> Result<(), CloudError> {
        self.client.clear_session().await
    }

    pub(crate) async fn clear_session_epoch(
        &self,
        epoch: super::client::RequestEpoch,
    ) -> Result<(), CloudError> {
        self.client.clear_session_epoch(epoch).await
    }

    /// 非用户主动结束会话：先发 `SessionRevoked(reason)` 再清会话。
    pub async fn revoke_session(
        &self,
        reason: fluxdown_protocol::ErrorReason,
    ) -> Result<(), CloudError> {
        self.client.revoke_session(reason).await
    }

    pub(crate) async fn revoke_session_epoch(
        &self,
        reason: fluxdown_protocol::ErrorReason,
        epoch: super::client::RequestEpoch,
    ) -> Result<(), CloudError> {
        self.client.revoke_session_epoch(reason, epoch).await
    }

    /// 当前登录账号 id；未登录为 `None`。
    pub async fn current_user_id(&self) -> Option<String> {
        self.client.current_user_id().await
    }

    #[must_use]
    pub fn endpoint(&self) -> fluxdown_protocol::CloudEndpointDto {
        self.client.endpoint()
    }

    pub async fn set_endpoint(
        &self,
        base_url: &str,
    ) -> Result<fluxdown_protocol::CloudEndpointDto, CloudError> {
        self.client.set_endpoint(base_url).await
    }

    // ───────────── FluxMesh（`/api/v1/share/*`，契约见 docs/temp-share-design.md §9.2） ─────────────

    /// 是否持有登录凭据：FluxMesh在账号鉴权与游客鉴权之间据此选择。
    pub async fn has_account(&self) -> bool {
        self.client.has_account().await
    }

    /// `GET /share/config`（公开，ETag 条件请求）：`None` = 304，沿用调用方缓存。
    pub async fn share_config<R: serde::de::DeserializeOwned>(
        &self,
        etag: Option<&str>,
    ) -> Result<Option<(R, Option<String>)>, CloudError> {
        self.client
            .public_conditional("/api/v1/share/config", etag)
            .await
    }

    /// `GET /share/usage`（账号鉴权）：额度与贡献。
    pub async fn share_usage<R: serde::de::DeserializeOwned>(&self) -> Result<R, CloudError> {
        self.authed_as(Method::GET, "/api/v1/share/usage", None::<&Value>)
            .await
    }

    /// `GET /share/keys`（公开）：离线验票公钥。
    pub async fn share_keys<R: serde::de::DeserializeOwned>(&self) -> Result<R, CloudError> {
        self.client
            .public::<Value, R>(Method::GET, "/api/v1/share/keys", None)
            .await
    }

    /// `PUT /share/endpoint`：登记本设备 endpoint（仅登录设备）。
    pub async fn share_register_endpoint<R: serde::de::DeserializeOwned>(
        &self,
        endpoint_id: &str,
    ) -> Result<R, CloudError> {
        self.authed_as(
            Method::PUT,
            "/api/v1/share/endpoint",
            Some(&serde_json::json!({ "endpointId": endpoint_id })),
        )
        .await
    }

    /// `POST /share/guest-session`（公开）：未登录发送方凭 endpoint 私钥签名换游客令牌。
    pub async fn share_guest_session<P: Serialize, R: serde::de::DeserializeOwned>(
        &self,
        body: &P,
    ) -> Result<R, CloudError> {
        self.client
            .public(Method::POST, "/api/v1/share/guest-session", Some(body))
            .await
    }

    /// `POST /share/shares`：`guest_token` 为 `Some` 时以游客发送方身份创建，否则用账号。
    pub async fn share_create<P: Serialize, R: serde::de::DeserializeOwned>(
        &self,
        body: &P,
        guest_token: Option<&str>,
    ) -> Result<R, CloudError> {
        self.share_owner_call(
            Method::POST,
            "/api/v1/share/shares",
            Some(body),
            guest_token,
        )
        .await
    }

    /// `GET /share/shares`：账号名下（`None`）或游客 endpoint 名下（`Some`）的分享。
    pub async fn share_list<R: serde::de::DeserializeOwned>(
        &self,
        guest_token: Option<&str>,
    ) -> Result<R, CloudError> {
        self.share_owner_call(
            Method::GET,
            "/api/v1/share/shares",
            None::<&Value>,
            guest_token,
        )
        .await
    }

    /// `PATCH /share/shares/{id}`：鉴权身份须与创建时一致（游客分享用游客令牌）。
    pub async fn share_patch<P: Serialize, R: serde::de::DeserializeOwned>(
        &self,
        id: &str,
        body: &P,
        guest_token: Option<&str>,
    ) -> Result<R, CloudError> {
        self.share_owner_call(
            Method::PATCH,
            &format!("/api/v1/share/shares/{}", encode(id)),
            Some(body),
            guest_token,
        )
        .await
    }

    /// `DELETE /share/shares/{id}`：停止分享，取件码释放。
    pub async fn share_stop(
        &self,
        id: &str,
        guest_token: Option<&str>,
    ) -> Result<Value, CloudError> {
        self.share_owner_call(
            Method::DELETE,
            &format!("/api/v1/share/shares/{}", encode(id)),
            None::<&Value>,
            guest_token,
        )
        .await
    }

    /// `POST /share/shares/{id}/resume`：重新开始已停止 / 已过期的分享（同 id、同链接，重新分配取件码）。
    pub async fn share_resume<R: serde::de::DeserializeOwned>(
        &self,
        id: &str,
        guest_token: Option<&str>,
    ) -> Result<R, CloudError> {
        self.share_owner_call(
            Method::POST,
            &format!("/api/v1/share/shares/{}/resume", encode(id)),
            None::<&Value>,
            guest_token,
        )
        .await
    }

    /// `POST /share/report`（公开）：接收方以 endpoint 签名举报已领取的分享。
    pub async fn share_report<P: Serialize>(&self, body: &P) -> Result<Value, CloudError> {
        self.client
            .public(Method::POST, "/api/v1/share/report", Some(body))
            .await
    }

    /// `POST /share/shares/{id}/appeal`：所有者对已下架的分享申诉。
    pub async fn share_appeal<P: Serialize, R: serde::de::DeserializeOwned>(
        &self,
        id: &str,
        body: &P,
        guest_token: Option<&str>,
    ) -> Result<R, CloudError> {
        self.share_owner_call(
            Method::POST,
            &format!("/api/v1/share/shares/{}/appeal", encode(id)),
            Some(body),
            guest_token,
        )
        .await
    }

    /// `DELETE /share/shares/{id}/record`：删除分享记录（分享中的云端先按停止处理）。
    pub async fn share_delete_record(
        &self,
        id: &str,
        guest_token: Option<&str>,
    ) -> Result<Value, CloudError> {
        self.share_owner_call(
            Method::DELETE,
            &format!("/api/v1/share/shares/{}/record", encode(id)),
            None::<&Value>,
            guest_token,
        )
        .await
    }

    /// `POST /share/shares/{id}/upload`：登记要上传的文件，返回每个文件的上传方案（分片预签名地址，只含缺失分片，可重复调用续传）。
    pub async fn share_upload_plan<P: Serialize, R: serde::de::DeserializeOwned>(
        &self,
        id: &str,
        body: &P,
    ) -> Result<R, CloudError> {
        self.authed_as(
            Method::POST,
            &format!("/api/v1/share/shares/{}/upload", encode(id)),
            Some(body),
        )
        .await
    }

    /// `POST /share/shares/{id}/upload/{index}/proof`：秒传持有性证明；204 = 该文件已完成。
    pub async fn share_upload_proof<P: Serialize>(
        &self,
        id: &str,
        index: u32,
        body: &P,
    ) -> Result<Value, CloudError> {
        self.authed(
            Method::POST,
            &format!("/api/v1/share/shares/{}/upload/{index}/proof", encode(id)),
            Some(body),
        )
        .await
    }

    /// `POST /share/shares/{id}/upload/{index}/complete`：提交分片 etag，云端完成 multipart 并校验大小。
    pub async fn share_upload_complete<P: Serialize>(
        &self,
        id: &str,
        index: u32,
        body: &P,
    ) -> Result<Value, CloudError> {
        self.authed(
            Method::POST,
            &format!(
                "/api/v1/share/shares/{}/upload/{index}/complete",
                encode(id)
            ),
            Some(body),
        )
        .await
    }

    /// `DELETE /share/shares/{id}/cloud`：删除云端副本，分享回到实时模式。
    pub async fn share_delete_cloud<R: serde::de::DeserializeOwned>(
        &self,
        id: &str,
    ) -> Result<R, CloudError> {
        self.authed_as(
            Method::DELETE,
            &format!("/api/v1/share/shares/{}/cloud", encode(id)),
            None::<&Value>,
        )
        .await
    }

    /// `POST /share/resolve`：公开，已登录时带账号 Bearer（记账号领取），否则游客。
    pub async fn share_resolve<P: Serialize, R: serde::de::DeserializeOwned>(
        &self,
        body: &P,
    ) -> Result<R, CloudError> {
        self.share_public_or_account("/api/v1/share/resolve", body)
            .await
    }

    /// `POST /share/download-ticket`：按块签发 CDN 区间下载地址（登录接收方计自己的 `download`，游客计分享方）。
    pub async fn share_download_ticket<P: Serialize, R: serde::de::DeserializeOwned>(
        &self,
        body: &P,
    ) -> Result<R, CloudError> {
        self.share_public_or_account("/api/v1/share/download-ticket", body)
            .await
    }

    /// `POST /share/bridge-ticket`：换取网关桥接地址与令牌。
    pub async fn share_bridge_ticket<P: Serialize, R: serde::de::DeserializeOwned>(
        &self,
        body: &P,
    ) -> Result<R, CloudError> {
        self.share_public_or_account("/api/v1/share/bridge-ticket", body)
            .await
    }

    /// `POST /share/swarm/peers`：按内容查在线提供者并换取 swarm 票据（旧云端 404 = 不支持共享）。
    pub async fn share_swarm_peers<P: Serialize, R: serde::de::DeserializeOwned>(
        &self,
        body: &P,
    ) -> Result<R, CloudError> {
        self.share_public_or_account("/api/v1/share/swarm/peers", body)
            .await
    }

    /// `POST /share/swarm/announce`：登记 / 撤销共享提供者；靠 endpoint 签名鉴权，不带 Bearer。
    pub async fn share_swarm_announce<P: Serialize, R: serde::de::DeserializeOwned>(
        &self,
        body: &P,
    ) -> Result<R, CloudError> {
        self.client
            .public(Method::POST, "/api/v1/share/swarm/announce", Some(body))
            .await
    }

    /// 公开接口：已登录时带账号 Bearer，否则匿名（游客身份由票据承载）。
    async fn share_public_or_account<P: Serialize, R: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        body: &P,
    ) -> Result<R, CloudError> {
        if self.client.has_account().await {
            self.authed_as(Method::POST, path, Some(body)).await
        } else {
            self.client.public(Method::POST, path, Some(body)).await
        }
    }

    /// `POST /share/telemetry/{net|sessions}`：登录设备或游客令牌。
    pub async fn share_telemetry<P: Serialize, R: serde::de::DeserializeOwned>(
        &self,
        kind: &str,
        body: &P,
        guest_token: Option<&str>,
    ) -> Result<R, CloudError> {
        self.share_call(
            Method::POST,
            &format!("/api/v1/share/telemetry/{kind}"),
            Some(body),
            guest_token,
        )
        .await
    }

    /// 分享归属方的调用：游客分享必须用游客令牌（即便本机已登录，账号身份也不拥有它）。
    async fn share_owner_call<P: Serialize, R: serde::de::DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<&P>,
        guest_token: Option<&str>,
    ) -> Result<R, CloudError> {
        match guest_token {
            Some(token) => self.client.with_bearer(method, path, body, token).await,
            None => self.authed_as(method, path, body).await,
        }
    }

    /// 账号鉴权优先；没有账号时用游客令牌；两者都没有 → 未登录。
    async fn share_call<P: Serialize, R: serde::de::DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<&P>,
        guest_token: Option<&str>,
    ) -> Result<R, CloudError> {
        if self.client.has_account().await {
            return self.authed_as(method, path, body).await;
        }
        match guest_token {
            Some(token) => self.client.with_bearer(method, path, body, token).await,
            None => Err(CloudError::unauthorized()),
        }
    }

    async fn authed_as<P: Serialize, R: serde::de::DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<&P>,
    ) -> Result<R, CloudError> {
        self.client
            .authenticated_epoch(method, path, body, self.request_epoch())
            .await
    }

    async fn authed<P: Serialize>(
        &self,
        method: Method,
        path: &str,
        body: Option<&P>,
    ) -> Result<Value, CloudError> {
        self.client
            .authenticated_epoch(method, path, body, self.request_epoch())
            .await
    }
}

/// `GET /plugins/favorites/counts` 响应。
#[derive(serde::Deserialize)]
struct FavoriteCountsBody {
    #[serde(default)]
    counts: std::collections::BTreeMap<String, u64>,
}

/// `GET /plugins/favorites` 响应。
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct FavoriteMineBody {
    #[serde(default)]
    plugin_ids: Vec<String>,
}

/// `POST /notifications/events` 响应：逐条结果与最新额度用量。
#[derive(Clone, Debug, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NotifyReportResponse {
    #[serde(default)]
    pub results: Vec<NotifyReportResult>,
    #[serde(default)]
    pub usage: Option<fluxdown_protocol::cloud_notify::CloudNotifyUsageDto>,
}

/// 单条事件的受理结果。
#[derive(Clone, Debug, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NotifyReportResult {
    pub delivery_id: String,
    /// `accepted` / `duplicate` / `no_channel` / `quota_exceeded` / `disabled`。
    pub outcome: String,
}

fn encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(byte as char);
        } else {
            encoded.push('%');
            encoded.push_str(&format!("{byte:02X}"));
        }
    }
    encoded
}
