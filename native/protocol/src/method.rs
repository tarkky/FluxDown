//! 本机服务 JSON-RPC 方法名与能力标识。

pub const SYSTEM_HELLO: &str = "system.hello";
pub const SYSTEM_PING: &str = "system.ping";
pub const SYSTEM_SNAPSHOT: &str = "system.snapshot";
/// 让服务优雅退出：daemon 关停引擎后退出；agent 先关停 daemon 再退出（完全退出）。
///
/// 允许作为连接首帧（握手前）调用：协议版本不兼容的新旧进程替换只能靠它完成。
pub const SYSTEM_SHUTDOWN: &str = "system.shutdown";

pub const DAEMON_TASK_LIST: &str = "daemon.task.list";
pub const DAEMON_TASK_GET: &str = "daemon.task.get";
pub const DAEMON_TASK_ACTIVITY: &str = "daemon.task.activity";
pub const DAEMON_TASK_CREATE: &str = "daemon.task.create";
pub const DAEMON_TASK_PAUSE: &str = "daemon.task.pause";
pub const DAEMON_TASK_RESUME: &str = "daemon.task.resume";
pub const DAEMON_TASK_RENAME: &str = "daemon.task.rename";
/// 更换任务下载源地址（`{taskId, url}`，语义同旧 `PUT /api/v1/tasks/{id}/url`）。
pub const DAEMON_TASK_CHANGE_URL: &str = "daemon.task.changeUrl";
pub const DAEMON_TASK_DELETE: &str = "daemon.task.delete";
pub const DAEMON_TASK_PAUSE_ALL: &str = "daemon.task.pauseAll";
pub const DAEMON_TASK_RESUME_ALL: &str = "daemon.task.resumeAll";
pub const DAEMON_TASK_RESCAN: &str = "daemon.task.rescan";
pub const DAEMON_TASK_SET_SEED_LIMITS: &str = "daemon.task.setSeedLimits";
/// 批量暂停 / 恢复（`{taskIds}`）：整批只推一次任务快照；未知 id 忽略，空列表为空操作。
pub const DAEMON_TASK_PAUSE_MANY: &str = "daemon.task.pauseMany";
pub const DAEMON_TASK_RESUME_MANY: &str = "daemon.task.resumeMany";
/// 批量删除（`{taskIds, deleteFiles}`）：语义同批量暂停。
pub const DAEMON_TASK_DELETE_MANY: &str = "daemon.task.deleteMany";

pub const DAEMON_QUEUE_LIST: &str = "daemon.queue.list";
pub const DAEMON_QUEUE_CREATE: &str = "daemon.queue.create";
pub const DAEMON_QUEUE_UPDATE: &str = "daemon.queue.update";
pub const DAEMON_QUEUE_DELETE: &str = "daemon.queue.delete";
pub const DAEMON_QUEUE_START: &str = "daemon.queue.start";
pub const DAEMON_QUEUE_STOP: &str = "daemon.queue.stop";
pub const DAEMON_QUEUE_SCHEDULE: &str = "daemon.queue.schedule";
pub const DAEMON_QUEUE_REORDER: &str = "daemon.queue.reorder";
pub const DAEMON_QUEUE_MOVE_TASK: &str = "daemon.queue.moveTask";
pub const DAEMON_QUEUE_BOOST: &str = "daemon.queue.boost";

pub const DAEMON_GROUP_LIST: &str = "daemon.group.list";
pub const DAEMON_GROUP_RESOLVE_PREVIEW: &str = "daemon.group.resolvePreview";
pub const DAEMON_GROUP_CREATE: &str = "daemon.group.create";
pub const DAEMON_GROUP_PAUSE: &str = "daemon.group.pause";
pub const DAEMON_GROUP_RESUME: &str = "daemon.group.resume";
pub const DAEMON_GROUP_DELETE: &str = "daemon.group.delete";

pub const DAEMON_CONFIG_GET: &str = "daemon.config.get";
pub const DAEMON_CONFIG_PATCH: &str = "daemon.config.patch";
pub const DAEMON_CONFIG_PROXY_TEST: &str = "daemon.config.proxyTest";
pub const DAEMON_CONFIG_CONN_POLICY: &str = "daemon.config.connPolicy";
pub const DAEMON_CONFIG_CLEAR_CONN_POLICY: &str = "daemon.config.clearConnPolicy";
pub const DAEMON_CONFIG_SYSTEM_PROXY: &str = "daemon.config.systemProxy";
pub const DAEMON_SITE_AUTH_LIST: &str = "daemon.siteAuth.list";
pub const DAEMON_SITE_AUTH_DELETE: &str = "daemon.siteAuth.delete";
/// 读取单站点凭据详情（含明文密码；站点键会先按 `host` / `host:port` 归一化）。
pub const DAEMON_SITE_AUTH_GET: &str = "daemon.siteAuth.get";
/// 保存单站点 HTTP Basic 凭据，返回脱敏后的 `SiteAuthEntryDto`。
pub const DAEMON_SITE_AUTH_SAVE: &str = "daemon.siteAuth.save";
pub const DAEMON_SITE_AUTH_CLEAR: &str = "daemon.siteAuth.clear";
/// 按下载链接匹配已保存的站点凭据（含明文密码，仅供本机官方 UI 表单回填）。
pub const DAEMON_SITE_AUTH_MATCH: &str = "daemon.siteAuth.match";
pub const DAEMON_RUNTIME_STATS: &str = "daemon.runtime.stats";
pub const DAEMON_FS_LIST: &str = "daemon.fs.list";

pub const DAEMON_RSS_LIST_SOURCES: &str = "daemon.rss.listSources";
pub const DAEMON_RSS_GET_ITEMS: &str = "daemon.rss.getItems";
pub const DAEMON_RSS_CREATE_SOURCE: &str = "daemon.rss.createSource";
pub const DAEMON_RSS_UPDATE_SOURCE: &str = "daemon.rss.updateSource";
pub const DAEMON_RSS_DELETE_SOURCE: &str = "daemon.rss.deleteSource";
pub const DAEMON_RSS_REFRESH_SOURCE: &str = "daemon.rss.refreshSource";
pub const DAEMON_RSS_ITEM_ACTION: &str = "daemon.rss.itemAction";
pub const DAEMON_RSS_VALIDATE: &str = "daemon.rss.validate";
/// 剪贴板识别用的只读链接探测：daemon 以全局代理/UA 只发一次 HEAD（不发 GET），判定是否下载资源。
pub const DAEMON_LINK_PROBE: &str = "daemon.link.probe";

pub const DAEMON_PLUGIN_LIST: &str = "daemon.plugin.list";
pub const DAEMON_PLUGIN_AUTH: &str = "daemon.plugin.auth";
pub const DAEMON_PLUGIN_SET_ENABLED: &str = "daemon.plugin.setEnabled";
pub const DAEMON_PLUGIN_UPDATE_SETTINGS: &str = "daemon.plugin.updateSettings";
pub const DAEMON_PLUGIN_INSTALL: &str = "daemon.plugin.install";
pub const DAEMON_PLUGIN_INSTALL_DEV: &str = "daemon.plugin.installDev";
/// 重新加载 dev 插件（`{identity}`）：重读 manifest + 源码并校验，失败保留登记。
pub const DAEMON_PLUGIN_RELOAD_DEV: &str = "daemon.plugin.reloadDev";
pub const DAEMON_PLUGIN_UNINSTALL: &str = "daemon.plugin.uninstall";
pub const DAEMON_PLUGIN_MARKET_LIST: &str = "daemon.plugin.marketList";
pub const DAEMON_PLUGIN_MARKET_INSTALL: &str = "daemon.plugin.marketInstall";
pub const DAEMON_PLUGIN_IGNORE_RETRY: &str = "daemon.plugin.ignoreRetry";
pub const DAEMON_COMPONENT_GET: &str = "daemon.component.get";
pub const DAEMON_COMPONENT_LIST_VERSIONS: &str = "daemon.component.listVersions";
pub const DAEMON_COMPONENT_INSTALL: &str = "daemon.component.install";
pub const DAEMON_COMPONENT_UNINSTALL: &str = "daemon.component.uninstall";

pub const DAEMON_SELECTION_SUBSCRIBE: &str = "daemon.selection.subscribe";
pub const DAEMON_SELECTION_UNSUBSCRIBE: &str = "daemon.selection.unsubscribe";
pub const DAEMON_SELECTION_RESOLVE: &str = "daemon.selection.resolve";

pub const DAEMON_WEBHOOK_GET: &str = "daemon.webhook.get";
pub const DAEMON_WEBHOOK_CLEAR_DELIVERIES: &str = "daemon.webhook.clearDeliveries";
pub const DAEMON_WEBHOOK_SIMULATE: &str = "daemon.webhook.simulate";
pub const DAEMON_WEBHOOK_TEST: &str = "daemon.webhook.test";
pub const DAEMON_CDN_REPORTS_PEEK: &str = "daemon.cdnReports.peek";
pub const DAEMON_CDN_REPORTS_ACK: &str = "daemon.cdnReports.ack";
pub const DAEMON_CDN_CONFIG_APPLY: &str = "daemon.cdnConfig.apply";

pub const DAEMON_BT_TRACKER_SUBSCRIPTION_REFRESH: &str = "daemon.bt.trackerSubscription.refresh";
pub const DAEMON_ED2K_SERVER_SUBSCRIPTION_REFRESH: &str = "daemon.ed2k.serverSubscription.refresh";
pub const DAEMON_DIAGNOSTICS_DESCRIBE: &str = "daemon.diagnostics.describe";
pub const DAEMON_DIAGNOSTICS_PREPARE_LOG_EXPORT: &str = "daemon.diagnostics.prepareLogExport";
/// Doctor 动态探测：在 daemon 进程内真实写入各下载目录 / 数据目录并运行外部组件，
/// 验证下载链路实际拥有的权限（agent 只读配置判断不出 TCC、只读挂载、隔离属性等）。
pub const DAEMON_DIAGNOSTICS_PROBE: &str = "daemon.diagnostics.probe";
/// Doctor 修复：补上托管组件（daemon 数据目录内）缺失的执行权限并重新探测，返回 `ComponentProbeDto`。
pub const DAEMON_DIAGNOSTICS_FIX_COMPONENT: &str = "daemon.diagnostics.fixComponent";
pub const DAEMON_MIGRATION_LINK_EXPORT: &str = "daemon.migration.linkExport";
pub const DAEMON_MIGRATION_LINK_ACK: &str = "daemon.migration.linkAck";
pub const DAEMON_MIGRATION_GATEWAY_EXPORT: &str = "daemon.migration.gatewayExport";
pub const DAEMON_MIGRATION_GATEWAY_ACK: &str = "daemon.migration.gatewayAck";

pub const AGENT_SESSION_GET: &str = "agent.session.get";
pub const AGENT_AUTH_REGISTER: &str = "agent.auth.register";
pub const AGENT_AUTH_REGISTER_VERIFY: &str = "agent.auth.registerVerify";
pub const AGENT_AUTH_LOGIN: &str = "agent.auth.login";
pub const AGENT_AUTH_LOGIN_VERIFY: &str = "agent.auth.loginVerify";
pub const AGENT_AUTH_SEND_CODE: &str = "agent.auth.sendCode";
pub const AGENT_AUTH_VERIFY_CODE: &str = "agent.auth.verifyCode";
pub const AGENT_AUTH_LOGOUT: &str = "agent.auth.logout";
pub const AGENT_AUTH_REFRESH_PROFILE: &str = "agent.auth.refreshProfile";
pub const AGENT_AUTH_SEND_PASSWORD_RESET_CODE: &str = "agent.auth.sendPasswordResetCode";
pub const AGENT_AUTH_RESET_PASSWORD: &str = "agent.auth.resetPassword";
pub const AGENT_PROFILE_SEND_EMAIL_CODE: &str = "agent.profile.sendEmailCode";
pub const AGENT_PROFILE_SEND_NEW_EMAIL_CODE: &str = "agent.profile.sendNewEmailCode";
pub const AGENT_PROFILE_CHANGE_EMAIL: &str = "agent.profile.changeEmail";
pub const AGENT_PROFILE_RANDOM_ORIGIN_ID: &str = "agent.profile.randomOriginId";
pub const AGENT_PROFILE_CHECK_ORIGIN_ID: &str = "agent.profile.checkOriginId";
pub const AGENT_PROFILE_CHANGE_ORIGIN_ID: &str = "agent.profile.changeOriginId";
pub const AGENT_PROFILE_CHANGE_NICKNAME: &str = "agent.profile.changeNickname";
pub const AGENT_PROFILE_SEND_PASSWORD_CODE: &str = "agent.profile.sendPasswordCode";
pub const AGENT_PROFILE_CHANGE_PASSWORD: &str = "agent.profile.changePassword";

pub const AGENT_GATEWAY_GET: &str = "agent.gateway.get";
pub const AGENT_GATEWAY_PATCH: &str = "agent.gateway.patch";
/// 仅供本机官方 UI 展示/复制用户 token；结果 `{ "userToken": "..." }`（未配置为空串）。
pub const AGENT_GATEWAY_REVEAL_TOKEN: &str = "agent.gateway.revealToken";
pub const AGENT_DEVICE_LIST: &str = "agent.device.list";
/// 请求立即重连任务 SSE；返回 `{ "accepted": true }` 不表示已经连接成功。
pub const AGENT_REMOTE_RECONNECT: &str = "agent.remote.reconnect";
pub const AGENT_DEVICE_RENAME: &str = "agent.device.rename";
pub const AGENT_DEVICE_DELETE: &str = "agent.device.delete";
pub const AGENT_PREFERENCES_PATCH: &str = "agent.preferences.patch";
pub const AGENT_SYNC_GET: &str = "agent.sync.get";
pub const AGENT_SYNC_ENABLE: &str = "agent.sync.enable";
pub const AGENT_SYNC_DISABLE: &str = "agent.sync.disable";
pub const AGENT_SYNC_NOW: &str = "agent.sync.now";
/// `SyncLocalOnlyParams` → `SyncStatusDto`：把同步目录键设为本设备专属 / 恢复同步。
pub const AGENT_SYNC_SET_LOCAL_ONLY: &str = "agent.sync.setLocalOnly";
/// FluxCloud 服务地址读取；正式构建 `editable=false`，地址恒为构建期固定值。
pub const AGENT_CLOUD_ENDPOINT_GET: &str = "agent.cloud.endpointGet";
/// 仅调试构建可用：覆盖/恢复 FluxCloud 服务地址，立即生效。
pub const AGENT_CLOUD_ENDPOINT_SET: &str = "agent.cloud.endpointSet";
pub const AGENT_REMOTE_LIST: &str = "agent.remote.list";
pub const AGENT_REMOTE_DISPATCH: &str = "agent.remote.dispatch";
pub const AGENT_REMOTE_COMMAND: &str = "agent.remote.command";
/// 局域网直连（L1）：展示本机配对码并开始广播 → `LinkPairingCodeDto`。
pub const AGENT_LINK_PAIRING_CODE: &str = "agent.link.pairingCode";
/// 停止展示配对码 / 停止配对广播。
pub const AGENT_LINK_STOP_PAIRING: &str = "agent.link.stopPairing";
/// `LinkDiscoveryParams`：开关 mDNS 发现；结果经 `LinkDiscoveredChanged` 推送。
pub const AGENT_LINK_DISCOVERY_SET: &str = "agent.link.discovery.set";
/// `LinkAddressParams` → `LinkDiscoveredPeer`：手动地址探测。
pub const AGENT_LINK_PROBE: &str = "agent.link.probe";
/// `LinkPairBeginParams` → `LinkPairBeginResponse`。
pub const AGENT_LINK_PAIR_BEGIN: &str = "agent.link.pairBegin";
/// `LinkPairFinishParams` → `LinkPairFinishResponse`。
pub const AGENT_LINK_PAIR_FINISH: &str = "agent.link.pairFinish";
/// `LinkApproveParams`：响应端确认 / 拒绝入站配对请求。
pub const AGENT_LINK_APPROVE: &str = "agent.link.approve";
/// `LinkDeviceParams`：解除配对。
pub const AGENT_LINK_REMOVE: &str = "agent.link.remove";
/// 探测全部已配对设备在线状态 → `Vec<LinkDeviceInfo>`（同时推送 `LinkedDevicesChanged`）。
pub const AGENT_LINK_REFRESH: &str = "agent.link.refresh";
/// `LinkDispatchParams` → `LinkDispatchResult`：下发下载到已配对设备。
pub const AGENT_LINK_DISPATCH: &str = "agent.link.dispatch";

pub const AGENT_PLAN_LIST: &str = "agent.plan.list";
pub const AGENT_ORDER_CREATE: &str = "agent.order.create";
pub const AGENT_ORDER_GET: &str = "agent.order.get";
pub const AGENT_ORDER_LIST: &str = "agent.order.list";
pub const AGENT_REFERRAL_SUMMARY: &str = "agent.referral.summary";
pub const AGENT_REFERRAL_LIST_CODES: &str = "agent.referral.listCodes";
pub const AGENT_REFERRAL_CREATE_CODE: &str = "agent.referral.createCode";
pub const AGENT_REFERRAL_DELETE_CODE: &str = "agent.referral.deleteCode";
pub const AGENT_REFERRAL_LIST_RECORDS: &str = "agent.referral.listRecords";
pub const AGENT_REFERRAL_VALIDATE: &str = "agent.referral.validate";

/// 云端推送状态 → `CloudNotifyStateDto`（读本机缓存；过期时后台刷新并经 `CloudNotifyChanged` 推送）。
pub const AGENT_CLOUD_NOTIFY_GET: &str = "agent.cloudNotify.get";
/// 立即向 FluxCloud 拉取概览 → `CloudNotifyStateDto`。
pub const AGENT_CLOUD_NOTIFY_REFRESH: &str = "agent.cloudNotify.refresh";
/// `CloudNotifyReportingParams` → `CloudNotifyStateDto`：本设备上报开关（设备本地）。
pub const AGENT_CLOUD_NOTIFY_SET_REPORTING: &str = "agent.cloudNotify.setReporting";
/// `CloudNotifyPrivacyParams` → `CloudNotifyStateDto`：上报附带字段（设备本地）。
pub const AGENT_CLOUD_NOTIFY_SET_PRIVACY: &str = "agent.cloudNotify.setPrivacy";
/// `CloudNotifyChannelCreateParams` → `CloudNotifyChannelDto`。
pub const AGENT_CLOUD_NOTIFY_CREATE_CHANNEL: &str = "agent.cloudNotify.createChannel";
/// `CloudNotifyChannelUpdateParams` → `CloudNotifyChannelDto`。
pub const AGENT_CLOUD_NOTIFY_UPDATE_CHANNEL: &str = "agent.cloudNotify.updateChannel";
/// `CloudNotifyChannelIdParams` → `{ "ok": true }`。
pub const AGENT_CLOUD_NOTIFY_DELETE_CHANNEL: &str = "agent.cloudNotify.deleteChannel";
/// `CloudNotifyChannelIdParams` → `CloudNotifyTestResult`（不计入额度）。
pub const AGENT_CLOUD_NOTIFY_TEST_CHANNEL: &str = "agent.cloudNotify.testChannel";
/// → `CloudNotifyTelegramBindDto`：生成一次性 Telegram 绑定码。
pub const AGENT_CLOUD_NOTIFY_TELEGRAM_BIND_START: &str = "agent.cloudNotify.telegramBindStart";
/// `CloudNotifyTelegramBindStatusParams` → `CloudNotifyTelegramBindStatusDto`。
pub const AGENT_CLOUD_NOTIFY_TELEGRAM_BIND_STATUS: &str = "agent.cloudNotify.telegramBindStatus";
/// `CloudNotifyDeliveriesParams` → `CloudNotifyDeliveriesPage`。
pub const AGENT_CLOUD_NOTIFY_DELIVERIES: &str = "agent.cloudNotify.deliveries";
/// `CloudNotifyEmailCodeParams` → `CloudNotifyEmailCodeResult`：向非账号邮箱发通知渠道验证码。
pub const AGENT_CLOUD_NOTIFY_SEND_EMAIL_CODE: &str = "agent.cloudNotify.sendEmailCode";
/// `CloudNotifyEmailVerifyParams` → `{ "ok": true }`：校验验证码，把地址记为本账号已验证的通知邮箱。
pub const AGENT_CLOUD_NOTIFY_VERIFY_EMAIL: &str = "agent.cloudNotify.verifyEmail";

pub const AGENT_PLATFORM_OPEN_TASK: &str = "agent.platform.openTask";
pub const AGENT_PLATFORM_REVEAL_TASK: &str = "agent.platform.revealTask";
pub const AGENT_PLATFORM_OPEN_PATH: &str = "agent.platform.openPath";
pub const AGENT_PLATFORM_INTEGRATION_GET: &str = "agent.platform.integrationGet";
pub const AGENT_PLATFORM_SET_AUTOSTART: &str = "agent.platform.setAutostart";
pub const AGENT_PLATFORM_SET_FILE_ASSOCIATION: &str = "agent.platform.setFileAssociation";
pub const AGENT_PLATFORM_SET_URL_PROTOCOL: &str = "agent.platform.setUrlProtocol";
/// 系统文件管理器为文件显示的图标（PNG）；参数见 `PlatformFileIconParams`。
pub const AGENT_PLATFORM_FILE_ICON: &str = "agent.platform.fileIcon";
pub const AGENT_CAPTURE_SUBMIT: &str = "agent.capture.submit";
/// 从本机 `.torrent` 文件建任务：agent 读文件、上传 daemon blob 后调用 `daemon.task.create`。
pub const AGENT_CAPTURE_SUBMIT_TORRENT_FILE: &str = "agent.capture.submitTorrentFile";
pub const AGENT_CAPTURE_LIST: &str = "agent.capture.list";
pub const AGENT_CAPTURE_RESOLVE: &str = "agent.capture.resolve";
/// 捕获上下文只读清单预解析，返回 `ResolvePreviewResponse`，不消费事务。
pub const AGENT_CAPTURE_PREVIEW: &str = "agent.capture.preview";
/// 用捕获上下文与最终清单选择建组，成功后消费事务，返回 `CreateGroupResponse`。
pub const AGENT_CAPTURE_CREATE_GROUP: &str = "agent.capture.createGroup";
/// 从本机插件包安装：agent 读文件、上传 daemon blob 后调用 `daemon.plugin.install`。
pub const AGENT_PLUGIN_INSTALL_FILE: &str = "agent.plugin.installFile";
pub const AGENT_DIAGNOSTICS_RUN: &str = "agent.diagnostics.run";
pub const AGENT_DIAGNOSTICS_REPAIR: &str = "agent.diagnostics.repair";
pub const AGENT_DIAGNOSTICS_LOG_PATHS: &str = "agent.diagnostics.logPaths";
pub const AGENT_DIAGNOSTICS_EXPORT_LOGS: &str = "agent.diagnostics.exportLogs";
/// 检查渠道最新版本：`{channel?}` → `UpdateStatusDto`（同时经 `UpdateChanged` 推送）。
pub const AGENT_UPDATE_CHECK: &str = "agent.update.check";
/// 后台下载并校验更新包（幂等）；不可一键更新时拒绝（`Unsupported`）。
pub const AGENT_UPDATE_DOWNLOAD: &str = "agent.update.download";
/// 一键「更新并重启」：包未就绪时先下载，就绪后替换程序并重启服务；返回 `UpdateStatusDto`。
pub const AGENT_UPDATE_INSTALL: &str = "agent.update.install";
/// 取消进行中的下载与待安装请求，返回 `UpdateStatusDto`。
pub const AGENT_UPDATE_CANCEL: &str = "agent.update.cancel";
/// 完成后关机：`{delaySecs}`；无活跃任务时拒绝（`InvalidArgument`）。
pub const AGENT_POWER_ARM: &str = "agent.power.arm";
pub const AGENT_POWER_DISARM: &str = "agent.power.disarm";

pub const SERVICE_EVENT: &str = "service.event";

pub const CAPABILITY_DAEMON_TASKS: &str = "daemon.tasks";
pub const CAPABILITY_DAEMON_QUEUES: &str = "daemon.queues";
pub const CAPABILITY_DAEMON_GROUPS: &str = "daemon.groups";
pub const CAPABILITY_DAEMON_CONFIG: &str = "daemon.config";
pub const CAPABILITY_DAEMON_RSS: &str = "daemon.rss";
pub const CAPABILITY_DAEMON_PLUGINS: &str = "daemon.plugins";
pub const CAPABILITY_DAEMON_COMPONENTS: &str = "daemon.components";
pub const CAPABILITY_DAEMON_WEBHOOKS: &str = "daemon.webhooks";
pub const CAPABILITY_DAEMON_SELECTIONS: &str = "daemon.selections";
pub const CAPABILITY_DAEMON_FILES: &str = "daemon.files";
pub const CAPABILITY_AGENT_GATEWAY: &str = "agent.gateway";
pub const CAPABILITY_AGENT_AUTH: &str = "agent.auth";
pub const CAPABILITY_AGENT_SYNC: &str = "agent.sync";
pub const CAPABILITY_AGENT_REMOTE_TASKS: &str = "agent.remoteTasks";
pub const CAPABILITY_AGENT_BILLING: &str = "agent.billing";
pub const CAPABILITY_AGENT_REFERRALS: &str = "agent.referrals";
pub const CAPABILITY_AGENT_DEVICE_LINK: &str = "agent.deviceLink";
pub const CAPABILITY_AGENT_EXTERNAL_CAPTURE: &str = "agent.externalCapture";
/// 本连接可调用 `agent.platform.openTask` / `revealTask`，在 agent 所在主机上打开 / 定位任务
/// 产物。按连接下发：Windows / macOS / Linux 桌面宿主恒有，嵌入式移动宿主没有；headless
/// server 只给本机来源（见 agent `server_mode::ServerHandle::local_platform_permitted`）。
pub const CAPABILITY_AGENT_OPEN_TASK_FILES: &str = "agent.openTaskFiles";
pub const CAPABILITY_CLIENT_SELECTIONS: &str = "client.selections";

/// 规范ALL_METHODS。
pub const ALL_METHODS: &[&str] = &[
    SYSTEM_HELLO,
    SYSTEM_PING,
    SYSTEM_SNAPSHOT,
    SYSTEM_SHUTDOWN,
    DAEMON_TASK_LIST,
    DAEMON_TASK_GET,
    DAEMON_TASK_ACTIVITY,
    DAEMON_TASK_CREATE,
    DAEMON_TASK_PAUSE,
    DAEMON_TASK_RESUME,
    DAEMON_TASK_RENAME,
    DAEMON_TASK_CHANGE_URL,
    DAEMON_TASK_DELETE,
    DAEMON_TASK_PAUSE_ALL,
    DAEMON_TASK_RESUME_ALL,
    DAEMON_TASK_RESCAN,
    DAEMON_TASK_SET_SEED_LIMITS,
    DAEMON_TASK_PAUSE_MANY,
    DAEMON_TASK_RESUME_MANY,
    DAEMON_TASK_DELETE_MANY,
    DAEMON_QUEUE_LIST,
    DAEMON_QUEUE_CREATE,
    DAEMON_QUEUE_UPDATE,
    DAEMON_QUEUE_DELETE,
    DAEMON_QUEUE_START,
    DAEMON_QUEUE_STOP,
    DAEMON_QUEUE_SCHEDULE,
    DAEMON_QUEUE_REORDER,
    DAEMON_QUEUE_MOVE_TASK,
    DAEMON_QUEUE_BOOST,
    DAEMON_GROUP_LIST,
    DAEMON_GROUP_RESOLVE_PREVIEW,
    DAEMON_GROUP_CREATE,
    DAEMON_GROUP_PAUSE,
    DAEMON_GROUP_RESUME,
    DAEMON_GROUP_DELETE,
    DAEMON_CONFIG_GET,
    DAEMON_CONFIG_PATCH,
    DAEMON_CONFIG_PROXY_TEST,
    DAEMON_CONFIG_CONN_POLICY,
    DAEMON_CONFIG_CLEAR_CONN_POLICY,
    DAEMON_CONFIG_SYSTEM_PROXY,
    DAEMON_SITE_AUTH_LIST,
    DAEMON_SITE_AUTH_DELETE,
    DAEMON_SITE_AUTH_GET,
    DAEMON_SITE_AUTH_SAVE,
    DAEMON_SITE_AUTH_CLEAR,
    DAEMON_SITE_AUTH_MATCH,
    DAEMON_RUNTIME_STATS,
    DAEMON_FS_LIST,
    DAEMON_RSS_LIST_SOURCES,
    DAEMON_RSS_GET_ITEMS,
    DAEMON_RSS_CREATE_SOURCE,
    DAEMON_RSS_UPDATE_SOURCE,
    DAEMON_RSS_DELETE_SOURCE,
    DAEMON_RSS_REFRESH_SOURCE,
    DAEMON_RSS_ITEM_ACTION,
    DAEMON_RSS_VALIDATE,
    DAEMON_LINK_PROBE,
    DAEMON_PLUGIN_LIST,
    DAEMON_PLUGIN_AUTH,
    DAEMON_PLUGIN_SET_ENABLED,
    DAEMON_PLUGIN_UPDATE_SETTINGS,
    DAEMON_PLUGIN_INSTALL,
    DAEMON_PLUGIN_INSTALL_DEV,
    DAEMON_PLUGIN_RELOAD_DEV,
    DAEMON_PLUGIN_UNINSTALL,
    DAEMON_PLUGIN_MARKET_LIST,
    DAEMON_PLUGIN_MARKET_INSTALL,
    DAEMON_PLUGIN_IGNORE_RETRY,
    DAEMON_COMPONENT_GET,
    DAEMON_COMPONENT_LIST_VERSIONS,
    DAEMON_COMPONENT_INSTALL,
    DAEMON_COMPONENT_UNINSTALL,
    DAEMON_SELECTION_SUBSCRIBE,
    DAEMON_SELECTION_UNSUBSCRIBE,
    DAEMON_SELECTION_RESOLVE,
    DAEMON_WEBHOOK_GET,
    DAEMON_WEBHOOK_CLEAR_DELIVERIES,
    DAEMON_WEBHOOK_SIMULATE,
    DAEMON_WEBHOOK_TEST,
    DAEMON_CDN_REPORTS_PEEK,
    DAEMON_CDN_REPORTS_ACK,
    DAEMON_CDN_CONFIG_APPLY,
    DAEMON_BT_TRACKER_SUBSCRIPTION_REFRESH,
    DAEMON_ED2K_SERVER_SUBSCRIPTION_REFRESH,
    DAEMON_DIAGNOSTICS_DESCRIBE,
    DAEMON_DIAGNOSTICS_PREPARE_LOG_EXPORT,
    DAEMON_DIAGNOSTICS_PROBE,
    DAEMON_DIAGNOSTICS_FIX_COMPONENT,
    DAEMON_MIGRATION_LINK_EXPORT,
    DAEMON_MIGRATION_LINK_ACK,
    DAEMON_MIGRATION_GATEWAY_EXPORT,
    DAEMON_MIGRATION_GATEWAY_ACK,
    AGENT_SESSION_GET,
    AGENT_AUTH_REGISTER,
    AGENT_AUTH_REGISTER_VERIFY,
    AGENT_AUTH_LOGIN,
    AGENT_AUTH_LOGIN_VERIFY,
    AGENT_AUTH_SEND_CODE,
    AGENT_AUTH_VERIFY_CODE,
    AGENT_AUTH_LOGOUT,
    AGENT_AUTH_REFRESH_PROFILE,
    AGENT_AUTH_SEND_PASSWORD_RESET_CODE,
    AGENT_AUTH_RESET_PASSWORD,
    AGENT_PROFILE_SEND_EMAIL_CODE,
    AGENT_PROFILE_SEND_NEW_EMAIL_CODE,
    AGENT_PROFILE_CHANGE_EMAIL,
    AGENT_PROFILE_RANDOM_ORIGIN_ID,
    AGENT_PROFILE_CHECK_ORIGIN_ID,
    AGENT_PROFILE_CHANGE_ORIGIN_ID,
    AGENT_PROFILE_CHANGE_NICKNAME,
    AGENT_PROFILE_SEND_PASSWORD_CODE,
    AGENT_PROFILE_CHANGE_PASSWORD,
    AGENT_GATEWAY_GET,
    AGENT_GATEWAY_PATCH,
    AGENT_GATEWAY_REVEAL_TOKEN,
    AGENT_DEVICE_LIST,
    AGENT_REMOTE_RECONNECT,
    AGENT_DEVICE_RENAME,
    AGENT_DEVICE_DELETE,
    AGENT_PREFERENCES_PATCH,
    AGENT_SYNC_GET,
    AGENT_SYNC_ENABLE,
    AGENT_SYNC_DISABLE,
    AGENT_SYNC_NOW,
    AGENT_SYNC_SET_LOCAL_ONLY,
    AGENT_CLOUD_ENDPOINT_GET,
    AGENT_CLOUD_ENDPOINT_SET,
    AGENT_REMOTE_LIST,
    AGENT_REMOTE_DISPATCH,
    AGENT_REMOTE_COMMAND,
    AGENT_LINK_PAIRING_CODE,
    AGENT_LINK_STOP_PAIRING,
    AGENT_LINK_DISCOVERY_SET,
    AGENT_LINK_PROBE,
    AGENT_LINK_PAIR_BEGIN,
    AGENT_LINK_PAIR_FINISH,
    AGENT_LINK_APPROVE,
    AGENT_LINK_REMOVE,
    AGENT_LINK_REFRESH,
    AGENT_LINK_DISPATCH,
    AGENT_PLAN_LIST,
    AGENT_ORDER_CREATE,
    AGENT_ORDER_GET,
    AGENT_ORDER_LIST,
    AGENT_REFERRAL_SUMMARY,
    AGENT_REFERRAL_LIST_CODES,
    AGENT_REFERRAL_CREATE_CODE,
    AGENT_REFERRAL_DELETE_CODE,
    AGENT_REFERRAL_LIST_RECORDS,
    AGENT_REFERRAL_VALIDATE,
    AGENT_CLOUD_NOTIFY_GET,
    AGENT_CLOUD_NOTIFY_REFRESH,
    AGENT_CLOUD_NOTIFY_SET_REPORTING,
    AGENT_CLOUD_NOTIFY_SET_PRIVACY,
    AGENT_CLOUD_NOTIFY_CREATE_CHANNEL,
    AGENT_CLOUD_NOTIFY_UPDATE_CHANNEL,
    AGENT_CLOUD_NOTIFY_DELETE_CHANNEL,
    AGENT_CLOUD_NOTIFY_TEST_CHANNEL,
    AGENT_CLOUD_NOTIFY_TELEGRAM_BIND_START,
    AGENT_CLOUD_NOTIFY_TELEGRAM_BIND_STATUS,
    AGENT_CLOUD_NOTIFY_DELIVERIES,
    AGENT_CLOUD_NOTIFY_SEND_EMAIL_CODE,
    AGENT_CLOUD_NOTIFY_VERIFY_EMAIL,
    AGENT_PLATFORM_OPEN_TASK,
    AGENT_PLATFORM_REVEAL_TASK,
    AGENT_PLATFORM_OPEN_PATH,
    AGENT_PLATFORM_INTEGRATION_GET,
    AGENT_PLATFORM_SET_AUTOSTART,
    AGENT_PLATFORM_SET_FILE_ASSOCIATION,
    AGENT_PLATFORM_SET_URL_PROTOCOL,
    AGENT_PLATFORM_FILE_ICON,
    AGENT_CAPTURE_SUBMIT,
    AGENT_CAPTURE_SUBMIT_TORRENT_FILE,
    AGENT_CAPTURE_LIST,
    AGENT_CAPTURE_RESOLVE,
    AGENT_CAPTURE_PREVIEW,
    AGENT_CAPTURE_CREATE_GROUP,
    AGENT_PLUGIN_INSTALL_FILE,
    AGENT_DIAGNOSTICS_RUN,
    AGENT_DIAGNOSTICS_REPAIR,
    AGENT_DIAGNOSTICS_LOG_PATHS,
    AGENT_DIAGNOSTICS_EXPORT_LOGS,
    AGENT_UPDATE_CHECK,
    AGENT_UPDATE_DOWNLOAD,
    AGENT_UPDATE_INSTALL,
    AGENT_UPDATE_CANCEL,
    AGENT_POWER_ARM,
    AGENT_POWER_DISARM,
    SERVICE_EVENT,
];

/// 会等待网络、下载或外部 IO 的 daemon 方法。daemon 对这些方法按请求并发处理，
/// agent 网关也把它们放到独立于普通 daemon 命令的通道，避免长耗时调用堵住暂停/恢复/设置写入。
pub const SLOW_DAEMON_METHODS: &[&str] = &[
    DAEMON_COMPONENT_LIST_VERSIONS,
    DAEMON_COMPONENT_INSTALL,
    DAEMON_PLUGIN_AUTH,
    DAEMON_PLUGIN_INSTALL,
    DAEMON_PLUGIN_INSTALL_DEV,
    DAEMON_PLUGIN_MARKET_LIST,
    DAEMON_PLUGIN_MARKET_INSTALL,
    DAEMON_GROUP_RESOLVE_PREVIEW,
    DAEMON_RSS_VALIDATE,
    DAEMON_LINK_PROBE,
    DAEMON_CONFIG_PROXY_TEST,
    DAEMON_WEBHOOK_TEST,
    DAEMON_BT_TRACKER_SUBSCRIPTION_REFRESH,
    DAEMON_ED2K_SERVER_SUBSCRIPTION_REFRESH,
    DAEMON_FS_LIST,
    DAEMON_DIAGNOSTICS_PROBE,
    DAEMON_DIAGNOSTICS_FIX_COMPONENT,
];

/// 是否属于 [`SLOW_DAEMON_METHODS`]。
#[must_use]
pub fn is_slow_daemon_method(method_name: &str) -> bool {
    SLOW_DAEMON_METHODS.contains(&method_name)
}

/// 规范DAEMON_CAPABILITIES。
pub const DAEMON_CAPABILITIES: &[&str] = &[
    CAPABILITY_DAEMON_TASKS,
    CAPABILITY_DAEMON_QUEUES,
    CAPABILITY_DAEMON_GROUPS,
    CAPABILITY_DAEMON_CONFIG,
    CAPABILITY_DAEMON_RSS,
    CAPABILITY_DAEMON_PLUGINS,
    CAPABILITY_DAEMON_COMPONENTS,
    CAPABILITY_DAEMON_WEBHOOKS,
    CAPABILITY_DAEMON_SELECTIONS,
    CAPABILITY_DAEMON_FILES,
];

/// 规范 `AGENT_CAPABILITIES`。
///
/// 声明 agent 可能下发能力的规范全集（用于字面量断言与能力全景索引）。
/// 注意：[`CAPABILITY_AGENT_OPEN_TASK_FILES`] 属于连接级条件能力，仅向本机来源或显式授权连接下发，
/// 服务级基线 hello 并不无条件包含它。
pub const AGENT_CAPABILITIES: &[&str] = &[
    CAPABILITY_AGENT_GATEWAY,
    CAPABILITY_AGENT_AUTH,
    CAPABILITY_AGENT_SYNC,
    CAPABILITY_AGENT_REMOTE_TASKS,
    CAPABILITY_AGENT_BILLING,
    CAPABILITY_AGENT_REFERRALS,
    CAPABILITY_AGENT_DEVICE_LINK,
    CAPABILITY_AGENT_EXTERNAL_CAPTURE,
    CAPABILITY_AGENT_OPEN_TASK_FILES,
];

/// 规范CLIENT_CAPABILITIES。
pub const CLIENT_CAPABILITIES: &[&str] = &[CAPABILITY_CLIENT_SELECTIONS];
