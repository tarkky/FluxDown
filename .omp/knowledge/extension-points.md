# FluxDown internals · 扩展点索引

> 本文件是 `FluxDown/AGENTS.md` 的深挖附录：只放**枚举性 / 可从源码复原**的细节，硬不变式与红线在 AGENTS.md。
> 路径以 `FluxDown/` 为根（cwd=工作区根时前置 `FluxDown/`）。事实层以源码为准，文档给坐标。

---

## 扩展点索引（"要加 X 改哪里"）

| 目标 | 步骤 |
|---|---|
| **新增下载协议** | 加 `is_X_url` 谓词 + `run_X_download`（照 `ed2k` 模板）；在 `download_manager` 的 `do_start_task` **与** `do_resume_task` if/else 各加一臂；新表加进 `SQLITE_SCHEMA`+`POSTGRES_SCHEMA`+迁移 |
| **调整 HLS 独立音轨 / MP4 封装** | `engine/src/hls_downloader.rs` 的 `external_audio_uri` 选 AUDIO 组音轨（优先 DEFAULT）→ `run_audio_track` 并行下载与独立续传 → `mux_video_audio`；通用 `-c copy` 封装在 `dash_downloader::ffmpeg_copy_to_mp4`，TS 收尾在 `remux_ts_to_mp4`（ffmpeg 优先，内存兜底限 192MiB，fMP4 只改名）。保留 ffmpeg 不可用 / mux 失败的 warning 与仅视频降级，取消保留续传输入 |
| **新增受管组件** | `components/` 下照 `ffmpeg.rs`/`ytdlp.rs` 加模块（resolve 优先级 + Status + install under cfg）；加 config 键；在 `plugin/dependencies.rs` 映射 |
| **新增插件工具面** | `runtime.rs` 加 Spec/Outcome/Availability（禁 rquickjs 类型）；`HostContext` 加门；`bridge.rs` 实现（semaphore + 牢笼）；`manifest` 加 permission |
| **扩展插件 yt-dlp 参数** | 改 `engine/src/plugin/bridge.rs` 的 `ytdlp_option_kind` 白名单与 `ytdlp_args_reject_reason` 的参数类型 / 数量校验；完整长选项和独立短选项才可放行，位置参数仅 HTTP/HTTPS URL，路径值守牢笼；不要改回黑名单或允许插件覆盖宿主 `--ffmpeg-location` |
| **调整插件工作区 / 清理** | 统一复用 `plugin/bridge.rs::plugin_workspace`（`[A-Za-z0-9-]` 保留，其余字节含下划线编码为 `_XX` 小写十六进制）；`flux.fs`、`run_ytdlp` 和 `remove_plugin_workspace` 必须使用同一无碰撞根路径，卸载入口在 `PluginManager::uninstall` |
| **插件通用认证** | `engine/src/auth.rs` 保存受控 `AuthProfile`；`manifest.permissions` 声明 `auth`；插件经 `flux.auth.save/get/remove` 管理登录结果，`flux.fetch({authRef})` 或插件+站点默认引用自动注入 Cookie/Bearer/Header |
| **调整站点 HTTP Basic 凭据** | `engine/src/site_auth.rs` 的 `site_key` / `parse_store` / `inject_basic_auth` + `download_manager::apply_site_auth`；站点键为 `scheme://host[:port]`，默认端口省略，HTTP/HTTPS 隔离；旧裸 host 键只迁为 HTTPS，新键优先，凭据保存在设备本地 `site_auth_credentials` |
| **新增移动 FFI 能力** | `native/mobile` 复用 protocol DTO 与现有 UniFFI 导出；同步 Android / iOS 适配与模型，重生成 Kotlin / Swift 绑定 |
| **新增 RSS 过滤规则** | `engine/src/rss/filter.rs` 改判定 + 补单测 → **同步** `web/src/pages/rss/filter.ts` 的规则预览镜像（预览与实际下载必须一致）→ 当前客户端控件与共享 i18n |
| **调整 RSS 失败退避** | `rss/mod.rs::effective_interval_secs` 管 feed 失败（配置间隔指数翻倍，6h 封顶但不缩短配置间隔）；`torrent_retry_delay_secs` / `record_torrent_failure` 管种子条目失败（600s 起、24h 封顶），`db.rs` 持久化 `fetch_failures` / `retry_after` 并只派发到期条目、未失败项优先；成功建任务 / 手动下载清零条目退避 |
| **调整 RSS enclosure 类型识别** | `rss/parser.rs` 的 `extract_enclosure` / `map_entry` 解析并规范化 MIME → `rss_items.enclosure_type` 落库 → `rss/mod.rs::plan_for` / `RssDownloadPlan::is_torrent_file` → `download_manager::create_rss_tasks`；`application/x-bittorrent` 的无扩展名链接也先抓种子字节，magnet 与二段 resolver 不沿用此分支 |
| **新增订阅 provider** | 插件 manifest 加 `subscriptions:[{providerId,entry,timeoutMs}]`，脚本实现 `globalThis.subscribe(ctx)` 并返回规范化条目 → `PluginManager` 动态路由到 `subscription::SubscriptionProvider` → 公共调度负责退避/去重/过滤/落库/建任务（`Engine::initialize` 经 `set_fallback_provider` 挂载路由，宿主无需接线）；订阅级「代理 / User-Agent」只对内置 `rss` provider 生效，插件请求走 `flux.fetch` 的全局出口 |
| **新增 HTTP 能力** | 扩 `ApiHost`（带默认 impl 保持现有宿主可编译）+ `api/server.rs` handler + `routes.rs` 常量；宿主（agent `native/agent/src/api_host.rs`，转发 daemon RPC）按需 override；跑 `gen_openapi` 重生成。`native/server` 已冻结不再跟进 |
| **新增本机 RPC 能力** | `native/protocol` 先加唯一 method/DTO/event/error → owner 进程（下载事实进 daemon actor；账户/云/UI Gateway 进 agent）实现 → `crates/app` 单会话 adapter 映射到 capability-local port；Web SPA 在 `web/src/lib/rpc/protocol/`（TS wire 镜像）+ `methods/` 补包装、事件语义改 `apply.ts`；二进制 body 留专用鉴权 HTTP 端点（headless 浏览器面 `/api/web/*` 在 `native/agent/src/server_mode.rs`） |
| **新增 aria2 方法** | `aria2.rs` `METHOD_NAMES` + `jsonrpc.rs` dispatch | 
| **新增 MCP 工具** | `mcp.rs` tool_definitions + call_tool |
| **新增引擎事件** | `events.rs` 的 `EngineEvent` + `EventSink`；在 `native/daemon/src/event_hub.rs` 映射规范 `DaemonEvent`，同步 daemon/agent 快照投影与 GPUI / Web / 原生移动端消费者（含重连快照），验证真实 sink 广播，避免仅更新快照而已有窗口漏刷 |
| **新增 webhook 事件** | `engine/src/webhook.rs` 的 `WebhookEventKind` 加变体（`wire()`/`title()` 同步）+ 在 `download_manager` 对应生命周期点位 `self.webhook.emit(...)`；同步 protocol 与各客户端事件选择器，wire 名必须逐字一致 |
| **调整 webhook 投递 / 积压策略** | `engine/src/webhook.rs` 的 `EndpointQueue` / `spawn_worker` / `dropped_record`；`emit` 同步入有界 FIFO，每端点串行、全局并发 4，待投递上限 256，满时丢最旧项并汇总失败日志（0 次尝试），不能回退为每条事件派生等待任务 |
| **新增 webhook 服务预设** | 只改 `engine/src/webhook.rs`：`Preset` 加变体 + `wire`/`label`/`content_type`/`escape`/`default_template`/`url_placeholder` 六个 match 各补一臂。模板由引擎下发，UI 零改动（只有品牌字标 `WebhookPresetMark`/`PRESET_MARKS` 想美化时才加） |
| **新增引擎设置** | `native/protocol/src/daemon_config.rs` 的 `DAEMON_CONFIG_FIELDS` 加一行（类型/范围/默认）→ daemon 校验与投影自动跟随 → `native/daemon/src/actor.rs::apply_live_config` 按需 live-apply → GPUI `crates/settings/src/sections/<page>.rs` 用 `ctx.daemon_switch/number/input/dropdown(key)` 加条目（文案键复用 `assets/i18n`），同步 Web 与原生移动端目录/消费者。跨设备同步在 protocol `settings.rs` 的目录登记；设备本地偏好（非 daemon）直接 `ctx.pref_*` 写 `agent.preferences` |
| **新增 GPUI 主题/基础组件** | `crates/theme` 改完整亮暗 `SemanticThemeTokens`（Base 新 token 必须同步两份）→ `crates/components` 只从 `active_theme().tokens()` 取值；文案仍只改共享 `assets/i18n/{en,zh}.json`，`crates/i18n/build.rs` 自动嵌入 |
| **新增内置文件图标包 / 调整内置映射** | 改 `scripts/gen_icon_packs.ts`（上游锁定版本 + 选取的图标名 + `kinds` 映射，许可证随包写入 `assets/icon-packs/LICENSE-*`）→ `bun scripts/gen_icon_packs.ts` → `crates/icon_pack/src/registry.rs` 的 `BUILTIN_SOURCES` / `BUILTIN_PACK_IDS` 与 `web/src/lib/icon-pack/registry.ts` 同步登记 → 设置卡片文案键 `fileIconPack<Name>`（en + zh）。改格式规则同改 Rust / TS 两端并补 `crates/icon_pack/tests/fixtures/cases.json` |
| **新增 GPUI capability / 设置分区** | 状态/命令/多页面满足拆 crate 条件时在 `crates/<capability>` 定义本地 port/controller/view；只依赖 `fluxdown_protocol` 和 UI 基础层 → `crates/app` 注入同一个 `Arc<AgentClient>` adapter 与有序 snapshot/event 流；shell 只收内容槽。设置页 = gpui-component `Settings` DSL：`crates/settings/src/sections/<page>.rs::page(&SectionContext, cx) -> SettingPage`，状态经 `Entity<SettingsStore>`（防抖合并写回、乐观覆盖、冲突重试），自定义控件用 `SettingField::render` + `window.use_keyed_state`，对话框走 `window.open_dialog`（shell 已渲染 dialog/sheet/notification 层） |
| **新增 NAS/分发目标** | `packaging/<target>/build_*.sh` 复用（`fluxdown-agent` + `fluxdownd` 同目录、入口 `fluxdown-agent --server` + `FLUXDOWN_*`；Web UI 已编译期内嵌，别再往包里塞 `web/` 目录）布局 → 接入 release.yml `build-server-nas-packages` |
| **新增发布组件** | `changes` job 加路径→输出映射 + 一对 `build-*`/`release-*` job（各自组件 tag） |
| **新增文档页** | `website-v2/src/content/docs/{en,zh}/<section>/<page>.md`（section 加进 `content.config` 枚举，zh 跑 `docs:hash`） |
| **新增 UI 文案** | 只补 **en + zh 基线对**：GPUI/Web/原生移动端共用 `assets/i18n/{en,zh}.json`；官网主站 `website-v2/src/i18n/messages/<ns>.ts` 的 `defineMessages({ en, zh })`；扩展 `fluxDown/utils/locales/{zh-CN,en}.ts`（`MessageKey` 由 zh-CN 推导）。**社区语言（`ja` 等）不碰**——Weblate 维护，运行时键级回退英文 |
