# FluxDown internals · 数据模型 · 下载引擎 · 插件系统

> 本文件是 `FluxDown/AGENTS.md` 的深挖附录：只放**枚举性 / 可从源码复原**的细节，硬不变式与红线在 AGENTS.md。
> 路径以 `FluxDown/` 为根（cwd=工作区根时前置 `FluxDown/`）。事实层以源码为准，文档给坐标。

---

## 状态与数据模型

- **任务状态码**: 0=pending, 1=downloading, 2=paused, 3=completed, 4=error, 5=preparing。
- **文件类型分类**: all/video/audio/document/image/program/archive/other；分类规范由 protocol DTO 与当前客户端分类模型承载，用户可自定义匹配模式与保存目录。
- **时间分组**: today/yesterday/thisWeek/thisMonth/older。
- **任务组**: 多文件下载的纯逻辑聚合壳（N 独立子任务 + `task_groups` 行）；组进度由前端 SUM 聚合；空组自动回收（`gc_empty_groups`）。

### 数据库（`native/engine/src/db.rs`，sqlx `Any` 池）

**双后端**：URL scheme 选后端（`sqlite:`/`postgres:`）；两份 DDL 常量（`SQLITE_SCHEMA`/`POSTGRES_SCHEMA`）处理二进制、字节计数与自增主键的后端差异；运行时 SQL 统一 `$N` 占位符；`add_column_if_missing` 幂等迁移（新库建表即全列，旧桌面库经 ALTER 升级）。SQLite 侧 WAL + 外键 + busy_timeout=5000。

**当前表（列以 db.rs 为准，此处仅索引）**：
- `tasks`(id PK, url, file_name, save_dir, status, total/downloaded_bytes, segments, created_at, error_message, proxy_url, queue_id, checksum, ignore_tls_errors, bt_selected_files, bt_custom_name, orig_etag, orig_last_modified, audio_url, file_missing, `range_verified`（配额端点续传验证）, queue_order；迁移列：cookies, referrer, extra_headers, resolver_plugin_id, segments_epoch, completed_at, group_id, resolver_item, rss_source_id（RSS 溯源，空=非 RSS 来源）, `origin_url`（展示用真实来源；`.torrent` 任务的 `url` 是 `torrent-file://local` 哨兵，「复制链接」类 UI **一律**读它并空则回退 `url`）, `auto_route`（`ProxyMode::Auto` 的任务级最终链路，wire 标签见 `auto_proxy::route`；空=非 Auto）, `unattended`（无人值守创建标记，`NewTaskSpec::unattended_selection` 置位：RSS / 远程派发 / 系统打开链接或拖入 / 外部接管命中设备本地偏好「免打扰跳过二次选择」`download.silent_skip_selection`；start/resume 读它让 HLS/DASH 画质与插件变体静默取默认、「文件已存在」询问按重命名；BT 不读此列——建任务时已按「全选」写 bt_selected_files））
  - 来源归因迁移列 `src_cdn_bytes` / `src_proxy_bytes` / `src_nic_bytes`（BIGINT）：coordinator 在既有 DB flush 节拍按 `NodePool::source_bytes` 把「多 CDN 钉定节点 / Auto 代理路径 / 多网卡链路」的增量以 `segments_epoch` 守卫累加；源站 = `downloaded_bytes` − 三者之和，P2P 协议由客户端整体归 P2P。任何把 `downloaded_bytes` 复位为 0 的写入同语句清零三列。实时值走 `TaskRuntimeDto.sourceBytes`（含在途），持久值走 `TaskDto.sourceBytes`；客户端详情常规页「来源构成」区块的切片算法 GPUI / Web 逐条镜像（见 AGENTS.md §5）。
  - 命名来源迁移列 `name_inferred`（1 = 引擎推断名，HTTP 完成期可精修）/ `resp_name` / `resp_mime`（实际 GET 响应的命名证据），语义见下方 `naming/`。
  - 「文件已存在」逐任务决定迁移列 `exists_decision`（`''` / `rename` / `overwrite` / `skip`），语义见下方「文件已存在策略」。
- `task_segments`(复合 PK task_id+segment_index；旧库遗留 id AUTOINCREMENT 不再读)
- `task_groups`(id PK, name, source_url, save_dir, created_at)
- `config`(key PK, value)——**所有设置键**都存这里
- `torrent_files`(task_id PK, file_bytes BLOB)
- `queues`(id PK, name, speed_limit_kbps, upload_limit_kbps, max_concurrent, default_save_dir, position, default_segments, default_user_agent, is_running, schedule_enabled/start/stop, schedule_days 位掩码)
- `ed2k_blocks`(复合 PK task_id+block_index, state, downloaded_bytes, retry_count)
- `ed2k_hashset`(task_id PK, hashes BLOB)
- `task_artifacts`(复合 PK task_id+file_name；追踪 sidecar/产物文件供清理)
- `rss_sources`(id PK, `provider_id`/`provider_config`（订阅 provider 适配器与其不透明配置，旧数据默认 `rss`/空）, url, name, enabled, auto_download, start_paused, queue_id, save_dir, interval_minutes, include/exclude_pattern, use_regex, smart_episode, size_min/max_bytes, send_referer, notify_on_download, max_per_fetch, cookies, user_agent, proxy_url, last_fetch_at, last_success_at, last_error, fail_count, `seeded`（首轮是否已完成）, position)
- `rss_items`(复合 PK source_id+guid, title, link, enclosure_url/length/type（声明的下载目标 MIME）, `resolver_item`（插件二段解析标识，空 = 普通直链）, pub_date, fetched_at, status 0..5, task_id 回链, episode_key, reason 原因码；`fetch_failures` / `retry_after` 持久化种子抓取失败退避；`ON DELETE CASCADE` 于 rss_sources)

**内置队列**: `main`（主）/`later`（稍后下载），播种于 `Engine::new`，不可删/改名；存量 `queue_id=''` 迁入 `main`。删除命名队列后，任务归属、排队位置与持久排序都要同步：`TaskQueueChanged` 只带归属，`QueuePositionsChanged` 只带待排位置，`TasksSnapshot` 才是 `queue_order` 归零后的权威来源。

**任务活动与实时采样**：`task_activity.rs` 拥有源端活动代理，`db.rs` 的活动表/保留标记在 SQLite 与 PostgreSQL 同步维护。状态边沿、完整错误、实际重试、分段拆分及 CDN 语义事件先持久化再广播；高频进度不逐帧入库。ID 在同一数据库内跨重启单调，任务删除级联清理；保留约束及分页上限以 `db.rs` 常量为准，清理过的历史有逐任务截断标记。所有宿主的 `progress_reporter` 必须使用 `Engine::activity_sink`，否则绕过统一记录；关闭时冲刷有超时并报告真实失败。`transfer_activity.rs` 的 RAII 计数只覆盖真实传输生命周期，采样携带源端单调序号；分段几何、活跃传输、配置上限与 BT peers 是不同事实，不能相互推算。

---

## 下载引擎（`native/engine`）

- 平台存储 FFI（`disk_space.rs`、`segment_coordinator.rs`、`bt_sparse.rs`、`bt_downloader.rs`）保留实际分配/并发扩容、大卷宽度和饱和计算语义，不机械替换成 fs2；具体锁定版本差异见 `rule://no-unsafe-in-rust`。空间出参仅成功后读取，fadvise 直接错误码显式记录；Windows 属性读取走安全元数据，错误不能被当作 sparse 标记，测试区分磁盘占用与逻辑长度。

### 6 种协议（分发 = `download_manager::do_start_task`/`do_resume_task` 内单条 if/else 链，每臂 `catch_unwind`）

| 协议 | 判定谓词 | 入口 | 文件 |
|---|---|---|---|
| **HTTP/HTTPS**（默认兜底） | fallthrough | `segment_coordinator`（IDM worker pool） | `downloader.rs` / `segment_coordinator.rs` / `segment_advisor.rs` |
| **FTP** | `is_ftp_url` | `ftp_downloader::run_ftp_download` | `ftp_downloader.rs`（suppaftp 同步 + spawn_blocking） |
| **BitTorrent** | `is_bt_url`（magnet 或 .torrent 哨兵） | librqbit `SharedBtSession` | `bt_downloader.rs` / `tracker_subscription.rs` |
| **HLS** | `hls_downloader::is_hls_url` | `run_hls_download` | `hls_downloader.rs`（M3U8/多码率/AES-128） |
| **DASH / 音视频轨合并** | `is_dash_url` 或有 `audio_url` | `run_dash_download` | `dash_downloader.rs` |
| **ED2K（仅下载）** | `ed2k::link::is_ed2k_url` | `ed2k::run_ed2k_download` | `ed2k/`（mod,link,proto,hash,server,peer,client,server_subscription,upnp,kad/） |

- **HLS 独立音轨与封装**：master 变体的 AUDIO 组若有 `EXT-X-MEDIA` 独立 URI，`external_audio_uri` 优先选 `DEFAULT=YES` 音轨。开跑前探测可用 ffmpeg；可用时音轨与视频并行下载，各自保留续传检查点，进度汇总两轨字节，收尾经 `mux_video_audio` + `dash_downloader::ffmpeg_copy_to_mp4` 以 `-c copy`（不转码）封装 MP4。音轨下载的真实错误会取消整任务；ffmpeg 不可用时仅下载视频并写 warning 活动，mux 失败则保留视频、尝试另存音轨 sidecar 并写 warning；mux 取消保留两轨临时文件与检查点供恢复。
- **HLS TS→MP4**：`remux_ts_to_mp4` 优先用同一 ffmpeg 流复制入口，不整文件读入内存，也不受内存转换的 192MiB 上限约束；无 ffmpeg 或执行失败才尝试有上限的 `ts2mp4` 内存转换，空间不足 / 转换失败保留 TS，取消不触发内存兜底。`EXT-X-MAP` 的 fMP4 输出不做 TS 转换，只按占名规则改名为 MP4。
- BT 任务绕过 pending 队列，且**不计入** http/ftp 并发计数（`max_concurrent`）。
- **BT 判定只认 `magnet:` 与 `torrent-file://` 哨兵**（`is_bt_url`）。HTTP 的
  `.torrent` **直链不会走 BT**——会被当普通文件下回来一个种子文件。要让直链
  变成真下载，必须先把字节抓下来再以 `NewTaskSpec::torrent_file_bytes` 建任务
  （RSS 订阅就是这么做的，见 `rss/` 的两段式）。
- **DHT 持久化是纯缓存，`SharedBtSession::new` 对它三级兜底**：带 `dht.json`
  起 → 失败则删掉它重试 → 再失败则关 DHT 起（tracker + PEX 仍可用）。起因是
  一份钉着 `addr: 0.0.0.0:58686` 的 `dht.json` 撞上 Windows 动态端口排除区间
  （`netsh interface ipv4 show excludedportrange protocol=udp`；`netstat` 看不到
  占用但 bind 返回 `WSAEACCES` 10013），导致**所有** BT 任务永久 status=4 而
  用户无从自救。**anyhow 错误一律用 `{e:#}` 打印**——`{e}` 只输出最外层
  context，会把这类根因整个吞掉。
- **BT 会话保活与完成校验**：`maybe_release_bt_session` 在存在「已暂停的未完成
  torrent（句柄在册）」时**不拆**共享会话——resume 走 `unpause`（Paused→Live
  零校验秒恢复）；全部 BT 任务终态化（完成/删除且无做种）才释放。暂停撞上
  librqbit 初检（只能从 Live 暂停）时经世代号「延迟暂停」兜底，防幽灵下载。
  Windows 上 staging 文件经 `bt_sparse` 打 FSCTL_SET_SPARSE（免整体簇预留 +
  免 VDL 零填充写放大）。完成期全量重哈希只在 fastresume 污点时执行（add 时
  存在既有 `.bitv` / 经缓存句柄跨暂停恢复 / 完成重试）；无污点任务的 have-bits
  全部有磁盘依据（全量初检读盘 / Live 写盘后读回校验），完成即时。
- **ED2K**：eDonkey2000 纯 leech。源发现 = 服务器 `GETSOURCES`（手动 `ed2k_server_list` + 订阅 `server.met` 缓存）+ Kad DHT 兜底 + UPnP-IGD 争 HighID + LowID 回调中继。逐块 MD4 + hashset 自校验（违规拉黑 peer）；分块 MD4 root hash（PART_SIZE=9.28MB，幻影尾处理）。进程级共享 `Ed2kClient` 持久服务器会话：读循环处理 IDCHANGE/EOF；`OP_CALLBACK_FAIL` 不带 client_id，按服务器顺序立即失败最早的待决回调；有活跃 ED2K 任务时断线后台重连（1→30s 指数退避封顶），无活跃任务不重连，不发协议外心跳。链接大小上限 256 GiB；建块/预分配前做磁盘余量预检（不足即失败、不建文件与块行）。块状态以内存快照为准供 200ms 上报读取，DB 只在启动恢复与终验坏块时全量读；块完成按批事务落库，且数据 `sync_data` 与 hashset 落库先于 verified 标记提交。
- **HTTP 临时文件独占**：多段与单流写临时文件前按规范化路径（Windows 忽略大小写）取进程内 RAII 独占；同一下载运行的嵌套调用与 worker 共享，另一任务撞同一路径时以 `DownloadError::Io(WouldBlock)` 失败进入错误态，不截断/覆盖对方数据，占用方结束后可重试。
- **文件已存在策略**（config `file_exists_behavior` = `rename` / `overwrite` / `skip` / `ask`；纯决策 `file_exists.rs::decide`，副作用在启动序幕 `finalize_start_file_name`）：只在目标路径已有**普通最终文件**时生效。优先级：跳过（逐任务决定或全局 skip，只看最终文件）→ 硬冲突（`.fdownloading` / 兄弟预订 / 同名目录，恒编号改名）→ `tasks.exists_decision` → 全局策略 → `ask` 降级（协议不可询问 HLS / BT / DASH 轨对、`tasks.unattended=1`、`HostSelection::can_prompt()=false` 均按重命名）→ 询问。可询问：HTTP/HTTPS/FTP（含跳过）、ED2K 与 DASH `.mpd`（无跳过）；非 GET 请求不询问。询问时任务**让出并发槽**挂起（`awaiting_decision`，DB 状态保持 0，`has_capacity` 不计），序幕 task 经 `HostSelection::select_file_exists`（60s）等待，答复经既有 `TaskDone` 通道以 `TaskDonePhase::{AwaitingDecision, DecisionReady}` 回流（宿主无需新增 drain 通道，只有 `Finished` 表示任务结束）；取消 = 暂停，其余答复落 `exists_decision` 后以队首重排，重入不重复触发 onStart / `task.started`。覆盖授权 `OverwritePolicy::{Never, Any, Only(name)}`：逐任务覆盖只绑定询问时的名字（完成期精修成别名则不覆盖），旧文件只在完成认领时替换。跳过 = 「采纳」已有文件：任务记 `exists_decision='skip'`，不拥有该文件（重新下载 / 删除任务和文件不删它，不打 MOTW、不发插件 Done，仍发 webhook completed）。用户改名与重新下载清空决定。
- **从未起跑的任务走启动序幕**：恢复路径（稍后下载、排队中暂停后恢复、重启自动恢复、重新下载、答复后重入）遇到无任何落盘进度的非 BT 任务（`never_started`）按全新起跑处理，命名 / dedup / 跳过 / 询问统一生效；已有进度的续传不再经过序幕，因此不会重复询问。HLS 起跑 dedup 同时避让同名 `<stem>.mp4` 成品。

### 引擎子系统（一句话职责）
- `download_manager.rs`（~7300 行）：任务生命周期、并发、队列（内置 + 命名，启停/每日定时边沿触发/顺序）、任务组、自动重试、协议分发、off-actor 插件解析插桩、速度平滑（EMA α=0.4，1s 采样窗）、WAL checkpoint。
- `downloader.rs`：共享原语（`DownloadError` 含 Ed2k/Ed2kIntegrity/Cancelled、`RequestSpec`、HTTP 探测与下载执行、完成期原子占名）。
- `naming/`：**文件命名管线**（唯一的名字推断 / 编码解码 / sanitize 实现，其余协议与宿主一律调用它）。证据权威顺序：显式名（用户 / API `out` / 扩展发来的非空名，只 sanitize）> 实际响应 `Content-Disposition`（RFC 6266 受限 quoted-pair、RFC 8187 任意 WHATWG 标签、RFC 2231 续行、RFC 2047 encoded-word、百分号 `filename=`）> URL 查询 `response-content-disposition`/`rscd` > URL 路径末段 > `Content-Type` / 文件头魔数（`sniff_extension`）补或换扩展名。未声明字符集的原始字节走 `decode_legacy_bytes`：严格 UTF-8 → 无打分器的页面字符集 / `.kr` EUC-KR → GBK/Big5/Shift_JIS/windows-1252 按原始字节「常用字区段」打分（TLD +1、页面字符集 +4）。规范与跨实现用例在 `naming/fixtures/content_disposition.json`，扩展 `fluxDown/utils/filename.ts` 逐条镜像（AGENTS.md §5）。
  - **来源与完成期定名**：`tasks.name_inferred` = 引擎自行推断的名字（无 CD 的探测名、URL 占位）；`tasks.resp_name` / `resp_mime` = 实际 GET 响应的 CD 名与 Content-Type（非 hint 在 probe 后、hint 在单流首响应 / 多段起点连接写入，先写者胜，换链清空）。HTTP 完成 rename 前，推断名按「CD 名 → 类型证据」精修后进入原子占名环（冲突重 dedup、落库、完成信号带新名）；显式名与用户改名（`set_task_file_name_with_source(.., false)`）永不改写。HEAD 探测只在 2xx 时产出名字；hint 任务（扩展给了大小）启动与排队期都不发 HEAD，只落 URL 占位名（一次性签名 URL 会被任何请求消耗）。
- `segment_advisor.rs`：按文件大小 + CPU 推荐连接上限（HTTP 是上限，coordinator 逐步爬升）。
- `segment_coordinator.rs`（~7300 行）：IDM 式动态分段（按需分配、按 ECF 挑选预计完成最晚的在传分段并按持有者/帮手速率比例均衡拆分、连接复用、per-domain 连接策略学习——负面上限 + 正面起步提示双观察面、`fallocate` 预分配）。子模块 `segment_coordinator/multipath.rs`：每 ramp 窗口的连接稳态采样（以租约真实开始时刻/字节建立基线；首个完整窗预热，短初窗累计，按实际时长算速率；限速窗不计，只有完整零增量窗判停滞）→ 路径估计、冷路径探索 worker、完成时间抢占（分段子令牌取消 → `WorkerEvent::Preempted` → 余量回 Pending 立即续派）、Auto 主导链路标签与先验回写。
- `speed_limiter.rs`：全局 token bucket（Arc 可克隆，limit==0=不限）。
- `meta_prober.rs`：队列任务后台探测文件名/大小（8s；HTTP HEAD（仅 2xx 产出名字）/ FTP SIZE / magnet dn= / torrent 跳过；hint 任务不调用），返回名字来源供 `name_inferred` 落库。
- `proxy_config.rs`：无/系统（Windows 注册表）/手动/**自动**（`ProxyMode::Auto`）；HTTP/HTTPS/SOCKS4/5；`test_proxy_connection` 测延迟。
- `auto_proxy.rs` + `path_scheduler.rs` + `cdn/node_pool.rs`：`ProxyMode::Auto` 是**多路径调度**，没有「采样→一次性热切换」状态机。直连（SYS + 多 CDN 钉定节点）、手动代理、系统代理（同端点去重）都是同一 `NodePool` 的路径；探索即真实分段下载（字节写入文件、零丢弃）。租借规则（`path_scheduler` 纯判据）：冷路径只在 ≥1MiB 的工作上分 1 条探索连接（coordinator 额外放出至多 1 个探索 worker）；失败只降排序不降速率估计（连续 3 次才踢）；单连接估计低于最优一半的路径出竞争集；竞争集内按 cap 分散、按 score 择优；开放式首段/plain GET 只留起飞路径。在途连接按完成时间判据（剩余/本连接速率 > 2×剩余/最优实证速率 + 1s；不设剩余字节下限——建连开销已在交接侧计入，下限会让极慢连接握住拆分最小片以下的尾部碎片拖尾；最优基准只认本任务窗口样本或完成租约，不认先验）在当前字节处抢占交接，整窗零字节即判停滞。代理路径错误归因路径本身（含 Range 失效/错位，翻译为 `CdnNodeFailed` 回收重派），validator 不一致（`VersionChanged` 或 206 路径的 `Other("validator mismatch")`）立即踢除该路径并记 NoSwitch；SYS 上的传输层失败在仍有其它路径存活时同样回收重派（每任务 8 次配额，耗尽后按原语义上抛）；交接窗口抑制 ramp 评估/收缩，用过备选路径的任务不学习域名连接上限/起步提示。跨任务先验 `route_health.rs`：每 (host, 路径) 的单连接速率对数折扣均值（半衰 12h，config `auto_route_health` v2，网络指纹 epoch——换网整表丢弃、**离线=unknown 不清表**），只用于起飞排序（代理先验须领先直连 1.5× 才以代理起飞）与备选路径初值，实测首窗即覆盖。云端只提供 CDN hints 排序先验，不参与路由决策、不新增遥测。failover **独立于通用重试配额**：手动代理、系统代理、本地直连在一个自动恢复周期内各尝试至多一次（先验更快的代理优先），三路均失败后只服从通用重试，杜绝 ping-pong。主导链路（窗口累计字节最多的路径）落 `tasks.auto_route` + `EngineEvent::TaskRouteChanged`（wire 标签不变：`direct[:sampled|:pinned|:failover]` / `proxy:{sampled,cached,failover}:{manual,system}`）。代理设置变更同点清 `route_health` + `domain_conn_caps` + failover 状态。
- `multi_nic.rs`（config `multi_nic_enabled`，默认关，仅桌面 target 编译 `if-addrs`）：多网卡聚合下载。manager 只折算任务级输入（走代理/Auto 多路径 → `blocked_by_proxy`）；coordinator 起飞后后台 `prepare_links`（解析目标 → UDP connect 探主链路 → 枚举网卡 → 纯判据 `plan_links`），首个完整 ramp 窗口经 `NodePool::add_links` 挂入 `RoutePath::Link(ifindex)` 冷槽位，首连接不等待。规划拒绝：fake-IP（198.18/15，TUN 代理）、局域网/CGNAT 目标、主链路是隧道/点对点（防绕开 VPN）；额外链路排除隧道/虚拟网卡、与已选链路同子网或同 /64（同一路由器 = 同一上游）、目标不具备的地址族、5 分钟内被踢过的网卡（进程内记忆，键含本地地址）。出口绑定：Linux/macOS `interface`（SO_BINDTODEVICE/IP_BOUND_IF），Windows `local_address`（强主机模型，单地址族），链路 client 的 DNS 只返回该链路地址族。调度：链路之间按独立容量注水（容量 = 单连接估计 × 上窗连接数，新租约给「容量/(在途+1)」最高者，≥5% 总容量的空闲链路保底 1 条），不走竞争集；链路最后一条连接只在停滞时抢占；链路路径不写 `route_health` 先验、计入 `alternates_used`（不学域名连接上限）。事件：`TaskCdnEvent` kind `links`/`links_off`（原因码 `multi_nic::off_reason`），节点标签 `NIC:<网卡名>`；单节点池无 sink 时事件暂存，由 `Multipath::poll_links` 每窗转发。`native/server` 不接线（废弃路径），生产宿主为 daemon；GPUI（`crates/settings` 下载页「连接与性能」，`SettingsRow::explain` 点击「?」出说明对话框）提供开关。
- `disk_space.rs`：跨平台余量查询（HLS remux/DASH mux ENOSPC 预检）；Unix 原始块数与块大小经 `Into<u64>` 无损扩展后饱和相乘，兼容不同 ABI 的字段宽度，避免同类型强转触发 Clippy。
- `proc.rs`：`no_console_window` —— **每个 console 子进程 spawn 都必须包裹**（ffmpeg/ffprobe/yt-dlp/tar/探版），防 Windows 闪窗。
- `data_dir.rs`：数据目录解析（Windows 便携 `<exe>/portable_data` via `portable` 标记 vs 安装 `%LOCALAPPDATA%`；Linux XDG；macOS App Support；Android files dir）+ 旧版迁移。已有数据迁移不随旧客户端工程退役删除。
- `logger.rs`：全局文件日志宏 `log_info!`/`log_error!`（`#[macro_export]`，`$crate` 前缀跨 crate 安全；每文件顶显式 `use`）。
- `model.rs`/`events.rs`/`selection.rs`：领域类型 / `EngineEvent`（`#[non_exhaustive]`）+ `EventSink` / `HostSelection`。
- `site_auth.rs`：站点 HTTP Basic 凭据存于设备本地 config `site_auth_credentials`。`site_key` 仅支持 HTTP/HTTPS，键为 `scheme://host[:port]`（host 小写、默认端口省略），HTTP 与 HTTPS 隔离；`parse_store` 读取旧 `host` / `host:port` 键时补 `https://`，同站点新旧键并存以新键优先。`download_manager::apply_site_auth` 把凭据注入任务 `extra_headers`，显式用户名/密码优先；无显式凭据且已有 Authorization 时不覆盖。
- `webhook.rs`：`WebhookDispatcher::emit` 同步筛选端点并入队，网络 IO 在端点 worker，失败不影响下载状态。每端点一个有界 FIFO（待投递上限 256），保留事件按 `emit` 入队顺序串行发送；满队列丢最旧待投递项并累计丢弃数，worker 优先写汇总失败投递记录（0 次尝试），不是每条事件派生一个等待任务。全局出站并发 4；dispatcher 释放时关闭队列、唤醒 worker 退出。端点表 `webhook.endpoints` 宽松解析：整份不是 JSON 数组才保留旧表，非对象元素逐项跳过，字段类型不符回退默认值（`lenient` 模块；GPUI `parse_endpoints` / Web `parseEndpoint` 同规则）。`task.created` / `task.started` 早于探测，`download_manager::webhook_task_event` 对空 `file_name` 用 `webhook_provisional_file_name`（http(s)/ftp 路径末段、磁力 `dn=`）补临时名，不带 query。
- `rss/`（`model`/`parser`/`filter`/`mod`）：订阅自动下载。`RssManager` 挂在 `DownloadManager.rss` 上；宿主只提供 60s 节拍（`tick_rss_sources()`）与回流 drain（`on_rss_event()`），抓取 off-actor，建任务仍收敛到 `create_task`。`subscription::SubscriptionProvider` 是 RSS 与插件订阅共用的 provider 分支：内置 `rss` provider 保持原流程；插件在 manifest 的 `subscriptions` 声明 `providerId` + `entry`，由 `PluginManager` 动态路由到 `globalThis.subscribe(ctx)`，返回规范化 `ParsedFeed`，并可通过 `flux.fetch` 请求平台接口。provider 不负责调度、退避、去重、过滤、落库或建任务。三层去重：guid → 单轮上限（超额留 `New` 下轮从旧到新续派）→ 智能剧集去重（识别失败即放行）。

  **两层失败退避不能混为一谈**：订阅 feed 抓取按 `effective_interval_secs` 使用「配置间隔 × `2^fail_count`」，封顶 6h 但绝不短于配置间隔，成功清零，**不自动停用订阅**。条目的种子抓取失败由 `record_torrent_failure` 保持 `New` 并写 `torrent_fetch_failed`，退避为 `600s × 2^(fetch_failures-1)`、封顶 24h；`rss_items.fetch_failures` / `retry_after` 落库，重启不丢退避。`db::rss_dispatchable_items` 仅选到期条目，未失败条目优先，避免失效种子长期占住 `max_per_fetch`；建任务成功或用户手动下载清零条目退避，手动下载不受其限制。

  **enclosure MIME 参与种子识别**：`parser::extract_enclosure` / `map_entry` 保存下载目标声明的 `enclosure_type`（RSS enclosure / media:content、Atom enclosure link；没有 enclosure 时取回退链接的 type），去参数、去空白并转小写。`application/x-bittorrent` 即使 URL 没有扩展名也走「先抓种子字节 → 建 BT 任务」，不只看 URL 形态；magnet 不走种子抓取，二段 resolver 条目的计划不沿用 enclosure MIME。

  `filter.rs` 是纯函数单测主战场，**Web TS 有一份逐条对齐的镜像**（`web/src/pages/rss/filter.ts`）供规则预览用——改任一侧必须同步两处。

  **「无人值守」是 RSS 的核心不变式**——订阅可能半夜抓到 5 集，任何需要用户点一下才能继续的东西都是 bug：① BT 条目建任务时 `NewTaskSpec.unattended_selection=true`，**启动前**落库全选与 `tasks.unattended=1`（HLS/变体选择也静默），否则条目已标「已下载」却仍在等选择；② 引擎内部创建任务也必须通过宿主发布完整任务事实，不能只推缺少队列等字段的进度；③ 手动「重新下载」对**任何**状态（含已下载）都放行。

  **删除任务不触发自动重下**：单删/批删在同一事务清空 RSS 条目的任务回链，将原 `Downloaded` 改为 `Ignored`（已读），其余处置状态保留；不能退回 `New`，否则下一轮自动抓取会重新派发。提交后仅向受影响订阅广播条目快照及源计数，手动下载仍对任何状态开放。

### 受管组件子系统（`components/`，`components` feature）
外部二进制 **ffmpeg + yt-dlp** 的按需安装器/解析器（**不打包**，合规边界——用户在设置「组件」页触发下载）。解析优先级 `manual`（config path）→ `managed`（`<data_dir>/bin/`）→ `system` PATH，wire 为 `ComponentSource{Manual,Managed,System,None}`。ffmpeg = BtbN 静态归档（取单文件，macOS 不支持受管）；yt-dlp = 单平台二进制（全平台）。版本列表经官方镜像 `fluxdown.zerx.dev/api/components` + GitHub 兜底。**被两处消费**：插件 `flux.ffmpeg`/`flux.ytdlp` 能力面 + 设置「组件」UI。

---

## 插件系统（`native/engine/src/plugin`，`plugins` feature）

**可选、可失败的下载任务中间层**，JS 编写（rquickjs 沙箱），声明式设置项（双端自动生成表单）。两个正交能力平面 + 门控工具面：

- **订阅平面**：manifest `subscriptions:[{providerId,entry,timeoutMs}]` 声明 provider，脚本导出 `globalThis.subscribe(ctx)`，通过 `flux.fetch` 自主完成平台请求/解析，返回 `{title,link,items:[{guid,title,link,enclosureUrl,resolverItem,enclosureLength,pubDate}]}`（`providerId` 不得占用内置 `rss`；单条非法跳过并记日志，全部非法才算失败；`ctx.providerConfig` 空值归一为 `{}`）；宿主自动把它挂到公共订阅调度，插件安装/启停后由动态路由读取最新快照。
- **Resolver 平面**：`resolve(url,ctx)→{url}|{manifest}|null`。协议判定**之前**惰性执行、**off-actor**（防冻结 actor），命中后 fail-closed（失败进 status=4，绝不把 HTML 当视频存）。惰性 = 每次 start/resume 重跑，天然防直链过期。支持两段式：初段返 manifest 清单 → 引擎裂变为任务组；二段（`ctx.resolverItem`）返直链。`multi:true` 触发新建对话框前置预解析（`begin_resolve_preview` 只读）。
- **通知平面**：onStart/onDone/onError/onMetaProbed，全 fire-and-forget（失败仅记日志/超时/`try_acquire`，绝不影响任务状态）；仅 onError 内可 `flux.task.requestRetry`。
- **门控工具面**（manifest `permissions` 声明才注入）：
  - `flux.auth`（`permissions:["auth"]`）：宿主持久化插件认证档案；`save/get/remove` 管理
    Cookie、Bearer、Basic 或自定义 Header。站点键**含 scheme**（`{scheme}://host[:port]`，
    `auth::site_key`）——同 host 的 http/https 是两个不同站点，https 登录态绝不隐式复用到
    http 请求（H-3，防明文 MITM 窃取）；裸 host 输入默认按 https 规范化，插件要登记明确
    允许明文的站点必须自己传 `"http://host"`。`flux.fetch` 按显式 `authRef`（空串视同未传，
    M-5）或插件+站点默认引用自动注入（仅对声明 `auth` 权限的插件推导默认引用），认证档案
    绑定插件和目标站点，过期时隐式引用跳过注入、显式 `authRef` 返回 `authentication_required`。
    档案按 `plugin.<identity>.auth.<site>` 独立配置键保存；旧版整表键 `plugin_auth_profiles`
    仅作迁移兼容，解析失败时记日志丢弃而非 fail-closed 卡死 save/remove/purge（M-1）。
    `flux.fetch` 返回的同名多值响应头以换行符（`\n`）拼接。auth 平面独立信号量
    （容量 2）+独立预算（`auth.timeoutMs` 可选，默认 30s，30s 硬顶），不与 resolve 共享、
    不计入 resolve 熔断（M-2）。凭据清理只挂用户主动 `uninstall`，安装失败的回滚
    `purge`（含其内部 `plugin.<id>.` 前缀清理）不删凭据（M-4）。
  - 登录入口（manifest `auth.entry`）：宿主 RPC `daemon.plugin.auth` 以
    `begin`/`poll`/`cancel`/`logout`/`status` 调用 `globalThis.authenticate(ctx)`；插件返回二维码挑战，
    成功后用 `flux.auth.save` 提交凭据。`logout` 在插件被禁用时也放行——不跑插件 JS，
    直接走宿主兜底删除凭据（M-3）；`poll` 回包省略 `challenge`/`challengeType` 时客户端
    保留上一帧。
  - `flux.ffmpeg`/`flux.ffprobe`（`permissions:["ffmpeg"]`）：近乎全量 argv，**封网 + 封越牢路径**（拒 URL scheme/绝对路径/`..`），牢笼 = 产物目录（仅 onDone 类有产物钩子可用），sema=2，300s/1800s 超时。
  - `flux.ytdlp`（`permissions:["ytdlp"]`）：允许网络抓站，但参数走 `bridge::ytdlp_option_kind` **白名单**，不是危险开关黑名单。只接受列出的完整长选项（带值选项可用 `--name=value`）和独立短选项，拒绝长选项缩写、合并 / 紧贴值的短选项、未知开关及参数数量不匹配；位置参数仅允许 HTTP/HTTPS URL。路径型参数（含 `-o` / `-P` 的类型前缀与 `--print-to-file` 的目标文件）必须是牢笼内相对路径，拒绝绝对路径、盘符、`..`、家目录 / 环境变量展开；`--js-runtimes` 只收裸运行时名，不收可执行路径。执行 / 配置 / 插件加载、浏览器 / netrc 凭据、任意下载器 / 后处理参数等能力不在允许表内。宿主注入 `--ignore-config`、可信 `--ffmpeg-location`（插件不能覆盖）和牢笼内 `--cache-dir`；resolve + 全 hook 可用。
  - `flux.fs`：per-plugin 通用临时文件读写（扁平安全名 + 单文件 8MB/总量 64MB/文件数 100 上限 + unix 0600），取代"每种输入给工具加类型化字段"的反模式。

**插件工作区**：`bridge::plugin_workspace` 统一生成 `<data_dir>/plugins-work/<encoded_id>/`，供 `flux.fs`、`flux.ytdlp` 的牢笼 / cwd 及卸载清理共用（yt-dlp 可另选牢笼内安全 subdir）。identity 按字节作无碰撞编码：`[A-Za-z0-9-]` 原样保留，其余字节一律写成 `_XX`（两位小写十六进制），包括 `_` → `_5f`、`@` → `_40`、`.` → `_2e`；不能用「特殊字符统一替换成下划线」的有碰撞目录名。

**模块**：`auth`（受控认证档案存储、站点绑定、过期判断和请求注入）、`manifest`（校验器 + subscription/provider 声明 + `permissions`⊆{auth,ffmpeg,ytdlp} + `auth.entry`）、`semver`、`runtime`（**无 rquickjs 类型**——可换 deno_core；含 Spec/Outcome 跨界结构 + `HostContext`）、`quickjs`（v1 唯一 impl，rquickjs 限在此文件；memory_limit + interrupt + timeout 三重兜底 + 连续 3 次熔断）、`bridge`（网络出口 SSRF 守卫 + flux.* 面）、`manager`（`RwLock<Arc<Vec>>` 整表原子替换，含 `authenticate` 登录入口 + 动态订阅路由）、`dependencies`（权限→组件依赖：ffmpeg→[ffmpeg]，ytdlp→[ytdlp,ffmpeg]，**提醒式非阻断**）、`install`（.fxplug zip：zip-slip + 压缩炸弹防护 + 单层剥壳）、`market`（去中心化市场：Git 版本化联邦索引 `zerx-lab/fluxdown-plugin-index`、内容寻址 `contentHash=sha256(zip)`、多源 failover、per-index sequence 防回滚；v1 无作者签名，schema 预留）。

**市场下载诊断与代理（2026-10-01 修复）**：`MarketClient` 复用 `downloader::build_client_builder`，索引与插件包继承应用的 None/System/Manual、代理认证与 `no_proxy` 规则；daemon 每次取最新下载配置，manager 使用当前已应用代理。每个源/镜像失败记录插件 ID、版本、域名/路径和完整根因链，URL 去除 userinfo/query/fragment。哈希不符或超限仍尝试后续镜像，最终保留内容验证错误而非统统折叠为网络失败；包下载的逐跳 HTTPS / 公网字面量 IP 守卫保持启用。官方索引发布规则在 `fluxdown-plugin-index`：分片/包 append-only、镜像固定到 commit、联网完整 GET/hash，历史 sequence 冲突只向高水位以上修复。

**off-actor 惰性 resolve 接线**：`create_task` 命中 `match_resolver` → 落 `tasks.resolver_plugin_id`（仅存 ID）+ 跳过 meta_prober。`do_start/resume_task` 体首守卫：resolver 非空且未解析 → 占位 active_tasks + off-actor spawn → return。worker 经 `resolve_rx` 回流，actor `select!` 分支 `on_resolve_ready`（复查生命周期 → 用解析后 url 重算五路协议分派）。**宿主 actor 必须接线 `resolve_rx` + `plugin_retry_rx`**。

**config 命名空间**：`plugin.<identity>.enabled`/`.disabled_reason`/`.setting.<key>`/`.kv.<key>`；`plugin.dev.<identity>`（devMode 路径）；`market.<index_id>.sequence`。identity 格式 `^[a-z0-9_-]+@[a-z0-9_-]+$`。
