# FluxDown Advisor 严重级分诊

规范细节以项目根 AGENTS.md 为准；本文件只定义违规 → 严重级映射。

## Blocker（打断）
- crate 边界破坏：engine 引入 UI/FFI/HTTP 宿主依赖，或 api 直接依赖 engine
- 非测试 Rust 代码出现 unwrap/expect/通配导入；任意项目 Rust 代码（含测试/脚本）出现未经准入或违反边界条件的 unsafe。符合 `rule://no-unsafe-in-rust` 的必要平台 FFI 不因关键字本身误报；UniFFI（`native/mobile`）宏展开的生成胶水属独立审计边界，手写源码仍须零 unsafe。安全接口接管任意裸指针、缺 SAFETY、跨线程释放受限资源、初始化失败仍继续均属 Blocker。禁止关闭 unsafe_op_in_unsafe_fn / undocumented_unsafe_blocks。
- 任意 Rust 代码出现 `todo!` / `unimplemented!`、静默丢弃 `Result` / `Future`，或通过忽错 lint 的 `allow` / 命令行降低 lint 等级绕过门禁（测试代码同样适用；见 `rule://no-ignored-errors-in-rust`）
- SQL 绕过 `db.rs::Db` 或非 `$N` 占位符
- 未经用户要求的 git commit/push/tag
- 在 `stable` 上直接提交功能；`git log stable --not main` 非空（stable 出现 main 没有的提交）
- 在 `main` 打稳定 tag，或在 `stable` 打 `-rc.N` tag

## Concern（转向提醒）
- 改 native/api 未重新生成 openapi.json；改移动 FFI 未更新 Kotlin / Swift 绑定
- 同步阻塞调用未包 `spawn_blocking`
- 翻译只改单语（en/zh 基线必须成对补齐；ja 等社区语言不检查，也不该由 AI 补）；UI 里硬编码中文文案未走 i18n 查表；新增依赖未见用户确认
- 绕过已有 trait/error/日志宏平行造轮子
- workspace clippy 未排除冻结的 `fluxdown_server` 或遗漏测试目标；正确门禁为 `cargo clippy --workspace --exclude fluxdown_server --all-targets -- -D warnings`（见 `.github/workflows/ci.yml`；`clippy.toml` 仅允许测试 unwrap/expect，其余 lint 一视同仁）

## Nit（旁注）
- 导入顺序、日志 `_tag` 缺失、公开 API 缺 doc comment、超长文件/函数未说明
