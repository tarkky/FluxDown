---
description: 拦截项目禁止命令与 Rust 忽错门禁绕过
scope: tool:bash
condition:
  - 'cargo\s+test\s+--workspace'
  - 'git\s+push\s+\S*\s*v\d'
  - 'git\s+push\s+.*--tags'
  - '(?:-A\s*|--allow(?:=|\s+))(?:clippy::)?(?:todo|unimplemented|let_underscore_must_use|let_underscore_future|unused_result_ok|unused_must_use|unsafe_op_in_unsafe_fn|undocumented_unsafe_blocks)\b'
  - '--cap-lints(?:=|\s+)(?:allow|warn)\b'
repeatMode: after-gap
repeatGap: 2
---

该命令属于 FluxDown 项目禁令：

- `cargo test --workspace`：禁止全量跑。用 `cargo nextest run -p <crate> <filter>` 或 `cargo test -p <crate> -- <filter>` 精准跑。静态门禁不同：使用 `cargo clippy --workspace --exclude fluxdown_server --all-targets -- -D warnings`，含测试目标，冻结的 `native/server` 不构建。
- 推送 `v*` tag 会触发 GitHub Actions 全平台发布流水线，必须由用户明确要求后执行。
- 用 `-A` / `--allow` 关闭 todo、unimplemented、忽错或 unsafe 边界 lint（unsafe_op_in_unsafe_fn / undocumented_unsafe_blocks），或用 `--cap-lints allow/warn` 降低门禁：修复实现及安全论证，不用命令行消音。

请换用合规命令后继续。
