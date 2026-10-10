---
description: web 与桌面 App 的 UI 信息架构必须对齐（基准 = 桌面）
condition: web/src/pages/settings/**
interruptMode: never
---

你正在修改 Web 设置页。FluxDown 硬约束（AGENTS.md「镜像契约」）：**同一功能在 Web 与桌面的归属位置必须一致，基准 = GPUI 桌面**。

1. 新增/移动设置块前，先确认它在 `crates/settings/src/view.rs::build_pages` 与 `crates/settings/src/sections/` 的分类；Web 对齐 `web/src/pages/settings/categories.ts` 与 `sections/`。
2. 排序/分组也尽量跟随桌面同分类内的相对位置。
3. 文案键 en/zh 成对；跨页面共享逻辑放 `web/src/lib/` 复用单一实现，不复制。
4. 交付前自查：桌面里该功能在哪个菜单，web 就在哪个菜单。
