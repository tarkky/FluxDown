---
description: FluxDown 多语言只以英文 + 简体中文为开发基线，其余语言由社区经 Weblate 完善，开发中不检查也不手改
condition:
  - 'assets/i18n/*.json'
  - 'web/src/i18n/*'
  - 'website-v2/src/i18n/messages/*'
  - 'fluxDown/utils/locales/*'
interruptMode: never
---

你正在改 FluxDown 的翻译文件。**语言完成度只按 en + zh 两条基线判定**，其余语言（ja 及未来任何新增语言）一律**不检查、不阻塞、不由 AI 补**。

## 三个 i18n 文件面各自的基线对（缺一不可，多的不管）

| 面 | 基线文件 | 备注 |
|---|---|---|
| GPUI（`crates/*`）+ Web SPA（`web/`）+ 原生 Android/iOS（`mobile/`） | `assets/i18n/en.json` + `assets/i18n/zh.json` | 四端共用同一 camelCase 键：GPUI `ctx.t("key")`；Web `useT()`；Android 构建期生成 `R.string`；iOS `L("key")`。删键前检查 `crates/`、`web/src/` 与 `mobile/` 的消费者 |
| 官网（主站 `website-v2/`） | `website-v2/src/i18n/messages/<ns>.ts` | 每个模块用 `defineMessages({ en, zh })` 同时声明两语，zh 结构必须与 en 完全一致，缺键/多键 `astro check` 直接报错；只有 en + zh，无社区语言。`website/` 是 /v1 旧站存档，文案不再改 |
| 浏览器扩展 | `fluxDown/utils/locales/zh-CN.ts` + `en.ts` | 反过来：`MessageKey` 类型由 **`zh-CN.ts`** 推导，`en.ts` 是 `Record<MessageKey, string>`，少一个键直接 tsc 报错 |

官网文档另算：`website-v2/src/content/docs/{en,zh}/`，改了 en 正文要在 `website-v2/` 下跑 `npm run docs:hash <zh 文件>` 刷新 zh 的 `sourceHash`；en-only 页会自动回退，不必为其造 zh 占位。

## 加/改一个 UI 字符串的完整动作

1. `en.json` 与 `zh.json`（官网为同一 `defineMessages` 的 `en`/`zh` 两支；扩展为 `zh-CN.ts` + `en.ts`）**同名键、同时补上、都非空**。
2. 各端通过已有 i18n 查表/生成入口消费共享键，参数化文案使用 `{name}` 占位；不要建立平行翻译表。
3. 删键同理两边一起删；改键名等于删旧加新。
4. **不要碰社区语言文件**（Weblate 回写的 `ja` 等）：这些由 Weblate 同步，手写/机翻会与 Weblate 回写冲突。缺键在运行时按键级回退英文，不是 bug，也不是"未完成"。
5. GPUI/Web SPA 自动发现共享语言文件；原生移动端语言打包范围以各自构建入口为准。官网 v2 的语言集固定在 `website-v2/src/i18n/config.ts::LANGS`（en + zh）。

## 自检（在 FluxDown 仓根执行）

```bash
node -e "const a=require('./assets/i18n/en.json'),b=require('./assets/i18n/zh.json');const A=Object.keys(a),B=Object.keys(b);console.log('zh 缺/空:',A.filter(k=>!(k in b)||!b[k]));console.log('zh 多余:',B.filter(k=>!(k in a)))"
```

上面的脚本只适用于 `assets/i18n`：官网由 `cd website-v2 && bun run check`（`astro check`）把关；扩展侧由 `tsc` 把关，跑 `cd fluxDown && npx tsc --noEmit` 或直接看 IDE 报错。**只对基线对跑这个检查，别拿社区语言文件跑。**
