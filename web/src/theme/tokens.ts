// GPUI 主题 token → CSS 自定义属性。
//
// 解析完全复用网站端 GPUI 主题实现（`@gpui-theme`：registry.json + resolve.ts，与 Rust
// `crates/theme` 同算法），本文件只负责：
//   1. 运行时强调色覆盖（Rust `apply_accent` + 亮色主色对比度调整）——以「钉住字面量」方式
//      写进文档，等价于覆盖 extends 基底，且无需改动网站解析器；
//   2. ui_scale 缩放（`scales` token）；
//   3. 序列化为 `--fx-*` 变量。

import { TOKENS } from '@gpui-theme/registry'
import { setPath } from '@gpui-theme/document'
import { resolveTheme } from '@gpui-theme/resolve'
import type { Hsla, ThemeDocument, TokenValue } from '@gpui-theme/types'
import { colorHex, parseHexColor, relativeLuminance } from '@gpui-theme/value'
import { accentHex, builtinExtends } from './appearance'
import type { AppearancePreferences, ThemeModeName } from './appearance'

/** 与 Rust `MIN_PRIMARY_CONTRAST` 一致。 */
const MIN_PRIMARY_CONTRAST = 4.5

const NEAR_BLACK = '#09090bff'
const WHITE = '#ffffffff'

/** 强调色相对亮度 > 0.5 取近黑，否则取白。 */
function foregroundFor(accent: Hsla): string {
  return relativeLuminance(accent) > 0.5 ? NEAR_BLACK : WHITE
}

/** Rust `primary_with_contrast`：主色相对前景对比度不足时逐步压暗。 */
function primaryWithContrast(primary: Hsla, foreground: Hsla): Hsla {
  const fg = relativeLuminance(foreground)
  let current = primary
  for (let i = 0; i < 40; i += 1) {
    const bg = relativeLuminance(current)
    const [light, dark] = fg > bg ? [fg, bg] : [bg, fg]
    if ((light + 0.05) / (dark + 0.05) >= MIN_PRIMARY_CONTRAST || fg < bg) break
    current = { ...current, l: Math.max(current.l - 0.01, 0) }
  }
  return current
}

function baseDocument(extendsValue: string): ThemeDocument {
  return { extends: extendsValue, tokens: {}, dark: {}, light: {}, extra: {} }
}

export type FlatTokenValues = Readonly<Record<string, TokenValue>>

/** 解析指定主题槽位为最终 token（含强调色覆盖）。 */
export function resolveTokens(prefs: AppearancePreferences, mode: ThemeModeName): Map<string, TokenValue> {
  const extendsValue = builtinExtends(mode === 'dark' ? prefs.darkTheme : prefs.lightTheme)
  const document = baseDocument(extendsValue)
  const base = resolveTheme(document, mode).tokens
  const color = (path: string): Hsla | undefined => {
    const value = base.get(path)
    return value?.type === 'color' ? value.color : undefined
  }
  const accent = parseHexColor(`${accentHex(prefs)}ff`)
  const primary = color('colors.primary')
  const primaryForeground = color('colors.primaryForeground')
  const baseAccent = color('colors.accent')
  if (accent && primary && primaryForeground && baseAccent) {
    const autoForeground = colorHex(primaryForeground) === foregroundFor(primary)
    const nextForeground = autoForeground ? (parseHexColor(foregroundFor(accent)) ?? primaryForeground) : primaryForeground
    // 亮色模式：主色压暗到与前景达到 WCAG AA；accentForeground / ring 与主色同步。
    const effective = mode === 'light' ? primaryWithContrast(accent, nextForeground) : accent
    const layer = document[mode]
    setPath(layer, 'colors.primary', colorHex(effective))
    setPath(layer, 'colors.primaryForeground', colorHex(nextForeground))
    setPath(layer, 'colors.accent', colorHex({ ...accent, a: baseAccent.a }))
    setPath(layer, 'colors.accentForeground', colorHex(effective))
    setPath(layer, 'colors.ring', colorHex(effective))
  }
  return resolveTheme(document, mode).tokens
}

const MONO_STACK = 'ui-monospace, "SF Mono", Menlo, Consolas, "DejaVu Sans Mono", monospace'
const SANS_FALLBACK =
  '-apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Microsoft YaHei", "Noto Sans CJK SC", system-ui, sans-serif'

/** `colors.primaryForeground` → `--fx-colors-primary-foreground`。 */
export function varName(path: string): string {
  return `--fx-${path.replace(/\./g, '-').replace(/[A-Z]/g, (ch) => `-${ch.toLowerCase()}`)}`
}

function px(value: number): string {
  return `${Math.round(value * 100) / 100}px`
}

/** token 集合 → CSS 变量表（`--fx-*` → 值）。`scale` 为 ui_scale（1 = 100%）。 */
export function toCssVariables(tokens: ReadonlyMap<string, TokenValue>, scale: number): Record<string, string> {
  const vars: Record<string, string> = {}
  for (const spec of TOKENS) {
    const value = tokens.get(spec.path)
    if (!value) continue
    const factor = spec.scales ? scale : 1
    const name = varName(spec.path)
    switch (value.type) {
      case 'color':
        vars[name] = colorHex(value.color)
        break
      case 'number':
        vars[name] = spec.kind === 'fontWeight' || spec.kind === 'number' ? String(value.value) : px(value.value * factor)
        break
      case 'font': {
        const family = spec.path === 'typography.mono' ? (value.value === 'monospace' || value.value === '' ? MONO_STACK : `${value.value}, ${MONO_STACK}`) : `"${value.value}", ${SANS_FALLBACK}`
        vars[name] = family
        break
      }
      case 'shadow':
        vars[name] =
          value.layers.length === 0
            ? 'none'
            : value.layers
                .map(
                  (layer) =>
                    `${layer.inset ? 'inset ' : ''}${px(layer.x * factor)} ${px(layer.y * factor)} ${px(layer.blur * factor)} ${px(layer.spread * factor)} ${colorHex(layer.color)}`,
                )
                .join(', ')
        break
    }
  }
  return vars
}

/** 一步到位：偏好 + 模式 → CSS 变量。 */
export function themeVariables(prefs: AppearancePreferences, mode: ThemeModeName): Record<string, string> {
  return toCssVariables(resolveTokens(prefs, mode), prefs.uiScalePercent / 100)
}
