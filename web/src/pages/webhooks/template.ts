// 端点对话框的纯逻辑：占位符预览、URL 校验、签名密钥生成。
// 语义与 `crates/settings/src/sections/webhook_dialog.rs`（及引擎 `render_template`）逐条对齐。

import type { WebhookPresetDto } from '../../lib/rpc'

export const PRESET_CUSTOM = 'custom'
/** 默认订阅任务完成与失败事件。 */
export const DEFAULT_EVENTS: readonly string[] = ['task.completed', 'task.failed']

/** 预览用样例变量——与引擎 `WebhookEvent::sample()` 对齐，仅用于预览。 */
const SAMPLE_VARS: Readonly<Record<string, string>> = {
  '{event}': 'task.completed',
  '{event.title}': 'Download completed',
  '{event.summary}': 'ubuntu-24.04.2-desktop-amd64.iso · 6.0 GB',
  '{timestamp}': '2026-07-17T12:34:56Z',
  '{instance.app}': 'fluxdown',
  '{instance.version}': '0.1.44',
  '{instance.host}': 'DESKTOP',
  '{task.id}': '00000000-0000-4000-8000-000000000000',
  '{task.fileName}': 'ubuntu-24.04.2-desktop-amd64.iso',
  '{task.url}': 'https://releases.ubuntu.com/24.04/ubuntu.iso',
  '{task.saveDir}': '/downloads',
  '{task.totalBytes}': '6442450944',
  '{task.totalBytesHuman}': '6.0 GB',
  '{task.status}': '3',
  '{task.errorMessage}': '',
  '{queue.id}': 'main',
  '{queue.name}': 'Main',
  '{ntfy.topic}': 'my-topic',
}

const UTF8 = new TextEncoder()

/** `application/x-www-form-urlencoded` 组件编码。 */
export function formEncode(value: string): string {
  let out = ''
  for (const byte of UTF8.encode(value)) {
    const ch = String.fromCharCode(byte)
    if (/[A-Za-z0-9\-_.!~*'()]/.test(ch)) out += ch
    else if (byte === 0x20) out += '+'
    else out += `%${byte.toString(16).toUpperCase().padStart(2, '0')}`
  }
  return out
}

/** JSON 字符串上下文转义：转义后剥掉外层引号。 */
function jsonEscape(value: string): string {
  return JSON.stringify(value).slice(1, -1)
}

/**
 * 占位符替换——与引擎 `render_template` 同规则：占位符是不含嵌套 `{` 的 `{…}` 段，
 * 未知段原样保留，因此 JSON 字面量不会被破坏。
 */
export function renderPreview(template: string, formEscape: boolean): string {
  let out = ''
  let i = 0
  while (i < template.length) {
    if (template[i] !== '{') {
      const start = i
      while (i < template.length && template[i] !== '{') i += 1
      out += template.slice(start, i)
      continue
    }
    let j = i + 1
    while (j < template.length && template[j] !== '}' && template[j] !== '{') j += 1
    if (j >= template.length || template[j] === '{') {
      out += '{'
      i += 1
      continue
    }
    const key = template.slice(i, j + 1)
    const value = SAMPLE_VARS[key]
    if (value === undefined) out += key
    else out += formEscape ? formEncode(value) : jsonEscape(value)
    i = j + 1
  }
  return out
}

/** 无模板（custom 预设）时的信封原文。 */
function envelopePreview(): string {
  return JSON.stringify(
    {
      schemaVersion: 1,
      event: SAMPLE_VARS['{event}'],
      deliveryId: '5f2a91c7-8b3e-4d10-a6f4-c2d90b7e13aa',
      timestamp: SAMPLE_VARS['{timestamp}'],
      instance: {
        app: SAMPLE_VARS['{instance.app}'],
        version: SAMPLE_VARS['{instance.version}'],
        host: SAMPLE_VARS['{instance.host}'],
      },
      queue: { id: SAMPLE_VARS['{queue.id}'], name: SAMPLE_VARS['{queue.name}'] },
      task: {
        id: SAMPLE_VARS['{task.id}'],
        fileName: SAMPLE_VARS['{task.fileName}'],
        url: SAMPLE_VARS['{task.url}'],
        saveDir: SAMPLE_VARS['{task.saveDir}'],
        totalBytes: 6442450944,
        status: 3,
        errorMessage: '',
      },
    },
    null,
    2,
  )
}

/** URL 内联校验错误的文案键；`null` = 通过。空 URL 由保存按钮禁用兜底。 */
export function urlErrorKey(rawUrl: string, allowHttp: boolean): 'webhookUrlInvalid' | 'webhookUrlWarnHttp' | null {
  const raw = rawUrl.trim()
  if (raw === '') return null
  const sep = raw.indexOf('://')
  if (sep < 0) return 'webhookUrlInvalid'
  const scheme = raw.slice(0, sep).toLowerCase()
  const host = raw.slice(sep + 3).split(/[/?#]/, 1)[0]
  if (!host) return 'webhookUrlInvalid'
  if (scheme === 'https') return null
  if (scheme === 'http') return allowHttp ? null : 'webhookUrlWarnHttp'
  return 'webhookUrlInvalid'
}

/** URL 字段提示文案键：ntfy 需带主题路径，钉钉需配「自定义关键词」，其余通用。 */
export function urlHintKey(preset: string): 'webhookUrlHintNtfy' | 'webhookUrlHintDingtalk' | 'webhookUrlHint' {
  if (preset === 'ntfy') return 'webhookUrlHintNtfy'
  if (preset === 'dingtalk') return 'webhookUrlHintDingtalk'
  return 'webhookUrlHint'
}

/** HMAC 密钥起点（`whsec_` + 32 位十六进制）。 */
export function generateSecret(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(16))
  return `whsec_${Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('')}`
}

/** 请求体预览：自带模板优先，其次预设默认模板，都没有则是信封原文。 */
export function previewBody(ownTemplate: string, preset: WebhookPresetDto | undefined): string {
  const template = ownTemplate.trim() === '' ? (preset?.defaultTemplate ?? '') : ownTemplate
  if (template === '') return envelopePreview()
  const isForm = preset?.contentType.startsWith('application/x-www-form') ?? false
  const rendered = renderPreview(template, isForm)
  if (isForm) return rendered
  // 渲染结果若是合法 JSON 就美化一下，方便扫读；不是就原样显示。
  try {
    return JSON.stringify(JSON.parse(rendered), null, 2)
  } catch {
    return rendered
  }
}

/** 右栏「实时请求预览」全文；`firstEvent` = 按规范顺序第一个已订阅事件。 */
export function previewRequest(input: {
  url: string
  firstEvent: string | undefined
  signEnabled: boolean
  template: string
  preset: WebhookPresetDto | undefined
}): string {
  const { preset } = input
  const url = input.url.trim() === '' ? (preset?.urlPlaceholder ?? '') : input.url.trim()
  const lines = [
    `POST ${url}`,
    `Content-Type: ${preset?.contentType ?? 'application/json'}`,
    `X-FluxDown-Event: ${input.firstEvent ?? 'task.completed'}`,
    'X-FluxDown-Delivery: 5f2a91c7-…',
  ]
  if (input.signEnabled) lines.push('X-FluxDown-Signature: t=1789647128,v1=9c41f2…')
  lines.push('─'.repeat(28))
  lines.push(...previewBody(input.template, preset).split('\n'))
  return lines.join('\n')
}
