// 「新建下载」表单的纯模型：链接解析、代理 / UA / 线程预设与请求构建。
// 与 crates/downloads/src/model/new_download.rs 的桌面端规则保持同步。

import type { CreateTaskRequest } from '../../../lib/rpc'
import { MAX_TASK_SEGMENTS } from '../../../lib/threadsRisk'

/** 一条解析出的下载条目（aria2 风格：URL + 可选 `out=` / `checksum=` 选项行）。 */
export interface UrlEntry {
  url: string
  fileName: string
  checksum: string
}

const entry = (url: string): UrlEntry => ({ url, fileName: '', checksum: '' })

/** ASCII 小写（保持字符索引与原串一致）。 */
function asciiLower(text: string): string {
  return text.replace(/[A-Z]/g, (c) => c.toLowerCase())
}

/** 外部捕获 → 条目：捕获文件名与链接路径末段（百分号解码后）逐字相同时省略 `out=`。 */
export function captureEntry(url: string, fileName: string): UrlEntry {
  const name = fileName.trim()
  const segment = urlFileSegment(url)
  const redundant = segment !== null && (segment === name || safeDecode(segment) === name)
  return { url, fileName: redundant ? '' : name, checksum: '' }
}

/** UTF-8 百分号解码；非法序列返回 null（调用方保留原文）。 */
function safeDecode(segment: string): string | null {
  try {
    return decodeURIComponent(segment)
  } catch {
    return null
  }
}

/** `scheme://host/a/b.zip?x#y` → `b.zip`；无路径或以 `/` 结尾时为 null。 */
function urlFileSegment(url: string): string | null {
  const schemeAt = url.indexOf('://')
  if (schemeAt < 0) return null
  let rest = url.slice(schemeAt + 3)
  const cut = rest.search(/[?#]/)
  if (cut >= 0) rest = rest.slice(0, cut)
  const slash = rest.indexOf('/')
  if (slash < 0) return null
  const segment = rest.slice(slash).split('/').pop() ?? ''
  return segment === '' ? null : segment
}

const LEADING_URL = /^(?:https?|ftp):\/\/\S+/i
const INLINE_URL = /(?:https?|ftp):\/\/\S+/i

function trimUrlTail(url: string): string {
  return url.replace(/[.,;:!?()[\]{}]+$/, '')
}

/**
 * 解析多行文本为下载条目。
 * - 原始行以空格 / Tab 开头 = 选项行，附着到上一条（`out=` / `checksum=`）；
 * - `#` 开头为注释，空行跳过；
 * - 含 `magnet:?` / `ed2k://` 时从该位置截取到行尾；
 * - 其余行：`loose` 时取行内首个 http(s)/ftp 链接并去掉尾部标点（TXT 导入），否则要求链接位于行首。
 */
export function parseEntries(text: string, loose: boolean): UrlEntry[] {
  const entries: UrlEntry[] = []
  let current: UrlEntry | null = null
  for (const line of text.split('\n')) {
    if (line.startsWith(' ') || line.startsWith('\t')) {
      if (!current) continue
      const trimmed = line.trim()
      if (trimmed.startsWith('out=')) current.fileName = trimmed.slice(4)
      else if (trimmed.startsWith('checksum=')) current.checksum = trimmed.slice(9)
      continue
    }
    const trimmed = line.trim()
    if (trimmed === '' || trimmed.startsWith('#')) continue
    if (current) entries.push(current)
    current = null
    const lower = asciiLower(trimmed)
    const magnet = lower.indexOf('magnet:?')
    const ed2k = lower.indexOf('ed2k://')
    if (magnet >= 0) {
      current = entry(trimmed.slice(magnet))
    } else if (ed2k >= 0) {
      current = entry(trimmed.slice(ed2k))
    } else if (loose) {
      const found = INLINE_URL.exec(trimmed)
      if (found) {
        const url = trimUrlTail(found[0])
        if (url !== '') current = entry(url)
      }
    } else {
      const found = LEADING_URL.exec(trimmed)
      if (found) current = entry(found[0])
    }
  }
  if (current) entries.push(current)
  return entries
}

/** 条目 → aria2 风格文本（含 `out=` / `checksum=` 选项行）。 */
export function entryToText(item: UrlEntry): string {
  let text = item.url
  if (item.fileName !== '') text += `\n  out=${item.fileName}`
  if (item.checksum !== '') text += `\n  checksum=${item.checksum}`
  return text
}

/** 把导入条目追加到现有文本：按 URL 去重（保留已有条目），返回新的文本框内容。 */
export function mergeImported(existingText: string, imported: UrlEntry[]): string {
  const merged = parseEntries(existingText, false)
  const seen = new Set(merged.map((item) => item.url))
  for (const item of imported) {
    if (!seen.has(item.url)) {
      seen.add(item.url)
      merged.push(item)
    }
  }
  return merged.map(entryToText).join('\n')
}

/** 把条目追加到现有文本末尾：原文逐字保留（含注释 / 尚未输完的行），已存在的 URL 跳过。 */
export function appendEntries(existingText: string, entries: Iterable<UrlEntry>): string {
  const seen = new Set(parseEntries(existingText, false).map((item) => item.url))
  let text = existingText.replace(/\s+$/, '')
  for (const item of entries) {
    if (seen.has(item.url)) continue
    seen.add(item.url)
    if (text !== '') text += '\n'
    text += entryToText(item)
  }
  return text
}

export type ProxyChoice = 'follow' | 'direct' | 'system' | 'globalManual' | 'custom'

/** 下拉顺序。 */
export const PROXY_CHOICES: readonly ProxyChoice[] = ['follow', 'direct', 'system', 'globalManual', 'custom']

const PROXY_DIRECT_SENTINEL = 'direct://'
const PROXY_SYSTEM_SENTINEL = 'system://'

/** 生成 `proxyUrl` wire 值；`manualUrl` 为全局手动代理 URL（空 = 未配置）。 */
export function proxyWire(choice: ProxyChoice, manualUrl: string, customUrl: string): string {
  switch (choice) {
    case 'follow':
      return ''
    case 'direct':
      return PROXY_DIRECT_SENTINEL
    case 'system':
      return PROXY_SYSTEM_SENTINEL
    case 'globalManual':
      return manualUrl
    case 'custom':
      return customUrl.trim()
  }
}

/** 从 daemon 配置拼出全局手动代理 URL：`type://[user[:pass]@]host:port`；未配置返回空串。 */
export function manualProxyUrl(config: Readonly<Record<string, string>>): string {
  const value = (key: string) => (config[key] ?? '').trim()
  const host = value('proxy_host')
  const port = Number.parseInt(value('proxy_port'), 10)
  if (host === '' || !Number.isFinite(port) || port <= 0) return ''
  const type = value('proxy_type') || 'http'
  const user = config.proxy_username ?? ''
  const password = config.proxy_password ?? ''
  let url = `${type}://`
  if (user !== '') {
    url += encodeURIComponent(user)
    if (password !== '') url += `:${encodeURIComponent(password)}`
    url += '@'
  }
  return `${url}${host}:${port}`
}

/** 预设 UA（key → UA 字符串）。与桌面端 new_download.rs 保持同步。 */
export const UA_PRESETS: readonly { key: string; value: string }[] = [
  {
    key: 'chrome',
    value: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
  },
  { key: 'firefox', value: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:147.0) Gecko/20100101 Firefox/147.0' },
  {
    key: 'edge',
    value:
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36 Edg/145.0.3800.70',
  },
  {
    key: 'safari',
    value: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.3.1 Safari/605.1.15',
  },
]

export const UA_PRESET_DEFAULT = 'default'
export const UA_PRESET_CUSTOM = 'custom'

/** UA 预设下拉顺序：继承全局 → 各浏览器 → 自定义。 */
export const UA_PRESET_KEYS: readonly string[] = [UA_PRESET_DEFAULT, ...UA_PRESETS.map((p) => p.key), UA_PRESET_CUSTOM]

/** UA 字符串 → 预设键；空 = default，未命中 = custom。 */
export function detectUaPreset(ua: string): string {
  if (ua === '') return UA_PRESET_DEFAULT
  return UA_PRESETS.find((p) => p.value === ua)?.key ?? UA_PRESET_CUSTOM
}

/** 预设键 → UA 字符串（default → 空）。 */
export function uaPresetValue(key: string): string {
  return UA_PRESETS.find((p) => p.key === key)?.value ?? ''
}

/** 线程数下拉预设（「自动」与「自定义」之间的固定档位）。 */
export const THREAD_PRESETS: readonly number[] = [4, 8, 16, 32, 64]
/** 自定义线程数上限（= 引擎单任务连接上限）。 */
export const MAX_THREADS = MAX_TASK_SEGMENTS

/** 线程选择：`auto` / 预设档位数字字符串 / `custom`。 */
export type ThreadChoice = 'auto' | 'custom' | `${number}`

/** 由默认分段数推导初始选择：0 = 自动，预设内 = 预设，其余 = 自定义。 */
export function threadChoiceFromSegments(segments: number): ThreadChoice {
  if (segments <= 0) return 'auto'
  if (THREAD_PRESETS.includes(segments)) return `${segments}`
  return 'custom'
}

/** 自定义输入 → segments：非法 / 小于 1 = 0（自动），上限 MAX_THREADS。 */
export function customSegments(text: string): number {
  const value = Number.parseInt(text.trim(), 10)
  return Number.isFinite(value) && value >= 1 ? Math.min(value, MAX_THREADS) : 0
}

/** 哈希校验算法（wire 字面量，无本地化），默认 sha-256。 */
export const HASH_ALGORITHMS: readonly string[] = ['md5', 'sha-1', 'sha-256', 'sha-512']
export const DEFAULT_HASH_ALGORITHM = 'sha-256'

/** `algo=hexhash`；哈希值为空时返回空串（跳过校验）。 */
export function checksumSpec(algorithm: string, hash: string): string {
  const trimmed = hash.trim()
  return trimmed === '' ? '' : `${algorithm}=${trimmed}`
}

/** 整批共享的表单选项。`rename` / HTTP 认证 / 高级面板校验值只作用于单条提交。 */
export interface DraftOptions {
  saveDir: string
  segments: number
  cookies: string
  proxyUrl: string
  userAgent: string
  queueId: string
  checksum: string
  ignoreTlsErrors: boolean
  headers: Record<string, string>
  startPaused: boolean
  rename: string
  httpUser: string
  httpPassword: string
  saveSiteAuth: boolean
}

/**
 * 每条条目一个 CreateTaskRequest，共享字段来自 DraftOptions。
 * 单条时重命名优先于 `out=`、高级面板校验值优先于 `checksum=`、附带 HTTP 认证；
 * 多条时仅使用条目自带的文件名 / 校验值。
 */
export function buildRequests(entries: readonly UrlEntry[], options: DraftOptions): CreateTaskRequest[] {
  const single = entries.length === 1
  const hasHeaders = Object.keys(options.headers).length > 0
  return entries.map((item) => ({
    url: item.url,
    fileName: single && options.rename !== '' ? options.rename : item.fileName,
    saveDir: options.saveDir,
    segments: options.segments,
    cookies: options.cookies,
    referrer: '',
    proxyUrl: options.proxyUrl,
    userAgent: options.userAgent,
    queueId: options.queueId,
    checksum: single && options.checksum !== '' ? options.checksum : item.checksum,
    ignoreTlsErrors: options.ignoreTlsErrors,
    headers: hasHeaders ? options.headers : undefined,
    startPaused: options.startPaused,
    httpUser: single ? options.httpUser : '',
    httpPassword: single ? options.httpPassword : '',
    saveSiteAuth: single && options.saveSiteAuth,
  }))
}

/** 是否单条 http(s) 链接（清单预解析 / 站点凭据回填的前置条件）。 */
export function isHttpUrl(url: string): boolean {
  const lower = asciiLower(url)
  return lower.startsWith('http://') || lower.startsWith('https://')
}

export function isMagnetUrl(url: string): boolean {
  return asciiLower(url.slice(0, 7)) === 'magnet:'
}
