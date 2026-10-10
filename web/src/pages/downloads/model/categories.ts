// 分类匹配索引（移植 crates/downloads/src/model/categories.rs）：
// 由偏好 `custom_categories` 构建，按扩展名 / 正则把任务归类。

import type { CustomCategoryDto } from '../../../lib/rpc'
import { compileEngineRegex } from '../../rss/filter'
import { extensionOf } from './task'
import type { DownloadTaskView } from './task'

type Matcher =
  | { type: 'all' }
  | { type: 'other' }
  | { type: 'extensions'; set: ReadonlySet<string> }
  | { type: 'regex'; regex: RegExp | null }

export interface CategoryRule {
  dto: CustomCategoryDto
  matcher: Matcher
}

export const BUILTIN_ALL_ID = 'builtin_all'
export const BUILTIN_OTHER_ID = 'builtin_other'

/** 内置分类基线：固定显示顺序与默认扩展名。 */
export function builtinDefaults(): CustomCategoryDto[] {
  const make = (id: string, icon: string, extensions: string[], position: number): CustomCategoryDto => ({
    id: `builtin_${id}`,
    name: '',
    icon,
    matchMode: 'extension',
    extensions,
    regexPattern: '',
    position,
    visible: true,
    isBuiltin: true,
    builtinType: id,
    saveDir: '',
  })
  return [
    make('all', 'folders', [], 0),
    make('video', 'film', ['mp4', 'mkv', 'avi', 'mov', 'wmv', 'flv', 'webm', 'm4v', 'ts', 'm3u8'], 1),
    make('audio', 'music', ['mp3', 'flac', 'wav', 'aac', 'ogg', 'm4a', 'wma', 'opus'], 2),
    make('document', 'fileText', ['pdf', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx', 'txt', 'epub', 'md'], 3),
    make('image', 'image', ['jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp', 'svg', 'heic', 'avif'], 4),
    make('program', 'cpu', ['exe', 'msi', 'dmg', 'pkg', 'deb', 'rpm', 'apk', 'appimage'], 5),
    make('archive', 'archive', ['zip', 'rar', '7z', 'tar', 'gz', 'bz2', 'xz', 'iso'], 6),
    make('other', 'file', [], 7),
  ]
}

function coerceDto(raw: unknown): CustomCategoryDto | null {
  if (typeof raw !== 'object' || raw === null) return null
  const item = raw as Record<string, unknown>
  if (typeof item.id !== 'string') return null
  return {
    id: item.id,
    name: typeof item.name === 'string' ? item.name : '',
    icon: typeof item.icon === 'string' ? item.icon : 'file',
    matchMode: typeof item.matchMode === 'string' ? item.matchMode : 'extension',
    extensions: Array.isArray(item.extensions) ? item.extensions.filter((ext): ext is string => typeof ext === 'string') : [],
    regexPattern: typeof item.regexPattern === 'string' ? item.regexPattern : '',
    position: typeof item.position === 'number' ? item.position : 0,
    visible: item.visible !== false,
    isBuiltin: item.isBuiltin === true,
    builtinType: typeof item.builtinType === 'string' ? item.builtinType : null,
    saveDir: typeof item.saveDir === 'string' ? item.saveDir : '',
  }
}

/** 从偏好值解析分类列表（JSON 字符串或数组）；空 / 损坏时回退内置基线，按 position 排序。 */
export function categoriesFromPreference(value: unknown): CustomCategoryDto[] {
  let raw: unknown = value
  if (typeof raw === 'string') {
    try {
      raw = JSON.parse(raw)
    } catch {
      raw = null
    }
  }
  const parsed = Array.isArray(raw) ? raw.map(coerceDto).filter((dto): dto is CustomCategoryDto => dto !== null) : []
  const list = parsed.length > 0 ? parsed : builtinDefaults()
  return list.slice().sort((a, b) => a.position - b.position)
}

export class CategoryIndex {
  readonly rules: readonly CategoryRule[]

  constructor(dtos: readonly CustomCategoryDto[]) {
    this.rules = dtos.map((dto): CategoryRule => {
      if (dto.builtinType === 'all') return { dto, matcher: { type: 'all' } }
      if (dto.builtinType === 'other') return { dto, matcher: { type: 'other' } }
      if (dto.matchMode === 'regex') {
        // 与 agent（Rust regex）同语义；Rust 不支持的写法（环视、反向引用）编译失败 → 不匹配。
        const regex = dto.regexPattern === '' ? null : compileEngineRegex(dto.regexPattern)
        return { dto, matcher: { type: 'regex', regex } }
      }
      return {
        dto,
        matcher: {
          type: 'extensions',
          set: new Set(dto.extensions.map((ext) => ext.replace(/^\.+/, '').toLowerCase())),
        },
      }
    })
  }

  /** 可见规则（`visible=false` 的隐藏）。 */
  visible(): CategoryRule[] {
    return this.rules.filter((rule) => rule.dto.visible)
  }

  private static ruleMatches(rule: CategoryRule, view: DownloadTaskView): boolean {
    const matcher = rule.matcher
    switch (matcher.type) {
      case 'all':
        return true
      case 'other':
        return false
      case 'extensions': {
        const ext = extensionOf(view.name)
        return ext !== null && matcher.set.has(ext)
      }
      case 'regex':
        return matcher.regex !== null && matcher.regex.test(view.name)
    }
  }

  /** 任务是否属于分类 `id`；未知 id 视为不匹配。 */
  matches(id: string, view: DownloadTaskView): boolean {
    const rule = this.rules.find((item) => item.dto.id === id)
    if (!rule) return false
    if (rule.matcher.type === 'other') {
      return !this.rules
        .filter((other) => other.matcher.type !== 'all' && other.matcher.type !== 'other')
        .some((other) => CategoryIndex.ruleMatches(other, view))
    }
    return CategoryIndex.ruleMatches(rule, view)
  }

  /** 任务命中的首个（非 all/other）分类 id；无 → `builtin_other`。 */
  categoryOf(view: DownloadTaskView): string {
    const hit = this.rules.find(
      (rule) => rule.matcher.type !== 'all' && rule.matcher.type !== 'other' && CategoryIndex.ruleMatches(rule, view),
    )
    return hit ? hit.dto.id : BUILTIN_OTHER_ID
  }
}
