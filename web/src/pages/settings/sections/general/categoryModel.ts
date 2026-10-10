// 自定义分类模型（镜像 crates/settings/src/sections/categories.rs + category_dialog.rs）。
// 目录名净化 / 拼接复用 `lib/category-dir`（与 GPUI 分类目录保持逐字一致），
// 列表读取与内置基线复用下载页的 `model/categories`。

import type { TFunction } from '../../../../i18n'
import { categoryDirUnder } from '../../../../lib/category-dir'
import type { CustomCategoryDto, JsonValue } from '../../../../lib/rpc'
import { CUSTOM_CATEGORIES_PREF_KEY } from '../../../../lib/rpc'
import { builtinDefaults, categoriesFromPreference } from '../../../downloads/model/categories'
import { setPref } from '../../kit'

export { CUSTOM_CATEGORIES_PREF_KEY, builtinDefaults, categoriesFromPreference }

/** 内置分类显示名的文案键。 */
export function builtinLabelKey(builtinType: string): string {
  switch (builtinType) {
    case 'all':
      return 'categoryAll'
    case 'video':
      return 'categoryVideo'
    case 'audio':
      return 'categoryAudio'
    case 'document':
      return 'categoryDocument'
    case 'image':
      return 'categoryImage'
    case 'program':
      return 'categoryProgram'
    case 'archive':
      return 'categoryArchive'
    default:
      return 'categoryOther'
  }
}

export function displayName(t: TFunction, entry: CustomCategoryDto): string {
  return entry.isBuiltin && entry.builtinType ? t(builtinLabelKey(entry.builtinType)) : entry.name
}

/** 内置分类的目录名基线（英文，与 assets/i18n/en.json 的 `categoryXxx` 逐字一致）。 */
function builtinDirLabel(kind: string): string {
  switch (kind) {
    case 'video':
      return 'Video'
    case 'audio':
      return 'Audio'
    case 'document':
      return 'Document'
    case 'image':
      return 'Image'
    case 'program':
      return 'Programs'
    case 'archive':
      return 'Archive'
    default:
      return 'Other'
  }
}

/** 写回分类列表：按数组下标重写 position（同 `write_categories`）。 */
export function writeCategories(list: readonly CustomCategoryDto[]): void {
  const reindexed = list.map((entry, index) => ({ ...entry, position: index }))
  setPref(CUSTOM_CATEGORIES_PREF_KEY, reindexed as unknown as JsonValue)
}

/** 把 `from` 移到 `to` 当前所在位置；无变化返回 null。 */
export function reorderCategories(list: readonly CustomCategoryDto[], from: string, to: string): CustomCategoryDto[] | null {
  const fromIndex = list.findIndex((entry) => entry.id === from)
  const toIndex = list.findIndex((entry) => entry.id === to)
  if (fromIndex < 0 || toIndex < 0 || fromIndex === toIndex) return null
  const next = list.slice()
  const [moved] = next.splice(fromIndex, 1)
  if (!moved) return null
  next.splice(toIndex, 0, moved)
  return next
}

/** 「一键分类目录」：每个分类（「全部文件」除外）指向默认下载目录下的同名子目录。 */
export function applyAutoDirs(list: readonly CustomCategoryDto[], baseDir: string): CustomCategoryDto[] | null {
  if (baseDir.trim() === '') return null
  return list.map((entry) => {
    if (entry.builtinType === 'all') return entry
    const label = entry.isBuiltin && entry.builtinType ? builtinDirLabel(entry.builtinType) : entry.name
    return { ...entry, saveDir: categoryDirUnder(baseDir, label) }
  })
}

/** 扩展名文本 → 规范化列表：逗号 / 中文逗号 / 空白分隔，去点、转小写、去空。 */
export function parseExtensions(text: string): string[] {
  return text
    .split(/[,\uff0c\s]+/)
    .map((part) => part.trim().replaceAll('.', '').toLowerCase())
    .filter((part) => part !== '')
}

/** 无引擎依赖的最小正则健全性检查：圆括号 / 方括号配对与尾部悬挂转义（同 `regex_looks_valid`）。 */
export function regexLooksValid(pattern: string): boolean {
  let depth = 0
  let inClass = false
  for (let index = 0; index < pattern.length; index += 1) {
    const ch = pattern[index]
    if (ch === '\\') {
      if (index + 1 >= pattern.length) return false
      index += 1
    } else if (ch === '[' && !inClass) {
      inClass = true
    } else if (ch === ']' && inClass) {
      inClass = false
    } else if (ch === '(' && !inClass) {
      depth += 1
    } else if (ch === ')' && !inClass) {
      depth -= 1
      if (depth < 0) return false
    }
  }
  return depth === 0 && !inClass
}
