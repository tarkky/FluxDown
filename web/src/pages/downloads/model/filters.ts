// 侧栏选中项与状态筛选（移植 crates/downloads/src/model/mod.rs）。

import type { CategoryIndex } from './categories'
import type { DownloadTaskView, TaskState } from './task'

export type DownloadStatusFilter = 'all' | 'completed' | 'incomplete' | 'failed' | 'paused'

/** 侧栏状态文件夹顺序。 */
export const STATUS_FILTERS: readonly DownloadStatusFilter[] = ['all', 'incomplete', 'completed', 'failed', 'paused']

export function statusMatches(filter: DownloadStatusFilter, state: TaskState): boolean {
  switch (filter) {
    case 'all':
      return true
    case 'completed':
      return state === 'completed'
    // 下载中筛选包含下载中与等待中的任务。
    case 'incomplete':
      return state === 'downloading' || state === 'pending'
    case 'failed':
      return state === 'failed'
    case 'paused':
      return state === 'paused'
  }
}

/** 状态 + 分类（分类 id 来自 `CustomCategoryDto.id`）。 */
export interface DownloadFilter {
  status: DownloadStatusFilter
  category: string | null
}

export const FILTER_ALL: DownloadFilter = { status: 'all', category: null }

export function filterMatches(filter: DownloadFilter, view: DownloadTaskView, categories: CategoryIndex): boolean {
  return (
    statusMatches(filter.status, view.state) && (filter.category === null || categories.matches(filter.category, view))
  )
}

export const LOCAL_DEVICE = 'local'

/** 侧栏当前选中项；决定表格的筛选来源。 */
export type SidebarSelection =
  | { kind: 'download'; filter: DownloadFilter }
  | { kind: 'queue'; queueId: string }
  /** `local` = 本机，其余为云设备 id / 配对设备指纹。 */
  | { kind: 'device'; deviceId: string }

export const SELECTION_ALL: SidebarSelection = { kind: 'download', filter: FILTER_ALL }

export function sameSelection(a: SidebarSelection, b: SidebarSelection): boolean {
  if (a.kind !== b.kind) return false
  switch (a.kind) {
    case 'download':
      return (
        b.kind === 'download' && a.filter.status === b.filter.status && a.filter.category === b.filter.category
      )
    case 'queue':
      return b.kind === 'queue' && a.queueId === b.queueId
    case 'device':
      return b.kind === 'device' && a.deviceId === b.deviceId
  }
}

/** 侧栏分区可见性偏好键（同步偏好）。 */
export const SIDEBAR_SECTION_PREFS = {
  status: 'ui.show_sidebar_status',
  queues: 'ui.show_sidebar_queues',
  devices: 'ui.show_sidebar_devices',
  category: 'ui.show_sidebar_category',
} as const
