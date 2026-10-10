// 套餐徽标（与 GPUI profile.rs `plan_tag` 同步）：outline | solid | medal | ribbon | plain，纯色无渐变。

import { Crown } from 'lucide-react'
import type { CSSProperties } from 'react'
import type { CloudPlan } from '../../../../lib/rpc'
import { badgeText, parseBadgeColor } from './planBadgeText'

const TEXT = 'text-caption leading-[14px]'

export function PlanBadge({ plan, ordinal }: { plan: CloudPlan; ordinal: number | null }) {
  const text = badgeText(plan, ordinal)
  if (!text) return null
  const color = parseBadgeColor(plan.badgeColor) ?? 'var(--fx-colors-accent-foreground, currentColor)'
  const tint = (alpha: number): CSSProperties['background'] => `color-mix(in srgb, ${color} ${alpha * 100}%, transparent)`
  const pill = 'inline-flex shrink-0 items-center rounded-full'
  switch (plan.badgeStyle) {
    case 'outline':
      return (
        <span className={`${pill} gap-0.5 border px-2 ${TEXT} font-semibold`} style={{ color, borderColor: color, background: tint(0.08) }}>
          <Crown className="size-3" strokeWidth={1.75} />
          {text}
        </span>
      )
    case 'solid':
      return (
        <span className={`${pill} px-2 ${TEXT} font-semibold text-white`} style={{ background: color }}>
          {text}
        </span>
      )
    case 'medal':
      return (
        <span className={`${pill} overflow-hidden border ${TEXT} font-semibold`} style={{ color, borderColor: color }}>
          <span className="flex items-center self-stretch px-1 text-white" style={{ background: color }}>
            <Crown className="size-3" strokeWidth={1.75} />
          </span>
          <span className="px-1">{text}</span>
        </span>
      )
    case 'ribbon':
      return (
        <span className={`${pill} px-2 ${TEXT} font-semibold text-white`} style={{ background: color }}>
          {text}
        </span>
      )
    default:
      return (
        <span className={`${pill} px-1.5 ${TEXT} font-semibold`} style={{ color, background: tint(0.12) }}>
          {text}
        </span>
      )
  }
}
