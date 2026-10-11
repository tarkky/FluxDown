//! 多路径分段调度的纯决策函数（直连 / 多 CDN 钉定节点 / 候选代理）。
//!
//! 下载被建模为「一组字节区间 × 多条路径」的应用层多路径调度（与
//! MSPlayer 用 HTTP Range 聚合多路径同构，CoNEXT'14）。本模块只放
//! 无状态、可确定性单测的判据；状态由 [`crate::cdn::NodePool`]（每路径
//! 速率估计、租借选择）与 segment coordinator（窗口采样、拆分、抢占）
//! 持有。
//!
//! # 判据来源
//!
//! - **稳态速率采样**：每条连接每个 ramp 窗口一个样本，建连首窗不计
//!   （TCP 慢启动期，高 RTT 链路首窗只有稳态的一半甚至更低），限速窗
//!   不计——同 BBR 把 app-limited 样本排除在带宽估计之外
//!   （draft-ietf-ccwg-bbr）。同路径多连接取中位数，抗单连接异常。
//! - **竞争集派工**：路径单连接速率低于最优路径 [`COMPETITIVE_RATIO`]
//!   的不参与派工。连接总数由 ramp 控制，一条连接放在慢路径上就少一份
//!   快路径吞吐；快路径饱和时其单连接速率自然下降，慢路径重新进入竞争
//!   集——收敛到各在用路径单连接速率相当的均衡（按边际吞吐注水）。
//! - **均衡完成拆分**：帮手拆分持有者剩余区间时按双方速率比例切分，
//!   两边同时完成（DEMS，MobiCom'17 的「平衡子流完成」），取代固定中点。
//!   拆分对象按预计完成时间而非剩余字节挑选（ECF，CoNEXT'17）。
//! - **完成时间抢占**：在途连接按自身速率完成剩余字节的时间，显著长于
//!   交给最优路径（含一次建连）的时间 → 在当前字节处切断交接。不做重复
//!   请求对冲：进度精确到字节且可取消，接管严格优于重复下载。

use std::time::Duration;

/// 竞争集门槛：单连接速率 ≥ 最优路径 × 该比例的路径才参与派工。
pub const COMPETITIVE_RATIO: f64 = 0.5;

/// 路径速率估计的 EWMA 新样本权重（每窗一次，约 2 窗半衰）。
pub const RATE_EWMA_ALPHA: f64 = 0.5;

/// 抢占的时间判据倍数：慢连接完成时间须超过「最优路径完成时间」的
/// 该倍数（再加一次建连）才抢占，滞回防抖。
pub const PREEMPT_SLACK: f64 = 2.0;

/// 交接给最优路径时计入的建连耗时估计（TCP + TLS + 可能的代理
/// CONNECT，保守取 1s）。
pub const PREEMPT_SETUP_SECS: f64 = 1.0;

/// 冷路径探索所需的最小工作量：探索连接须跨过首个慢启动窗口后还有
/// 字节可测，小于此值的尾部碎片不拿来探索（否则慢路径会握着碎片拖尾）。
pub const EXPLORE_MIN_PIECE: i64 = 1024 * 1024;

/// 连接须已存在的最少完整窗口数（首窗为慢启动期，第二窗才有稳态样本）。
pub const MIN_SAMPLE_WINDOWS: u32 = 2;

/// 拆分时双方各自至少保留的字节数（与尾部微拆分阈值同级）。
pub const MIN_SPLIT_PIECE: i64 = 64 * 1024;

/// 独立容量链路（多网卡）保底 1 条连接的最小容量占比：低于全部链路总容量
/// 此比例的链路收益可忽略，不值得占用连接（徒增蜂窝流量与尾段风险）。
pub const LINK_FLOOR_SHARE: f64 = 0.05;

/// 一个窗口内单连接速率（B/s）。
#[must_use]
pub fn window_rate(bytes: i64, window: Duration) -> f64 {
    let secs = window.as_secs_f64();
    if secs <= 0.0 {
        return 0.0;
    }
    bytes.max(0) as f64 / secs
}

/// 中位数（抗单连接异常）；空输入 `None`。
#[must_use]
pub fn median(values: &mut [f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(f64::total_cmp);
    let mid = values.len() / 2;
    Some(if values.len().is_multiple_of(2) {
        (values[mid - 1] + values[mid]) / 2.0
    } else {
        values[mid]
    })
}

/// 路径估计融合一个窗口的中位样本。首个实测样本直接替换先验（先验只
/// 用于起飞排序，实测永远覆盖先验）。
#[must_use]
pub fn blend_rate(current: f64, sample: f64, first_measurement: bool) -> f64 {
    if first_measurement {
        sample
    } else {
        (1.0 - RATE_EWMA_ALPHA) * current + RATE_EWMA_ALPHA * sample
    }
}

/// 路径是否处于竞争集（`score` 与 `best` 同量纲：单连接 B/s）。
#[must_use]
pub fn is_competitive(score: f64, best: f64) -> bool {
    best <= 0.0 || score >= best * COMPETITIVE_RATIO
}

/// 独立容量链路（多网卡）的边际单连接估计：链路容量（B/s）按饱和假设
/// 均摊到「在途 + 1」条连接上。新租约给该值最高的链路 = 注水式分配，
/// 各链路连接数随容量成比例，已饱和链路不再被加码。
#[must_use]
pub fn link_marginal(capacity_bps: f64, outstanding: u32) -> f64 {
    if !(capacity_bps.is_finite() && capacity_bps > 0.0) {
        return 0.0;
    }
    capacity_bps / f64::from(outstanding.saturating_add(1))
}

/// 均衡完成拆分：持有者速率 `holder_bps`、帮手速率 `helper_bps` 下，
/// 持有者从剩余 `remaining` 字节中保留的字节数。任一速率未知 → 中点
/// （与历史行为一致）。结果保证双方各至少 `min(MIN_SPLIT_PIECE,
/// remaining/2)` 字节。
#[must_use]
pub fn balanced_keep(remaining: i64, holder_bps: Option<f64>, helper_bps: Option<f64>) -> i64 {
    let half = remaining / 2;
    let (Some(holder), Some(helper)) = (holder_bps, helper_bps) else {
        return half;
    };
    if !(holder.is_finite() && helper.is_finite()) || holder < 0.0 || helper <= 0.0 {
        return half;
    }
    let piece = MIN_SPLIT_PIECE.min(half).max(1);
    let keep = (remaining as f64 * holder / (holder + helper)) as i64;
    keep.clamp(piece, (remaining - piece).max(piece))
}

/// 计入帮手请求建立耗时的均衡完成拆分：帮手要 `setup_secs` 后才开始收字节，
/// 双方同时完成需满足 `keep/h = setup + (R − keep)/g`，即
/// `keep = h·(setup·g + R)/(h + g)`。持有者独自完成剩余字节不比帮手建立请求
/// 更久（`R/h ≤ setup`）时拆分只会多发一个请求、白占一条连接，返回 `None`。
/// 任一速率未知或 `setup_secs ≤ 0` → 退化为 [`balanced_keep`]。
#[must_use]
pub fn split_keep_with_setup(
    remaining: i64,
    holder_bps: Option<f64>,
    helper_bps: Option<f64>,
    setup_secs: f64,
) -> Option<i64> {
    let (Some(holder), Some(helper)) = (holder_bps, helper_bps) else {
        return Some(balanced_keep(remaining, holder_bps, helper_bps));
    };
    if !(setup_secs.is_finite() && setup_secs > 0.0 && holder > 0.0) {
        return Some(balanced_keep(remaining, holder_bps, helper_bps));
    }
    if completion_secs(remaining, holder) <= setup_secs {
        return None;
    }
    if !(helper.is_finite() && helper > 0.0 && holder.is_finite()) {
        return Some(balanced_keep(remaining, holder_bps, helper_bps));
    }
    let half = remaining / 2;
    let piece = MIN_SPLIT_PIECE.min(half).max(1);
    let keep = (holder * (setup_secs * helper + remaining as f64) / (holder + helper)) as i64;
    Some(keep.clamp(piece, (remaining - piece).max(piece)))
}

/// 预计完成秒数；速率未知或为 0 → `f64::INFINITY`。
#[must_use]
pub fn completion_secs(remaining: i64, bps: f64) -> f64 {
    if bps > 0.0 && bps.is_finite() {
        remaining.max(0) as f64 / bps
    } else {
        f64::INFINITY
    }
}

/// 完成时间抢占判据：在途连接（本窗实测 `conn_bps`，0 = 停滞）完成
/// `remaining` 字节的时间，是否显著长于交给最优路径（单连接 `best_bps`）
/// 并付出一次建连的时间。
///
/// 不设剩余字节下限：建连开销已在交接侧计入，微段上速率相近的连接自然
/// 不满足判据；而下限会让极慢连接无限期握住尾部碎片（2 KiB/s 握 64 KiB
/// 即拖尾 32s），且碎片已低于拆分最小片、无法再被帮手分走。
#[must_use]
pub fn should_preempt(remaining: i64, conn_bps: f64, best_bps: f64) -> bool {
    if remaining <= 0 || !(best_bps > 0.0 && best_bps.is_finite()) {
        return false;
    }
    let own = completion_secs(remaining, conn_bps);
    let handover = PREEMPT_SLACK * completion_secs(remaining, best_bps) + PREEMPT_SETUP_SECS;
    own > handover
}

#[cfg(test)]
mod tests {
    use super::*;

    const KB: i64 = 1024;
    const MB: i64 = 1024 * 1024;

    #[test]
    fn median_is_robust_to_single_outlier() {
        let mut v = vec![1.0, 100.0, 2.0];
        assert_eq!(median(&mut v), Some(2.0));
        let mut even = vec![4.0, 1.0, 3.0, 2.0];
        assert_eq!(median(&mut even), Some(2.5));
        assert_eq!(median(&mut []), None);
    }

    #[test]
    fn first_measurement_overrides_prior() {
        assert_eq!(blend_rate(10.0 * MB as f64, 100.0, true), 100.0);
        let blended = blend_rate(100.0, 300.0, false);
        assert!((blended - 200.0).abs() < 1e-9);
    }

    #[test]
    fn competitive_set_excludes_paths_below_half_of_best() {
        assert!(is_competitive(5.0, 10.0));
        assert!(!is_competitive(4.9, 10.0));
        assert!(is_competitive(1.0, 0.0), "无最优基准时不排除");
    }

    #[test]
    fn balanced_keep_equalizes_completion_times() {
        // 持有者 1MB/s、帮手 3MB/s：持有者保留 1/4，双方同时完成。
        let keep = balanced_keep(8 * MB, Some(MB as f64), Some(3.0 * MB as f64));
        assert_eq!(keep, 2 * MB);
        let holder_t = completion_secs(keep, MB as f64);
        let helper_t = completion_secs(8 * MB - keep, 3.0 * MB as f64);
        assert!((holder_t - helper_t).abs() < 1e-6);
    }

    #[test]
    fn balanced_keep_falls_back_to_midpoint_without_rates() {
        assert_eq!(balanced_keep(10 * MB, None, Some(1.0)), 5 * MB);
        assert_eq!(balanced_keep(10 * MB, Some(1.0), None), 5 * MB);
    }

    #[test]
    fn balanced_keep_never_starves_either_side() {
        // 停滞持有者：只保留最小片，其余全部交给帮手。
        assert_eq!(
            balanced_keep(8 * MB, Some(0.0), Some(MB as f64)),
            MIN_SPLIT_PIECE
        );
        // 极快持有者：帮手至少拿到最小片。
        assert_eq!(
            balanced_keep(8 * MB, Some(1e12), Some(1.0)),
            8 * MB - MIN_SPLIT_PIECE
        );
        // 小区间：最小片按半区间收缩，结果仍在 (0, remaining) 内。
        let keep = balanced_keep(100 * KB, Some(0.0), Some(1.0));
        assert!(keep > 0 && keep < 100 * KB);
    }

    #[test]
    fn preempt_slow_or_stalled_connection_with_meaningful_remainder() {
        // 20KB/s 的代理连接还剩 8MB，最优路径 1MB/s：远超 2×8s+1s。
        assert!(should_preempt(8 * MB, 20.0 * KB as f64, MB as f64));
        // 停滞连接（本窗 0 字节）。
        assert!(should_preempt(MB, 0.0, MB as f64));
        // 尾部碎片：2KB/s 的慢路径握着 100KB，需 50s；交接只需约 1s。
        assert!(should_preempt(100 * KB, 2.0 * KB as f64, MB as f64));
        // 拆分最小片以下的尾部碎片：2KB/s 握 60KB 需 30s，交接约 1s。
        assert!(should_preempt(60 * KB, 2.0 * KB as f64, 3.0 * MB as f64));
    }

    #[test]
    fn no_preempt_for_comparable_speed_or_tiny_tail() {
        // 0.6× 最优速率：完成时间 < 2× 最优 + 建连，不抢占（滞回）。
        assert!(!should_preempt(8 * MB, 0.6 * MB as f64, MB as f64));
        // 微段：100KB/s 的连接剩 32KB 只需 0.32s，交接的建连开销覆盖不了收益。
        assert!(!should_preempt(
            32 * KB,
            100.0 * KB as f64,
            10.0 * MB as f64
        ));
        // 已无剩余字节。
        assert!(!should_preempt(0, 0.0, MB as f64));
        // 无最优基准。
        assert!(!should_preempt(8 * MB, 0.0, 0.0));
    }

    #[test]
    fn preempt_accounts_for_setup_cost_on_short_remainders() {
        // 剩 300KB：慢连接 150KB/s 需 2s；最优 10MB/s 需 0.03s×2+1s ≈ 1.06s → 抢占。
        assert!(should_preempt(
            300 * KB,
            150.0 * KB as f64,
            10.0 * MB as f64
        ));
        // 慢连接 300KB/s 需 1s < 1.06s → 不抢占（建连开销吃掉收益）。
        assert!(!should_preempt(
            300 * KB,
            300.0 * KB as f64,
            10.0 * MB as f64
        ));
    }

    #[test]
    fn split_refused_when_holder_finishes_before_helper_starts() {
        // 持有者 500KB/s 剩 256KB 需 0.5s，帮手建立请求要 1.2s：拆了也帮不上。
        let rate = 500.0 * KB as f64;
        assert_eq!(
            split_keep_with_setup(256 * KB, Some(rate), Some(rate), 1.2),
            None
        );
    }

    #[test]
    fn split_point_compensates_helper_setup() {
        // 同速 1MB/s、剩 10MB、帮手建连 2s：持有者留 6MB，双方都在 6s 完成。
        let rate = MB as f64;
        assert_eq!(
            split_keep_with_setup(10 * MB, Some(rate), Some(rate), 2.0),
            Some(6 * MB)
        );
        // 无延迟样本 / 速率未知时与历史拆分点一致。
        assert_eq!(
            split_keep_with_setup(10 * MB, Some(rate), Some(rate), 0.0),
            Some(balanced_keep(10 * MB, Some(rate), Some(rate)))
        );
        assert_eq!(
            split_keep_with_setup(10 * MB, None, Some(rate), 2.0),
            Some(5 * MB)
        );
        // 停滞持有者（0 B/s）照旧把几乎全部余量交给帮手。
        assert_eq!(
            split_keep_with_setup(10 * MB, Some(0.0), Some(rate), 2.0),
            Some(MIN_SPLIT_PIECE)
        );
    }
}
