//! 同一 coordinator generation 内跨 worker 共享的请求路径观测。
//!
//! - **重定向终点复用**：原始 URL 经 302 落到 CDN / 对象存储时，每个分段请求都
//!   重放一遍跳转链。实测热连接下每跳 +0.4~0.7s（GitHub release → release-assets、
//!   VS Code update → prss CDN），冷连接 +1~2s，而直连终点只要几十毫秒。首个成功
//!   响应学到终点后，后续 SYS 租约的 Range 请求直接发往终点；终点请求一旦失败
//!   （签名过期 / 一次性终点 / 按请求分配的镜像），本任务内永久退回原始 URL。
//! - **请求建立延迟**：发出请求到收到响应头的 EWMA（含建连与跳转），供拆分判据
//!   估计「帮手开始收字节前要等多久」。

use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

/// 学到的终点能否被直接复用。跨源终点只在本任务请求不携带凭据时复用：reqwest
/// 跟随跨主机重定向时会剥掉 `Cookie` / `Authorization` / `Proxy-Authorization`，而
/// 直连终点的请求由 `build_request` 重新附加任务的 cookies 与 extra_headers——复用
/// 就会把源站凭据发给 CDN / 对象存储（S3 还会因双重鉴权回 400）。同源终点不涉及
/// 凭据外泄，照常复用。
pub(super) fn reuse_is_safe(
    origin: &str,
    target: &reqwest::Url,
    spec: &crate::downloader::RequestSpec,
) -> bool {
    let same_origin = reqwest::Url::parse(origin).is_ok_and(|origin| {
        origin.scheme() == target.scheme()
            && origin.host_str() == target.host_str()
            && origin.port_or_known_default() == target.port_or_known_default()
    });
    same_origin
        || (spec.cookies.trim().is_empty()
            && !spec.extra_headers.keys().any(|name| {
                ["authorization", "cookie", "proxy-authorization"]
                    .iter()
                    .any(|sensitive| name.eq_ignore_ascii_case(sensitive))
            }))
}
#[derive(Default)]
pub(super) struct RequestPath {
    redirect_target: StdMutex<Option<String>>,
    reuse_disabled: AtomicBool,
    /// 请求建立延迟 EWMA（微秒）；0 = 尚无样本。
    setup_micros: AtomicU64,
}

impl RequestPath {
    /// 已学到且仍可复用的重定向终点；`None` = 使用原始 URL。
    pub(super) fn redirect_target(&self) -> Option<String> {
        if self.reuse_disabled.load(Ordering::Relaxed) {
            return None;
        }
        self.redirect_target
            .lock()
            .ok()
            .and_then(|target| target.clone())
    }

    /// 发往原始 URL 的请求最终落在 `final_url`：记住终点供后续分段直连。
    pub(super) fn learn_redirect(&self, origin: &str, final_url: &str) {
        if final_url == origin || self.reuse_disabled.load(Ordering::Relaxed) {
            return;
        }
        if let Ok(mut target) = self.redirect_target.lock()
            && target.as_deref() != Some(final_url)
        {
            *target = Some(final_url.to_owned());
        }
    }

    /// 直连终点失败：本任务内不再复用（终点可能过期、一次性或按请求分配）。
    pub(super) fn abandon_redirect(&self) {
        self.reuse_disabled.store(true, Ordering::Relaxed);
        if let Ok(mut target) = self.redirect_target.lock() {
            *target = None;
        }
    }

    /// 记录一次「发出请求 → 收到响应头」耗时（新样本权重 1/4）。
    pub(super) fn record_setup(&self, elapsed: Duration) {
        let sample = u64::try_from(elapsed.as_micros())
            .unwrap_or(u64::MAX)
            .max(1);
        self.setup_micros
            .update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                if old == 0 {
                    sample
                } else {
                    old.saturating_mul(3).saturating_add(sample) / 4
                }
            });
    }

    /// 请求建立延迟估计（秒）；无样本 = 0（不对拆分施加延迟约束）。
    pub(super) fn setup_secs(&self) -> f64 {
        Duration::from_micros(self.setup_micros.load(Ordering::Relaxed)).as_secs_f64()
    }
}

#[cfg(test)]
mod tests {
    use super::{RequestPath, reuse_is_safe};
    use std::time::Duration;

    #[test]
    fn learned_target_is_reused_until_abandoned_for_good() {
        let path = RequestPath::default();
        assert_eq!(path.redirect_target(), None);
        path.learn_redirect("https://a/x", "https://a/x");
        assert_eq!(path.redirect_target(), None, "未跳转不产生终点");

        path.learn_redirect("https://a/x", "https://cdn/x?sig=1");
        assert_eq!(
            path.redirect_target().as_deref(),
            Some("https://cdn/x?sig=1")
        );

        path.abandon_redirect();
        assert_eq!(path.redirect_target(), None);
        path.learn_redirect("https://a/x", "https://cdn/x?sig=2");
        assert_eq!(path.redirect_target(), None, "失败过的任务不再复用终点");
    }

    /// 跨源终点在任务携带凭据时绝不复用（否则凭据外泄给 CDN）；同源或无凭据照常。
    #[test]
    fn cross_origin_reuse_requires_credential_free_requests() {
        let cdn = reqwest::Url::parse("https://cdn.example.net/x?sig=1").expect("url");
        let same = reqwest::Url::parse("https://a.example.com:443/y").expect("url");
        let plain = crate::downloader::RequestSpec::empty_get();
        assert!(reuse_is_safe("https://a.example.com/x", &cdn, &plain));

        let mut cookie = crate::downloader::RequestSpec::empty_get();
        cookie.cookies = "sid=1".to_owned();
        assert!(!reuse_is_safe("https://a.example.com/x", &cdn, &cookie));
        assert!(reuse_is_safe("https://a.example.com/x", &same, &cookie));

        let mut auth = crate::downloader::RequestSpec::empty_get();
        auth.extra_headers
            .insert("AUTHORIZATION".to_owned(), "Bearer t".to_owned());
        assert!(!reuse_is_safe("https://a.example.com/x", &cdn, &auth));
    }

    #[test]
    fn setup_latency_is_smoothed() {
        let path = RequestPath::default();
        assert_eq!(path.setup_secs(), 0.0);
        path.record_setup(Duration::from_millis(800));
        assert!((path.setup_secs() - 0.8).abs() < 1e-9);
        path.record_setup(Duration::from_millis(0));
        // (800ms × 3 + 1µs) / 4 ≈ 600ms：单个快样本不会把估计清零。
        assert!((path.setup_secs() - 0.6).abs() < 1e-3);
    }
}
