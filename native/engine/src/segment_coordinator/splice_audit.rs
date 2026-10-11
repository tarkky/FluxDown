//! 多段下载完成后的「版本证据缺失」拼接审计。
//!
//! 多段下载靠响应的 ETag 比对（探测基线 / 首个 206 latch）发现「文件在下载中途被
//! 替换」。但续传与分段请求按设计不发送 `If-Range`（一次性签名 URL 会把它视为签名
//! 外请求头而作废，见 `do_segment` 注释），部分 CDN 又会在 206 上剥离 ETag：
//! 探测基线明明带版本标识、分段 206 却全无 ETag，或仅剩被当作 edge 时钟差异而
//! 容忍的 Last-Modified 漂移时，新旧版本的段会被静默拼成既非旧也非新的损坏文件。
//!
//! 触发条件由 [`super::GenerationEvidence::content_audit_warranted`] 判定，刻意收窄
//! 到上述「确有嫌疑」的情形：从未有过任何版本基线的任务（hint 且全程无 validator）
//! 不审计，避免对每个无 ETag 的普通服务器在完成时多打请求、消耗一次性 URL / 配额。
//!
//! 此时唯一剩下的证据是内容本身：下载完成后向服务器重新取几个小窗口（文件首、
//! 中、尾，并发），与落盘字节逐字节比较。被拼接的文件必有一端来自旧版本，而服务器
//! 此刻只会返回一个版本，两端至少一端与服务器不符。
//!
//! 判定原则是「只在拿到确凿矛盾时才报错」：任何网络失败、非 206、区间错位、压缩
//! 响应、长度不符都只代表本次审计无结论（返回 `None`），绝不拿审计失败去推翻一份
//! 已完整下载的文件。动态生成 / 每节点水印的服务器理论上可能让本地与远端字节不同，
//! 故每个任务（进程内）至多允许一次由审计触发的重下（[`redownload_allowed`] /
//! [`record_redownload`]），重下后同一任务不再审计，杜绝反复清盘。

use std::collections::HashSet;
use std::path::Path;
use std::sync::{LazyLock, Mutex as StdMutex};
use std::time::Duration;

use futures_util::StreamExt;
use reqwest::Client;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::sync::CancellationToken;

use crate::downloader::{DownloadSpec, build_request, parse_content_range_start};

/// 单个采样窗口的字节数。
const SAMPLE_LEN: i64 = 4096;
/// 单个窗口从发请求到读完 body 的总时限（审计是尽力而为，不得拖住完成路径；
/// 窗口并发，故整次审计的最坏附加延迟即此值）。
const SAMPLE_TIMEOUT: Duration = Duration::from_secs(5);

/// 已经因审计触发过一次重下的任务 id。
static AUDITED_REDOWNLOADS: LazyLock<StdMutex<HashSet<String>>> =
    LazyLock::new(|| StdMutex::new(HashSet::new()));

/// 该任务是否还有「审计触发重下」的名额（每任务 1 次）。
pub(super) fn redownload_allowed(task_id: &str) -> bool {
    let set = AUDITED_REDOWNLOADS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    !set.contains(task_id)
}

/// 记下该任务已消耗审计重下名额。
pub(super) fn record_redownload(task_id: &str) {
    AUDITED_REDOWNLOADS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(task_id.to_string());
}

/// 采样窗口 `(起点, 长度)`：文件首、中、尾。文件不足三个窗口时自然去重。
pub(super) fn sample_windows(total: i64) -> Vec<(i64, i64)> {
    if total <= 0 {
        return Vec::new();
    }
    let len = SAMPLE_LEN.min(total);
    let last = total - len;
    let mut starts = vec![0, last / 2, last];
    starts.dedup();
    starts.into_iter().map(|start| (start, len)).collect()
}

/// 以服务器当前版本取 `[start, start+len)`。仅在响应是起点对齐、未压缩、长度
/// 恰为 `len` 的 206 时返回字节；其余一律 `None`（无结论）。
async fn fetch_window(
    client: &Client,
    url: &str,
    spec: &DownloadSpec,
    start: i64,
    len: i64,
    cancel: &CancellationToken,
) -> Option<Vec<u8>> {
    let want = usize::try_from(len).ok()?;
    let range = format!("bytes={}-{}", start, start + len - 1);
    let request = build_request(client, url, reqwest::Method::GET, spec.current())
        .header("Range", range)
        .header("Accept-Encoding", "identity");
    let fetch = async {
        let resp = request.send().await.ok()?;
        if resp.status() != reqwest::StatusCode::PARTIAL_CONTENT
            || resp
                .headers()
                .contains_key(reqwest::header::CONTENT_ENCODING)
            || parse_content_range_start(resp.headers()) != Some(start)
        {
            return None;
        }
        let mut body = Vec::with_capacity(want);
        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            body.extend_from_slice(&chunk.ok()?);
            if body.len() > want {
                return None;
            }
        }
        (body.len() == want).then_some(body)
    };
    tokio::select! {
        _ = cancel.cancelled() => None,
        r = tokio::time::timeout(SAMPLE_TIMEOUT, fetch) => r.ok().flatten(),
    }
}

/// 读取落盘文件 `[start, start+len)`。
async fn read_window(dest: &Path, start: i64, len: i64) -> Option<Vec<u8>> {
    let mut file = tokio::fs::File::open(dest).await.ok()?;
    file.seek(std::io::SeekFrom::Start(u64::try_from(start).ok()?))
        .await
        .ok()?;
    let mut buf = vec![0u8; usize::try_from(len).ok()?];
    file.read_exact(&mut buf).await.ok()?;
    Some(buf)
}

/// 审计落盘文件与服务器当前版本是否一致。返回 `Some(起点)` 表示该窗口落盘字节与
/// 服务器当前内容确凿不同（文件被替换后拼接）；其余情形（含全部无结论）返回 `None`。
pub(super) async fn find_version_mismatch(
    client: &Client,
    url: &str,
    spec: &DownloadSpec,
    dest: &Path,
    total: i64,
    cancel: &CancellationToken,
) -> Option<i64> {
    let windows = sample_windows(total);
    let remotes = futures_util::future::join_all(
        windows
            .iter()
            .map(|&(start, len)| fetch_window(client, url, spec, start, len, cancel)),
    )
    .await;
    for (&(start, len), remote) in windows.iter().zip(remotes) {
        // 服务器不愿回答（签名 URL 已失效、限流等）：该窗口无结论，不据此判任何事。
        let Some(remote) = remote else { continue };
        let Some(local) = read_window(dest, start, len).await else {
            continue;
        };
        if local != remote {
            return Some(start);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{SAMPLE_LEN, record_redownload, redownload_allowed, sample_windows};

    #[test]
    fn windows_cover_head_middle_tail() {
        let total = 10 * SAMPLE_LEN;
        let w = sample_windows(total);
        assert_eq!(w.len(), 3);
        assert_eq!(w[0], (0, SAMPLE_LEN));
        assert_eq!(w[2], (total - SAMPLE_LEN, SAMPLE_LEN));
        assert!(w[1].0 > w[0].0 && w[1].0 < w[2].0);
    }

    #[test]
    fn windows_dedup_on_small_files() {
        assert_eq!(sample_windows(100), vec![(0, 100)]);
        assert_eq!(sample_windows(SAMPLE_LEN), vec![(0, SAMPLE_LEN)]);
        assert_eq!(
            sample_windows(SAMPLE_LEN + 1),
            vec![(0, SAMPLE_LEN), (1, SAMPLE_LEN)]
        );
        assert!(sample_windows(0).is_empty());
    }

    #[test]
    fn redownload_budget_is_one_per_task() {
        let id = "splice-audit-cap-test-task";
        assert!(redownload_allowed(id));
        record_redownload(id);
        assert!(!redownload_allowed(id), "同一任务不得第二次由审计触发重下");
        assert!(redownload_allowed("splice-audit-cap-test-other"));
    }
}
