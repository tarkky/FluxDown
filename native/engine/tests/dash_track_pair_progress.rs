//! DASH track-pair（离散音视频轨对）进度回归测试。
//!
//! 守护 BUG：track-pair 模式整轨即单 segment，若进度只在 segment 完成后上报，
//! UI 将全程 0% / 总大小未知（YouTube 插件 240MB 视频轨实测现象）。修复后：
//!   1. 下载**中途**必须收到 status=1 且 downloaded_bytes ∈ (0, total) 的进度事件；
//!   2. 进度事件的 total_bytes 必须为两轨 Content-Length 之和（Range 0-0 探测）。
//!
//! 用法（绑定本地端口，默认 ignore）：
//!   cargo test -p fluxdown_engine --test dash_track_pair_progress -- --ignored
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use fluxdown_engine::db::Db;
use fluxdown_engine::downloader::{DownloadParams, ProgressUpdate, RequestSpec, build_client};
use fluxdown_engine::events::{EngineEvent, EventSink};
use fluxdown_engine::proxy_config::ProxyConfig;
use fluxdown_engine::speed_limiter::SpeedLimiter;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

struct NoopTestSink;
impl EventSink for NoopTestSink {
    fn emit(&self, _event: EngineEvent) {}
}

/// 极简 Range 能力 HTTP/1.1 服务器：`bytes=0-0` → 206 + `Content-Range .../N`，
/// 其余请求 → 200 全量。两条路径（/video、/audio）返回不同大小的确定性 body。
async fn start_server(video: Arc<Vec<u8>>, audio: Arc<Vec<u8>>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let video = video.clone();
            let audio = audio.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                let Ok(n) = stream.read(&mut buf).await else {
                    return;
                };
                let head = String::from_utf8_lossy(&buf[..n]).to_string();
                let body: &Arc<Vec<u8>> = if head.contains("/audio") {
                    &audio
                } else {
                    &video
                };
                let total = body.len();
                let is_probe = head.to_ascii_lowercase().contains("range: bytes=0-0");
                if is_probe {
                    let h = format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Length: 1\r\nContent-Range: bytes 0-0/{total}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n"
                    );

                    stream
                        .write_all(h.as_bytes())
                        .await
                        .unwrap_or_else(|error| {
                            assert!(
                                matches!(
                                    error.kind(),
                                    std::io::ErrorKind::BrokenPipe
                                        | std::io::ErrorKind::ConnectionReset
                                        | std::io::ErrorKind::ConnectionAborted
                                ),
                                "test server response failed: {error}"
                            );
                        });

                    stream.write_all(&body[..1]).await.unwrap_or_else(|error| {
                        assert!(
                            matches!(
                                error.kind(),
                                std::io::ErrorKind::BrokenPipe
                                    | std::io::ErrorKind::ConnectionReset
                                    | std::io::ErrorKind::ConnectionAborted
                            ),
                            "test server response failed: {error}"
                        );
                    });
                } else {
                    let h = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {total}\r\nConnection: close\r\n\r\n"
                    );

                    stream
                        .write_all(h.as_bytes())
                        .await
                        .unwrap_or_else(|error| {
                            assert!(
                                matches!(
                                    error.kind(),
                                    std::io::ErrorKind::BrokenPipe
                                        | std::io::ErrorKind::ConnectionReset
                                        | std::io::ErrorKind::ConnectionAborted
                                ),
                                "test server response failed: {error}"
                            );
                        });

                    stream.write_all(body).await.unwrap_or_else(|error| {
                        assert!(
                            matches!(
                                error.kind(),
                                std::io::ErrorKind::BrokenPipe
                                    | std::io::ErrorKind::ConnectionReset
                                    | std::io::ErrorKind::ConnectionAborted
                            ),
                            "test server response failed: {error}"
                        );
                    });
                }

                stream.flush().await.unwrap_or_else(|error| {
                    assert!(
                        matches!(
                            error.kind(),
                            std::io::ErrorKind::BrokenPipe
                                | std::io::ErrorKind::ConnectionReset
                                | std::io::ErrorKind::ConnectionAborted
                        ),
                        "test server response failed: {error}"
                    );
                });
            });
        }
    });
    format!("http://{addr}")
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "binds a local port; run with --ignored"]
async fn track_pair_reports_midway_progress_with_real_total() {
    let video: Arc<Vec<u8>> = Arc::new((0..400_000u32).map(|i| (i % 251) as u8).collect());
    let audio: Arc<Vec<u8>> = Arc::new((0..100_000u32).map(|i| (i % 241) as u8).collect());
    let expected_total = (video.len() + audio.len()) as i64;
    let base = start_server(video.clone(), audio.clone()).await;

    let work_dir = std::env::temp_dir().join("fluxdown-rt-trackpair");

    if let Err(error) = tokio::fs::remove_dir_all(&work_dir).await {
        assert_eq!(
            error.kind(),
            std::io::ErrorKind::NotFound,
            "clean test path: {error}"
        );
    }
    tokio::fs::create_dir_all(&work_dir).await.unwrap();
    let db = Db::open(&work_dir).await.expect("db");
    db.insert_task(
        "tp",
        &format!("{base}/video"),
        "pair.mp4",
        &work_dir.to_string_lossy(),
        1,
        0,
        "",
        "",
        "",
        0,
    )
    .await
    .expect("insert_task");

    let (tx, mut rx) = mpsc::channel::<ProgressUpdate>(1024);
    let updates = Arc::new(std::sync::Mutex::new(Vec::<ProgressUpdate>::new()));
    let sink_updates = updates.clone();
    let collector = tokio::spawn(async move {
        while let Some(u) = rx.recv().await {
            sink_updates.lock().expect("lock").push(u);
        }
    });

    let params = DownloadParams {
        spawn_gen: 1,
        unattended: false,
        preprobed: None,
        auto_proxy: None,
        multi_nic: None,
        task_id: "tp".to_string(),
        url: format!("{base}/video"),
        save_dir: work_dir.to_string_lossy().to_string(),
        file_name: "pair.mp4".to_string(),
        segment_count: 1,
        is_resume: false,
        range_verified: true,
        db: db.clone(),
        client: build_client(&ProxyConfig::default(), "FluxDownRealTest/1.0").expect("client"),
        progress_tx: tx,
        cancel_token: CancellationToken::new(),
        sink: Arc::new(NoopTestSink),
        cookies: String::new(),
        referrer: String::new(),
        // 限速 250KB/s：500KB 两轨 ≈ 2s，保证 200ms 节流的块级上报有多次机会。
        speed_limiter: {
            let limiter = SpeedLimiter::new(250_000);
            limiter.spawn_refill_task();
            limiter
        },
        hint_file_size: 0,
        proxy_config: ProxyConfig::default(),
        selector: Arc::new(fluxdown_engine::NoopSelection),
        checksum: String::new(),
        extra_headers: std::collections::HashMap::new(),
        spec: RequestSpec::empty_get(),
        audio_url: Some(format!("{base}/audio")),
        auto_max_connections: 0,
        use_server_time: false,
        overwrite: fluxdown_engine::file_exists::OverwritePolicy::Never,
        ffmpeg_path: None,
        cdn: fluxdown_engine::cdn::CdnTaskInput::default(),
    };

    fluxdown_engine::dash_downloader::run_dash_download(params).await;

    collector
        .await
        .expect("test background task must not panic");

    let summary: Vec<(i32, i64, i64)> = {
        let updates = updates.lock().expect("lock");
        updates
            .iter()
            .map(|u| (u.status, u.downloaded_bytes, u.total_bytes))
            .collect()
    };
    let final_update = summary.last().expect("at least one update");
    assert_eq!(final_update.0, 3, "download must complete: {summary:?}");

    // 1. 中途进度：status=1 且 0 < downloaded < total 的事件必须存在。
    let midway: Vec<(i32, i64, i64)> = summary
        .iter()
        .copied()
        .filter(|(st, dl, _)| *st == 1 && *dl > 0 && *dl < expected_total)
        .collect();
    assert!(
        !midway.is_empty(),
        "expected midway progress updates, got only: {summary:?}"
    );
    // 2. 中途事件必须携带两轨之和的真实总大小。
    assert!(
        midway.iter().all(|(_, _, total)| *total == expected_total),
        "midway total_bytes must be {expected_total}: {midway:?}"
    );

    // 3. 进入合并阶段前必须上报 status=5（准备/合并中）。
    let preparing: Vec<(i32, i64, i64)> = summary
        .iter()
        .copied()
        .filter(|(st, _, _)| *st == 5)
        .collect();
    assert!(
        !preparing.is_empty(),
        "expected status=5 (preparing/muxing) event before completion, got only: {summary:?}"
    );

    // 产物字节完整（mux 失败是非致命 warning：无 ffmpeg 时视频/音频各自成文件）。
    let video_out = work_dir.join("pair.mp4");
    let meta = tokio::fs::metadata(&video_out).await.expect("video output");
    assert_eq!(meta.len() as usize, video.len(), "video track byte-exact");

    if let Err(error) = tokio::fs::remove_dir_all(&work_dir).await
        && error.kind() != std::io::ErrorKind::NotFound
    {
        eprintln!("best-effort test directory cleanup: {error}");
    }
}
