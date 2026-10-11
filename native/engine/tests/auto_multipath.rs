//! `ProxyMode::Auto` 多路径调度的确定性 e2e。
//!
//! 两个本地 raw-tokio HTTP/1.1 服务：一个当源站（直连路径），一个当 HTTP
//! 代理（reqwest 对 `http://` URL 经 HTTP 代理发 absolute-form 请求，测试代理
//! 忽略 URI 里的 host，直接以同一份 body 响应）。两者各自按连接限速，并统计
//! 实际写出的 body 字节，用于断言哪条路径承担了主要流量。
//!
//! 覆盖：快代理接管慢直连、慢代理不拖尾、代理发完响应头后卡死被放弃、
//! 代理 ETag 不一致时钉死直连（`direct:pinned`）。

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use fluxdown_engine::auto_proxy::{AutoProxyCtx, CandidateSource, PathCandidate, RoutePath, route};
use fluxdown_engine::db::Db;
use fluxdown_engine::downloader::{
    DownloadParams, ProgressUpdate, RequestSpec, build_client, run_download,
};
use fluxdown_engine::events::{EngineEvent, EventSink};
use fluxdown_engine::proxy_config::{ProxyConfig, ProxyMode, ProxyType};
use fluxdown_engine::speed_limiter::SpeedLimiter;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;
const BODY_LEN: usize = 24 * MIB as usize;
const SEGMENTS: i32 = 8;
const TEST_UA: &str = "FluxDownAutoMultipathTest/1.0";
const ORIGIN_ETAG: &str = "\"auto-mp-v1\"";
const LAST_MODIFIED: &str = "Wed, 01 Jan 2025 00:00:00 GMT";

struct NoopTestSink;
impl EventSink for NoopTestSink {
    fn emit(&self, _event: EngineEvent) {}
}

/// 确定性伪随机 body（LCG）。
fn gen_body(len: usize, seed: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut x = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
    while out.len() < len {
        x = x
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(len);
    out
}

/// 把引擎 tracing 日志接到测试输出（失败时由 libtest 打印）。
fn init_test_logging() {
    if let Err(error) = tracing_subscriber::fmt()
        .with_test_writer()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init()
    {
        eprintln!("test tracing subscriber already configured: {error}");
    }
}

// ===========================================================================
// 测试服务器（源站 / 代理两用）
// ===========================================================================

#[derive(Clone)]
struct ServerCfg {
    body: Arc<Vec<u8>>,
    /// 单连接限速（B/s）；0 = 不限速。
    rate_bps: u64,
    etag: String,
    /// 写完响应头后永久卡住（不发 body）。
    stall_after_headers: bool,
}

/// 每服务器计数：实际写出的 body 字节 + 收到的 GET 请求数。
#[derive(Default)]
struct Counters {
    body_bytes: AtomicU64,
    gets: AtomicU64,
}

struct TestServer {
    port: u16,
    counters: Arc<Counters>,
    accept_task: tokio::task::JoinHandle<()>,
}

impl TestServer {
    fn served(&self) -> u64 {
        self.counters.body_bytes.load(Ordering::Relaxed)
    }

    fn gets(&self) -> u64 {
        self.counters.gets.load(Ordering::Relaxed)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.accept_task.abort();
    }
}

async fn start_server(cfg: ServerCfg) -> TestServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("local_addr").port();
    let counters = Arc::new(Counters::default());
    let shared = counters.clone();
    let accept_task = tokio::spawn(async move {
        let mut conns = tokio::task::JoinSet::new();
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let cfg = cfg.clone();
            let counters = shared.clone();
            conns.spawn(async move {
                if let Err(error) = serve_conn(stream, cfg, counters).await {
                    assert!(
                        matches!(
                            error.kind(),
                            std::io::ErrorKind::BrokenPipe
                                | std::io::ErrorKind::ConnectionReset
                                | std::io::ErrorKind::ConnectionAborted
                                | std::io::ErrorKind::UnexpectedEof
                        ),
                        "test server connection failed: {error}"
                    );
                }
            });
            // 回收已结束的连接任务；accept 任务被 abort 时 JoinSet 随之
            // drop，所有在途连接一并终止。
            while conns.try_join_next().is_some() {}
        }
    });
    TestServer {
        port,
        counters,
        accept_task,
    }
}

/// 单请求连接（`Connection: close`）：解析请求头 → 200/206 响应 → 关闭。
async fn serve_conn(
    mut stream: TcpStream,
    cfg: ServerCfg,
    counters: Arc<Counters>,
) -> std::io::Result<()> {
    let mut buf = Vec::with_capacity(1024);
    let mut tmp = [0u8; 1024];
    loop {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        if buf.len() > 64 * 1024 {
            return Ok(());
        }
    }
    let text = String::from_utf8_lossy(&buf);
    let mut is_head = false;
    let mut range: Option<(usize, Option<usize>)> = None;
    for (i, line) in text.split("\r\n").enumerate() {
        if i == 0 {
            is_head = line.starts_with("HEAD ");
            continue;
        }
        if let Some((name, value)) = line.split_once(':')
            && name.trim().eq_ignore_ascii_case("range")
            && let Some(v) = value.trim().strip_prefix("bytes=")
            && let Some((s, e)) = v.split_once('-')
            && let Ok(start) = s.trim().parse::<usize>()
        {
            range = Some((start, e.trim().parse::<usize>().ok()));
        }
    }

    let total = cfg.body.len();
    let (status, start, end) = match range {
        Some((s, _)) if s >= total => {
            let head = format!(
                "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */{total}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
            stream.write_all(head.as_bytes()).await?;
            return stream.shutdown().await;
        }
        Some((s, e)) => (
            "206 Partial Content",
            s,
            e.unwrap_or(total - 1).min(total - 1),
        ),
        None => ("200 OK", 0, total - 1),
    };
    let len = end - start + 1;
    let mut head = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {len}\r\nAccept-Ranges: bytes\r\nETag: {}\r\nLast-Modified: {LAST_MODIFIED}\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n",
        cfg.etag
    );
    if range.is_some() {
        head.push_str(&format!("Content-Range: bytes {start}-{end}/{total}\r\n"));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).await?;
    stream.flush().await?;
    if is_head {
        return stream.shutdown().await;
    }
    counters.gets.fetch_add(1, Ordering::Relaxed);
    if cfg.stall_after_headers {
        // 客户端放弃连接前一直挂着；accept 任务 abort 时一并终止。
        std::future::pending::<()>().await;
    }

    // 限速：每 50ms 一块（rate/20 字节）；不限速时 64KiB 一块直写。
    const TICK: Duration = Duration::from_millis(50);
    let chunk = if cfg.rate_bps == 0 {
        64 * 1024
    } else {
        ((cfg.rate_bps / 20).max(1)) as usize
    };
    let mut off = start;
    let mut next = tokio::time::Instant::now();
    while off <= end {
        let chunk_end = (off + chunk - 1).min(end);
        stream.write_all(&cfg.body[off..=chunk_end]).await?;
        counters
            .body_bytes
            .fetch_add((chunk_end + 1 - off) as u64, Ordering::Relaxed);
        off = chunk_end + 1;
        if cfg.rate_bps > 0 && off <= end {
            next += TICK;
            tokio::time::sleep_until(next).await;
        }
    }
    stream.flush().await?;
    stream.shutdown().await
}

// ===========================================================================
// run_download 驱动
// ===========================================================================

fn work_dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("fluxdown_automp_{}_{}", tag, std::process::id()));

    if let Err(error) = std::fs::remove_dir_all(&d) {
        assert_eq!(
            error.kind(),
            std::io::ErrorKind::NotFound,
            "clean test path: {error}"
        );
    }
    std::fs::create_dir_all(&d).expect("create work dir");
    d
}

fn auto_ctx(origin_port: u16, proxy_port: u16) -> AutoProxyCtx {
    AutoProxyCtx {
        host: format!("127.0.0.1:{origin_port}"),
        user_agent: TEST_UA.to_string(),
        start_route: RoutePath::Direct,
        start_prior_bps: None,
        start_label: route::DIRECT,
        alternates: vec![PathCandidate {
            route: RoutePath::Proxy(CandidateSource::ManualFields),
            config: ProxyConfig {
                mode: ProxyMode::Manual,
                proxy_type: ProxyType::Http,
                host: "127.0.0.1".into(),
                port: proxy_port,
                username: String::new(),
                password: String::new(),
                no_proxy_list: String::new(),
            },
            prior_bps: None,
        }],
    }
}

struct RunOutcome {
    status: i32,
    elapsed: Duration,
    dest: PathBuf,
    auto_route: String,
}

/// 用 Auto 多路径上下文跑完整 `run_download`，返回终态、耗时与 DB 标签。
async fn run_auto(tag: &str, origin: &TestServer, proxy: &TestServer) -> RunOutcome {
    init_test_logging();
    let dir = work_dir(tag);
    let db = Db::open(&dir).await.expect("Db::open");
    let task_id = format!("automp-{tag}");
    let url = format!("http://127.0.0.1:{}/file.bin", origin.port);
    let file_name = "file.bin";
    db.insert_task(
        &task_id,
        &url,
        file_name,
        &dir.to_string_lossy(),
        SEGMENTS,
        BODY_LEN as i64,
        "",
        "",
        "",
        0,
    )
    .await
    .expect("insert_task");

    let (tx, mut rx) = mpsc::channel::<ProgressUpdate>(256);
    let last_status = Arc::new(AtomicI32::new(0));
    let ls = last_status.clone();
    let collector = tokio::spawn(async move {
        while let Some(u) = rx.recv().await {
            if u.status >= 3 {
                ls.store(u.status, Ordering::SeqCst);
            }
        }
    });

    let params = DownloadParams {
        spawn_gen: 1,
        unattended: false,
        preprobed: None,
        auto_proxy: Some(Arc::new(auto_ctx(origin.port, proxy.port))),
        multi_nic: None,
        auto_max_connections: 0,
        task_id: task_id.clone(),
        url,
        save_dir: dir.to_string_lossy().to_string(),
        file_name: file_name.to_string(),
        segment_count: SEGMENTS,
        is_resume: false,
        range_verified: true,
        db: db.clone(),
        client: build_client(&ProxyConfig::default(), TEST_UA).expect("build_client"),
        progress_tx: tx,
        cancel_token: CancellationToken::new(),
        speed_limiter: SpeedLimiter::new(0),
        cookies: String::new(),
        referrer: String::new(),
        hint_file_size: 0,
        proxy_config: ProxyConfig::default(),
        sink: Arc::new(NoopTestSink),
        selector: Arc::new(fluxdown_engine::NoopSelection),
        checksum: String::new(),
        extra_headers: std::collections::HashMap::new(),
        spec: RequestSpec::empty_get(),
        audio_url: None,
        use_server_time: false,
        overwrite: fluxdown_engine::file_exists::OverwritePolicy::Never,
        ffmpeg_path: None,
        cdn: fluxdown_engine::cdn::CdnTaskInput::default(),
    };

    let started = Instant::now();
    tokio::time::timeout(Duration::from_secs(120), run_download(params))
        .await
        .expect("run_download must finish within 120s");
    let elapsed = started.elapsed();

    collector
        .await
        .expect("test background task must not panic");

    let auto_route = db
        .load_task_by_id(&task_id)
        .await
        .expect("load_task_by_id")
        .expect("task row")
        .auto_route;
    RunOutcome {
        status: last_status.load(Ordering::SeqCst),
        elapsed,
        dest: dir.join(file_name),
        auto_route,
    }
}

async fn assert_bytes_identical(path: &Path, expected: &[u8]) {
    let got = tokio::fs::read(path).await.expect("read dest");
    assert_eq!(got.len(), expected.len(), "dest length mismatch");
    assert!(got == expected, "dest bytes differ from origin body");
}

fn origin_cfg(body: &Arc<Vec<u8>>, rate_bps: u64) -> ServerCfg {
    ServerCfg {
        body: body.clone(),
        rate_bps,
        etag: ORIGIN_ETAG.to_string(),
        stall_after_headers: false,
    }
}

// ===========================================================================
// 场景
// ===========================================================================

/// 慢直连（150 KiB/s/conn）+ 快代理（4 MiB/s/conn）：代理经探索与抢占接管
/// 主要流量，主导标签为 `proxy:sampled*`，总耗时远低于纯直连下界（≈20s）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fast_proxy_takes_over_slow_direct() {
    let body = Arc::new(gen_body(BODY_LEN, 0xA1));
    let origin = start_server(origin_cfg(&body, 150 * KIB)).await;
    let proxy = start_server(origin_cfg(&body, 4 * MIB)).await;

    let out = run_auto("fast_proxy", &origin, &proxy).await;
    eprintln!(
        "[fast_proxy] status={} elapsed={:?} origin={} proxy={} route={}",
        out.status,
        out.elapsed,
        origin.served(),
        proxy.served(),
        out.auto_route
    );

    assert_eq!(out.status, 3, "task must complete");
    assert_bytes_identical(&out.dest, &body).await;
    assert!(
        proxy.served() > BODY_LEN as u64 / 2,
        "proxy should carry most bytes: proxy={} origin={}",
        proxy.served(),
        origin.served()
    );
    assert!(
        out.auto_route.starts_with(route::PROXY_SAMPLED),
        "auto_route = {:?}",
        out.auto_route
    );
    assert!(
        out.elapsed < Duration::from_secs(15),
        "took {:?}, direct-only lower bound ≈ 20s",
        out.elapsed
    );
}

/// 快直连（1 MiB/s/conn）+ 极慢代理（2 KiB/s/conn）：探索连接被抢占，
/// 不拖尾；主导标签 `direct:sampled`。直连单连接速率须让任务跨过首个完整
/// ramp 窗口（起步 4 条连接），否则冷路径根本不会被探索。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_proxy_never_drags_tail() {
    let body = Arc::new(gen_body(BODY_LEN, 0xB2));
    let origin = start_server(origin_cfg(&body, MIB)).await;
    let proxy = start_server(origin_cfg(&body, 2 * KIB)).await;

    let out = run_auto("slow_proxy", &origin, &proxy).await;
    eprintln!(
        "[slow_proxy] status={} elapsed={:?} origin={} proxy={} route={}",
        out.status,
        out.elapsed,
        origin.served(),
        proxy.served(),
        out.auto_route
    );

    assert_eq!(out.status, 3, "task must complete");
    assert_bytes_identical(&out.dest, &body).await;
    // 纯直连估计：24 MiB / (8 × 1 MiB/s) = 3s。
    let direct_only = Duration::from_secs_f64(BODY_LEN as f64 / (8.0 * MIB as f64));
    assert!(
        out.elapsed < direct_only + Duration::from_secs(8),
        "took {:?}, direct-only estimate {:?}",
        out.elapsed,
        direct_only
    );
    assert_eq!(out.auto_route, route::DIRECT_SAMPLED);
}

/// 代理发完响应头后永久卡死：连接被判定停滞放弃，段回到直连，任务按时完成。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stalled_proxy_is_abandoned() {
    let body = Arc::new(gen_body(BODY_LEN, 0xC3));
    let origin = start_server(origin_cfg(&body, MIB)).await;
    let proxy = start_server(ServerCfg {
        stall_after_headers: true,
        ..origin_cfg(&body, 0)
    })
    .await;

    let out = run_auto("stalled_proxy", &origin, &proxy).await;
    eprintln!(
        "[stalled_proxy] status={} elapsed={:?} origin={} proxy={} proxy_gets={} route={}",
        out.status,
        out.elapsed,
        origin.served(),
        proxy.served(),
        proxy.gets(),
        out.auto_route
    );

    assert_eq!(out.status, 3, "task must complete");
    assert_bytes_identical(&out.dest, &body).await;
    assert!(
        proxy.gets() > 0,
        "stalled proxy path must actually be explored"
    );
    assert_eq!(proxy.served(), 0, "stalled proxy never sends body bytes");
    assert!(
        out.elapsed < Duration::from_secs(45),
        "stalled proxy dragged the task: {:?}",
        out.elapsed
    );
}

/// 代理返回不同 ETag（且内容不同）：代理路径被踢出，文件内容取自源站，
/// 标签钉死为 `direct:pinned`。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn proxy_etag_mismatch_pins_direct() {
    let body = Arc::new(gen_body(BODY_LEN, 0xD4));
    let other = Arc::new(gen_body(BODY_LEN, 0xE5));
    let origin = start_server(origin_cfg(&body, MIB)).await;
    let proxy = start_server(ServerCfg {
        body: other,
        rate_bps: MIB,
        etag: "\"auto-mp-other\"".to_string(),
        stall_after_headers: false,
    })
    .await;

    let out = run_auto("etag_mismatch", &origin, &proxy).await;
    eprintln!(
        "[etag_mismatch] status={} elapsed={:?} origin={} proxy={} route={}",
        out.status,
        out.elapsed,
        origin.served(),
        proxy.served(),
        out.auto_route
    );

    assert_eq!(out.status, 3, "task must complete");
    assert_bytes_identical(&out.dest, &body).await;
    assert_eq!(out.auto_route, route::DIRECT_PINNED);
}
