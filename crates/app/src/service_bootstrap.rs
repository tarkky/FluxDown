//! GPUI 对同级 `fluxdown-agent` 的单飞启动与异步回收。

use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::sync::Mutex;

#[derive(Default)]
struct BootstrapState {
    generation: u64,
    running: bool,
    reapers: Vec<tokio::task::JoinHandle<()>>,
}

pub struct ServiceBootstrap {
    state: Arc<Mutex<BootstrapState>>,
    /// 完全退出或 agent 已声明退出后置位：此后连接拒绝不再拉起 agent。
    stopped: AtomicBool,
    /// 本进程曾拉起过 agent（即本次启动是 FluxDown 服务的冷启动）。
    spawned: AtomicBool,
}

impl ServiceBootstrap {
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(BootstrapState::default())),
            stopped: AtomicBool::new(false),
            spawned: AtomicBool::new(false),
        }
    }

    /// 永久停止拉起 agent（不可恢复）。
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
    }

    /// 本进程是否拉起过 agent：界面启动时据此区分「冷启动服务」与「打开已驻留的应用」。
    #[must_use]
    pub fn spawned_agent(&self) -> bool {
        self.spawned.load(Ordering::Acquire)
    }

    /// 仅由 connection-refused/no-listener 路径调用；返回本次是否新拉起了 agent。
    ///
    /// `probe_listener` 为 true（本机尚无 bearer）时先探测目标端口：另一桌面进程或直接启动的
    /// agent 可能已监听、只是还没写出 bearer，此时拉起的子进程会立即因独占锁退出。连接已被
    /// 拒绝时此刻确定无人监听，跳过探测直接拉起。
    pub async fn ensure_running(
        &self,
        rpc_url: &str,
        probe_listener: bool,
    ) -> Result<bool, BootstrapError> {
        let mut state = self.state.lock().await;
        state.reapers.retain(|task| !task.is_finished());
        if state.running || self.stopped.load(Ordering::Acquire) {
            return Ok(false);
        }
        if probe_listener && agent_is_listening(rpc_url).await? {
            return Ok(false);
        }
        let mut command = agent_command()?;
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let program = command.get_program().to_string_lossy().into_owned();
        let mut child = tokio::process::Command::from(command)
            .spawn()
            .map_err(|error| BootstrapError::Spawn(format!("{program}: {error:#}")))?;
        state.generation = state.generation.saturating_add(1);
        let generation = state.generation;
        state.running = true;
        self.spawned.store(true, Ordering::Release);
        log::info!(
            "spawned fluxdown-agent {program} (generation {generation}, pid {:?})",
            child.id()
        );
        let spawned_at = std::time::Instant::now();
        let bootstrap_state = self.state.clone();
        state.reapers.push(tokio::spawn(async move {
            // agent 常驻、与界面解耦，通常比界面活得久；先于界面退出（崩溃、锁冲突）才会记到。
            // macOS 经 `open -g` 拉起时这里等的是 `open` 本身，立即成功退出属正常。
            match child.wait().await {
                Ok(status) if status.success() => log::info!(
                    "fluxdown-agent (generation {generation}) exited after {}s",
                    spawned_at.elapsed().as_secs()
                ),
                Ok(status) => log::warn!(
                    "fluxdown-agent (generation {generation}) exited with {status} after {}s",
                    spawned_at.elapsed().as_secs()
                ),
                Err(error) => log::warn!("failed to reap fluxdown-agent: {error}"),
            }
            let mut state = bootstrap_state.lock().await;
            if state.generation == generation {
                state.running = false;
            }
        }));
        Ok(true)
    }
}

/// 回环上连接 agent 端口的上限。端口在监听时握手由内核即时完成（亚毫秒级，不依赖 agent 是否已
/// 开始 accept）；Windows 对未监听的回环端口收到 RST 后还会重传 SYN（约 0.5s + 1s），`connect`
/// 要约 2s 才返回 `ConnectionRefused`。超时即按「无人监听」处理，冷启动不再白等这 2s；极端负载
/// 下的误判只会多拉起一个因 agent 数据目录独占锁立即退出的进程。
const LOOPBACK_CONNECT_TIMEOUT: Duration = Duration::from_millis(300);

/// 连接 agent 监听端口；`Ok(None)` 表示此刻无人监听（被拒或超时）。
pub(crate) async fn connect_listener(rpc_url: &str) -> Result<Option<TcpStream>, BootstrapError> {
    let target = agent_socket_target(rpc_url)?;
    match tokio::time::timeout(LOOPBACK_CONNECT_TIMEOUT, TcpStream::connect(target)).await {
        Ok(Ok(stream)) => Ok(Some(stream)),
        Ok(Err(error))
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
            ) =>
        {
            Ok(None)
        }
        Ok(Err(error)) => Err(BootstrapError::Probe(error.to_string())),
        Err(_) => Ok(None),
    }
}

async fn agent_is_listening(rpc_url: &str) -> Result<bool, BootstrapError> {
    Ok(connect_listener(rpc_url).await?.is_some())
}

/// 等被替换的旧 agent 关闭监听（关停 daemon 后退出）；超时后交回重连循环自愈。
pub async fn wait_until_stopped(rpc_url: &str, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if matches!(agent_is_listening(rpc_url).await, Ok(false)) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn agent_socket_target(rpc_url: &str) -> Result<std::net::SocketAddr, BootstrapError> {
    crate::agent_endpoint::socket_target(rpc_url)
        .map_err(|error| BootstrapError::Probe(error.to_string()))
}
/// macOS 打包布局中 agent 所在的辅助 bundle（相对外层 `Contents/`）；与
/// `fluxdown_agent::platform` 的布局约定及 `scripts/package_gpui_macos.sh` 保持一致。
#[cfg(target_os = "macos")]
const MACOS_AGENT_HELPER_APP: &str = "Helpers/FluxDownAgent.app";
#[cfg(target_os = "macos")]
const MACOS_AGENT_HELPER_EXE: &str = "Contents/MacOS/fluxdown-agent";

/// 拉起 agent 的命令。
///
/// macOS 打包布局下经 Launch Services（`open -g`）启动辅助 bundle：直接 spawn 的
/// agent 会被系统记为桌面 App 的附属进程，桌面退出后 Dock 仍以
/// `exited-with-subordinates` 保留其图标，直到托盘驻留的 agent 退出。`open` 在辅助
/// App 已运行时不会再起第二个实例，`-g` 不抢前台。
fn agent_command() -> Result<std::process::Command, std::io::Error> {
    #[cfg(target_os = "macos")]
    if std::env::var_os("FLUXDOWN_AGENT_BIN").is_none()
        && let Some(helper_app) = bundled_agent_app(&std::env::current_exe()?)
    {
        let mut command = std::process::Command::new("/usr/bin/open");
        command.arg("-g").arg(helper_app);
        return Ok(command);
    }
    let mut command = std::process::Command::new(agent_executable()?);
    detach_background_process(&mut command);
    Ok(command)
}

/// agent 可执行文件：`FLUXDOWN_AGENT_BIN` 覆盖，否则取桌面程序同级（macOS 打包布局由
/// [`agent_command`] 经辅助 bundle 启动，不走这里）。
pub(crate) fn agent_executable() -> Result<std::path::PathBuf, std::io::Error> {
    if let Some(path) = std::env::var_os("FLUXDOWN_AGENT_BIN") {
        return Ok(path.into());
    }
    Ok(std::env::current_exe()?.with_file_name(if cfg!(windows) {
        "fluxdown-agent.exe"
    } else {
        "fluxdown-agent"
    }))
}

/// 桌面程序位于 `<App>.app/Contents/MacOS/` 且辅助 bundle 内存在 agent 时返回辅助
/// bundle 路径；开发期平铺布局返回 `None`，回退同级查找。
#[cfg(target_os = "macos")]
fn bundled_agent_app(desktop_exe: &std::path::Path) -> Option<std::path::PathBuf> {
    let macos_dir = desktop_exe.parent()?;
    if !macos_dir.ends_with("Contents/MacOS") {
        return None;
    }
    let helper_app = macos_dir.parent()?.join(MACOS_AGENT_HELPER_APP);
    helper_app
        .join(MACOS_AGENT_HELPER_EXE)
        .is_file()
        .then_some(helper_app)
}

/// 后台服务与界面解耦：Windows 不弹控制台窗；Unix 进入独立进程组，终端里对桌面程序的
/// Ctrl-C 不连带终止常驻 agent。
#[cfg(windows)]
fn detach_background_process(command: &mut std::process::Command) {
    use std::os::windows::process::CommandExt;
    command.creation_flags(0x0800_0000);
}

#[cfg(unix)]
fn detach_background_process(command: &mut std::process::Command) {
    use std::os::unix::process::CommandExt;
    command.process_group(0);
}

#[cfg(not(any(windows, unix)))]
fn detach_background_process(_command: &mut std::process::Command) {}

#[derive(Debug, thiserror::Error)]
pub enum BootstrapError {
    #[error("could not locate fluxdown-agent: {0}")]
    Locate(#[from] std::io::Error),
    #[error("could not spawn fluxdown-agent: {0}")]
    Spawn(String),
    #[error("could not probe fluxdown-agent listener: {0}")]
    Probe(String),
}
#[cfg(test)]
mod tests {
    use super::{agent_socket_target, connect_listener};

    #[test]
    fn listener_probe_uses_loopback_without_dns_and_accepts_ipv6() {
        assert_eq!(
            agent_socket_target("ws://localhost:17800/rpc").expect("localhost target"),
            "127.0.0.1:17800"
                .parse::<std::net::SocketAddr>()
                .expect("IPv4 address")
        );
        assert_eq!(
            agent_socket_target("ws://[::1]:17800/rpc").expect("IPv6 target"),
            "[::1]:17800"
                .parse::<std::net::SocketAddr>()
                .expect("IPv6 address")
        );
        assert!(agent_socket_target("ws://192.0.2.1:17800/rpc").is_err());
    }

    /// 冷启动时 agent 先绑定端口、装配完才开始 accept：已绑定但尚未 accept 的端口必须算「在监听」，
    /// 否则桌面会在装配期间重复拉起 agent；端口释放后必须立即判为无人监听。
    #[tokio::test]
    async fn bound_port_counts_as_listening_before_accept_and_not_after_close() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let url = format!("ws://{}/rpc", listener.local_addr().expect("local addr"));
        assert!(
            connect_listener(&url)
                .await
                .expect("probe bound port")
                .is_some()
        );
        drop(listener);
        assert!(
            connect_listener(&url)
                .await
                .expect("probe released port")
                .is_none()
        );
    }
}
