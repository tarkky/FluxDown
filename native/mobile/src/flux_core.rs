//! `FluxCore`：进程内唯一入口，拥有一个多线程 tokio runtime 与至多一个本机主机。
//!
//! 所有会话驱动、daemon、agent 都跑在这个 runtime 上；暴露给宿主语言的 async 方法只
//! `spawn` 到它并等待 `JoinHandle`，因此不依赖宿主侧 future 执行器的 tokio 上下文。

use std::sync::{Arc, OnceLock};

use tokio::runtime::{Builder, Handle, Runtime};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::driver::Timing;
use crate::error::FluxError;
use crate::local::{LocalHost, LocalHostConfig};
use crate::remote::RemoteConnector;
use crate::session::{HostSession, open_session};
use crate::update::{AppUpdateConfig, AppUpdater};

#[derive(uniffi::Object)]
pub struct FluxCore {
    /// `Drop` 时转后台关闭：宿主语言可能在任意（含异步）上下文里释放对象，直接丢弃
    /// runtime 会 panic。
    runtime: Option<Runtime>,
    local: Arc<Mutex<Option<LocalHost>>>,
    updater: OnceLock<Arc<AppUpdater>>,
}

impl FluxCore {
    fn handle(&self) -> Result<&Handle, FluxError> {
        self.runtime
            .as_ref()
            .map(Runtime::handle)
            .ok_or(FluxError::Closed)
    }
}

impl Drop for FluxCore {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

/// 在 `FluxCore` 的 runtime 上执行并等待结果。
async fn on_runtime<T, F>(handle: &Handle, future: F) -> Result<T, FluxError>
where
    T: Send + 'static,
    F: Future<Output = Result<T, FluxError>> + Send + 'static,
{
    handle
        .spawn(future)
        .await
        .map_err(|error| FluxError::internal(format!("core task failed: {error}")))?
}

#[uniffi::export]
impl FluxCore {
    #[uniffi::constructor]
    pub fn new() -> Result<Arc<Self>, FluxError> {
        let workers = std::thread::available_parallelism()
            .map_or(2, std::num::NonZero::get)
            .clamp(2, 4);
        let runtime = Builder::new_multi_thread()
            .worker_threads(workers)
            .thread_name("fluxdown-core")
            .enable_all()
            .build()
            .map_err(|error| FluxError::transport(format!("tokio runtime failed: {error}")))?;
        Ok(Arc::new(Self {
            runtime: Some(runtime),
            local: Arc::new(Mutex::new(None)),
            updater: OnceLock::new(),
        }))
    }

    /// 启动（仅一次）进程内 daemon + 嵌入式 agent，并返回连到它的会话。
    /// 已在运行时复用同一个本机主机（忽略 `config`）。
    pub async fn open_local(&self, config: LocalHostConfig) -> Result<Arc<HostSession>, FluxError> {
        let handle = self.handle()?.clone();
        let local = Arc::clone(&self.local);
        let runtime = handle.clone();
        on_runtime(&handle, async move {
            let mut guard = local.lock().await;
            if guard.is_none() {
                *guard = Some(LocalHost::start(&config).await?);
            }
            let Some(host) = guard.as_ref() else {
                return Err(FluxError::internal("local host missing after start"));
            };
            let (session, driver) = open_session(
                &runtime,
                host.connector(),
                host.session_token(),
                Timing::default(),
            )
            .await?;
            host.track_session(driver);
            Ok(session)
        })
        .await
    }

    /// `endpoint`：`http(s)://host:port` 或 `ws(s)://host:port`（规范化为 `ws(s)://…/rpc`）。
    /// 会完成连接、鉴权、握手与首个快照后才返回；密钥错误 / 主机不可达在此以错误返回。
    pub async fn open_remote(
        &self,
        endpoint: String,
        access_key: String,
    ) -> Result<Arc<HostSession>, FluxError> {
        let handle = self.handle()?.clone();
        let runtime = handle.clone();
        on_runtime(&handle, async move {
            let connector = Arc::new(RemoteConnector::new(&endpoint, &access_key)?);
            let (session, _driver) = open_session(
                &runtime,
                connector,
                CancellationToken::new(),
                Timing::default(),
            )
            .await?;
            Ok(session)
        })
        .await
    }

    /// App 自更新器（进程内唯一；首次调用的 `config` 生效）。与主机会话无关。
    pub fn app_updater(&self, config: AppUpdateConfig) -> Result<Arc<AppUpdater>, FluxError> {
        let handle = self.handle()?.clone();
        Ok(Arc::clone(
            self.updater
                .get_or_init(|| Arc::new(AppUpdater::new(handle, config))),
        ))
    }

    /// 停止本机主机（若在运行）：先放开所有本机会话，再停 agent 与 daemon。
    pub async fn shutdown_local(&self) {
        let Ok(handle) = self.handle() else {
            return;
        };
        let local = Arc::clone(&self.local);
        let stopped = on_runtime(handle, async move {
            let host = local.lock().await.take();
            if let Some(host) = host {
                host.shutdown().await;
            }
            Ok(())
        })
        .await;
        if let Err(error) = stopped {
            tracing::warn!(%error, "shutting down the local host failed");
        }
    }
}
