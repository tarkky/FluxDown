//! 应用自更新（原生 Android）：复用 agent 的 `UpdateService` 状态机（多站点检查 / 续传下载 /
//! SHA-256 校验 / 安装对账），只把安装交给 App（PackageInstaller）。
//!
//! 与主机会话无关：本机与远端模式下更新的都是 App 本身。调度（何时检查、何时自动下载）由
//! App 按前后台 / 网络计量 / 用户偏好决定；这里不起任何周期任务（空闲静默）。

use std::path::{Path, PathBuf};
use std::sync::Arc;

use fluxdown_agent::update::{
    DelegatedParts, InstallTarget, UpdateDelegate, UpdateError, UpdateService,
};
use fluxdown_protocol::{
    ReleaseNoteDto, UpdateFailure, UpdateManualReason, UpdatePhase, UpdateStatusDto,
};
use tokio::runtime::Handle;
use tokio::sync::{Mutex, mpsc, watch};

use crate::error::{ErrorCodeDto, FluxError};

/// 安装请求排队上限：安装请求只在用户确认后产生，正常情况下至多一个在途。
const INSTALL_QUEUE: usize = 4;

/// 自更新参数（来自 Kotlin）。
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct AppUpdateConfig {
    /// 应用私有、不参与备份的目录；更新包落在其 `updates/` 下。
    pub data_dir: String,
    /// 已安装版本（`PackageInfo.versionName`）；不可解析（开发构建）时只能手动升级。
    pub current_version: String,
    /// `Build.SUPPORTED_ABIS`（设备偏好顺序），决定下载哪个分包。
    pub supported_abis: Vec<String>,
    /// App 侧判定的不可自更新原因：商店安装 → `ManagedPackage`；可调试 / 非官方签名 → `UnofficialBuild`。
    pub manual_reason: Option<UpdateManualReasonDto>,
}

/// `UpdatePhase`。
#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum UpdatePhaseDto {
    Idle,
    Checking,
    UpToDate,
    Available,
    Downloading,
    Ready,
    Installing,
    Failed,
}

/// `UpdateManualReason`。
#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum UpdateManualReasonDto {
    ManagedPackage,
    NotWritable,
    NoAsset,
    ElevationUnavailable,
    ReadOnlyLocation,
    UnofficialBuild,
    Unsupported,
    Unknown,
}

/// `UpdateFailure`。`ElevationCancelled` 在 Android 上表示用户在系统安装确认中取消。
#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum UpdateFailureDto {
    Network,
    Verify,
    Storage,
    Install,
    ElevationCancelled,
    InstallIncomplete,
    Unknown,
}

/// `ReleaseNoteDto`。
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct AppReleaseNoteDto {
    pub version: String,
    pub published_at: String,
    pub body: String,
}

/// `UpdateStatusDto` 的 App 子集（安装形态恒为 Android APK，不下发）。
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct AppUpdateStatusDto {
    pub phase: UpdatePhaseDto,
    pub current_version: String,
    pub channel: String,
    pub latest_version: String,
    pub has_update: bool,
    pub manual_reason: Option<UpdateManualReasonDto>,
    pub asset_name: String,
    pub asset_size: u64,
    pub downloaded_bytes: u64,
    pub install_pending: bool,
    pub download_url: String,
    pub release_page_url: String,
    pub notes: Vec<AppReleaseNoteDto>,
    pub failure: Option<UpdateFailureDto>,
    pub error_detail: String,
    pub checked_at_ms: u64,
}

/// 自更新信号（拉取式）。
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum AppUpdateSignalDto {
    /// 状态变化（只保留最新；下载进度已节流）。
    Status { status: AppUpdateStatusDto },
    /// 已校验的 APK 就绪且用户已确认安装：App 提交系统安装会话；失败 / 取消经
    /// [`AppUpdater::report_install_failure`] 回报，成功时系统替换应用并结束进程。
    InstallRequested {
        package_path: String,
        version: String,
    },
}

struct Channels {
    status: watch::Sender<UpdateStatusDto>,
    installs: mpsc::Sender<(PathBuf, String)>,
}

impl UpdateDelegate for Channels {
    fn status_changed(&self, status: &UpdateStatusDto) {
        self.status
            .send_modify(|current| current.clone_from(status));
    }

    fn install_package(&self, package: &Path, version: &str) {
        if let Err(error) = self
            .installs
            .try_send((package.to_path_buf(), version.to_owned()))
        {
            tracing::warn!(%error, "update install request dropped");
        }
    }
}

struct Receivers {
    status: watch::Receiver<UpdateStatusDto>,
    installs: mpsc::Receiver<(PathBuf, String)>,
}

/// App 自更新器；经 [`crate::FluxCore::app_updater`] 取得，进程内唯一。
#[derive(uniffi::Object)]
pub struct AppUpdater {
    service: Arc<UpdateService>,
    runtime: Handle,
    receivers: Mutex<Receivers>,
}

impl AppUpdater {
    pub(crate) fn new(runtime: Handle, config: AppUpdateConfig) -> Self {
        let (status_tx, mut status_rx) = watch::channel(UpdateStatusDto::default());
        let (install_tx, install_rx) = mpsc::channel(INSTALL_QUEUE);
        let delegate = Arc::new(Channels {
            status: status_tx,
            installs: install_tx,
        });
        let service = Arc::new(UpdateService::delegated(DelegatedParts {
            data_dir: PathBuf::from(config.data_dir),
            current_version: config.current_version,
            target: InstallTarget::android(
                &config.supported_abis,
                config.manual_reason.map(UpdateManualReason::from),
            ),
            delegate: delegate.clone(),
            user_agent_product: "fluxdown-android",
        }));
        // 首个 `next_signal` 立即给出当前状态。
        delegate
            .status
            .send_modify(|current| *current = service.status());
        status_rx.mark_changed();
        Self {
            service,
            runtime,
            receivers: Mutex::new(Receivers {
                status: status_rx,
                installs: install_rx,
            }),
        }
    }

    /// 在 `FluxCore` 的 runtime 上执行（HTTP 客户端与下载任务都依赖 tokio 上下文）。
    async fn on_runtime<T, F>(&self, future: F) -> Result<T, FluxError>
    where
        T: Send + 'static,
        F: Future<Output = Result<T, UpdateError>> + Send + 'static,
    {
        self.runtime
            .spawn(future)
            .await
            .map_err(|error| FluxError::internal(format!("update task failed: {error}")))?
            .map_err(FluxError::from)
    }
}

#[uniffi::export]
impl AppUpdater {
    /// 拉取式信号流：首个为当前状态；安装请求优先于状态。
    pub async fn next_signal(&self) -> Option<AppUpdateSignalDto> {
        let mut receivers = self.receivers.lock().await;
        let Receivers { status, installs } = &mut *receivers;
        tokio::select! {
            biased;
            install = installs.recv() => install.map(|(path, version)| AppUpdateSignalDto::InstallRequested {
                package_path: path.to_string_lossy().into_owned(),
                version,
            }),
            changed = status.changed() => match changed {
                Ok(()) => Some(AppUpdateSignalDto::Status {
                    status: status.borrow_and_update().clone().into(),
                }),
                Err(_) => None,
            },
        }
    }

    pub fn status(&self) -> AppUpdateStatusDto {
        self.service.status().into()
    }

    /// 对账上次安装：返回刚完成升级的版本（App 据此提示「已更新到」），否则 `None`。
    pub async fn reconcile(&self) -> Result<Option<String>, FluxError> {
        let service = Arc::clone(&self.service);
        self.on_runtime(async move { Ok(service.reconcile().await) })
            .await
    }

    /// `channel`：`stable` | `frontier`。网络失败时状态转 `Failed(Network)` 并返回错误。
    pub async fn check(&self, channel: String) -> Result<AppUpdateStatusDto, FluxError> {
        let service = Arc::clone(&self.service);
        self.on_runtime(async move { service.check(&channel).await })
            .await
            .map(AppUpdateStatusDto::from)
    }

    /// 后台下载并校验（幂等）；完成后进入 `Ready`。
    pub async fn download(&self) -> Result<AppUpdateStatusDto, FluxError> {
        let service = Arc::clone(&self.service);
        self.on_runtime(async move { service.download().await })
            .await
            .map(AppUpdateStatusDto::from)
    }

    /// 用户确认安装：包未就绪时先下载（`installPending`），就绪后发出 `InstallRequested`。
    pub async fn install(&self) -> Result<AppUpdateStatusDto, FluxError> {
        let service = Arc::clone(&self.service);
        self.on_runtime(async move { service.install().await })
            .await
            .map(AppUpdateStatusDto::from)
    }

    /// 取消下载与待安装请求（`.part` 保留供续传）。
    pub fn cancel(&self) -> AppUpdateStatusDto {
        self.service.cancel().into()
    }

    /// 系统安装失败 / 用户取消。`Verify`（签名 / 包名 / 版本不符）同时丢弃该安装包。
    pub async fn report_install_failure(
        &self,
        failure: UpdateFailureDto,
        detail: String,
    ) -> Result<AppUpdateStatusDto, FluxError> {
        let service = Arc::clone(&self.service);
        self.on_runtime(async move {
            Ok(service
                .install_failed(UpdateFailure::from(failure), detail)
                .await)
        })
        .await
        .map(AppUpdateStatusDto::from)
    }
}

impl From<UpdateError> for FluxError {
    fn from(error: UpdateError) -> Self {
        let detail = format!("{error:#}");
        match error {
            UpdateError::InvalidChannel(_) => Self::invalid_argument(detail),
            UpdateError::Http(_)
            | UpdateError::Client(_)
            | UpdateError::Status(_)
            | UpdateError::Decode(_) => Self::transport(detail),
            UpdateError::NoUpdate => Self::rpc(ErrorCodeDto::Conflict, detail),
            UpdateError::Manual(_) => Self::rpc(ErrorCodeDto::Unsupported, detail),
        }
    }
}

impl From<UpdatePhase> for UpdatePhaseDto {
    fn from(phase: UpdatePhase) -> Self {
        match phase {
            UpdatePhase::Idle => Self::Idle,
            UpdatePhase::Checking => Self::Checking,
            UpdatePhase::UpToDate => Self::UpToDate,
            UpdatePhase::Available => Self::Available,
            UpdatePhase::Downloading => Self::Downloading,
            UpdatePhase::Ready => Self::Ready,
            UpdatePhase::Installing => Self::Installing,
            UpdatePhase::Failed => Self::Failed,
        }
    }
}

impl From<UpdateManualReason> for UpdateManualReasonDto {
    fn from(reason: UpdateManualReason) -> Self {
        match reason {
            UpdateManualReason::ManagedPackage => Self::ManagedPackage,
            UpdateManualReason::NotWritable => Self::NotWritable,
            UpdateManualReason::NoAsset => Self::NoAsset,
            UpdateManualReason::ElevationUnavailable => Self::ElevationUnavailable,
            UpdateManualReason::ReadOnlyLocation => Self::ReadOnlyLocation,
            UpdateManualReason::UnofficialBuild => Self::UnofficialBuild,
            UpdateManualReason::Unsupported => Self::Unsupported,
            UpdateManualReason::Unknown => Self::Unknown,
        }
    }
}

impl From<UpdateManualReasonDto> for UpdateManualReason {
    fn from(reason: UpdateManualReasonDto) -> Self {
        match reason {
            UpdateManualReasonDto::ManagedPackage => Self::ManagedPackage,
            UpdateManualReasonDto::NotWritable => Self::NotWritable,
            UpdateManualReasonDto::NoAsset => Self::NoAsset,
            UpdateManualReasonDto::ElevationUnavailable => Self::ElevationUnavailable,
            UpdateManualReasonDto::ReadOnlyLocation => Self::ReadOnlyLocation,
            UpdateManualReasonDto::UnofficialBuild => Self::UnofficialBuild,
            UpdateManualReasonDto::Unsupported => Self::Unsupported,
            UpdateManualReasonDto::Unknown => Self::Unknown,
        }
    }
}

impl From<UpdateFailure> for UpdateFailureDto {
    fn from(failure: UpdateFailure) -> Self {
        match failure {
            UpdateFailure::Network => Self::Network,
            UpdateFailure::Verify => Self::Verify,
            UpdateFailure::Storage => Self::Storage,
            UpdateFailure::Install => Self::Install,
            UpdateFailure::ElevationCancelled => Self::ElevationCancelled,
            UpdateFailure::InstallIncomplete => Self::InstallIncomplete,
            UpdateFailure::Unknown => Self::Unknown,
        }
    }
}

impl From<UpdateFailureDto> for UpdateFailure {
    fn from(failure: UpdateFailureDto) -> Self {
        match failure {
            UpdateFailureDto::Network => Self::Network,
            UpdateFailureDto::Verify => Self::Verify,
            UpdateFailureDto::Storage => Self::Storage,
            UpdateFailureDto::Install => Self::Install,
            UpdateFailureDto::ElevationCancelled => Self::ElevationCancelled,
            UpdateFailureDto::InstallIncomplete => Self::InstallIncomplete,
            UpdateFailureDto::Unknown => Self::Unknown,
        }
    }
}

impl From<ReleaseNoteDto> for AppReleaseNoteDto {
    fn from(note: ReleaseNoteDto) -> Self {
        Self {
            version: note.version,
            published_at: note.published_at,
            body: note.body,
        }
    }
}

impl From<UpdateStatusDto> for AppUpdateStatusDto {
    fn from(status: UpdateStatusDto) -> Self {
        Self {
            phase: status.phase.into(),
            current_version: status.current_version,
            channel: status.channel,
            latest_version: status.latest_version,
            has_update: status.has_update,
            manual_reason: status.manual_reason.map(Into::into),
            asset_name: status.asset_name,
            asset_size: status.asset_size,
            downloaded_bytes: status.downloaded_bytes,
            install_pending: status.install_pending,
            download_url: status.download_url,
            release_page_url: status.release_page_url,
            notes: status.notes.into_iter().map(Into::into).collect(),
            failure: status.failure.map(Into::into),
            error_detail: status.error_detail,
            checked_at_ms: status.checked_at_ms,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("fluxdown-app-upd-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn config(dir: &Path, version: &str) -> AppUpdateConfig {
        AppUpdateConfig {
            data_dir: dir.to_string_lossy().into_owned(),
            current_version: version.to_owned(),
            supported_abis: vec!["arm64-v8a".to_owned()],
            manual_reason: None,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn first_signal_is_current_status_and_manual_reason_passes_through() {
        let dir = temp_dir();
        let mut cfg = config(&dir, "1.0.0");
        cfg.manual_reason = Some(UpdateManualReasonDto::ManagedPackage);
        let updater = AppUpdater::new(Handle::current(), cfg);
        let Some(AppUpdateSignalDto::Status { status }) = updater.next_signal().await else {
            panic!("status signal expected");
        };
        assert_eq!(status.phase, UpdatePhaseDto::Idle);
        assert_eq!(status.current_version, "1.0.0");
        assert_eq!(
            status.manual_reason,
            Some(UpdateManualReasonDto::ManagedPackage)
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dev_version_is_unofficial_and_reconcile_without_record_is_none() {
        let dir = temp_dir();
        let updater = AppUpdater::new(Handle::current(), config(&dir, "0.1.0-dev+local"));
        // SemVer 可解析的开发版仍按版本比较；完全不可解析的版本才判为非官方构建。
        assert_eq!(updater.status().manual_reason, None);
        let dev = AppUpdater::new(Handle::current(), config(&dir, "dev"));
        assert_eq!(
            dev.status().manual_reason,
            Some(UpdateManualReasonDto::UnofficialBuild)
        );
        assert_eq!(updater.reconcile().await.unwrap(), None);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn install_failure_outside_installing_is_ignored() {
        let dir = temp_dir();
        let updater = AppUpdater::new(Handle::current(), config(&dir, "1.0.0"));
        let status = updater
            .report_install_failure(UpdateFailureDto::Install, "late".to_owned())
            .await
            .unwrap();
        assert_eq!(status.phase, UpdatePhaseDto::Idle);
        assert_eq!(status.failure, None);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
