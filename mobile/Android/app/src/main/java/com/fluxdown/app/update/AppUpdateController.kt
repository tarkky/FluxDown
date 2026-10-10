package com.fluxdown.app.update

import android.content.Context
import android.net.ConnectivityManager
import android.net.NetworkCapabilities
import android.util.Log
import com.fluxdown.app.data.DeviceSettings
import com.fluxdown.bridge.FluxBridge
import com.fluxdown.core.host.HostException
import com.fluxdown.core.update.AppUpdateConfig
import com.fluxdown.core.update.AppUpdatePort
import com.fluxdown.core.update.AppUpdateSignal
import com.fluxdown.core.update.AppUpdateStatus
import com.fluxdown.core.update.UpdateFailure
import com.fluxdown.core.update.UpdatePhase
import com.fluxdown.core.update.UpdatePolicy
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull

/** 安装会中断本机下载时的确认请求（[activeDownloads] = 进行中 + 排队的本机任务数）。 */
data class InstallInterrupt(val packagePath: String, val version: String, val activeDownloads: Int)

/**
 * 应用自更新控制器（进程级，挂在 `AppContainer`，惰性创建）。
 *
 * 状态机在 Rust（[AppUpdatePort]）；这里负责：信号 → [status]、安装请求 → 提交 [ApkInstaller]、前台 / 后台检查与自动下载、
 * 通知与一次性提示。永不在无用户点击时安装：[install] 只来自 UI 点按，后台路径（[backgroundCheck]）只检查 / 下载 / 通知。
 * 所有 Rust 调用的 [HostException] 在此捕获并记 warn，不让协程崩溃；失败态由 Rust 状态机经 [status] 呈现。
 */
class AppUpdateController internal constructor(
    private val context: Context,
    private val scope: CoroutineScope,
    private val settings: DeviceSettings,
    /** 本机进行中 + 排队的下载任务数（安装重启会中断它们）。 */
    private val localDownloads: () -> Int,
) {
    private val env = InstallEnvironment(context)
    private val installer = ApkInstaller(context, env)
    private val connectivity = context.getSystemService(ConnectivityManager::class.java)

    private val _status = MutableStateFlow(AppUpdateStatus())

    /** 最新更新状态（更新器首次启动前为 Idle 空状态）。 */
    val status: StateFlow<AppUpdateStatus> = _status.asStateFlow()

    private val _interrupt = MutableStateFlow<InstallInterrupt?>(null)

    /** 非空 = 需要用户确认「安装会中断下载」；UI 调 [confirmInterrupt] / [dismissInterrupt]。 */
    val interrupt: StateFlow<InstallInterrupt?> = _interrupt.asStateFlow()

    private val _needsInstallPermission = MutableStateFlow(false)

    /** true = 安装已就绪但系统尚未授予「安装未知应用」：UI 显示授权引导；授权返回前台自动重试。 */
    val needsInstallPermission: StateFlow<Boolean> = _needsInstallPermission.asStateFlow()

    private val _installedVersion = MutableStateFlow<String?>(null)

    /** 启动对账发现刚升级完成的版本；UI 提示后调 [consumeInstalledNotice]。 */
    val installedVersion: StateFlow<String?> = _installedVersion.asStateFlow()

    private val startLock = Mutex()
    private val opLock = Mutex()
    private var port: AppUpdatePort? = null

    /** 等待授权的安装包路径（仅主线程协程访问）。 */
    private var awaitingPermissionPath: String? = null

    /** 当前渠道：用户选择，否则按当前版本取默认（预览构建默认 frontier）。 */
    val channel: String get() = settings.updateChannelChoice ?: UpdatePolicy.defaultChannel(env.currentVersion)

    /** 设备当前网络是否非计量（无网络视为计量）。 */
    fun isUnmetered(): Boolean {
        val caps = connectivity.getNetworkCapabilities(connectivity.activeNetwork) ?: return false
        return caps.hasCapability(NetworkCapabilities.NET_CAPABILITY_NOT_METERED)
    }

    // ───────────────────────────── 用户操作 ─────────────────────────────

    /** 手动检查（忽略节流）。 */
    fun check() = launchOp { checkNow(it) }

    fun download() = launchOp { p -> call("download") { p.download() } }

    /** 用户点按「更新 / 安装」：包未就绪先下载，就绪后经 [AppUpdateSignal.InstallRequested] 进入安装。 */
    fun install() = launchOp { p -> call("install") { p.install() } }

    /** 取消下载；记下该版本，之后不再自动下载它。 */
    fun cancel() = launchOp { p ->
        val current = _status.value
        if (current.phase == UpdatePhase.Downloading) settings.updateDeclinedVersion = current.latestVersion
        _status.value = p.cancel()
    }

    fun setChannel(value: String) {
        if (value == channel) return
        settings.updateUpdateChannel(value)
        check()
    }

    /** 「跳过此版本」：不再提示该版本（仍可在设置页手动更新）。 */
    fun skipVersion(version: String) {
        settings.updateSkippedVersion = version
        UpdateNotifier.cancel(context)
    }

    fun confirmInterrupt() {
        val request = _interrupt.value ?: return
        _interrupt.value = null
        scope.launch { commit(request.packagePath) }
    }

    fun dismissInterrupt() {
        if (_interrupt.value == null) return
        _interrupt.value = null
        scope.launch { reportFailure(UpdateFailure.ElevationCancelled, "cancelled before install") }
    }

    /** 放弃等待授权的安装（用户不想授权）。 */
    fun abandonPendingInstall() {
        if (awaitingPermissionPath == null) return
        awaitingPermissionPath = null
        _needsInstallPermission.value = false
        scope.launch { reportFailure(UpdateFailure.ElevationCancelled, "install permission not granted") }
    }

    fun consumeInstalledNotice() {
        _installedVersion.value = null
    }

    /** [ApkInstaller] 的结果接收者回报：安装失败 / 用户取消。 */
    suspend fun onInstallFailed(failure: UpdateFailure, detail: String) {
        awaitingPermissionPath = null
        _needsInstallPermission.value = false
        reportFailure(failure, detail)
    }

    // ───────────────────────────── 生命周期 / 后台 ─────────────────────────────

    /** 进程回到前台：重试挂起的安装；按节流检查；条件允许自动下载。 */
    fun onForeground() {
        launchOp { p ->
            retryPendingInstall()
            if (UpdatePolicy.shouldCheck(System.currentTimeMillis(), settings.updateLastCheckMs, settings.updateAutoCheck)) {
                checkNow(p)
            }
            if (settings.updateAutoCheck) autoDownloadIfEligible(p)
        }
    }

    /**
     * JobScheduler 后台检查：节流检查 → 条件允许时下载并等待 Ready / Failed（[BACKGROUND_TIMEOUT_MS] 超时，
     * `.part` 保留续传）→ 有新版本且未提示 / 未跳过则发通知。从不安装。
     */
    suspend fun backgroundCheck() {
        val p = ensurePort() ?: return
        if (!settings.updateAutoCheck) return
        if (UpdatePolicy.shouldCheck(System.currentTimeMillis(), settings.updateLastCheckMs, true)) checkNow(p)
        if (autoDownloadIfEligible(p)) {
            withTimeoutOrNull(BACKGROUND_TIMEOUT_MS) {
                _status.first { it.phase != UpdatePhase.Downloading && it.phase != UpdatePhase.Available }
            }
        }
        val current = p.status()
        if (UpdatePolicy.shouldNotify(current, settings.updateNotifiedVersion, settings.updateSkippedVersion)) {
            settings.updateNotifiedVersion = current.latestVersion
            UpdateNotifier.notify(context, current)
        }
    }

    // ───────────────────────────── 内部 ─────────────────────────────

    private fun launchOp(block: suspend (AppUpdatePort) -> Unit) {
        scope.launch {
            val p = ensurePort() ?: return@launch
            try {
                block(p)
            } catch (e: HostException) {
                Log.w(TAG, "update operation failed: ${e.code} ${e.message}")
            }
        }
    }

    /** 调用 Rust 动作；失败（含网络）由状态机转 Failed，这里只记 warn。 */
    private suspend fun call(name: String, block: suspend () -> AppUpdateStatus) {
        try {
            _status.value = block()
        } catch (e: HostException) {
            Log.w(TAG, "$name failed: ${e.code} ${e.message}")
        }
    }

    private suspend fun checkNow(p: AppUpdatePort) = opLock.withLock {
        try {
            val result = p.check(channel)
            // 只有真正完成的检查才计入节流；失败（含离线）下次前台再试。
            settings.updateLastCheckMs = result.checkedAtMs.takeIf { it > 0 } ?: System.currentTimeMillis()
        } catch (e: HostException) {
            Log.w(TAG, "check failed: ${e.code} ${e.message}")
        }
    }

    /** @return 是否已发起（或已在进行）自动下载。 */
    private suspend fun autoDownloadIfEligible(p: AppUpdatePort): Boolean {
        val current = p.status()
        if (current.phase == UpdatePhase.Downloading) return true
        if (!UpdatePolicy.shouldAutoDownload(current, isUnmetered(), settings.updateWifiOnly, settings.updateDeclinedVersion)) return false
        return try {
            _status.value = p.download()
            true
        } catch (e: HostException) {
            Log.w(TAG, "auto download failed: ${e.code} ${e.message}")
            false
        }
    }

    private suspend fun ensurePort(): AppUpdatePort? = startLock.withLock {
        port?.let { return@withLock it }
        try {
            val config = withContext(Dispatchers.IO) {
                AppUpdateConfig(
                    dataDir = context.noBackupFilesDir.path,
                    currentVersion = env.currentVersion,
                    supportedAbis = env.supportedAbis,
                    manualReason = env.manualReason(),
                )
            }
            val p = FluxBridge.appUpdater(config)
            port = p
            _status.value = p.status()
            scope.launch { collectSignals(p) }
            scope.launch { reconcile(p) }
            p
        } catch (e: CancellationException) {
            throw e
        } catch (e: HostException) {
            Log.w(TAG, "updater unavailable: ${e.code} ${e.message}")
            null
        }
    }

    private suspend fun collectSignals(p: AppUpdatePort) {
        try {
            p.signals.collect { signal ->
                when (signal) {
                    is AppUpdateSignal.Status -> _status.value = signal.status
                    is AppUpdateSignal.InstallRequested -> onInstallRequested(signal.packagePath, signal.version)
                }
            }
        } catch (e: HostException) {
            Log.w(TAG, "update signals ended: ${e.code} ${e.message}")
        }
    }

    private suspend fun reconcile(p: AppUpdatePort) {
        try {
            p.reconcile()?.let {
                _installedVersion.value = it
                UpdateNotifier.cancel(context)
            }
        } catch (e: HostException) {
            Log.w(TAG, "reconcile failed: ${e.code} ${e.message}")
        }
    }

    private suspend fun onInstallRequested(path: String, version: String) {
        val busy = localDownloads()
        if (busy > 0) {
            _interrupt.value = InstallInterrupt(path, version, busy)
        } else {
            commit(path)
        }
    }

    private suspend fun commit(path: String) {
        when (val outcome = installer.install(path)) {
            InstallOutcome.Submitted -> {
                awaitingPermissionPath = null
                _needsInstallPermission.value = false
            }
            InstallOutcome.PermissionMissing -> {
                awaitingPermissionPath = path
                _needsInstallPermission.value = true
            }
            is InstallOutcome.Rejected -> {
                awaitingPermissionPath = null
                _needsInstallPermission.value = false
                reportFailure(outcome.failure, outcome.detail)
            }
        }
    }

    private suspend fun retryPendingInstall() {
        val path = awaitingPermissionPath ?: return
        if (installer.canRequestInstalls()) commit(path)
    }

    private suspend fun reportFailure(failure: UpdateFailure, detail: String) {
        val p = port ?: return
        try {
            _status.value = p.reportInstallFailure(failure, detail)
        } catch (e: HostException) {
            Log.w(TAG, "report install failure failed: ${e.code} ${e.message}")
        }
    }

    private companion object {
        const val TAG = "FluxDown.update"
        const val BACKGROUND_TIMEOUT_MS = 8L * 60 * 1000
    }
}
