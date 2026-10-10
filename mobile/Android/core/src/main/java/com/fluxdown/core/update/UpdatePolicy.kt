package com.fluxdown.core.update

/** 更新的调度 / 提示 / 安装结果判定（纯函数，无平台依赖）。 */
object UpdatePolicy {
    /** 前台自动检查的最短间隔：6 小时。 */
    const val CHECK_INTERVAL_MS = 6L * 60 * 60 * 1000

    /** [ApkInstaller][com.fluxdown.app.update.ApkInstaller] 预检发现签名证书不符时的详情前缀（[UpdateView.failureText] 据此选文案）。 */
    const val SIGNATURE_MISMATCH = "signature mismatch"

    // PackageInstaller.STATUS_*（值固定于平台 API；:core 不依赖 android）。
    private const val STATUS_PENDING_USER_ACTION = -1 // STATUS_PENDING_USER_ACTION
    private const val STATUS_SUCCESS = 0 // STATUS_SUCCESS
    private const val STATUS_FAILURE_ABORTED = 3 // STATUS_FAILURE_ABORTED
    private const val STATUS_FAILURE_STORAGE = 6 // STATUS_FAILURE_STORAGE

    const val CHANNEL_STABLE = "stable"
    const val CHANNEL_FRONTIER = "frontier"

    /** 默认更新渠道：当前版本带 `-rc` 预发布后缀（预览构建）→ frontier，否则 stable。 */
    fun defaultChannel(versionName: String): String =
        if (versionName.contains("-rc")) CHANNEL_FRONTIER else CHANNEL_STABLE

    /** 前台检查节流：开关关闭不检查；从未检查（[lastCheckMs] ≤ 0）立即检查；否则距上次满 [CHECK_INTERVAL_MS]。 */
    fun shouldCheck(nowMs: Long, lastCheckMs: Long, autoCheck: Boolean): Boolean {
        if (!autoCheck) return false
        if (lastCheckMs <= 0) return true
        return nowMs - lastCheckMs >= CHECK_INTERVAL_MS
    }

    /** 自动下载：有可一键更新的新版本、用户没在下载中取消过该版本，且网络允许（非计量，或未限制仅 Wi-Fi）。 */
    fun shouldAutoDownload(status: AppUpdateStatus, unmetered: Boolean, wifiOnly: Boolean, declinedVersion: String): Boolean =
        status.phase == UpdatePhase.Available &&
            status.hasUpdate &&
            status.manualReason == null &&
            status.latestVersion != declinedVersion &&
            (unmetered || !wifiOnly)

    /**
     * 是否提示用户有新版本（通知 / 应用内 toast）：确有新版本、未被跳过、同一版本只提示一次。
     * 阶段为 Available / Downloading / Ready 之一（用户还有事可做）；手动升级原因也提示（可前往官网）。
     */
    fun shouldNotify(status: AppUpdateStatus, notifiedVersion: String, skippedVersion: String): Boolean {
        if (!status.hasUpdate || status.latestVersion.isEmpty()) return false
        if (status.phase !in NOTIFY_PHASES) return false
        return status.latestVersion != notifiedVersion && status.latestVersion != skippedVersion
    }

    /** PackageInstaller 结果码 → 回报给 Rust 的失败分类；成功 / 待确认码返回 null。 */
    fun installFailureFor(statusCode: Int): UpdateFailure? = when (statusCode) {
        STATUS_FAILURE_ABORTED -> UpdateFailure.ElevationCancelled
        STATUS_FAILURE_STORAGE -> UpdateFailure.Storage
        STATUS_SUCCESS, STATUS_PENDING_USER_ACTION -> null
        // FAILURE / BLOCKED / INVALID / CONFLICT / INCOMPATIBLE 及未来新增的失败码一律按安装失败。
        else -> UpdateFailure.Install
    }

    private val NOTIFY_PHASES = setOf(UpdatePhase.Available, UpdatePhase.Downloading, UpdatePhase.Ready)
}
