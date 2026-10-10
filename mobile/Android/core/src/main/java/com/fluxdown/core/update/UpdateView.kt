package com.fluxdown.core.update

/**
 * 更新状态 → 文案与可用操作的纯判定。
 *
 * 逐条镜像桌面 `crates/settings/src/update_view.rs`（status_line / manual_line / can_install / can_cancel /
 * manual_url / download_percent / failure_key）。Android 差异只在文案：
 * - `Ready` 用「已下载，点按安装」（桌面是「重启即可完成」）；
 * - `Installing` 用「正在安装更新…」（桌面会重启）；
 * - `ElevationCancelled` 用「已取消安装」（Android = 系统安装确认被取消）；
 * - `ManagedPackage` 用「应用商店安装」说明（桌面按 Docker / NAS 安装形态区分）；
 * - `Verify` 且详情为签名不符（[UpdatePolicy.SIGNATURE_MISMATCH]）用专门的签名说明。
 * 键为 [UpdateText]，由 `:app` 映射到 `R.string`。
 */
enum class UpdateText {
    Checking, UpToDate, NewVersionFound, Downloading, ReadyToInstall, Installing,
    FailedNetwork, FailedVerify, FailedSignature, FailedStorage, FailedInstall, InstallCancelled, FailedIncomplete, FailedUnknown,
    ManualStore, ManualNotWritable, ManualNoAsset, ManualElevation, ManualReadOnly, ManualUnofficial, ManualUnsupported,
}

/** 一条待插值的文案：[version] 对应 `{v}`，[percent] 对应 `{percent}`，[suffix] 为尾注。 */
data class UpdateLine(
    val text: UpdateText,
    val version: String = "",
    val percent: Int = 0,
    val suffix: String? = null,
)

object UpdateView {
    /** 手动升级原因 → 说明文案。 */
    fun manualReasonText(reason: UpdateManualReason): UpdateText = when (reason) {
        UpdateManualReason.ManagedPackage -> UpdateText.ManualStore
        UpdateManualReason.NotWritable -> UpdateText.ManualNotWritable
        UpdateManualReason.NoAsset -> UpdateText.ManualNoAsset
        UpdateManualReason.ElevationUnavailable -> UpdateText.ManualElevation
        UpdateManualReason.ReadOnlyLocation -> UpdateText.ManualReadOnly
        UpdateManualReason.UnofficialBuild -> UpdateText.ManualUnofficial
        UpdateManualReason.Unsupported, UpdateManualReason.Unknown -> UpdateText.ManualUnsupported
    }

    /** 失败分类 → 文案；缺省按未知失败。 */
    fun failureText(failure: UpdateFailure?, detail: String = ""): UpdateText = when (failure) {
        UpdateFailure.Network -> UpdateText.FailedNetwork
        UpdateFailure.Verify ->
            if (detail.startsWith(UpdatePolicy.SIGNATURE_MISMATCH)) UpdateText.FailedSignature else UpdateText.FailedVerify
        UpdateFailure.Storage -> UpdateText.FailedStorage
        UpdateFailure.Install -> UpdateText.FailedInstall
        UpdateFailure.ElevationCancelled -> UpdateText.InstallCancelled
        UpdateFailure.InstallIncomplete -> UpdateText.FailedIncomplete
        UpdateFailure.Unknown, null -> UpdateText.FailedUnknown
    }

    /** 下载进度百分比（0–100）；大小未知时为 0。 */
    fun downloadPercent(status: AppUpdateStatus): Int {
        if (status.assetSize <= 0) return 0
        val done = status.downloadedBytes.coerceIn(0, status.assetSize)
        return (done * 100 / status.assetSize).toInt()
    }

    /** 当前阶段的状态行；尚未检查（Idle）或无信息可说时为 null。 */
    fun statusLine(status: AppUpdateStatus): UpdateLine? {
        val latest = status.latestVersion
        return when (status.phase) {
            UpdatePhase.Idle -> null
            UpdatePhase.Checking -> UpdateLine(UpdateText.Checking)
            UpdatePhase.UpToDate -> UpdateLine(UpdateText.UpToDate, suffix = if (latest.isEmpty()) null else " (v$latest)")
            UpdatePhase.Available ->
                if (status.hasUpdate) UpdateLine(UpdateText.NewVersionFound, version = latest) else null
            UpdatePhase.Downloading -> UpdateLine(UpdateText.Downloading, version = latest, percent = downloadPercent(status))
            UpdatePhase.Ready -> UpdateLine(UpdateText.ReadyToInstall, version = latest)
            UpdatePhase.Installing -> UpdateLine(UpdateText.Installing)
            UpdatePhase.Failed -> UpdateLine(failureText(status.failure, status.errorDetail))
        }
    }

    /** 不可一键更新时的手动升级说明（仅在确有新版本时）。 */
    fun manualLine(status: AppUpdateStatus): UpdateLine? {
        if (!status.hasUpdate) return null
        return status.manualReason?.let { UpdateLine(manualReasonText(it)) }
    }

    /** 是否提供「更新」：有新版本、可一键更新，且当前阶段允许发起安装；下载中但已点过安装时不再重复提供。 */
    fun canInstall(status: AppUpdateStatus): Boolean =
        status.hasUpdate && status.manualReason == null && when (status.phase) {
            UpdatePhase.Available, UpdatePhase.Ready, UpdatePhase.Failed -> true
            UpdatePhase.Downloading -> !status.installPending
            else -> false
        }

    /** 下载中可取消。 */
    fun canCancel(status: AppUpdateStatus): Boolean = status.phase == UpdatePhase.Downloading

    /** 手动升级的打开地址：优先资产直链，其次发布页；无新版本或非手动时为 null。 */
    fun manualUrl(status: AppUpdateStatus): String? {
        if (!status.hasUpdate || status.manualReason == null) return null
        return listOf(status.downloadUrl, status.releasePageUrl).firstOrNull { it.isNotEmpty() }
    }

    /**
     * 按系统语言取更新说明的语言区块（镜像 GPUI `sections/about.rs::localized_release_body`）：
     * 正文以 `<!-- fluxdown:lang:zh -->` / `<!-- fluxdown:lang:en -->` 分段；中文系统取中文，其余取英文，
     * 缺目标语言时用已有语言，无标记的历史说明原样返回。[locale] 为语言标签（如 `zh-Hans-CN`、`en_US`）。
     */
    fun localizedReleaseBody(body: String, locale: String): String {
        val chinese = locale.trim().split('-', '_', '.', '@').first().equals("zh", ignoreCase = true)
        val sections = arrayOfNulls<String>(2)
        var previous: Pair<Int, Int>? = null
        var cursor = 0
        while (true) {
            val start = body.indexOf("<!--", cursor)
            if (start < 0) break
            val contentStart = start + 4
            val contentEnd = body.indexOf("-->", contentStart)
            if (contentEnd < 0) break
            cursor = contentEnd + 3
            val index = when (body.substring(contentStart, contentEnd).trim()) {
                "fluxdown:lang:zh" -> 0
                "fluxdown:lang:en" -> 1
                else -> continue
            }
            previous?.let { (i, sectionStart) -> sections[i] = body.substring(sectionStart, start).trim() }
            previous = index to cursor
        }
        previous?.let { (i, sectionStart) -> sections[i] = body.substring(sectionStart).trim() }
        return sections[if (chinese) 0 else 1] ?: sections[0] ?: sections[1] ?: body
    }
}
