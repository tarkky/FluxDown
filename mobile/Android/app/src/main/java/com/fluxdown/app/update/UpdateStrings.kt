package com.fluxdown.app.update

import androidx.annotation.StringRes
import androidx.compose.runtime.Composable
import com.fluxdown.app.R
import com.fluxdown.app.i18n.str
import com.fluxdown.core.update.UpdateLine
import com.fluxdown.core.update.UpdateText

/** `:core` 文案键 → 基线 i18n 字符串（`UpdateView` 的差异说明见其文件头）。 */
@StringRes
internal fun UpdateText.res(): Int = when (this) {
    UpdateText.Checking -> R.string.checking
    UpdateText.UpToDate -> R.string.upToDate
    UpdateText.NewVersionFound -> R.string.newVersionFound
    UpdateText.Downloading -> R.string.updateDownloadingProgress
    UpdateText.ReadyToInstall -> R.string.mobileUpdateReady
    UpdateText.Installing -> R.string.mobileUpdateInstalling
    UpdateText.FailedNetwork -> R.string.updateFailedNetwork
    UpdateText.FailedVerify -> R.string.updateFailedVerify
    UpdateText.FailedSignature -> R.string.mobileUpdateSignatureMismatch
    UpdateText.FailedStorage -> R.string.updateFailedStorage
    UpdateText.FailedInstall -> R.string.updateFailedInstall
    UpdateText.InstallCancelled -> R.string.mobileUpdateInstallCancelled
    UpdateText.FailedIncomplete -> R.string.updateFailedIncomplete
    UpdateText.FailedUnknown -> R.string.updateFailedUnknown
    UpdateText.ManualStore -> R.string.mobileUpdateManualStore
    UpdateText.ManualNotWritable -> R.string.updateManualNotWritable
    UpdateText.ManualNoAsset -> R.string.updateManualNoAsset
    UpdateText.ManualElevation -> R.string.updateManualElevation
    UpdateText.ManualReadOnly -> R.string.updateManualReadOnly
    UpdateText.ManualUnofficial -> R.string.updateManualUnofficial
    UpdateText.ManualUnsupported -> R.string.updateManualUnsupported
}

/** 渲染一条更新文案（`{v}` / `{percent}` 插值 + 尾注）。 */
@Composable
internal fun UpdateLine.render(): String =
    str(text.res(), "v" to version, "percent" to percent) + suffix.orEmpty()
