package com.fluxdown.app.update

import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberUpdatedState
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.LocalResources
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import com.fluxdown.app.R
import com.fluxdown.app.data.DeviceSettings
import com.fluxdown.app.i18n.fill
import com.fluxdown.app.shell.LocalAppContainer
import com.fluxdown.core.update.UpdatePolicy
import com.fluxdown.fluxui.icons.FluxIcons
import com.fluxdown.fluxui.overlay.FluxDialogButton
import com.fluxdown.fluxui.overlay.FluxDialogButtonStyle
import com.fluxdown.fluxui.overlay.FluxDialogSpec
import com.fluxdown.fluxui.overlay.FluxToastAction
import com.fluxdown.fluxui.overlay.FluxToastKind
import com.fluxdown.fluxui.overlay.LocalFluxOverlays

/**
 * 应用更新的全局提示（壳层根部挂一次）：
 * - 有新版本且未被跳过：App 内 toast 一次（按版本去重，附「跳过此版本」）；
 * - 启动对账发现刚升级完成：toast「已更新到 vX」；
 * - 安装会中断本机下载：确认对话框（确认才继续安装）。
 */
@Composable
internal fun UpdateEffects() {
    val container = LocalAppContainer.current
    val overlays = LocalFluxOverlays.current
    val context = LocalContext.current
    val resources = LocalResources.current
    val controller = container.updates
    val settings = remember(context) { DeviceSettings.of(context) }
    val status by controller.status.collectAsStateWithLifecycle()
    val interrupt by controller.interrupt.collectAsStateWithLifecycle()
    val installed by controller.installedVersion.collectAsStateWithLifecycle()
    val latestOverlays by rememberUpdatedState(overlays)
    val latestResources by rememberUpdatedState(resources)

    LaunchedEffect(installed) {
        val version = installed ?: return@LaunchedEffect
        latestOverlays.toast(
            latestResources.getString(R.string.updateInstalledToast).fill("v" to version),
            FluxToastKind.Success,
        )
        controller.consumeInstalledNotice()
    }

    LaunchedEffect(status.phase, status.latestVersion, status.hasUpdate) {
        if (!UpdatePolicy.shouldNotify(status, settings.updateNotifiedVersion, settings.updateSkippedVersion)) return@LaunchedEffect
        val version = status.latestVersion
        settings.updateNotifiedVersion = version
        UpdateNotifier.cancel(context)
        latestOverlays.toast(
            latestResources.getString(R.string.updateAvailableToast).fill("v" to version),
            FluxToastKind.Accent,
            icon = FluxIcons.Download,
            action = FluxToastAction(latestResources.getString(R.string.skipThisVersion)) { controller.skipVersion(version) },
            durationMs = 6000L,
        )
    }

    LaunchedEffect(interrupt) {
        val request = interrupt ?: return@LaunchedEffect
        latestOverlays.showDialog(
            FluxDialogSpec(
                title = latestResources.getString(R.string.mobileUpdateInterruptTitle),
                message = latestResources.getString(R.string.mobileUpdateInterruptMessage).fill("n" to request.activeDownloads),
                icon = FluxIcons.TriangleAlert,
                buttons = listOf(
                    FluxDialogButton(latestResources.getString(R.string.cancel), FluxDialogButtonStyle.Secondary) {
                        controller.dismissInterrupt()
                    },
                    FluxDialogButton(latestResources.getString(R.string.mobileUpdateInstallButton), FluxDialogButtonStyle.Primary) {
                        controller.confirmInterrupt()
                    },
                ),
                onDismiss = { controller.dismissInterrupt() },
            ),
        )
    }
}
