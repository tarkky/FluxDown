package com.fluxdown.app.feature.settings

import android.net.Uri
import android.provider.Settings
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.remember
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalConfiguration
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.unit.dp
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import com.fluxdown.app.R
import com.fluxdown.app.data.DeviceSettings
import com.fluxdown.app.i18n.str
import com.fluxdown.app.shell.LocalAppContainer
import com.fluxdown.app.update.UpdateCheckJobService
import com.fluxdown.app.update.render
import com.fluxdown.core.format.Format
import com.fluxdown.core.update.AppUpdateStatus
import com.fluxdown.core.update.UpdatePhase
import com.fluxdown.core.update.UpdatePolicy
import com.fluxdown.core.update.UpdateView
import com.fluxdown.fluxui.controls.ButtonSize
import com.fluxdown.fluxui.controls.ButtonVariant
import com.fluxdown.fluxui.controls.FluxButton
import com.fluxdown.fluxui.controls.FluxListRow
import com.fluxdown.fluxui.controls.FluxProgressLine
import com.fluxdown.fluxui.controls.FluxSwitchRow
import com.fluxdown.fluxui.controls.GlassSection
import com.fluxdown.fluxui.controls.SelectOption
import com.fluxdown.fluxui.icons.FluxIcons
import com.fluxdown.fluxui.overlay.FluxDialogButton
import com.fluxdown.fluxui.overlay.FluxDialogButtonStyle
import com.fluxdown.fluxui.overlay.FluxDialogSpec
import com.fluxdown.fluxui.overlay.LocalFluxOverlays
import com.fluxdown.fluxui.theme.FluxText
import com.fluxdown.fluxui.theme.FluxTheme

/** 更新说明折叠时显示 1 条、展开最多显示的条数。 */
private const val NOTES_EXPANDED_MAX = 5

/**
 * 关于页「软件更新」分组：状态 / 下载进度 / 主操作 / 手动说明 / 更新说明 / 自动检查 · 仅非计量网络自动下载 · 渠道。
 * 文案与可用操作由 `:core` 的 [UpdateView] 判定（镜像桌面 `update_view.rs`）；状态机在 Rust，这里只呈现与转发点按。
 * 与主机无关：远端主机模式下同样更新本机 App。
 */
@Composable
internal fun UpdateSection(ctx: SettingsCtx, openPicker: (PickerSpec) -> Unit) {
    val context = LocalContext.current
    val overlays = LocalFluxOverlays.current
    val controller = LocalAppContainer.current.updates
    val settings = remember(context) { DeviceSettings.of(context) }
    val status by controller.status.collectAsStateWithLifecycle()
    val needsPermission by controller.needsInstallPermission.collectAsStateWithLifecycle()
    val c = FluxTheme.colors
    val t = FluxTheme.type
    var notesExpanded by rememberSaveable { mutableStateOf(false) }

    val statusText = UpdateView.statusLine(status)?.render()
    val manualText = UpdateView.manualLine(status)?.render()
    val manualUrl = UpdateView.manualUrl(status)
    val noBrowser = str(R.string.mobileNoBrowser)
    val noSettings = str(R.string.mobileNoSettingsApp)
    val channelName = str(R.string.updateChannel)
    val channelOptions = listOf(
        SelectOption(UpdatePolicy.CHANNEL_STABLE, str(R.string.updateChannelStable)),
        SelectOption(UpdatePolicy.CHANNEL_FRONTIER, str(R.string.updateChannelFrontier)),
    )
    val channel = controller.channel
    val sizeText = Format.bytes(status.assetSize).toString()
    val meteredTitle = str(R.string.mobileUpdateMeteredTitle)
    val meteredMessage = str(R.string.mobileUpdateMeteredMessage, "size" to sizeText)
    val downloadLabel = str(R.string.downloadUpdate, "size" to sizeText)
    val cancelLabel = str(R.string.cancel)
    // 更新说明按系统语言取区块（同 GPUI 关于页：跟随系统语言，不跟随手动界面语言）。
    val locale = LocalConfiguration.current.locales[0].toLanguageTag()
    val notes = remember(status.notes, notesExpanded, locale) {
        status.notes.take(if (notesExpanded) NOTES_EXPANDED_MAX else 1)
            .map { it.copy(body = UpdateView.localizedReleaseBody(it.body, locale)) }
    }

    GlassSection(title = str(R.string.softwareUpdate)) {
        row {
            SettingRowBox(ctx, "about.update", null) {
                FluxListRow(
                    title = statusText ?: str(R.string.checkUpdate),
                    subtitle = if (statusText == null) str(R.string.checkUpdateDesc) else null,
                )
            }
        }
        if (status.phase == UpdatePhase.Downloading) {
            custom(padded = true) { FluxProgressLine(UpdateView.downloadPercent(status) / 100f) }
        }
        if (manualText != null) {
            custom(padded = true) { FluxText(manualText, style = t.sm, color = c.inkMuted) }
        }
        if (needsPermission) {
            custom(padded = true) {
                Column(verticalArrangement = Arrangement.spacedBy(10.dp)) {
                    FluxText(str(R.string.mobileUpdatePermissionNeeded), style = t.sm, color = c.amberText)
                    Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                        FluxButton(
                            text = str(R.string.mobileUpdateGrantPermission),
                            onClick = {
                                val intent = android.content.Intent(
                                    Settings.ACTION_MANAGE_UNKNOWN_APP_SOURCES,
                                    Uri.parse("package:${context.packageName}"),
                                )
                                startSettingsIntent(context, overlays, intent, noSettings)
                            },
                            variant = ButtonVariant.Primary,
                            size = ButtonSize.Sm,
                        )
                        FluxButton(
                            text = str(R.string.cancel),
                            onClick = controller::abandonPendingInstall,
                            variant = ButtonVariant.Ghost,
                            size = ButtonSize.Sm,
                        )
                    }
                }
            }
        }
        custom(padded = true) {
            UpdateActions(
                status = status,
                manualUrl = manualUrl,
                onCheck = controller::check,
                onInstall = {
                    // 计量网络下手动下载前确认流量（包已就绪则直接进入安装）。
                    if (status.phase != UpdatePhase.Ready && status.assetSize > 0 && !controller.isUnmetered()) {
                        overlays.showDialog(
                            FluxDialogSpec(
                                title = meteredTitle,
                                message = meteredMessage,
                                icon = FluxIcons.TriangleAlert,
                                buttons = listOf(
                                    FluxDialogButton(cancelLabel, FluxDialogButtonStyle.Secondary),
                                    FluxDialogButton(downloadLabel, FluxDialogButtonStyle.Primary) { controller.install() },
                                ),
                            ),
                        )
                    } else {
                        controller.install()
                    }
                },
                onCancel = controller::cancel,
                onOpenSite = { url -> openLink(context, overlays, url, noBrowser) },
            )
        }
        if (status.hasUpdate && notes.isNotEmpty()) {
            custom(padded = true) {
                Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
                    FluxText(str(R.string.mobileUpdateNotesTitle), style = t.sm, color = c.inkMuted)
                    for (note in notes) {
                        FluxText("v${note.version}", style = t.mono, color = c.ink)
                        if (note.body.isNotBlank()) {
                            FluxText(note.body.trim(), style = t.sm, color = c.inkMuted, maxLines = if (notesExpanded) Int.MAX_VALUE else 6)
                        }
                    }
                    if (status.notes.size > 1 || notes.first().body.length > 200) {
                        FluxButton(
                            text = str(if (notesExpanded) R.string.mobileUpdateNotesCollapse else R.string.mobileUpdateNotesExpand),
                            onClick = { notesExpanded = !notesExpanded },
                            variant = ButtonVariant.Ghost,
                            size = ButtonSize.Xs,
                        )
                    }
                }
            }
        }
        row {
            SettingRowBox(ctx, "about.update.auto", null) {
                FluxSwitchRow(
                    title = str(R.string.autoCheckUpdate),
                    subtitle = str(R.string.autoCheckUpdateDesc),
                    checked = settings.updateAutoCheck,
                    onCheckedChange = { on ->
                        settings.updateUpdateAutoCheck(on)
                        UpdateCheckJobService.sync(context, on)
                    },
                )
            }
        }
        row {
            SettingRowBox(ctx, "about.update.wifiOnly", null) {
                FluxSwitchRow(
                    title = str(R.string.mobileUpdateWifiOnly),
                    subtitle = str(R.string.mobileUpdateWifiOnlyDesc),
                    checked = settings.updateWifiOnly,
                    enabled = settings.updateAutoCheck,
                    onCheckedChange = settings::updateUpdateWifiOnly,
                )
            }
        }
        row {
            SettingRowBox(ctx, "about.update.channel", null) {
                FluxListRow(
                    title = channelName,
                    subtitle = str(R.string.updateChannelDesc),
                    value = channelOptions.firstOrNull { it.value == channel }?.label ?: channel,
                    chevron = true,
                    onClick = { openPicker(PickerSpec(channelName, channelOptions, channel) { controller.setChannel(it) }) },
                )
            }
        }
    }
}

/** 主操作区：按 [UpdateView] 的判定显示 更新 / 安装 / 取消 / 前往官网 / 检查。 */
@Composable
private fun UpdateActions(
    status: AppUpdateStatus,
    manualUrl: String?,
    onCheck: () -> Unit,
    onInstall: () -> Unit,
    onCancel: () -> Unit,
    onOpenSite: (String) -> Unit,
) {
    val canInstall = UpdateView.canInstall(status)
    val checking = status.phase == UpdatePhase.Checking
    val canCheck = status.phase in setOf(UpdatePhase.Idle, UpdatePhase.UpToDate, UpdatePhase.Available, UpdatePhase.Failed)
    Column(Modifier.fillMaxWidth(), verticalArrangement = Arrangement.spacedBy(8.dp)) {
        if (canInstall) {
            FluxButton(
                text = str(if (status.phase == UpdatePhase.Ready) R.string.mobileUpdateInstallButton else R.string.updateNow),
                onClick = onInstall,
                variant = ButtonVariant.Primary,
                icon = FluxIcons.Download,
                fullWidth = true,
            )
        }
        if (manualUrl != null) {
            FluxButton(
                text = str(R.string.updateFailedOpenSite),
                onClick = { onOpenSite(manualUrl) },
                variant = ButtonVariant.Primary,
                icon = FluxIcons.ExternalLink,
                fullWidth = true,
            )
        }
        if (UpdateView.canCancel(status)) {
            FluxButton(text = str(R.string.cancel), onClick = onCancel, fullWidth = true)
        }
        if (checking || canCheck) {
            val label = if (status.phase == UpdatePhase.Idle || status.phase == UpdatePhase.UpToDate) R.string.checkUpdate else R.string.recheck
            FluxButton(text = str(label), onClick = onCheck, loading = checking, enabled = canCheck, fullWidth = true)
        }
    }
}
