package com.fluxdown.app.feature.settings

import android.content.Context
import android.content.pm.PackageManager
import android.os.Build
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.runtime.Composable
import androidx.compose.runtime.derivedStateOf
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.produceState
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.unit.dp
import com.fluxdown.app.R
import com.fluxdown.app.feature.settings.diagnostics.LOGS_ITEM_KEY
import com.fluxdown.app.feature.settings.diagnostics.LogsSection
import com.fluxdown.app.feature.settings.diagnostics.rememberLogExportModel
import com.fluxdown.app.i18n.str
import com.fluxdown.app.nav.LocalNavigator
import com.fluxdown.app.shell.hostState
import com.fluxdown.fluxui.controls.FluxIconButton
import com.fluxdown.fluxui.controls.FluxListRow
import com.fluxdown.fluxui.controls.FluxTag
import com.fluxdown.fluxui.controls.GlassSection
import com.fluxdown.fluxui.controls.IconButtonSize
import com.fluxdown.fluxui.controls.Tone
import com.fluxdown.fluxui.feedback.DotMatrix
import com.fluxdown.fluxui.icons.FluxIcon
import com.fluxdown.fluxui.icons.FluxIcons
import com.fluxdown.fluxui.material.rememberFlowInGate
import com.fluxdown.fluxui.overlay.FluxPortal
import com.fluxdown.fluxui.overlay.FluxSheet
import com.fluxdown.fluxui.overlay.FluxSheetDetent
import com.fluxdown.fluxui.overlay.FluxSheetHeader
import com.fluxdown.fluxui.overlay.LocalFluxOverlays
import com.fluxdown.fluxui.theme.FluxText
import com.fluxdown.fluxui.theme.FluxTheme
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.IOException

private const val SITE_HOST = "fluxdown.zerx.dev"
private const val SITE_URL = "https://$SITE_HOST"
private const val SPONSOR_URL = "$SITE_URL/sponsor"
private const val PRIVACY_URL = "$SITE_URL/privacy"
private const val CHANGELOG_URL = "$SITE_URL/changelog"
private const val LICENSE_URL = "https://github.com/zerx-lab/FluxDown/blob/main/LICENSE"
private const val DEPENDENCIES_URL = "https://github.com/zerx-lab/FluxDown/blob/main/Cargo.lock"

/** 远端 `--server` 主机的发布页（升级由服务器管理员在服务器上完成，App 内不提供）。 */
private const val SERVER_RELEASES_URL = "https://github.com/zerx-lab/FluxDown/releases"

/** 品牌点阵 “FD”：11×7，点 6 / 间距 3 ≈ 96dp 宽；右下一枚强调点。 */
private val BrandGlyph = listOf(
    "#####.####.",
    "#.....#...#",
    "#.....#...#",
    "####..#...#",
    "#.....#...#",
    "#.....#...#",
    "#.....####o",
)

/** 随 FluxUI 打包在 assets 的第三方许可文本。名称 / 许可证名为专有名词，不本地化。 */
private class BundledLicense(val name: String, val license: String, val asset: String)

private val BundledLicenses = listOf(
    BundledLicense("Geist · Geist Mono", "SIL Open Font License 1.1", "licenses/geist-OFL.txt"),
    BundledLicense("Lucide", "ISC License", "licenses/lucide-LICENSE.txt"),
)

/** 应用版本名（`PackageInfo.versionName`；BuildConfig 未启用）。 */
internal fun Context.appVersionName(): String {
    val info = if (Build.VERSION.SDK_INT >= 33) {
        packageManager.getPackageInfo(packageName, PackageManager.PackageInfoFlags.of(0))
    } else {
        @Suppress("DEPRECATION")
        packageManager.getPackageInfo(packageName, 0)
    }
    return info.versionName.orEmpty()
}

/**
 * S14 · 关于：品牌头、版本信息（应用 / 协议 / 远端服务版本）、软件更新、日志工具、支持 / 网站 / 更新日志 / 隐私 / 开源许可。
 * 同 iOS `AboutPage`；Android 没有 App Store 审核限制，保留捐赠入口。
 * 软件更新（[UpdateSection]）是应用内自更新：Rust 更新器检查 / 下载 / 校验，经系统 `PackageInstaller` 安装（必须用户点按确认）；
 * 商店安装 / 非官方构建只给手动说明与官网入口。更新的是本机 App，与当前主机（含远端）无关。
 */
@Composable
internal fun AboutPage() {
    val nav = LocalNavigator.current
    val context = LocalContext.current
    val overlays = LocalFluxOverlays.current
    val hostState = hostState()
    val protocol by remember { derivedStateOf { hostState.value.info?.protocolVersion } }
    val serviceVersion by remember { derivedStateOf { hostState.value.info?.serviceVersion.orEmpty() } }
    val version = remember(context) { context.appVersionName() }
    val gate = rememberFlowInGate()
    val ctx = rememberSettingsCtx { }
    val logModel = rememberLogExportModel()
    val scope = androidx.compose.runtime.rememberCoroutineScope()
    var showLicenses by remember { mutableStateOf(false) }
    var picker by remember { mutableStateOf<PickerSpec?>(null) }
    val noBrowser = str(R.string.mobileNoBrowser)
    val c = FluxTheme.colors
    val remote = !ctx.isLocalHost

    Box(Modifier.fillMaxSize()) {
        SettingsPageFrame(title = str(R.string.settingsCatAbout), onBack = { nav.pop() }) {
            flowItem(0, gate, "brand") { BrandHeader(version, protocol) }
            flowItem(1, gate, "info") {
                GlassSection {
                    row {
                        SettingRowBox(ctx, "about.version", null) {
                            FluxListRow(title = str(R.string.currentVersion), value = "v$version")
                        }
                    }
                    protocol?.let { p ->
                        row {
                            SettingRowBox(ctx, "about.protocol", null) {
                                FluxListRow(title = str(R.string.protocolVersionLabel), value = "v$p")
                            }
                        }
                    }
                    // 远端主机的服务版本与本机应用版本不同，单独列出；本机引擎与应用同版本，不重复。
                    if (remote && serviceVersion.isNotEmpty()) {
                        row {
                            SettingRowBox(ctx, "about.serviceVersion", null) {
                                FluxListRow(title = str(R.string.mobileServiceVersion), value = "v$serviceVersion")
                            }
                        }
                    }
                }
            }
            flowItem(2, gate, "update") { UpdateSection(ctx) { picker = it } }
            flowItem(3, gate, LOGS_ITEM_KEY) { LogsSection(ctx, logModel, scope) }
            flowItem(4, gate, "support") {
                GlassSection(title = str(R.string.donateTitle), footer = str(R.string.donateThanks)) {
                    row(hasIcon = true) {
                        FluxListRow(
                            title = str(R.string.donateButton),
                            icon = FluxIcons.Heart,
                            iconTone = Tone.Accent,
                            trailing = { ExternalMark() },
                            onClick = { openLink(context, overlays, SPONSOR_URL, noBrowser) },
                        )
                    }
                }
            }
            flowItem(5, gate, "links") {
                GlassSection {
                    row(hasIcon = true) {
                        SettingRowBox(ctx, "about.website", null) {
                            FluxListRow(
                                title = str(R.string.officialWebsite),
                                icon = FluxIcons.Globe,
                                value = SITE_HOST,
                                trailing = { ExternalMark() },
                                onClick = { openLink(context, overlays, SITE_URL, noBrowser) },
                            )
                        }
                    }
                    row(hasIcon = true) {
                        SettingRowBox(ctx, "about.changelog", null) {
                            FluxListRow(
                                title = str(R.string.mobileReleaseNotes),
                                icon = FluxIcons.ListChecks,
                                trailing = { ExternalMark() },
                                onClick = { openLink(context, overlays, CHANGELOG_URL, noBrowser) },
                            )
                        }
                    }
                    if (remote) {
                        row(hasIcon = true) {
                            SettingRowBox(ctx, "about.serverReleases", null) {
                                FluxListRow(
                                    title = str(R.string.webServerReleases),
                                    icon = FluxIcons.Server,
                                    trailing = { ExternalMark() },
                                    onClick = { openLink(context, overlays, SERVER_RELEASES_URL, noBrowser) },
                                )
                            }
                        }
                    }
                    row(hasIcon = true) {
                        SettingRowBox(ctx, "about.privacy", null) {
                            FluxListRow(
                                title = str(R.string.mobilePrivacyPolicy),
                                icon = FluxIcons.ShieldCheck,
                                trailing = { ExternalMark() },
                                onClick = { openLink(context, overlays, PRIVACY_URL, noBrowser) },
                            )
                        }
                    }
                    row(hasIcon = true) {
                        SettingRowBox(ctx, "about.licenses", null) {
                            FluxListRow(
                                title = str(R.string.mobileOpenSource),
                                icon = FluxIcons.ScrollText,
                                chevron = true,
                                onClick = { showLicenses = true },
                            )
                        }
                    }
                }
            }
            flowItem(6, gate, "footer") {
                FluxText(
                    text = str(R.string.mobileFooter),
                    modifier = Modifier.fillMaxWidth().padding(top = 4.dp),
                    style = FluxTheme.type.sm.copy(textAlign = TextAlign.Center),
                    color = c.inkFaint,
                )
            }
        }
        LicensesSheet(visible = showLicenses, onDismiss = { showLicenses = false })
        PickerSheetHost(picker = picker, onDismiss = { picker = null })
    }
}

@Composable
private fun ExternalMark() {
    FluxIcon(FluxIcons.ExternalLink, null, size = 16.dp, tint = FluxTheme.colors.inkFaint)
}

@Composable
private fun BrandHeader(version: String, protocol: Int?) {
    val c = FluxTheme.colors
    val t = FluxTheme.type
    Column(
        Modifier.fillMaxWidth().padding(top = 8.dp),
        horizontalAlignment = Alignment.CenterHorizontally,
        verticalArrangement = Arrangement.spacedBy(10.dp),
    ) {
        DotMatrix(rows = BrandGlyph, dot = 6.dp, gap = 3.dp, label = "FluxDown")
        FluxText("FluxDown", style = t.title, color = c.ink, modifier = Modifier.padding(top = 10.dp))
        FluxText(str(R.string.appDescription), style = t.sm, color = c.inkMuted, maxLines = 2)
        Row(horizontalArrangement = Arrangement.spacedBy(8.dp), verticalAlignment = Alignment.CenterVertically) {
            FluxText("v$version", style = t.mono, color = c.inkMuted)
            if (protocol != null) {
                FluxTag(str(R.string.mobileProtocolPill, "v" to protocol), tone = Tone.Accent)
            }
        }
    }
}

/**
 * 开源许可（Full Sheet）：FluxDown 自身许可证、下载引擎依赖清单链接，以及随包第三方许可文本（字体 / 图标）。
 */
@Composable
private fun LicensesSheet(visible: Boolean, onDismiss: () -> Unit) {
    val context = LocalContext.current
    val overlays = LocalFluxOverlays.current
    val title = str(R.string.mobileOpenSource)
    val closeDescription = str(R.string.close)
    val noBrowser = str(R.string.mobileNoBrowser)
    FluxPortal {
        FluxSheet(
            visible = visible,
            onDismissRequest = onDismiss,
            detent = FluxSheetDetent.Full,
            title = title,
            header = {
                FluxSheetHeader(
                    title = title,
                    actions = {
                        FluxIconButton(FluxIcons.X, closeDescription, onDismiss, size = IconButtonSize.Sm)
                    },
                )
            },
        ) {
            Column(verticalArrangement = Arrangement.spacedBy(SectionGap)) {
                GlassSection(title = "FluxDown", footer = "GNU Affero General Public License v3.0") {
                    row {
                        FluxListRow(
                            title = str(R.string.mobileLicenseViewFull),
                            trailing = { ExternalMark() },
                            onClick = { openLink(context, overlays, LICENSE_URL, noBrowser) },
                        )
                    }
                }
                GlassSection(footer = str(R.string.mobileLicenseEngineDepsDesc)) {
                    row {
                        FluxListRow(
                            title = str(R.string.mobileLicenseEngineDeps),
                            trailing = { ExternalMark() },
                            onClick = { openLink(context, overlays, DEPENDENCIES_URL, noBrowser) },
                        )
                    }
                }
                for (entry in BundledLicenses) LicenseBlock(entry)
            }
        }
    }
}

@Composable
private fun LicenseBlock(entry: BundledLicense) {
    val context = LocalContext.current
    val c = FluxTheme.colors
    val t = FluxTheme.type
    val text by produceState<String?>(initialValue = null, entry.asset) {
        value = withContext(Dispatchers.IO) {
            try {
                context.assets.open(entry.asset).bufferedReader().use { it.readText() }.trim()
            } catch (e: IOException) {
                ""
            }
        }
    }
    GlassSection(title = entry.name, footer = entry.license) {
        custom(padded = true) {
            val body = text
            when {
                body == null -> Unit
                body.isEmpty() -> FluxText(str(R.string.mobileLicenseLoadFailed), style = t.sm, color = c.coralText)
                else -> FluxText(body, style = t.monoS, color = c.inkMuted)
            }
        }
    }
}
