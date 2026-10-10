package com.fluxdown.app

import android.content.Intent
import android.net.Uri
import android.os.Bundle
import android.util.Log
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.runtime.Composable
import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateListOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.rememberUpdatedState
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.lifecycle.lifecycleScope
import com.fluxdown.app.actions.LocalTaskActions
import com.fluxdown.app.actions.TaskActions
import com.fluxdown.app.feature.newtask.NewDownloadSheet
import com.fluxdown.app.nav.AppNavigator
import com.fluxdown.app.nav.LocalNavigator
import com.fluxdown.app.nav.SheetRoute
import com.fluxdown.app.service.DownloadServiceController
import com.fluxdown.app.shell.FluxAppRoot
import com.fluxdown.app.shell.LocalAppContainer
import com.fluxdown.app.ui.isLocalDirWritable
import com.fluxdown.app.feature.newtask.TorrentImport
import com.fluxdown.core.capture.ExternalDownload
import com.fluxdown.core.capture.ExternalIntake
import com.fluxdown.core.capture.SilentCapture
import com.fluxdown.core.capture.TorrentFile
import com.fluxdown.core.host.HostException
import com.fluxdown.core.model.HostRef
import com.fluxdown.core.protocol.HostMethod
import com.fluxdown.core.protocol.callUnit
import com.fluxdown.core.store.Connection
import com.fluxdown.core.store.HostState
import com.fluxdown.fluxui.overlay.FluxOverlayHost
import com.fluxdown.fluxui.overlay.FluxToastKind
import com.fluxdown.fluxui.overlay.LocalFluxOverlays
import com.fluxdown.fluxui.overlay.rememberFluxOverlayState
import com.fluxdown.fluxui.theme.FluxTheme
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull

private const val TAG = "FluxExternal"

/** 等主机首个快照（默认目录 / 队列 / 免打扰偏好都在里面）的上限；超时按当前状态继续（弹窗会显示只读提示）。 */
private const val READY_TIMEOUT_MS = 8_000L

/** Sheet 退场动画（`FluxMotion.snap` 弹簧）走完再结束任务，避免弹窗被直接截断。 */
private const val SHEET_EXIT_MS = 250L

/** 种子建完后保留窗口的时长：让成功 / 失败 toast 先展示完再结束任务。 */
private const val TOAST_LINGER_MS = 1_800L

/**
 * 外部下载唤起入口（同 Flutter 版同名 Activity；类名与 manifest 中两个 http(s) 别名都不可改——浏览器可能按
 * 组件名保存了它们）。透明窗口 + 独立任务（`singleInstance`、`taskAffinity=""`、不进最近任务）：只在来源应用
 * （浏览器 / 文件管理器）上方弹出「新建下载」，关闭后结束整个任务、露出来源应用。
 *
 * 来源：浏览器「外部下载器」（X / Via 等的 http(s) VIEW，extra `User-Agent` / `Cookie` / `Referer`）、浏览器扩展
 * `fluxdown://download?…`、系统分享、`magnet:` / `ed2k://` 链接。弹窗打开期间的后续唤起追加进同一表单。
 * 「免打扰下载」开启时不弹窗，按默认设置直接建任务后返回；算不出保存目录、本机目录不可写或建任务失败时退回弹窗。
 * 与主界面共用进程级 [AppContainer]（同一主机会话）。
 */
class ExternalDownloadActivity : ComponentActivity() {
    private val navigator = AppNavigator()

    /** 按到达顺序串行处理（解析可能较重：长 Cookie / 请求头 JSON）。 */
    private val intents = Channel<Intent>(Channel.UNLIMITED)

    /** 待导入的 `.torrent`（VIEW content / file URI）：由舞台逐个提交，完成后回调 [onTorrentDone]。 */
    private val torrents = mutableStateListOf<PendingTorrent>()
    private var closeJob: Job? = null

    /** 免打扰建任务失败的原因：交给组合层 toast 后清空。 */
    private var silentFailure by mutableStateOf<HostException?>(null)

    override fun onCreate(savedInstanceState: Bundle?) {
        enableEdgeToEdge()
        super.onCreate(savedInstanceState)
        val container = (application as FluxApplication).container
        // 进程被回收后恢复：弹窗状态已丢失，按原 intent 重新处理（建成即结束，不会重复建任务）。
        intents.trySend(intent)
        lifecycleScope.launch {
            for (next in intents) {
                if (withContext(Dispatchers.IO) { isTorrentView(next) }) {
                    closeJob?.cancel()
                    next.data?.let { torrents.add(PendingTorrent(it)) }
                    continue
                }
                val request = withContext(Dispatchers.Default) { parse(next) }
                when {
                    request != null -> dispatch(container, request)
                    navigator.sheet == null && torrents.isEmpty() -> finishAndRemoveTask()
                }
            }
        }
        setContent {
            FluxAppRoot(container, navigator) {
                ExternalDownloadStage(
                    failure = silentFailure,
                    torrents = torrents,
                    onFailureShown = { silentFailure = null },
                    onSubmitted = { keepLocalDownloadsAlive(container) },
                    onTorrentDone = ::onTorrentDone,
                    onClosed = ::finishAndRemoveTask,
                )
            }
        }
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        setIntent(intent)
        intents.trySend(intent)
    }

    /** 种子建完：窗口只为它而开时，留出 toast 展示时间再结束；其间又来新的唤起则取消。 */
    private fun onTorrentDone(done: PendingTorrent) {
        torrents.remove(done)
        closeJob?.cancel()
        closeJob = lifecycleScope.launch {
            delay(TOAST_LINGER_MS)
            if (navigator.sheet == null && torrents.isEmpty()) finishAndRemoveTask()
        }
    }

    /** VIEW 一个 content / file 种子：MIME 为 `application/x-bittorrent`，或（octet-stream 兜底）文件名以 `.torrent` 结尾。 */
    private fun isTorrentView(intent: Intent): Boolean {
        if (intent.action != Intent.ACTION_VIEW) return false
        val uri = intent.data ?: return false
        if (uri.scheme != "content" && uri.scheme != "file") return false
        return intent.type == TorrentFile.MIME_TYPE || TorrentFile.isTorrentFileName(TorrentImport.displayName(this, uri))
    }

    override fun onDestroy() {
        intents.close()
        super.onDestroy()
    }

    /** 免打扰优先；否则打开弹窗，已打开时追加进当前表单。 */
    private suspend fun dispatch(container: AppContainer, request: ExternalDownload) {
        val state = awaitReady(container)
        val plan = SilentCapture.plan(request, state)
        if (plan != null && silentDirUsable(container, plan)) {
            try {
                container.session.callUnit(HostMethod.daemonTaskCreate, plan.params())
                keepLocalDownloadsAlive(container)
                if (navigator.sheet == null) finishAndRemoveTask()
                return
            } catch (e: HostException) {
                Log.w(TAG, "silent download failed, falling back to the sheet: ${e.code} ${e.message}")
                silentFailure = e
            }
        }
        navigator.openSheet(SheetRoute.NewDownload(external = request))
    }

    private suspend fun awaitReady(container: AppContainer): HostState =
        withTimeoutOrNull(READY_TIMEOUT_MS) {
            container.store.state.first { it.connection == Connection.Live || it.connection is Connection.Failed }
        } ?: container.store.state.value

    /** 本机主机：目录不可写（分区存储外 / 拼错路径）时退回弹窗，避免静默建出必然失败的任务。 */
    private suspend fun silentDirUsable(container: AppContainer, plan: SilentCapture): Boolean =
        container.host.value !is HostRef.Local || withContext(Dispatchers.IO) { isLocalDirWritable(plan.request.saveDir) }

    /** 本机建了任务：趁窗口仍可见拉起前台服务（Android 12+ 禁止后台启动），服务在本机空闲后自行停止。 */
    private fun keepLocalDownloadsAlive(container: AppContainer) {
        if (container.host.value is HostRef.Local) DownloadServiceController.start(this)
    }

    /** 分享取文本，VIEW 取 data 与浏览器 extra。 */
    private fun parse(intent: Intent?): ExternalDownload? = when (intent?.action) {
        Intent.ACTION_SEND, Intent.ACTION_SEND_MULTIPLE ->
            ExternalIntake.fromSharedText(intent.getStringExtra(Intent.EXTRA_TEXT)?.takeIf { it.isNotBlank() } ?: clipText(intent))
        Intent.ACTION_VIEW -> ExternalIntake.fromView(
            data = intent.dataString,
            userAgent = intent.getStringExtra(EXTRA_USER_AGENT),
            cookie = intent.getStringExtra(EXTRA_COOKIE),
            referer = intent.getStringExtra(EXTRA_REFERER),
        )
        else -> null
    }

    /** ClipData 中首个非空文本（分享缺 `EXTRA_TEXT` 时的兜底）。 */
    private fun clipText(intent: Intent): String? {
        val clip = intent.clipData ?: return null
        for (i in 0 until clip.itemCount) {
            val text = clip.getItemAt(i).text?.toString()
            if (!text.isNullOrBlank()) return text
        }
        return null
    }

    private companion object {
        /** X 浏览器等调用外部下载器时随 VIEW 附带的请求上下文（键名区分大小写）。 */
        const val EXTRA_USER_AGENT = "User-Agent"
        const val EXTRA_COOKIE = "Cookie"
        const val EXTRA_REFERER = "Referer"
    }
}

/** 待导入的种子；按引用区分（同一 URI 连续唤起两次也各处理一次）。 */
private class PendingTorrent(val uri: Uri)

/** 透明舞台：只有「新建下载」Sheet 与浮层（对话框 / toast）；Sheet 关闭后结束窗口。 */
@Composable
private fun ExternalDownloadStage(
    failure: HostException?,
    torrents: List<PendingTorrent>,
    onFailureShown: () -> Unit,
    onSubmitted: () -> Unit,
    onTorrentDone: (PendingTorrent) -> Unit,
    onClosed: () -> Unit,
) {
    val container = LocalAppContainer.current
    val nav = LocalNavigator.current
    val context = LocalContext.current
    val overlays = rememberFluxOverlayState()
    val scope = rememberCoroutineScope()
    val haptics = FluxTheme.haptics
    val actions = remember(overlays, haptics) {
        TaskActions(context.applicationContext, scope, { container.session }, container.store, overlays, nav, haptics)
    }
    val close by rememberUpdatedState(onClosed)
    CompositionLocalProvider(LocalFluxOverlays provides overlays, LocalTaskActions provides actions) {
        Box(Modifier.fillMaxSize()) {
            NewDownloadSheet(route = nav.sheet as? SheetRoute.NewDownload, onDismiss = nav::closeSheet, onSubmitted = onSubmitted)
            FluxOverlayHost(overlays)
        }
    }
    LaunchedEffect(failure) {
        if (failure != null) {
            overlays.toast(actions.errorText(failure), FluxToastKind.Error)
            onFailureShown()
        }
    }
    var opened by remember { mutableStateOf(false) }
    val sheet = nav.sheet
    LaunchedEffect(sheet) {
        if (sheet != null) {
            opened = true
        } else if (opened) {
            delay(SHEET_EXIT_MS)
            if (torrents.isEmpty()) close()
        }
    }
    // 种子逐个提交（默认目录 / 默认队列，同 iOS `openFromSystem`）；每个结束即回调，由 Activity 决定何时关窗。
    val head = torrents.firstOrNull()
    LaunchedEffect(head) {
        if (head == null) return@LaunchedEffect
        TorrentImport.submit(
            context = context,
            container = container,
            overlays = overlays,
            errorText = actions::errorText,
            uris = listOf(head.uri),
            saveDir = "",
            queueId = "",
            startPaused = false,
        ).let { if (it > 0) onSubmitted() }
        onTorrentDone(head)
    }
}
