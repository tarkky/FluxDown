package com.fluxdown.app.feature.settings.service

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.foundation.text.selection.SelectionContainer
import androidx.compose.runtime.Composable
import androidx.compose.runtime.derivedStateOf
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.state.ToggleableState
import androidx.compose.ui.text.input.KeyboardCapitalization
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.unit.dp
import com.fluxdown.app.R
import com.fluxdown.app.feature.settings.PickerSheetHost
import com.fluxdown.app.feature.settings.PickerSpec
import com.fluxdown.app.feature.settings.SettingsEntry
import com.fluxdown.app.feature.settings.SettingsSearchContext
import com.fluxdown.app.feature.settings.SettingsPageFrame
import com.fluxdown.app.feature.settings.flowItem
import com.fluxdown.app.feature.settings.rememberLastNonNull
import com.fluxdown.app.feature.settings.settingsFocus
import com.fluxdown.app.i18n.str
import com.fluxdown.app.nav.LocalNavigator
import com.fluxdown.app.shell.hostState
import com.fluxdown.core.model.Queue
import com.fluxdown.core.protocol.HostCapability
import com.fluxdown.core.protocol.HostSection
import com.fluxdown.core.protocol.WebhookDelivery
import com.fluxdown.core.protocol.WebhookEndpoint
import com.fluxdown.core.protocol.WebhookEvent
import com.fluxdown.core.protocol.WebhookPreset
import com.fluxdown.core.protocol.WebhookTemplate
import com.fluxdown.core.protocol.has
import com.fluxdown.fluxui.controls.ButtonSize
import com.fluxdown.fluxui.controls.ButtonVariant
import com.fluxdown.fluxui.controls.FluxActionRow
import com.fluxdown.fluxui.controls.FluxButton
import com.fluxdown.fluxui.controls.FluxCheckboxRow
import com.fluxdown.fluxui.controls.FluxField
import com.fluxdown.fluxui.controls.FluxFieldAction
import com.fluxdown.fluxui.controls.FluxIconButton
import com.fluxdown.fluxui.controls.FluxKeyValue
import com.fluxdown.fluxui.controls.FluxListRow
import com.fluxdown.fluxui.controls.FluxSelect
import com.fluxdown.fluxui.controls.FluxSwitch
import com.fluxdown.fluxui.controls.FluxSwitchRow
import com.fluxdown.fluxui.controls.GlassSection
import com.fluxdown.fluxui.controls.IconButtonSize
import com.fluxdown.fluxui.controls.SelectOption
import com.fluxdown.fluxui.controls.Tone
import com.fluxdown.fluxui.feedback.FluxEmpty
import com.fluxdown.fluxui.feedback.FluxGlyph
import com.fluxdown.fluxui.icons.FluxIcons
import com.fluxdown.fluxui.material.rememberFlowInGate
import com.fluxdown.fluxui.overlay.FluxBanner
import com.fluxdown.fluxui.overlay.FluxBannerKind
import com.fluxdown.fluxui.overlay.FluxDialogButton
import com.fluxdown.fluxui.overlay.FluxDialogButtonStyle
import com.fluxdown.fluxui.overlay.FluxDialogSpec
import com.fluxdown.fluxui.overlay.FluxPortal
import com.fluxdown.fluxui.overlay.FluxSheet
import com.fluxdown.fluxui.overlay.FluxSheetDetent
import com.fluxdown.fluxui.overlay.FluxSheetFooter
import com.fluxdown.fluxui.overlay.FluxSheetHeader
import com.fluxdown.fluxui.overlay.FluxToastKind
import com.fluxdown.fluxui.overlay.LocalFluxOverlays
import com.fluxdown.fluxui.theme.FluxText
import com.fluxdown.fluxui.theme.FluxTheme
import androidx.compose.ui.text.input.PasswordVisualTransformation
import androidx.compose.ui.text.input.VisualTransformation
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.Stable
import androidx.compose.runtime.key
import androidx.compose.runtime.mutableStateListOf
import androidx.compose.runtime.rememberCoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch

/** 页面只渲染最近这么多条（GPUI 同）。 */
private const val VISIBLE_DELIVERIES = 50

private const val KEY_UNAVAILABLE = "unavailable"
private const val KEY_BANNER = "banner"
private const val KEY_ENDPOINTS = "endpoints"
private const val KEY_LOG = "log"

internal const val ID_ENDPOINTS = "webhook.endpoints"
internal const val ID_LOG = "webhook.log"

private fun WebhookEvent.labelRes(): Int = when (this) {
    WebhookEvent.Created -> R.string.webhookEventCreated
    WebhookEvent.Started -> R.string.webhookEventStarted
    WebhookEvent.Completed -> R.string.webhookEventCompleted
    WebhookEvent.Failed -> R.string.webhookEventFailed
    WebhookEvent.Paused -> R.string.webhookEventPaused
    WebhookEvent.QueueDrained -> R.string.webhookEventQueueDrained
}

/** 编辑器目标：新增时 [seed] 的端点 id 在打开时生成一次（`wh_<毫秒>`），[nonce] 区分每次打开。 */
private class EditorTarget(val seed: WebhookEndpoint, val isNew: Boolean, val nonce: Long)

/**
 * S12 · Webhook：端点列表（配置键 `webhook.endpoints`）+ 推送记录（`daemon.webhookDeliveries` 分区）。
 * 端点写入一律「读最新配置 → 变换 → `expectedRevision` 补丁；冲突时重读并重放（≤3 次）」，串行执行
 * （`WebhookEndpointWriter`；AGENTS.md §5）。
 */
@Composable
internal fun WebhookPage() {
    val nav = LocalNavigator.current
    val overlays = LocalFluxOverlays.current
    val controller = rememberWebhookController()
    val hostState = hostState()
    val available by remember { derivedStateOf { hostState.value.has(HostCapability.daemonWebhooks) } }
    val readOnly by remember { derivedStateOf { hostState.value.isReadOnly } }
    val rawConfig by remember { derivedStateOf { hostState.value.config[WebhookEndpoint.CONFIG_KEY] } }
    val rawLog by remember { derivedStateOf { hostState.value.sections[HostSection.daemonWebhookDeliveries] } }
    val queues by remember { derivedStateOf { hostState.value.queues } }
    val endpoints = remember(rawConfig) { WebhookEndpoint.parseList(rawConfig) }
    val log = remember(rawLog) { WebhookDelivery.parseList(com.fluxdown.core.protocol.Json.parseOrNull(rawLog)) }
    val blank = isWebhookPageBlank(endpoints, log)

    var editor by remember { mutableStateOf<EditorTarget?>(null) }
    var detail by remember { mutableStateOf<WebhookDelivery?>(null) }
    var picker by remember { mutableStateOf<PickerSpec?>(null) }
    var logFilter by remember { mutableStateOf("") }
    val gate = rememberFlowInGate()

    fun openNew() {
        val now = System.currentTimeMillis()
        editor = EditorTarget(WebhookEndpoint(id = "wh_$now"), isNew = true, nonce = now)
    }

    val context = LocalContext.current
    fun confirmDelete(endpoint: WebhookEndpoint) {
        overlays.showDialog(
            FluxDialogSpec(
                title = context.str(R.string.webhookRowDeleteConfirm),
                message = endpoint.name.ifEmpty { endpoint.id },
                icon = FluxIcons.Trash,
                buttons = listOf(
                    FluxDialogButton(context.str(R.string.cancel)),
                    FluxDialogButton(context.str(R.string.webhookRowDelete), FluxDialogButtonStyle.Destructive) { controller.remove(endpoint) },
                ),
            ),
        )
    }

    fun confirmClear() {
        overlays.showDialog(
            FluxDialogSpec(
                title = context.str(R.string.webhookLogClear),
                message = context.str(R.string.webhookLogSubtitle),
                icon = FluxIcons.Trash,
                buttons = listOf(
                    FluxDialogButton(context.str(R.string.cancel)),
                    FluxDialogButton(context.str(R.string.webhookLogClear), FluxDialogButtonStyle.Destructive) { controller.clearDeliveries() },
                ),
            ),
        )
    }

    SettingsPageFrame(title = str(R.string.webhookNavTitle), onBack = { nav.pop() }) {
        if (!available) {
            flowItem(0, gate, KEY_UNAVAILABLE) {
                GlassSection {
                    row {
                        FluxListRow(
                            title = str(R.string.webhookNavTitle),
                            subtitle = str(R.string.settingsUnsupportedOnPlatform),
                            icon = FluxIcons.Webhook,
                        )
                    }
                }
            }
            return@SettingsPageFrame
        }
        if (readOnly) {
            flowItem(0, gate, KEY_BANNER) {
                FluxBanner(str(R.string.localServiceDisconnected), kind = FluxBannerKind.Warn, slim = true)
            }
        }
        if (blank) {
            flowItem(1, gate, KEY_ENDPOINTS) {
                FluxEmpty(
                    glyph = FluxGlyph.Inbox,
                    title = str(R.string.webhookEmptyTitle),
                    subtitle = str(R.string.webhookEmptyDesc),
                    modifier = Modifier.fillMaxWidth().settingsFocus(ID_ENDPOINTS),
                    action = {
                        FluxButton(
                            str(R.string.webhookAddEndpoint),
                            onClick = ::openNew,
                            variant = ButtonVariant.Primary,
                            icon = FluxIcons.Plus,
                            enabled = !readOnly,
                        )
                    },
                )
            }
            return@SettingsPageFrame
        }
        flowItem(1, gate, KEY_ENDPOINTS) {
            Box(Modifier.settingsFocus(ID_ENDPOINTS)) {
                GlassSection(title = str(R.string.notifyGroupWebhook), footer = str(R.string.webhookSemantics)) {
                    if (endpoints.isEmpty()) {
                        row {
                            FluxListRow(title = str(R.string.webhookEmptyTitle), icon = FluxIcons.Webhook)
                        }
                    }
                    endpoints.forEach { endpoint ->
                        row {
                            EndpointRow(
                                endpoint = endpoint,
                                latest = com.fluxdown.core.protocol.latestWebhookDelivery(log, endpoint.id),
                                controller = controller,
                                readOnly = readOnly,
                                onEdit = { editor = EditorTarget(endpoint, isNew = false, nonce = System.nanoTime()) },
                                onDelete = { confirmDelete(endpoint) },
                            )
                        }
                    }
                    row(hasIcon = true) {
                        FluxActionRow(
                            title = str(R.string.webhookAddEndpoint),
                            icon = FluxIcons.Plus,
                            enabled = !readOnly,
                            onClick = ::openNew,
                        )
                    }
                }
            }
        }
        flowItem(2, gate, KEY_LOG) {
            val known = endpoints.map { it.id }.toSet()
            val filter = if (logFilter.isEmpty() || logFilter in known || log.any { it.endpointId == logFilter }) logFilter else ""
            val visible = log
                .filter { filter.isEmpty() || it.endpointId == filter }
                .sortedByDescending { it.timestampMs }
                .take(VISIBLE_DELIVERIES)
            val context = LocalContext.current
            val allLabel = str(R.string.webhookLogFilterAll)
            val filterTitle = str(R.string.webhookLogFilterLabel)
            Box(Modifier.settingsFocus(ID_LOG)) {
                GlassSection(
                    title = str(R.string.webhookDeliveryLog),
                    footer = str(R.string.webhookLogSubtitle) + "\n" + str(R.string.webhookLogSimulateHint),
                    action = if (endpoints.size > 1 || filter.isNotEmpty()) {
                        {
                            val current = if (filter.isEmpty()) allLabel else endpoints.firstOrNull { it.id == filter }?.name ?: filter
                            FluxButton(
                                current,
                                onClick = {
                                    picker = PickerSpec(
                                        title = filterTitle,
                                        options = listOf(SelectOption("", allLabel)) +
                                            endpoints.map { SelectOption(it.id, it.name.ifEmpty { it.id }) },
                                        selected = filter,
                                    ) { logFilter = it }
                                },
                                variant = ButtonVariant.Ghost,
                                size = ButtonSize.Xs,
                                icon = FluxIcons.ListFilter,
                            )
                        }
                    } else {
                        null
                    },
                ) {
                    controller.simulateText?.let { text ->
                        row {
                            FluxListRow(title = text)
                        }
                    }
                    if (visible.isEmpty()) {
                        row {
                            FluxListRow(title = str(R.string.webhookLogEmpty))
                        }
                    }
                    visible.forEach { delivery ->
                        row {
                            FluxListRow(
                                title = "${delivery.endpointName} · ${delivery.event}",
                                subtitle = webhookDeliverySummary(context, delivery) + " · " +
                                    str(R.string.webhookAttempts, "n" to delivery.attempts),
                                value = formatWebhookTime(delivery.timestampMs),
                                chevron = true,
                                onClick = { detail = delivery },
                            )
                        }
                    }
                    row(hasIcon = true) {
                        FluxActionRow(
                            title = str(if (controller.simulating) R.string.webhookLogPending else R.string.webhookLogSimulate),
                            icon = FluxIcons.Send,
                            loading = controller.simulating,
                            enabled = !readOnly && !controller.simulating,
                            onClick = controller::simulate,
                        )
                    }
                    row(hasIcon = true) {
                        FluxActionRow(
                            title = str(R.string.webhookLogClear),
                            icon = FluxIcons.Trash,
                            tone = Tone.Coral,
                            loading = controller.clearing,
                            enabled = !readOnly && log.isNotEmpty() && !controller.clearing,
                            onClick = ::confirmClear,
                        )
                    }
                }
            }
        }
    }

    WebhookEditorSheet(
        target = editor,
        controller = controller,
        queues = queues,
        openPicker = { picker = it },
        onDismiss = { editor = null },
    )
    DeliveryDetailSheet(delivery = detail, onDismiss = { detail = null })
    // 选择器最后挂载：叠在编辑器之上。
    PickerSheetHost(picker = picker, onDismiss = { picker = null })
}

// ───────────────────────────── 端点行 ─────────────────────────────

@Composable
private fun EndpointRow(
    endpoint: WebhookEndpoint,
    latest: WebhookDelivery?,
    controller: WebhookController,
    readOnly: Boolean,
    onEdit: () -> Unit,
    onDelete: () -> Unit,
) {
    val context = LocalContext.current
    val overlays = LocalFluxOverlays.current
    val c = FluxTheme.colors
    val type = FluxTheme.type
    val report = controller.testReport?.takeIf { it.endpointId == endpoint.id }
    val testing = controller.testingId == endpoint.id
    val copiedText = str(R.string.webhookCopied)
    Column(Modifier.fillMaxWidth()) {
        FluxListRow(
            title = endpoint.name.ifEmpty { endpoint.id },
            subtitle = "${endpoint.url} · ${endpoint.events.joinToString(", ")}",
            trailing = {
                FluxSwitch(
                    checked = endpoint.enabled,
                    onCheckedChange = { controller.setEnabled(endpoint, it) },
                    enabled = !readOnly,
                )
            },
            enabled = !readOnly,
            onClick = onEdit,
        )
        Column(
            Modifier.fillMaxWidth().padding(start = 16.dp, end = 16.dp, bottom = 10.dp),
            verticalArrangement = Arrangement.spacedBy(6.dp),
        ) {
            val (healthText, healthColor) = when {
                !endpoint.enabled -> str(R.string.webhookHealthDisabled) to c.inkMuted
                latest == null -> str(R.string.webhookHealthNone) to c.inkMuted
                latest.success -> str(R.string.webhookHealthOk, "time" to "${latest.latencyMs} ms") to c.mintText
                else -> str(
                    R.string.webhookHealthFail,
                    "detail" to latest.error.ifEmpty { webhookHttpStatus(context, latest.statusCode) },
                ) to c.coralText
            }
            FluxText(healthText, style = type.sm, color = healthColor)
            if (report != null) {
                FluxText(report.text, style = type.sm, color = if (report.success) c.mintText else c.coralText)
            }
            Row(horizontalArrangement = Arrangement.spacedBy(8.dp), verticalAlignment = Alignment.CenterVertically) {
                FluxButton(
                    str(R.string.webhookRowTest),
                    onClick = { controller.test(endpoint) },
                    variant = ButtonVariant.Secondary,
                    size = ButtonSize.Xs,
                    icon = FluxIcons.Send,
                    enabled = !readOnly && controller.testingId == null,
                    loading = testing,
                )
                FluxButton(
                    str(R.string.copyUrl),
                    onClick = {
                        copyToClipboard(context, endpoint.name.ifEmpty { endpoint.id }, endpoint.url)
                        overlays.toast(copiedText, FluxToastKind.Success)
                    },
                    variant = ButtonVariant.Ghost,
                    size = ButtonSize.Xs,
                    icon = FluxIcons.Copy,
                )
                FluxButton(
                    str(R.string.webhookRowDelete),
                    onClick = onDelete,
                    variant = ButtonVariant.Ghost,
                    size = ButtonSize.Xs,
                    icon = FluxIcons.Trash,
                    enabled = !readOnly,
                )
            }
        }
    }
}

// ───────────────────────────── 投递详情 ─────────────────────────────

/** 推送详情：请求头 / 请求体 / 响应（4xx 不重试提示）。 */
@Composable
private fun DeliveryDetailSheet(delivery: WebhookDelivery?, onDismiss: () -> Unit) {
    val d = rememberLastNonNull(delivery)
    val context = LocalContext.current
    FluxPortal {
        FluxSheet(
            visible = delivery != null,
            onDismissRequest = onDismiss,
            detent = FluxSheetDetent.Full,
            title = d?.endpointName,
            header = {
                if (d != null) FluxSheetHeader(title = d.endpointName.ifEmpty { d.endpointId }, onClose = onDismiss)
            },
        ) {
            if (d == null) return@FluxSheet
            Column(Modifier.padding(bottom = 16.dp), verticalArrangement = Arrangement.spacedBy(16.dp)) {
                GlassSection {
                    row { FluxKeyValue(str(R.string.webhookFieldName), d.endpointName) }
                    row { FluxKeyValue(str(R.string.webhookFieldEvents), d.event) }
                    row { FluxKeyValue(str(R.string.webhookFieldUrl), d.url, mono = true) }
                    row { FluxKeyValue(str(R.string.mobileWebhookDeliveryStatus), webhookDeliverySummary(context, d), tone = if (d.success) Tone.Mint else Tone.Coral) }
                    row { FluxKeyValue(str(R.string.mobileWebhookDeliveryAttempts), d.attempts.toString()) }
                    row { FluxKeyValue(str(R.string.mobileWebhookDeliveryTime), formatWebhookTime(d.timestampMs)) }
                }
                if (d.statusCode in 400..499) {
                    FluxBanner(str(R.string.webhookLogHint4xx), kind = FluxBannerKind.Warn, slim = true)
                }
                if (d.requestHeaders.isNotEmpty()) MonoBlock(str(R.string.mobileWebhookRequestHeaders), d.requestHeaders)
                if (d.requestBody.isNotEmpty()) MonoBlock(str(R.string.mobileWebhookRequestBody), d.requestBody)
                if (d.responseBody.isNotEmpty()) MonoBlock(str(R.string.webhookLogResponse), d.responseBody)
            }
        }
    }
}

@Composable
private fun MonoBlock(title: String, text: String) {
    GlassSection(title = title) {
        custom(padded = true) {
            SelectionContainer {
                FluxText(text, style = FluxTheme.type.monoS, color = FluxTheme.colors.inkMuted, overflow = androidx.compose.ui.text.style.TextOverflow.Clip)
            }
        }
    }
}

// ───────────────────────────── 编辑器 ─────────────────────────────

@Stable
private class HeaderRow(val id: Long, key: String, value: String) {
    var key by mutableStateOf(key)
    var value by mutableStateOf(value)
}

@Stable
private class EditorForm(seed: WebhookEndpoint, val isNew: Boolean) {
    val endpointId = seed.id
    val enabled = seed.enabled
    var name by mutableStateOf(seed.name)
    var url by mutableStateOf(seed.url)
    var template by mutableStateOf(seed.bodyTemplate)
    var preset by mutableStateOf(seed.preset.ifEmpty { WebhookEndpoint.PRESET_CUSTOM })
    val events = mutableStateListOf<String>().also { it.addAll(seed.events) }
    var queueId by mutableStateOf(seed.queueId)
    private var headerSeq = 0L
    val headers = mutableStateListOf<HeaderRow>().also { list ->
        seed.headers.entries.sortedBy { it.key }.forEach { list += HeaderRow(headerSeq++, it.key, it.value) }
    }
    var signEnabled by mutableStateOf(seed.signSecret.isNotEmpty())
    var secret by mutableStateOf(seed.signSecret)
    var allowHttp by mutableStateOf(seed.allowHttp)
    var useProxy by mutableStateOf(seed.useProxy)
    var advancedOpen by mutableStateOf(
        headers.isNotEmpty() || seed.bodyTemplate.isNotEmpty() || seed.signSecret.isNotEmpty() || seed.allowHttp || seed.useProxy,
    )
    var urlTouched by mutableStateOf(false)
    var showSecret by mutableStateOf(false)
    var secretCopied by mutableStateOf(false)
    var presets by mutableStateOf<List<WebhookPreset>>(emptyList())
    var variables by mutableStateOf<List<String>>(emptyList())
    var testing by mutableStateOf(false)
    var testReport by mutableStateOf<WebhookTestReport?>(null)
    var saving by mutableStateOf(false)

    /** 打开时的草稿：与之相比判断是否有未保存修改。 */
    val baseline: WebhookEndpoint = buildDraft()

    fun newHeader() {
        headers += HeaderRow(headerSeq++, "", "")
    }

    /** 草稿 → 模型。 */
    fun buildDraft(): WebhookEndpoint {
        val map = LinkedHashMap<String, String>()
        for (row in headers) {
            val key = row.key.trim()
            if (key.isNotEmpty()) map[key] = row.value
        }
        return WebhookEndpoint(
            id = endpointId,
            name = name.trim(),
            preset = preset,
            url = url.trim(),
            enabled = enabled,
            events = WebhookEvent.entries.map { it.wire }.filter { it in events },
            queueId = queueId,
            headers = map,
            bodyTemplate = template,
            signSecret = if (signEnabled) secret.trim() else "",
            allowHttp = allowHttp,
            useProxy = useProxy,
        )
    }

    val urlError get() = WebhookTemplate.urlError(url, allowHttp)
    val canSave get() = name.isNotBlank() && url.isNotBlank() && urlError == null
    val canTest get() = !testing && url.isNotBlank()
    val isDirty get() = buildDraft() != baseline
    val currentPreset get() = presets.firstOrNull { it.id == preset }
}

@Composable
private fun WebhookEditorSheet(
    target: EditorTarget?,
    controller: WebhookController,
    queues: List<Queue>,
    openPicker: (PickerSpec) -> Unit,
    onDismiss: () -> Unit,
) {
    val spec = rememberLastNonNull(target)
    FluxPortal {
        if (spec != null) {
            key(spec.nonce) {
                EditorSheetImpl(visible = target != null, target = spec, controller = controller, queues = queues, openPicker = openPicker, onDismiss = onDismiss)
            }
        }
    }
}

@Composable
private fun EditorSheetImpl(
    visible: Boolean,
    target: EditorTarget,
    controller: WebhookController,
    queues: List<Queue>,
    openPicker: (PickerSpec) -> Unit,
    onDismiss: () -> Unit,
) {
    val overlays = LocalFluxOverlays.current
    val context = LocalContext.current
    val scope = rememberCoroutineScope()
    val form = remember { EditorForm(target.seed, target.isNew) }
    val c = FluxTheme.colors
    val type = FluxTheme.type

    LaunchedEffect(Unit) {
        controller.loadCatalog()?.let {
            form.presets = it.presets
            form.variables = it.variables
        }
    }
    LaunchedEffect(form.secretCopied) {
        if (form.secretCopied) {
            delay(2000)
            form.secretCopied = false
        }
    }

    fun requestClose() {
        if (form.saving) return
        if (!form.isDirty) {
            onDismiss()
            return
        }
        overlays.showDialog(
            FluxDialogSpec(
                title = context.str(R.string.mobileWebhookDiscardTitle),
                icon = FluxIcons.TriangleAlert,
                buttons = listOf(
                    FluxDialogButton(context.str(R.string.mobileWebhookKeepEditing)),
                    FluxDialogButton(context.str(R.string.mobileWebhookDiscard), FluxDialogButtonStyle.Destructive) { onDismiss() },
                ),
            ),
        )
    }

    fun save() {
        if (!form.canSave || form.saving) return
        form.saving = true
        controller.save(form.buildDraft()) { ok ->
            form.saving = false
            if (ok) onDismiss()
        }
    }

    fun sendTest() {
        if (!form.canTest) return
        form.testing = true
        form.testReport = null
        val draft = form.buildDraft()
        scope.launch {
            try {
                form.testReport = controller.runTest(draft)
            } finally {
                form.testing = false
            }
        }
    }

    val title = str(if (form.isNew) R.string.webhookDialogAddTitle else R.string.webhookDialogEditTitle)
    FluxSheet(
        visible = visible,
        onDismissRequest = ::requestClose,
        detent = FluxSheetDetent.Full,
        dismissible = !form.saving,
        title = title,
        header = { FluxSheetHeader(title = title, subtitle = str(R.string.webhookDialogDesc), onClose = if (form.saving) null else ::requestClose) },
        footer = {
            FluxSheetFooter {
                FluxButton(
                    str(if (form.testing) R.string.webhookTesting else R.string.webhookSendTest),
                    onClick = ::sendTest,
                    variant = ButtonVariant.Secondary,
                    icon = FluxIcons.Send,
                    enabled = form.canTest,
                    loading = form.testing,
                    modifier = Modifier.weight(1f),
                )
                FluxButton(
                    str(R.string.webhookSaveEndpoint),
                    onClick = ::save,
                    variant = ButtonVariant.Primary,
                    enabled = form.canSave,
                    loading = form.saving,
                    modifier = Modifier.weight(1f),
                )
            }
        },
    ) {
        val preset = form.currentPreset
        val presetLabel = str(R.string.webhookFieldPreset)
        Column(Modifier.padding(top = 2.dp, bottom = 12.dp), verticalArrangement = Arrangement.spacedBy(14.dp)) {
            if (form.presets.isNotEmpty()) {
                FluxSelect(
                    value = preset?.label ?: form.preset,
                    onClick = {
                        openPicker(
                            PickerSpec(presetLabel, form.presets.map { SelectOption(it.id, it.label) }, form.preset) { form.preset = it },
                        )
                    },
                    label = presetLabel,
                    enabled = !form.saving,
                )
            }
            FluxField(
                value = form.name,
                onValueChange = { form.name = it },
                label = str(R.string.webhookFieldName),
                placeholder = preset?.label,
                keyboardOptions = KeyboardOptions(capitalization = KeyboardCapitalization.Words),
                enabled = !form.saving,
            )
            QueueSelect(form, queues, openPicker)
            FluxField(
                value = form.url,
                onValueChange = { form.url = it },
                label = str(R.string.webhookFieldUrl),
                placeholder = preset?.urlPlaceholder,
                hint = str(
                    when (form.preset) {
                        "ntfy" -> R.string.webhookUrlHintNtfy
                        "dingtalk" -> R.string.webhookUrlHintDingtalk
                        else -> R.string.webhookUrlHint
                    },
                ),
                error = if (form.urlTouched) {
                    when (form.urlError) {
                        WebhookTemplate.UrlError.Invalid -> str(R.string.webhookUrlInvalid)
                        WebhookTemplate.UrlError.WarnHttp -> str(R.string.webhookUrlWarnHttp)
                        null -> null
                    }
                } else {
                    null
                },
                mono = true,
                keyboardOptions = KeyboardOptions(capitalization = KeyboardCapitalization.None, autoCorrectEnabled = false, keyboardType = KeyboardType.Uri),
                onFocusChange = { focused -> if (!focused && form.url.isNotEmpty()) form.urlTouched = true },
                enabled = !form.saving,
            )
            form.testReport?.let { r ->
                FluxBanner(r.text, kind = if (r.success) FluxBannerKind.Success else FluxBannerKind.Error, slim = true)
            }

            GlassSection(title = str(R.string.webhookFieldEvents), footer = str(R.string.webhookEventsHint)) {
                WebhookEvent.entries.forEach { event ->
                    row {
                        val on = event.wire in form.events
                        FluxCheckboxRow(
                            title = str(event.labelRes()),
                            subtitle = event.wire,
                            state = if (on) ToggleableState.On else ToggleableState.Off,
                            onClick = { if (on) form.events.remove(event.wire) else form.events.add(event.wire) },
                            enabled = !form.saving,
                        )
                    }
                }
            }
            if (form.events.isEmpty()) {
                FluxBanner(str(R.string.webhookEventsEmpty), kind = FluxBannerKind.Warn, slim = true)
            }

            GlassSection {
                row {
                    FluxListRow(
                        title = str(R.string.webhookAdvanced),
                        icon = FluxIcons.Braces,
                        trailing = {
                            FluxSwitch(checked = form.advancedOpen, onCheckedChange = { form.advancedOpen = it })
                        },
                        onClick = { form.advancedOpen = !form.advancedOpen },
                    )
                }
            }
            if (form.advancedOpen) AdvancedContent(form, openPicker, context)

            // 实时请求预览
            GlassSection(title = str(R.string.webhookPreviewTitle), footer = str(R.string.webhookPreviewMeta)) {
                custom(padded = true) {
                    val text = WebhookTemplate.previewRequest(
                        url = form.url,
                        firstEvent = form.buildDraft().firstEventWire,
                        signEnabled = form.signEnabled,
                        template = form.template,
                        preset = preset,
                    )
                    SelectionContainer {
                        FluxText(text, style = type.monoS, color = c.inkMuted, overflow = androidx.compose.ui.text.style.TextOverflow.Clip)
                    }
                }
            }
        }
    }
}

@Composable
private fun QueueSelect(form: EditorForm, queues: List<Queue>, openPicker: (PickerSpec) -> Unit) {
    val allLabel = str(R.string.webhookQueueAll)
    val mainLabel = str(R.string.mainQueue)
    val laterLabel = str(R.string.laterQueue)
    val label = str(R.string.webhookFieldQueue)
    fun nameOf(q: Queue) = when (q.queueId) {
        Queue.MAIN -> mainLabel
        Queue.LATER -> laterLabel
        else -> q.name
    }
    val options = buildList {
        add(SelectOption("", allLabel))
        queues.forEach { add(SelectOption(it.queueId, nameOf(it))) }
        // 端点引用的队列已被删除：仍保留原值，避免编辑时被悄悄改成「全部队列」。
        if (form.queueId.isNotEmpty() && queues.none { it.queueId == form.queueId }) add(SelectOption(form.queueId, form.queueId))
    }
    FluxSelect(
        value = options.firstOrNull { it.value == form.queueId }?.label ?: allLabel,
        onClick = { openPicker(PickerSpec(label, options, form.queueId) { form.queueId = it }) },
        label = label,
        enabled = !form.saving,
    )
}

@Composable
private fun AdvancedContent(form: EditorForm, openPicker: (PickerSpec) -> Unit, context: android.content.Context) {
    val overlays = LocalFluxOverlays.current
    val type = FluxTheme.type
    val c = FluxTheme.colors
    Column(verticalArrangement = Arrangement.spacedBy(14.dp)) {
        // 请求头
        Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
            FluxText(str(R.string.webhookFieldHeaders), style = type.sm, color = c.inkMuted)
            form.headers.forEach { row ->
                key(row.id) {
                    Row(horizontalArrangement = Arrangement.spacedBy(8.dp), verticalAlignment = Alignment.CenterVertically) {
                        Column(Modifier.weight(1f), verticalArrangement = Arrangement.spacedBy(6.dp)) {
                            FluxField(
                                value = row.key,
                                onValueChange = { row.key = it },
                                placeholder = str(R.string.webhookHeaderName),
                                mono = true,
                                keyboardOptions = KeyboardOptions(capitalization = KeyboardCapitalization.None, autoCorrectEnabled = false),
                                enabled = !form.saving,
                            )
                            FluxField(
                                value = row.value,
                                onValueChange = { row.value = it },
                                placeholder = str(R.string.webhookHeaderValue),
                                mono = true,
                                keyboardOptions = KeyboardOptions(capitalization = KeyboardCapitalization.None, autoCorrectEnabled = false),
                                enabled = !form.saving,
                            )
                        }
                        FluxIconButton(
                            FluxIcons.Trash,
                            str(R.string.webhookRowDelete),
                            onClick = { form.headers.remove(row) },
                            size = IconButtonSize.Sm,
                            danger = true,
                        )
                    }
                }
            }
            FluxButton(
                str(R.string.webhookAddHeader),
                onClick = form::newHeader,
                variant = ButtonVariant.Ghost,
                size = ButtonSize.Sm,
                icon = FluxIcons.Plus,
            )
        }

        // 消息模板
        FluxField(
            value = form.template,
            onValueChange = { form.template = it },
            label = str(R.string.webhookFieldTemplate),
            placeholder = str(R.string.webhookTemplatePlaceholder),
            hint = str(R.string.webhookTemplateHint),
            mono = true,
            singleLine = false,
            rows = 5,
            keyboardOptions = KeyboardOptions(capitalization = KeyboardCapitalization.None, autoCorrectEnabled = false),
            enabled = !form.saving,
        )
        if (form.variables.isNotEmpty()) {
            val label = str(R.string.mobileWebhookInsertVariable)
            FluxButton(
                label,
                onClick = {
                    // 文本框没有光标 API：变量追加到模板末尾。
                    openPicker(PickerSpec(label, form.variables.map { SelectOption(it, it) }, null) { form.template += it })
                },
                variant = ButtonVariant.Secondary,
                size = ButtonSize.Sm,
                icon = FluxIcons.Braces,
                enabled = !form.saving,
            )
        }

        GlassSection {
            // 签名
            row {
                FluxSwitchRow(
                    title = str(R.string.webhookFieldSign),
                    subtitle = str(R.string.webhookSignDesc),
                    checked = form.signEnabled,
                    onCheckedChange = { on ->
                        form.signEnabled = on
                        // 开启签名时给一个够长够随机的起点；用户可随时改成自己的。
                        if (on && form.secret.isBlank()) form.secret = WebhookTemplate.generateSecret()
                    },
                )
            }
            if (form.signEnabled) {
                custom(padded = true) {
                    Column(verticalArrangement = Arrangement.spacedBy(10.dp)) {
                        FluxField(
                            value = form.secret,
                            onValueChange = {
                                form.secret = it
                                form.secretCopied = false
                            },
                            label = str(R.string.webhookFieldSign),
                            mono = true,
                            visualTransformation = if (form.showSecret) VisualTransformation.None else PasswordVisualTransformation(),
                            keyboardOptions = KeyboardOptions(capitalization = KeyboardCapitalization.None, autoCorrectEnabled = false, keyboardType = KeyboardType.Password),
                            trailing = {
                                FluxFieldAction(
                                    icon = if (form.showSecret) FluxIcons.EyeOff else FluxIcons.Eye,
                                    contentDescription = str(if (form.showSecret) R.string.webHideKey else R.string.webShowKey),
                                    onClick = { form.showSecret = !form.showSecret },
                                )
                            },
                            enabled = !form.saving,
                        )
                        Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                            FluxButton(
                                str(R.string.webhookRegenerate),
                                onClick = {
                                    form.secretCopied = false
                                    form.secret = WebhookTemplate.generateSecret()
                                },
                                variant = ButtonVariant.Secondary,
                                size = ButtonSize.Sm,
                                icon = FluxIcons.RefreshCw,
                            )
                            FluxButton(
                                str(if (form.secretCopied) R.string.webhookCopied else R.string.webhookCopy),
                                onClick = {
                                    copyToClipboard(context, "webhook secret", form.secret, sensitive = true)
                                    form.secretCopied = true
                                    overlays.toast(context.getString(R.string.webhookCopied), FluxToastKind.Success)
                                },
                                variant = ButtonVariant.Ghost,
                                size = ButtonSize.Sm,
                                icon = if (form.secretCopied) FluxIcons.Check else FluxIcons.Copy,
                                enabled = form.secret.isNotEmpty(),
                            )
                        }
                    }
                }
            }
            row {
                FluxSwitchRow(
                    title = str(R.string.webhookFieldAllowHttp),
                    subtitle = str(R.string.webhookAllowHttpDesc),
                    checked = form.allowHttp,
                    onCheckedChange = { form.allowHttp = it },
                )
            }
            row {
                FluxSwitchRow(
                    title = str(R.string.webhookFieldUseProxy),
                    subtitle = str(R.string.webhookUseProxyDesc),
                    checked = form.useProxy,
                    onCheckedChange = { form.useProxy = it },
                )
            }
        }
    }
}

// ───────────────────────────── 搜索 ─────────────────────────────

/** 页面空态（无端点也无投递记录）：只渲染空态卡片，不渲染端点列表与投递日志；页面与搜索共用。 */
private fun isWebhookPageBlank(endpoints: List<WebhookEndpoint>, log: List<WebhookDelivery>): Boolean =
    endpoints.isEmpty() && log.isEmpty()

/** 搜索条目：与页面共用同一可见性判定（无 `daemon.webhooks` 能力时页面只显示不可用说明，不产出行条目；空态无日志区）。 */
internal fun webhookSearchEntries(ctx: SettingsSearchContext): List<SettingsEntry> {
    if (!ctx.has(HostCapability.daemonWebhooks)) return emptyList()
    val crumb = ctx.crumb(R.string.webhookNavTitle)
    val endpoints = WebhookEndpoint.parseList(ctx.state.config[WebhookEndpoint.CONFIG_KEY])
    val log = WebhookDelivery.parseList(
        com.fluxdown.core.protocol.Json.parseOrNull(ctx.state.sections[HostSection.daemonWebhookDeliveries]),
    )
    val entries = ArrayList<SettingsEntry>(2)
    entries += ctx.entry(ID_ENDPOINTS, com.fluxdown.app.nav.SettingsPage.Webhook, KEY_ENDPOINTS, R.string.notifyGroupWebhook, R.string.webhookSemantics, crumb, FluxIcons.Webhook)
    if (!isWebhookPageBlank(endpoints, log)) {
        entries += ctx.entry(ID_LOG, com.fluxdown.app.nav.SettingsPage.Webhook, KEY_LOG, R.string.webhookDeliveryLog, R.string.webhookLogSubtitle, crumb, FluxIcons.Activity)
    }
    return entries
}
