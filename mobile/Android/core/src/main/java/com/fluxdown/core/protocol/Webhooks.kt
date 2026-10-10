package com.fluxdown.core.protocol

import com.fluxdown.core.host.HostErrorCode
import com.fluxdown.core.host.HostException
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/*
 * Webhook（`daemon.webhook.*` + 配置键 `webhook.endpoints`）：DTO 镜像 `native/protocol/src/daemon.rs` 的 Webhook 段
 * （同 iOS `Webhooks.swift`、Web `web/src/lib/rpc/protocol/webhook.ts`）；端点解析 / 写入对齐
 * `engine/src/webhook.rs::EndpointSpec`、GPUI `parse_endpoints`、Web `pages/webhooks/endpoints.ts`
 * （`parseEndpoints` / `patchEndpoints`）与 GPUI `SettingsStore::mutate_daemon`。
 */

// ───────────────────────────── DTO ─────────────────────────────

/** 一条投递记录（`WebhookDeliveryDto`；新的在前，上限 1000 条）。 */
data class WebhookDelivery(
    val deliveryId: String,
    /** Unix 毫秒。 */
    val timestampMs: Long = 0,
    /** 事件 wire 名（`task.completed` 等）。 */
    val event: String = "",
    val endpointId: String = "",
    val endpointName: String = "",
    val url: String = "",
    /** 请求头摘录，每行 `K: V`；鉴权类值已掩码。 */
    val requestHeaders: String = "",
    val requestBody: String = "",
    /** HTTP 状态码；0 = 未拿到响应。 */
    val statusCode: Int = 0,
    val responseBody: String = "",
    val latencyMs: Long = 0,
    val attempts: Int = 1,
    val success: Boolean = false,
    val error: String = "",
) {
    /** 行内状态文案：成功 `状态码 · 延迟ms`；失败 错误信息或 `HTTP 状态码`（与 GPUI / Web 一致）。 */
    val statusSummary: String
        get() = if (success) "$statusCode · ${latencyMs}ms" else error.ifEmpty { "HTTP $statusCode" }

    companion object {
        /** 缺 `deliveryId` 的元素无法作为列表键，视为无效而跳过。 */
        fun fromJson(v: JsonValue?): WebhookDelivery? {
            val id = v.strOrNull("deliveryId") ?: return null
            return WebhookDelivery(
                deliveryId = id,
                timestampMs = v.long("timestampMs"),
                event = v.str("event"),
                endpointId = v.str("endpointId"),
                endpointName = v.str("endpointName"),
                url = v.str("url"),
                requestHeaders = v.str("requestHeaders"),
                requestBody = v.str("requestBody"),
                statusCode = v.int("statusCode"),
                responseBody = v.str("responseBody"),
                latencyMs = v.long("latencyMs"),
                attempts = v.int("attempts", 1),
                success = v.bool("success", false),
                error = v.str("error"),
            )
        }

        /** 分区 `daemon.webhookDeliveries`（`Vec<WebhookDeliveryDto>`）→ 列表；非数组为空。 */
        fun parseList(v: JsonValue?): List<WebhookDelivery> = v.arrayOrNull.orEmpty().mapNotNull { fromJson(it) }
    }
}

/** 服务预设元数据（`WebhookPresetDto`）：前端只做占位符替换预览，不复制模板内容。 */
data class WebhookPreset(
    val id: String,
    val label: String,
    val urlPlaceholder: String = "",
    val defaultTemplate: String = "",
    val contentType: String = "application/json",
) {
    companion object {
        fun fromJson(v: JsonValue?): WebhookPreset? {
            val id = v.strOrNull("id") ?: return null
            return WebhookPreset(
                id = id,
                label = v.str("label", id),
                urlPlaceholder = v.str("urlPlaceholder"),
                defaultTemplate = v.str("defaultTemplate"),
                contentType = v.str("contentType", "application/json"),
            )
        }
    }
}

/** `daemon.webhook.get` 结果：投递日志 + 预设目录 + 可用占位符。 */
data class WebhookDeliveriesResponse(
    val deliveries: List<WebhookDelivery> = emptyList(),
    val presets: List<WebhookPreset> = emptyList(),
    /** 可用占位符清单（`{task.fileName}` 等）。 */
    val variables: List<String> = emptyList(),
) {
    companion object {
        fun fromJson(v: JsonValue?): WebhookDeliveriesResponse = WebhookDeliveriesResponse(
            deliveries = WebhookDelivery.parseList(v["deliveries"]),
            presets = v.list("presets").mapNotNull { WebhookPreset.fromJson(it) },
            variables = v.strings("variables"),
        )
    }
}

/** `daemon.webhook.test` 结果。 */
data class WebhookTestResponse(
    val success: Boolean,
    /** HTTP 状态码；0 = 未拿到响应。 */
    val statusCode: Int = 0,
    val latencyMs: Long = 0,
    /** 成功为空。 */
    val error: String = "",
) {
    companion object {
        fun fromJson(v: JsonValue?): WebhookTestResponse = WebhookTestResponse(
            success = v.bool("success", false),
            statusCode = v.int("statusCode"),
            latencyMs = v.long("latencyMs"),
            error = v.str("error"),
        )
    }
}

/** `daemon.webhook.simulate` 回执。 */
data class WebhookSimulateResponse(
    /** 投出的端点数；0 = 没有端点订阅 `task.completed`。 */
    val dispatched: Int,
) {
    companion object {
        fun fromJson(v: JsonValue?): WebhookSimulateResponse = WebhookSimulateResponse(v.int("dispatched"))
    }
}

// ───────────────────────────── 端点 ─────────────────────────────

/** 事件 wire 名（与引擎 `WebhookEventKind::wire()` 逐字一致；声明顺序 = 规范顺序）。 */
enum class WebhookEvent(val wire: String) {
    Created("task.created"),
    Started("task.started"),
    Completed("task.completed"),
    Failed("task.failed"),
    Paused("task.paused"),
    QueueDrained("queue.drained"),
    ;

    companion object {
        /** 新端点默认订阅任务完成与失败事件。 */
        val defaultWires: List<String> = listOf(Completed.wire, Failed.wire)
    }
}

/** 端点（`engine::webhook::EndpointSpec`，camelCase）。`daemon.webhook.test` 直接把它当草稿发送。 */
data class WebhookEndpoint(
    val id: String,
    val name: String = "",
    val preset: String = PRESET_CUSTOM,
    val url: String = "",
    val enabled: Boolean = true,
    val events: List<String> = WebhookEvent.defaultWires,
    val queueId: String = "",
    val headers: Map<String, String> = emptyMap(),
    val bodyTemplate: String = "",
    val signSecret: String = "",
    val allowHttp: Boolean = false,
    val useProxy: Boolean = false,
) {
    /** 草稿里订阅的第一个事件（按规范顺序），供预览用。 */
    val firstEventWire: String?
        get() = WebhookEvent.entries.map { it.wire }.firstOrNull { it in events }

    fun toJson(): JsonValue = jsonObject(
        "id" to id,
        "name" to name,
        "preset" to preset,
        "url" to url,
        "enabled" to enabled,
        "events" to events,
        "queueId" to queueId,
        "headers" to headers,
        "bodyTemplate" to bodyTemplate,
        "signSecret" to signSecret,
        "allowHttp" to allowHttp,
        "useProxy" to useProxy,
    )

    companion object {
        const val CONFIG_KEY = "webhook.endpoints"
        const val PRESET_CUSTOM = "custom"

        /**
         * 宽松解析一个元素：非对象为 null；字段类型不符回退默认值
         * （`enabled` 缺省 true，其余布尔缺省 false；字符串缺省空串；数组 / 对象里类型不符的成员跳过）。
         */
        fun fromJsonLenient(v: JsonValue?): WebhookEndpoint? {
            if (v !is JsonValue.Obj) return null
            return WebhookEndpoint(
                id = v.str("id"),
                name = v.str("name"),
                preset = v.str("preset"),
                url = v.str("url"),
                enabled = v.bool("enabled", true),
                events = v.strings("events"),
                queueId = v.str("queueId"),
                headers = v.stringMap("headers"),
                bodyTemplate = v.str("bodyTemplate"),
                signSecret = v.str("signSecret"),
                allowHttp = v.bool("allowHttp", false),
                useProxy = v.bool("useProxy", false),
            )
        }

        /** 配置串 → 端点列表（`parseEndpoints`）：空串 / 非 JSON 数组为空列表；非对象元素跳过。 */
        fun parseList(raw: String?): List<WebhookEndpoint> {
            if (raw == null || raw.isBlank()) return emptyList()
            val items = Json.parseOrNull(raw).arrayOrNull ?: return emptyList()
            return items.mapNotNull { fromJsonLenient(it) }
        }

        /** 端点列表 → 配置串（JSON 数组）。 */
        fun encodeList(list: List<WebhookEndpoint>): String = JsonValue.Arr(list.map { it.toJson() }).toJson()
    }
}

/** 端点的最近一次投递（按时间戳最大；同戳取先出现者）。 */
fun latestWebhookDelivery(deliveries: List<WebhookDelivery>, endpointId: String): WebhookDelivery? {
    var latest: WebhookDelivery? = null
    for (d in deliveries) {
        if (d.endpointId != endpointId) continue
        if (latest == null || d.timestampMs > latest.timestampMs) latest = d
    }
    return latest
}

// ───────────────────────────── 读-改-写 ─────────────────────────────

/** `daemon.config.get` 结果（`DaemonConfigSnapshot`）。 */
data class WebhookConfigSnapshot(val revision: Long, val values: Map<String, String>) {
    companion object {
        fun fromJson(v: JsonValue?): WebhookConfigSnapshot = WebhookConfigSnapshot(v.long("revision"), v.stringMap("values"))
    }
}

/** 端点写入依赖的主机端口（生产实现读 `HostStore` + `HostSession`；测试用假实现）。 */
interface WebhookConfigPort {
    /** 快照里的最新配置；未连接为 null。 */
    val cachedConfig: WebhookConfigSnapshot?

    /** `daemon.config.get`。 */
    suspend fun fetchConfig(): WebhookConfigSnapshot

    /** `daemon.config.patch`：revision 不符抛 [HostErrorCode.Conflict]。 */
    suspend fun patchConfig(expectedRevision: Long, values: Map<String, String>)
}

/**
 * 读-改-写端点列表（AGENTS.md §5 镜像契约：写入始终基于最新配置重算，冲突时重读并重放）。
 *
 * 这是唯一允许绕过 `ConfigEditor` 的设置写入：`webhook.endpoints` 是整份 JSON 数组，
 * `ConfigEditor` 的整键覆盖会把别的客户端（Web / GPUI）刚加的端点冲掉，所以每次写入都要
 * 「取最新值 → 应用幂等变更 → `expectedRevision` 补丁 → Conflict 重取重放」，与 GPUI
 * `SettingsStore::mutate_daemon`、Web `patchEndpoints` 一致。
 *
 * - 每次都基于最新快照重新计算；`expectedRevision` 冲突时重新 `daemon.config.get` 后再算一遍（最多 [MAX_CONFLICT_RETRIES] 次）。
 * - 写入串行化（FIFO 互斥）：连续操作不会互相制造冲突，也不会基于过期数组回写。
 */
class WebhookEndpointWriter(private val port: WebhookConfigPort) {
    private val lock = Mutex()

    /** [transform] 返回 null 表示无需写入。失败抛 [HostException]。 */
    suspend fun mutate(transform: (List<WebhookEndpoint>) -> List<WebhookEndpoint>?) {
        lock.withLock { perform(transform) }
    }

    /** 新增或按 id 覆盖。 */
    suspend fun upsert(draft: WebhookEndpoint) = mutate { list ->
        val index = list.indexOfFirst { it.id == draft.id }
        if (index < 0) list + draft else list.toMutableList().also { it[index] = draft }
    }

    suspend fun setEnabled(id: String, enabled: Boolean) = mutate { list ->
        if (list.none { it.id == id }) null else list.map { if (it.id == id) it.copy(enabled = enabled) else it }
    }

    suspend fun remove(id: String) = mutate { list -> list.filter { it.id != id } }

    private suspend fun perform(transform: (List<WebhookEndpoint>) -> List<WebhookEndpoint>?) {
        var config = port.cachedConfig
        var attempt = 0
        while (true) {
            val current = config ?: port.fetchConfig()
            val next = transform(WebhookEndpoint.parseList(current.values[WebhookEndpoint.CONFIG_KEY])) ?: return
            val encoded = WebhookEndpoint.encodeList(next)
            try {
                port.patchConfig(current.revision, mapOf(WebhookEndpoint.CONFIG_KEY to encoded))
                return
            } catch (e: HostException) {
                if (e.code != HostErrorCode.Conflict || attempt >= MAX_CONFLICT_RETRIES) throw e
                attempt += 1
                config = port.fetchConfig()
            }
        }
    }

    companion object {
        /** 与 GPUI `MAX_CONFLICT_RETRIES` 一致。 */
        const val MAX_CONFLICT_RETRIES = 3
    }
}
