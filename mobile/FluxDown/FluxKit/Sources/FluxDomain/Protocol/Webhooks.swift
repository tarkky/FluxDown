import Foundation

// Webhook（`daemon.webhook.*` + 配置键 `webhook.endpoints`）：DTO 镜像 `native/protocol/src/daemon.rs` 的
// Webhook 段（同 `web/src/lib/rpc/protocol/webhook.ts`）；端点解析 / 写入对齐
// `web/src/pages/webhooks/endpoints.ts`（`parseEndpoints` / `patchEndpoints`）与 GPUI `SettingsStore::mutate_daemon`。

// MARK: - DTO

/// 一条投递记录（`WebhookDeliveryDto`；新的在前，上限 1000 条）。
public struct WebhookDelivery: Codable, Sendable, Hashable, Identifiable {
    public var deliveryId: String
    /// Unix 毫秒。
    public var timestampMs: Int64
    /// 事件 wire 名（`task.completed` 等）。
    public var event: String
    public var endpointId: String
    public var endpointName: String
    public var url: String
    /// 请求头摘录，每行 `K: V`；鉴权类值已掩码。
    public var requestHeaders: String
    public var requestBody: String
    /// HTTP 状态码；0 = 未拿到响应。
    public var statusCode: Int
    public var responseBody: String
    public var latencyMs: Int64
    public var attempts: Int
    public var success: Bool
    public var error: String

    public var id: String { deliveryId }

    public init(
        deliveryId: String,
        timestampMs: Int64,
        event: String,
        endpointId: String,
        endpointName: String,
        url: String = "",
        requestHeaders: String = "",
        requestBody: String = "",
        statusCode: Int = 0,
        responseBody: String = "",
        latencyMs: Int64 = 0,
        attempts: Int = 1,
        success: Bool = true,
        error: String = ""
    ) {
        self.deliveryId = deliveryId
        self.timestampMs = timestampMs
        self.event = event
        self.endpointId = endpointId
        self.endpointName = endpointName
        self.url = url
        self.requestHeaders = requestHeaders
        self.requestBody = requestBody
        self.statusCode = statusCode
        self.responseBody = responseBody
        self.latencyMs = latencyMs
        self.attempts = attempts
        self.success = success
        self.error = error
    }

    public init(from decoder: any Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        deliveryId = try c.decode(String.self, forKey: .deliveryId)
        timestampMs = try c.decodeIfPresent(Int64.self, forKey: .timestampMs) ?? 0
        event = try c.decodeIfPresent(String.self, forKey: .event) ?? ""
        endpointId = try c.decodeIfPresent(String.self, forKey: .endpointId) ?? ""
        endpointName = try c.decodeIfPresent(String.self, forKey: .endpointName) ?? ""
        url = try c.decodeIfPresent(String.self, forKey: .url) ?? ""
        requestHeaders = try c.decodeIfPresent(String.self, forKey: .requestHeaders) ?? ""
        requestBody = try c.decodeIfPresent(String.self, forKey: .requestBody) ?? ""
        statusCode = try c.decodeIfPresent(Int.self, forKey: .statusCode) ?? 0
        responseBody = try c.decodeIfPresent(String.self, forKey: .responseBody) ?? ""
        latencyMs = try c.decodeIfPresent(Int64.self, forKey: .latencyMs) ?? 0
        attempts = try c.decodeIfPresent(Int.self, forKey: .attempts) ?? 1
        success = try c.decodeIfPresent(Bool.self, forKey: .success) ?? false
        error = try c.decodeIfPresent(String.self, forKey: .error) ?? ""
    }

    /// 行内状态文案：成功 `状态码 · 延迟ms`；失败 错误信息或 `HTTP 状态码`（与 GPUI / Web 一致）。
    public var statusSummary: String {
        if success { return "\(statusCode) · \(latencyMs)ms" }
        return error.isEmpty ? "HTTP \(statusCode)" : error
    }
}

/// 服务预设元数据（`WebhookPresetDto`）：前端只做占位符替换预览，不复制模板内容。
public struct WebhookPreset: Codable, Sendable, Hashable, Identifiable {
    public var id: String
    public var label: String
    public var urlPlaceholder: String
    public var defaultTemplate: String
    public var contentType: String

    public init(id: String, label: String, urlPlaceholder: String = "", defaultTemplate: String = "", contentType: String = "application/json") {
        self.id = id
        self.label = label
        self.urlPlaceholder = urlPlaceholder
        self.defaultTemplate = defaultTemplate
        self.contentType = contentType
    }

    public init(from decoder: any Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        id = try c.decode(String.self, forKey: .id)
        label = try c.decodeIfPresent(String.self, forKey: .label) ?? id
        urlPlaceholder = try c.decodeIfPresent(String.self, forKey: .urlPlaceholder) ?? ""
        defaultTemplate = try c.decodeIfPresent(String.self, forKey: .defaultTemplate) ?? ""
        contentType = try c.decodeIfPresent(String.self, forKey: .contentType) ?? "application/json"
    }
}

/// `daemon.webhook.get` 结果：投递日志 + 预设目录 + 可用占位符。
public struct WebhookDeliveriesResponse: Codable, Sendable, Hashable {
    public var deliveries: [WebhookDelivery]
    public var presets: [WebhookPreset]
    /// 可用占位符清单（`{task.fileName}` 等）。
    public var variables: [String]

    public init(deliveries: [WebhookDelivery] = [], presets: [WebhookPreset] = [], variables: [String] = []) {
        self.deliveries = deliveries
        self.presets = presets
        self.variables = variables
    }

    public init(from decoder: any Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        deliveries = try c.decodeIfPresent([WebhookDelivery].self, forKey: .deliveries) ?? []
        presets = try c.decodeIfPresent([WebhookPreset].self, forKey: .presets) ?? []
        variables = try c.decodeIfPresent([String].self, forKey: .variables) ?? []
    }
}

/// `daemon.webhook.test` 结果。
public struct WebhookTestResponse: Codable, Sendable, Hashable {
    public var success: Bool
    /// HTTP 状态码；0 = 未拿到响应。
    public var statusCode: Int
    public var latencyMs: Int64
    /// 成功为空。
    public var error: String

    public init(success: Bool, statusCode: Int = 0, latencyMs: Int64 = 0, error: String = "") {
        self.success = success
        self.statusCode = statusCode
        self.latencyMs = latencyMs
        self.error = error
    }

    public init(from decoder: any Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        success = try c.decode(Bool.self, forKey: .success)
        statusCode = try c.decodeIfPresent(Int.self, forKey: .statusCode) ?? 0
        latencyMs = try c.decodeIfPresent(Int64.self, forKey: .latencyMs) ?? 0
        error = try c.decodeIfPresent(String.self, forKey: .error) ?? ""
    }
}

/// `daemon.webhook.simulate` 回执。
public struct WebhookSimulateResponse: Codable, Sendable, Hashable {
    /// 投出的端点数；0 = 没有端点订阅 `task.completed`。
    public var dispatched: Int

    public init(dispatched: Int) {
        self.dispatched = dispatched
    }
}

// MARK: - 端点

/// 事件 wire 名（与引擎 `WebhookEventKind::wire()` 逐字一致）及其 i18n 键。
public enum WebhookEvent: CaseIterable, Sendable, Hashable {
    case created, started, completed, failed, paused, queueDrained

    public var wire: String {
        switch self {
        case .created: "task.created"
        case .started: "task.started"
        case .completed: "task.completed"
        case .failed: "task.failed"
        case .paused: "task.paused"
        case .queueDrained: "queue.drained"
        }
    }

    public var labelKey: String {
        switch self {
        case .created: "webhookEventCreated"
        case .started: "webhookEventStarted"
        case .completed: "webhookEventCompleted"
        case .failed: "webhookEventFailed"
        case .paused: "webhookEventPaused"
        case .queueDrained: "webhookEventQueueDrained"
        }
    }

    /// 新端点默认订阅任务完成与失败事件。
    public static let defaultWires: [String] = [WebhookEvent.completed.wire, WebhookEvent.failed.wire]
}

/// 端点（`engine::webhook::EndpointSpec`，camelCase）。`daemon.webhook.test` 直接把它当草稿发送。
public struct WebhookEndpoint: Codable, Sendable, Hashable, Identifiable {
    public static let configKey = "webhook.endpoints"
    public static let presetCustom = "custom"

    public var id: String
    public var name: String
    public var preset: String
    public var url: String
    public var enabled: Bool
    public var events: [String]
    public var queueId: String
    public var headers: [String: String]
    public var bodyTemplate: String
    public var signSecret: String
    public var allowHttp: Bool
    public var useProxy: Bool

    public init(
        id: String,
        name: String = "",
        preset: String = WebhookEndpoint.presetCustom,
        url: String = "",
        enabled: Bool = true,
        events: [String] = WebhookEvent.defaultWires,
        queueId: String = "",
        headers: [String: String] = [:],
        bodyTemplate: String = "",
        signSecret: String = "",
        allowHttp: Bool = false,
        useProxy: Bool = false
    ) {
        self.id = id
        self.name = name
        self.preset = preset
        self.url = url
        self.enabled = enabled
        self.events = events
        self.queueId = queueId
        self.headers = headers
        self.bodyTemplate = bodyTemplate
        self.signSecret = signSecret
        self.allowHttp = allowHttp
        self.useProxy = useProxy
    }

    /// 宽松解析一个元素：非对象为 nil；字段类型不符回退默认值（`enabled` 缺省 true，其余布尔缺省 false）。
    public init?(lenient value: JSONValue) {
        guard case let .object(item) = value else { return nil }
        func text(_ key: String) -> String { item[key]?.stringValue ?? "" }
        func flag(_ key: String, _ fallback: Bool) -> Bool { item[key]?.boolValue ?? fallback }
        var headers: [String: String] = [:]
        if case let .object(raw)? = item["headers"] {
            for (key, entry) in raw {
                if let string = entry.stringValue { headers[key] = string }
            }
        }
        var events: [String] = []
        if case let .array(raw)? = item["events"] {
            events = raw.compactMap(\.stringValue)
        }
        self.init(
            id: text("id"),
            name: text("name"),
            preset: text("preset"),
            url: text("url"),
            enabled: flag("enabled", true),
            events: events,
            queueId: text("queueId"),
            headers: headers,
            bodyTemplate: text("bodyTemplate"),
            signSecret: text("signSecret"),
            allowHttp: flag("allowHttp", false),
            useProxy: flag("useProxy", false)
        )
    }

    public init(from decoder: any Decoder) throws {
        let value = try JSONValue(from: decoder)
        guard let parsed = WebhookEndpoint(lenient: value) else {
            throw DecodingError.typeMismatch(
                WebhookEndpoint.self,
                DecodingError.Context(codingPath: decoder.codingPath, debugDescription: "webhook endpoint must be an object")
            )
        }
        self = parsed
    }

    /// 配置串 → 端点列表（`parseEndpoints`）：空串 / 非 JSON 数组为空列表；非对象元素跳过；字段类型不符回退默认值。
    public static func parseList(_ raw: String?) -> [WebhookEndpoint] {
        guard let raw, !raw.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty,
              let value = try? ProtocolJSON.makeDecoder().decode(JSONValue.self, from: Data(raw.utf8)),
              case let .array(items) = value
        else { return [] }
        return items.compactMap { WebhookEndpoint(lenient: $0) }
    }

    /// 端点列表 → 配置串（JSON 数组）。
    public static func encodeList(_ list: [WebhookEndpoint]) throws(HostError) -> String {
        let data = try ProtocolJSON.encode(list, what: WebhookEndpoint.configKey)
        return String(decoding: data, as: UTF8.self)
    }

    /// 草稿里订阅的第一个事件（按规范顺序），供预览用。
    public var firstEventWire: String? {
        WebhookEvent.allCases.map(\.wire).first { events.contains($0) }
    }
}

/// 端点的最近一次投递（按时间戳最大）。
public func latestWebhookDelivery(_ deliveries: [WebhookDelivery], endpointId: String) -> WebhookDelivery? {
    var latest: WebhookDelivery?
    for delivery in deliveries where delivery.endpointId == endpointId {
        if latest == nil || delivery.timestampMs > (latest?.timestampMs ?? 0) { latest = delivery }
    }
    return latest
}

// MARK: - 读-改-写

/// `daemon.config.get` 结果（`DaemonConfigSnapshot`）。
public struct WebhookConfigSnapshot: Codable, Sendable, Hashable {
    public var revision: UInt64
    public var values: [String: String]

    public init(revision: UInt64, values: [String: String]) {
        self.revision = revision
        self.values = values
    }
}

/// 端点写入依赖的主机端口（生产实现读 `HostStore` + `HostSession`；测试用假实现）。
@MainActor
public protocol WebhookConfigPort: AnyObject {
    /// 快照里的最新配置；未连接为 nil。
    var cachedConfig: WebhookConfigSnapshot? { get }
    /// `daemon.config.get`。
    func fetchConfig() async throws(HostError) -> WebhookConfigSnapshot
    /// `daemon.config.patch`：revision 不符抛 `.conflict`。
    func patchConfig(expectedRevision: UInt64, values: [String: String]) async throws(HostError)
}

/// 读-改-写端点列表（AGENTS.md §5：写入始终基于最新配置重算，冲突时重读并重放）。
///
/// - 每次都基于最新快照重新计算；`expectedRevision` 冲突时重新 `daemon.config.get` 后再算一遍（最多 3 次）。
/// - 写入串行化：连续操作不会互相制造冲突，也不会基于过期数组回写。
@MainActor
public final class WebhookEndpointWriter {
    /// 与 GPUI `MAX_CONFLICT_RETRIES` 一致。
    public static let maxConflictRetries = 3

    private let port: any WebhookConfigPort
    private var tail: Task<Void, Never>?

    public init(port: any WebhookConfigPort) {
        self.port = port
    }

    /// `transform` 返回 nil 表示无需写入。
    public func mutate(_ transform: @escaping @MainActor ([WebhookEndpoint]) -> [WebhookEndpoint]?) async throws(HostError) {
        let previous = tail
        let run = Task { @MainActor [self] () async -> Result<Void, HostError> in
            await previous?.value
            return await perform(transform)
        }
        tail = Task { _ = await run.value }
        try await run.value.get()
    }

    /// 新增或按 id 覆盖。
    public func upsert(_ draft: WebhookEndpoint) async throws(HostError) {
        try await mutate { list in
            guard let index = list.firstIndex(where: { $0.id == draft.id }) else { return list + [draft] }
            var next = list
            next[index] = draft
            return next
        }
    }

    public func setEnabled(id: String, enabled: Bool) async throws(HostError) {
        try await mutate { list in
            guard list.contains(where: { $0.id == id }) else { return nil }
            return list.map { entry in
                var copy = entry
                if copy.id == id { copy.enabled = enabled }
                return copy
            }
        }
    }

    public func remove(id: String) async throws(HostError) {
        try await mutate { list in list.filter { $0.id != id } }
    }

    private func perform(_ transform: @MainActor ([WebhookEndpoint]) -> [WebhookEndpoint]?) async -> Result<Void, HostError> {
        do throws(HostError) {
            var config = port.cachedConfig
            var attempt = 0
            while true {
                let current: WebhookConfigSnapshot
                if let config {
                    current = config
                } else {
                    current = try await port.fetchConfig()
                }
                guard let next = transform(WebhookEndpoint.parseList(current.values[WebhookEndpoint.configKey])) else {
                    return .success(())
                }
                let encoded = try WebhookEndpoint.encodeList(next)
                do throws(HostError) {
                    try await port.patchConfig(expectedRevision: current.revision, values: [WebhookEndpoint.configKey: encoded])
                    return .success(())
                } catch {
                    guard error.code == .conflict, attempt < Self.maxConflictRetries else { throw error }
                    attempt += 1
                    config = try await port.fetchConfig()
                }
            }
        } catch {
            return .failure(error)
        }
    }
}
