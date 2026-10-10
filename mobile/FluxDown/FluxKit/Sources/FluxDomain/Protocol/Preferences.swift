import Foundation

// agent 偏好（`agent.preferences.*`）与自定义分类（偏好键 `custom_categories`）。
// 镜像 `native/protocol/src/agent.rs`（AgentPreferencesDto / AgentPreferencesPatchResult / CustomCategoryDto）
// 与 `web/src/lib/rpc/protocol/*.ts`。

/// `agent.preferences` 分区值：偏好全集及其原子版本。
public struct AgentPreferencesDto: Sendable, Hashable, Codable {
    public var revision: UInt64
    public var values: [String: JSONValue]

    public init(revision: UInt64 = 0, values: [String: JSONValue] = [:]) {
        self.revision = revision
        self.values = values
    }

    private enum CodingKeys: String, CodingKey { case revision, values }

    public init(from decoder: any Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        revision = try container.decodeIfPresent(UInt64.self, forKey: .revision) ?? 0
        values = try container.decodeIfPresent([String: JSONValue].self, forKey: .values) ?? [:]
    }

    public subscript(key: String) -> JSONValue? { values[key] }

    public func bool(_ key: String, default fallback: Bool) -> Bool {
        values[key]?.boolValue ?? fallback
    }

    public func string(_ key: String, default fallback: String) -> String {
        values[key]?.stringValue ?? fallback
    }
}

/// `agent.preferences.patch` 参数。`sync` 省略 = 同步（目录外的键须显式 `false`）；`null` 值 = 恢复默认（墓碑）。
public struct PreferencesPatchParams: Sendable, Hashable, Encodable {
    public var values: [String: JSONValue]
    public var sync: Bool?

    public init(values: [String: JSONValue], sync: Bool? = nil) {
        self.values = values
        self.sync = sync
    }
}

/// `agent.preferences.patch` 结果：`revision` 为写入落定后的偏好版本；此后携带不低于它的
/// 分区 / 事件必然已包含本次写入（响应与事件不保证先后，客户端据此判断在途写入何时被确认）。
public struct PreferencesPatchResult: Sendable, Hashable, Codable {
    public var ok: Bool
    public var revision: UInt64

    public init(ok: Bool = true, revision: UInt64 = 0) {
        self.ok = ok
        self.revision = revision
    }
}

public extension HostState {
    /// 当前偏好（`agent.preferences` 分区）；分区缺失 / 损坏为空。相同字节只解码一次。
    var preferences: AgentPreferencesDto {
        guard let data = sections[HostSection.agentPreferences] else { return AgentPreferencesDto() }
        return PreferencesDecodeCache.shared.decode(data)
    }
}

/// 单项解码缓存：视图每次重绘都会读 `preferences`，而分区字节只在偏好变化时才变。
private final class PreferencesDecodeCache: @unchecked Sendable {
    static let shared = PreferencesDecodeCache()

    private let lock = NSLock()
    private var lastData: Data?
    private var lastValue = AgentPreferencesDto()

    func decode(_ data: Data) -> AgentPreferencesDto {
        lock.lock()
        defer { lock.unlock() }
        if lastData == data { return lastValue }
        let value = (try? ProtocolJSON.makeDecoder().decode(AgentPreferencesDto.self, from: data)) ?? AgentPreferencesDto()
        lastData = data
        lastValue = value
        return value
    }
}

// MARK: - 自定义分类

/// 自定义分类（偏好键 `custom_categories`，与 Rust protocol 的 `CustomCategoryDto` 同 JSON 形状）。
/// 与 `TaskCategory`（匹配用的只读投影）不同，这里保留编辑所需的全部字段（匹配模式 / 保存目录 / 内置标记）。
public struct CustomCategoryDto: Sendable, Hashable, Identifiable, Codable {
    public static let preferenceKey = "custom_categories"

    public var id: String
    public var name: String
    public var icon: String
    /// `extension` | `regex`。
    public var matchMode: String
    public var extensions: [String]
    public var regexPattern: String
    public var position: Int64
    public var visible: Bool
    public var isBuiltin: Bool
    public var builtinType: String?
    public var saveDir: String

    public init(
        id: String,
        name: String,
        icon: String = "file",
        matchMode: String = "extension",
        extensions: [String] = [],
        regexPattern: String = "",
        position: Int64 = 0,
        visible: Bool = true,
        isBuiltin: Bool = false,
        builtinType: String? = nil,
        saveDir: String = ""
    ) {
        self.id = id
        self.name = name
        self.icon = icon
        self.matchMode = matchMode
        self.extensions = extensions
        self.regexPattern = regexPattern
        self.position = position
        self.visible = visible
        self.isBuiltin = isBuiltin
        self.builtinType = builtinType
        self.saveDir = saveDir
    }

    private enum CodingKeys: String, CodingKey {
        case id, name, icon, matchMode, extensions, regexPattern, position, visible, isBuiltin, builtinType, saveDir
    }

    /// 宽松解码（serde `#[serde(default)]`）：缺失字段取默认值；`id` / `name` 缺失视为损坏。
    public init(from decoder: any Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        id = try c.decode(String.self, forKey: .id)
        name = try c.decode(String.self, forKey: .name)
        icon = try c.decodeIfPresent(String.self, forKey: .icon) ?? "file"
        matchMode = try c.decodeIfPresent(String.self, forKey: .matchMode) ?? "extension"
        extensions = try c.decodeIfPresent([String].self, forKey: .extensions) ?? []
        regexPattern = try c.decodeIfPresent(String.self, forKey: .regexPattern) ?? ""
        position = try c.decodeIfPresent(Int64.self, forKey: .position) ?? 0
        visible = try c.decodeIfPresent(Bool.self, forKey: .visible) ?? true
        isBuiltin = try c.decodeIfPresent(Bool.self, forKey: .isBuiltin) ?? false
        builtinType = try c.decodeIfPresent(String.self, forKey: .builtinType)
        saveDir = try c.decodeIfPresent(String.self, forKey: .saveDir) ?? ""
    }

    /// 与 Web 同形：`builtinType` 为空时写显式 `null`。
    public func encode(to encoder: any Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(id, forKey: .id)
        try c.encode(name, forKey: .name)
        try c.encode(icon, forKey: .icon)
        try c.encode(matchMode, forKey: .matchMode)
        try c.encode(extensions, forKey: .extensions)
        try c.encode(regexPattern, forKey: .regexPattern)
        try c.encode(position, forKey: .position)
        try c.encode(visible, forKey: .visible)
        try c.encode(isBuiltin, forKey: .isBuiltin)
        try c.encode(builtinType, forKey: .builtinType)
        try c.encode(saveDir, forKey: .saveDir)
    }

    public var isAll: Bool { isBuiltin && builtinType == "all" }
    public var isOther: Bool { isBuiltin && builtinType == "other" }
    /// `all` 完全锁定；`other` 用排除逻辑匹配——两者都不展示匹配规则区。
    public var hasMatchRules: Bool { !(isAll || isOther) }
    public var isRegex: Bool { matchMode == "regex" }

    /// 内置分类基线（与 `CustomCategoryDto::builtin_defaults` 同序同扩展名）。
    public static var builtinDefaults: [CustomCategoryDto] {
        func make(_ type: String, _ icon: String, _ exts: [String], _ position: Int64) -> CustomCategoryDto {
            CustomCategoryDto(
                id: "builtin_\(type)", name: "", icon: icon, extensions: exts, position: position,
                isBuiltin: true, builtinType: type
            )
        }
        return [
            make("all", "folders", [], 0),
            make("video", "film", ["mp4", "mkv", "avi", "mov", "wmv", "flv", "webm", "m4v", "ts", "m3u8"], 1),
            make("audio", "music", ["mp3", "flac", "wav", "aac", "ogg", "m4a", "wma", "opus"], 2),
            make("document", "fileText", ["pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "txt", "epub", "md"], 3),
            make("image", "image", ["jpg", "jpeg", "png", "gif", "webp", "bmp", "svg", "heic", "avif"], 4),
            make("program", "cpu", ["exe", "msi", "dmg", "pkg", "deb", "rpm", "apk", "appimage"], 5),
            make("archive", "archive", ["zip", "rar", "7z", "tar", "gz", "bz2", "xz", "iso"], 6),
            make("other", "file", [], 7),
        ]
    }

    /// 从偏好值解析分类列表（数组或 JSON 数组字符串）；空 / 损坏回退内置基线，按 `position` 稳定排序
    /// （同 `CustomCategoryDto::from_preference`）。
    public static func fromPreference(_ value: JSONValue?) -> [CustomCategoryDto] {
        let decoder = ProtocolJSON.makeDecoder()
        var parsed: [CustomCategoryDto]?
        switch value {
        case let .string(text)?:
            parsed = try? decoder.decode([CustomCategoryDto].self, from: Data(text.utf8))
        case let value?:
            if let data = try? ProtocolJSON.makeEncoder().encode(value) {
                parsed = try? decoder.decode([CustomCategoryDto].self, from: data)
            }
        case nil:
            parsed = nil
        }
        let list = (parsed?.isEmpty == false ? parsed : nil) ?? builtinDefaults
        return list.enumerated().sorted { ($0.element.position, $0.offset) < ($1.element.position, $1.offset) }.map(\.element)
    }

    /// 写回偏好用的 JSON 值（对象数组；`position` 已按下标重排）。
    public static func preferenceValue(_ list: [CustomCategoryDto]) -> JSONValue {
        let reindexed = CategoryRules.reindexed(list)
        guard let data = try? ProtocolJSON.makeEncoder().encode(reindexed),
              let value = try? ProtocolJSON.makeDecoder().decode(JSONValue.self, from: data)
        else { return .array([]) }
        return value
    }
}

// MARK: - 分类规则

/// 分类编辑的校验失败原因（对应文案键见 `i18nKey`）。
public enum CategoryValidationError: Error, Sendable, Hashable {
    case nameRequired
    case extensionsRequired
    case regexInvalid

    public var i18nKey: String {
        switch self {
        case .nameRequired: "categoryNameRequired"
        case .extensionsRequired: "extensionsRequired"
        case .regexInvalid: "regexInvalid"
        }
    }
}

/// 分类编辑器的草稿（S3a 表单）。
public struct CategoryDraft: Sendable, Hashable {
    /// nil = 新建。
    public var existing: CustomCategoryDto?
    public var name: String
    public var icon: String
    /// `extension` | `regex`。
    public var matchMode: String
    public var extensionsText: String
    public var regexText: String
    public var saveDir: String

    public init(existing: CustomCategoryDto?) {
        self.existing = existing
        name = existing?.name ?? ""
        icon = existing?.icon ?? "file"
        matchMode = existing?.matchMode == "regex" ? "regex" : "extension"
        extensionsText = existing?.extensions.joined(separator: ", ") ?? ""
        regexText = existing?.regexPattern ?? ""
        saveDir = existing?.saveDir ?? ""
    }
}

public enum CategoryRules {
    /// 25 个图标的 wire 名（顺序即选择网格顺序）。
    public static let iconNames = [
        "folders", "film", "music", "fileText", "image", "archive", "file", "code", "database", "gamepad",
        "globe", "bookmark", "box", "cpu", "disc", "font", "hardDrive", "library", "package2", "pen",
        "printer", "smartphone", "subtitles", "type", "zap",
    ]

    /// 新建分类的 `position`（保存时由 `reindexed` 按下标重写）。
    public static let newPosition: Int64 = 999

    /// 内置分类显示名的文案键。
    public static func builtinLabelKey(_ builtinType: String) -> String {
        switch builtinType {
        case "all": "categoryAll"
        case "video": "categoryVideo"
        case "audio": "categoryAudio"
        case "document": "categoryDocument"
        case "image": "categoryImage"
        case "program": "categoryProgram"
        case "archive": "categoryArchive"
        default: "categoryOther"
        }
    }

    /// 内置分类的目录名基线（英文，与 `assets/i18n/en.json` 的 `categoryXxx` 逐字一致）。
    public static func builtinDirLabel(_ builtinType: String) -> String {
        switch builtinType {
        case "video": "Video"
        case "audio": "Audio"
        case "document": "Document"
        case "image": "Image"
        case "program": "Programs"
        case "archive": "Archive"
        default: "Other"
        }
    }

    /// 分类显示名 → 目录名：非法字符（`\ / : * ? " < > |` 与控制字符）换空格、压缩空白、去掉 Windows 会丢弃的结尾点 / 空格
    /// （`sanitizeCategoryDirName`，GPUI / Web 同规）。
    public static func sanitizeDirName(_ label: String) -> String {
        let invalid: Set<Character> = ["\\", "/", ":", "*", "?", "\"", "<", ">", "|"]
        var replaced = ""
        replaced.reserveCapacity(label.count)
        for character in label {
            let isControl = character.unicodeScalars.contains { $0.properties.generalCategory == .control }
            replaced.append(invalid.contains(character) || isControl ? " " : character)
        }
        var out = replaced.split(whereSeparator: \.isWhitespace).joined(separator: " ")
        while let last = out.last, last == "." || last == " " { out.removeLast() }
        return out
    }

    /// 目标机器的路径分隔符：宿主可能是 Linux / Windows 服务器，只能从目录本身反推（Web `separatorOf`）。
    static func separator(of base: String) -> Character {
        let chars = Array(base)
        if chars.count >= 3, chars[0].isASCII, chars[0].isLetter, chars[1] == ":", chars[2] == "\\" || chars[2] == "/" {
            return "\\"
        }
        return base.contains("\\") && !base.contains("/") ? "\\" : "/"
    }

    /// 「默认下载目录 / 分类名」。目录为空或分类名净化后为空时返回空串（调用方跳过）。
    public static func dirUnder(base: String, label: String) -> String {
        var root = base.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !root.isEmpty else { return "" }
        let folder = sanitizeDirName(label)
        guard !folder.isEmpty else { return "" }
        let sep = separator(of: root)
        while root.count > 1, root.hasSuffix("/") || root.hasSuffix("\\") { root.removeLast() }
        if root.hasSuffix("/") || root.hasSuffix("\\") { return root + folder }
        return "\(root)\(sep)\(folder)"
    }

    /// 「一键分类目录」：每个分类（「全部」除外）指向默认下载目录下的同名子目录；默认目录为空返回 nil。
    /// 内置分类用英文基线目录名，自定义分类用其名称。
    public static func autoDirs(_ list: [CustomCategoryDto], baseDir: String) -> [CustomCategoryDto]? {
        guard !baseDir.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return nil }
        return list.map { entry in
            if entry.builtinType == "all" { return entry }
            var next = entry
            let label = entry.isBuiltin ? builtinDirLabel(entry.builtinType ?? "other") : entry.name
            next.saveDir = dirUnder(base: baseDir, label: label)
            return next
        }
    }

    /// 被「一键分类目录」改写（目录发生变化）的分类数。
    public static func autoDirsChangeCount(_ list: [CustomCategoryDto], baseDir: String) -> Int {
        guard let updated = autoDirs(list, baseDir: baseDir) else { return 0 }
        return zip(list, updated).filter { $0.saveDir != $1.saveDir }.count
    }

    /// 扩展名文本 → 规范化列表：逗号 / 中文逗号 / 空白分隔，去点、转小写、去空。
    public static func parseExtensions(_ text: String) -> [String] {
        text.split(whereSeparator: { $0 == "," || $0 == "\u{FF0C}" || $0.isWhitespace })
            .map { $0.replacingOccurrences(of: ".", with: "").lowercased() }
            .filter { !$0.isEmpty }
    }

    /// 无引擎依赖的最小正则健全性检查：圆括号 / 方括号配对与尾部悬挂转义（同 `regex_looks_valid`）。
    public static func regexLooksValid(_ pattern: String) -> Bool {
        var depth = 0
        var inClass = false
        var iterator = pattern.makeIterator()
        while let ch = iterator.next() {
            switch ch {
            case "\\":
                if iterator.next() == nil { return false }
            case "[" where !inClass:
                inClass = true
            case "]" where inClass:
                inClass = false
            case "(" where !inClass:
                depth += 1
            case ")" where !inClass:
                depth -= 1
                if depth < 0 { return false }
            default:
                break
            }
        }
        return depth == 0 && !inClass
    }

    /// `position` 按下标重写（保存前调用，同 `write_categories`）。
    public static func reindexed(_ list: [CustomCategoryDto]) -> [CustomCategoryDto] {
        list.enumerated().map { index, entry in
            var next = entry
            next.position = Int64(index)
            return next
        }
    }

    /// 把 `from` 移到 `to` 当前所在位置（先移除再插入：向下拖落在目标之后，向上拖落在目标之前）；无变化返回 nil。
    public static func reorder(_ list: [CustomCategoryDto], from: String, to: String) -> [CustomCategoryDto]? {
        guard let fromIndex = list.firstIndex(where: { $0.id == from }),
              let toIndex = list.firstIndex(where: { $0.id == to }),
              fromIndex != toIndex else { return nil }
        var next = list
        let moved = next.remove(at: fromIndex)
        next.insert(moved, at: toIndex)
        return next
    }

    /// `onMove(fromOffsets:toOffset:)` 语义的纯实现（`toOffset` 是移除前的目标插入点）；无变化返回 nil。
    public static func move(_ list: [CustomCategoryDto], from sources: [Int], to destination: Int) -> [CustomCategoryDto]? {
        let valid = Set(sources).filter { list.indices.contains($0) }.sorted()
        guard !valid.isEmpty else { return nil }
        let moving = valid.map { list[$0] }
        var rest = list
        for index in valid.reversed() { rest.remove(at: index) }
        let removedBefore = valid.filter { $0 < destination }.count
        let insertAt = min(max(destination - removedBefore, 0), rest.count)
        rest.insert(contentsOf: moving, at: insertAt)
        return rest == list ? nil : rest
    }

    /// 新建分类 id：`custom_<unix 毫秒>`。
    public static func newID(nowMs: Int64) -> String { "custom_\(nowMs)" }

    /// 校验草稿并生成分类条目（规则同 GPUI `category_dialog.rs::save` / Web `CategoryDialog.tsx`）。
    public static func build(_ draft: CategoryDraft, nowMs: Int64) -> Result<CustomCategoryDto, CategoryValidationError> {
        let isBuiltin = draft.existing?.isBuiltin ?? false
        let name = draft.name.trimmingCharacters(in: .whitespacesAndNewlines)
        if name.isEmpty, !isBuiltin { return .failure(.nameRequired) }
        var extensions = draft.existing?.extensions ?? []
        var regex = draft.existing?.regexPattern ?? ""
        let mode = draft.matchMode == "regex" ? "regex" : "extension"
        if draft.existing?.hasMatchRules ?? true {
            if mode == "extension" {
                extensions = parseExtensions(draft.extensionsText)
                if extensions.isEmpty, !isBuiltin { return .failure(.extensionsRequired) }
                regex = ""
            } else {
                regex = draft.regexText.trimmingCharacters(in: .whitespacesAndNewlines)
                if !regex.isEmpty, !regexLooksValid(regex) { return .failure(.regexInvalid) }
                extensions = []
            }
        }
        let saveDir = draft.saveDir.trimmingCharacters(in: .whitespacesAndNewlines)
        if var entry = draft.existing {
            entry.name = name
            entry.icon = draft.icon
            entry.matchMode = mode
            entry.extensions = extensions
            entry.regexPattern = regex
            entry.saveDir = saveDir
            return .success(entry)
        }
        return .success(CustomCategoryDto(
            id: newID(nowMs: nowMs), name: name, icon: draft.icon, matchMode: mode, extensions: extensions,
            regexPattern: regex, position: newPosition, visible: true, isBuiltin: false, builtinType: nil, saveDir: saveDir
        ))
    }

    /// 写入列表：同 id 替换，否则追加。
    public static func upserting(_ entry: CustomCategoryDto, in list: [CustomCategoryDto]) -> [CustomCategoryDto] {
        var next = list
        if let index = next.firstIndex(where: { $0.id == entry.id }) {
            next[index] = entry
        } else {
            next.append(entry)
        }
        return next
    }
}

/// `daemon.config.get` 结果（`DaemonConfigSnapshot`）：配置版本与全部键值（wire 字符串）。
/// 写入冲突后用它读取最新 `revision` 再重放（HostError 不携带冲突回带的 revision）。
public struct DaemonConfigSnapshotDto: Sendable, Hashable, Codable {
    public var revision: UInt64
    public var values: [String: String]

    public init(revision: UInt64 = 0, values: [String: String] = [:]) {
        self.revision = revision
        self.values = values
    }
}

/// `daemon.config.connPolicy` / `clearConnPolicy` 结果：引擎学习到的按域连接上限条数。
public struct ConnPolicySummaryDto: Sendable, Hashable, Codable {
    public var domainCount: UInt64

    public init(domainCount: UInt64 = 0) {
        self.domainCount = domainCount
    }

    private enum CodingKeys: String, CodingKey { case domainCount }

    public init(from decoder: any Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        domainCount = try container.decodeIfPresent(UInt64.self, forKey: .domainCount) ?? 0
    }
}

/// `daemon.fs.list` 参数：`path` 省略 / 空 = 默认保存目录。
public struct FsListParams: Sendable, Hashable, Encodable {
    public var path: String?

    public init(path: String? = nil) {
        self.path = path
    }
}

/// 目录项（`FsListResponse.dirs` 元素）。
public struct FsEntryDto: Sendable, Hashable, Codable, Identifiable {
    public var name: String
    public var path: String

    public var id: String { path }

    public init(name: String, path: String) {
        self.name = name
        self.path = path
    }
}

/// `daemon.fs.list` 结果：服务端目录列举（仅子目录）。
public struct FsListResponse: Sendable, Hashable, Codable {
    /// 实际列举的目录（绝对路径）。
    public var path: String
    /// 上级目录（根目录为 nil）。
    public var parent: String?
    public var dirs: [FsEntryDto]
    /// 服务进程对该目录无读取权限：`dirs` 必为空，但语义是「看不到」而非「没有」。
    public var denied: Bool

    public init(path: String, parent: String? = nil, dirs: [FsEntryDto] = [], denied: Bool = false) {
        self.path = path
        self.parent = parent
        self.dirs = dirs
        self.denied = denied
    }

    private enum CodingKeys: String, CodingKey { case path, parent, dirs, denied }

    public init(from decoder: any Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        path = try container.decode(String.self, forKey: .path)
        parent = try container.decodeIfPresent(String.self, forKey: .parent)
        dirs = try container.decodeIfPresent([FsEntryDto].self, forKey: .dirs) ?? []
        denied = try container.decodeIfPresent(Bool.self, forKey: .denied) ?? false
    }
}
