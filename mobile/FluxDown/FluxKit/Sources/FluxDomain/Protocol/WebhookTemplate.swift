import Foundation

/// 端点编辑器的纯逻辑：占位符预览、URL 校验、签名密钥生成。
/// 语义与 GPUI `crates/settings/src/sections/webhook_dialog.rs`、Web `pages/webhooks/template.ts`
/// 及引擎 `render_template` 逐条对齐。
public enum WebhookTemplate {
    /// 预览用样例变量——与引擎 `WebhookEvent::sample()` 对齐，仅用于预览。
    static let sampleVars: [String: String] = [
        "{event}": "task.completed",
        "{event.title}": "Download completed",
        "{event.summary}": "ubuntu-24.04.2-desktop-amd64.iso · 6.0 GB",
        "{timestamp}": "2026-07-17T12:34:56Z",
        "{instance.app}": "fluxdown",
        "{instance.version}": "0.1.44",
        "{instance.host}": "DESKTOP",
        "{task.id}": "00000000-0000-4000-8000-000000000000",
        "{task.fileName}": "ubuntu-24.04.2-desktop-amd64.iso",
        "{task.url}": "https://releases.ubuntu.com/24.04/ubuntu.iso",
        "{task.saveDir}": "/downloads",
        "{task.totalBytes}": "6442450944",
        "{task.totalBytesHuman}": "6.0 GB",
        "{task.status}": "3",
        "{task.errorMessage}": "",
        "{queue.id}": "main",
        "{queue.name}": "Main",
        "{ntfy.topic}": "my-topic",
    ]

    /// `application/x-www-form-urlencoded` 组件编码。
    public static func formEncode(_ value: String) -> String {
        var out = ""
        for byte in value.utf8 {
            switch byte {
            case UInt8(ascii: "A") ... UInt8(ascii: "Z"), UInt8(ascii: "a") ... UInt8(ascii: "z"), UInt8(ascii: "0") ... UInt8(ascii: "9"),
                 UInt8(ascii: "-"), UInt8(ascii: "_"), UInt8(ascii: "."), UInt8(ascii: "!"), UInt8(ascii: "~"),
                 UInt8(ascii: "*"), UInt8(ascii: "'"), UInt8(ascii: "("), UInt8(ascii: ")"):
                out.unicodeScalars.append(Unicode.Scalar(byte))
            case 0x20:
                out += "+"
            default:
                out += "%" + String(format: "%02X", Int(byte))
            }
        }
        return out
    }

    /// JSON 字符串上下文转义（不含外层引号）。
    static func jsonEscape(_ value: String) -> String {
        var out = ""
        for scalar in value.unicodeScalars {
            switch scalar {
            case "\"": out += "\\\""
            case "\\": out += "\\\\"
            case "\n": out += "\\n"
            case "\r": out += "\\r"
            case "\t": out += "\\t"
            case "\u{08}": out += "\\b"
            case "\u{0C}": out += "\\f"
            default:
                if scalar.value < 0x20 {
                    out += String(format: "\\u%04x", Int(scalar.value))
                } else {
                    out.unicodeScalars.append(scalar)
                }
            }
        }
        return out
    }

    /// 占位符替换——与引擎 `render_template` 同规则：占位符是不含嵌套 `{` 的 `{…}` 段，未知段原样保留，
    /// 因此 JSON 字面量不会被破坏。
    public static func renderPreview(_ template: String, formEscape: Bool) -> String {
        let chars = Array(template)
        var out = ""
        var i = 0
        while i < chars.count {
            if chars[i] != "{" {
                let start = i
                while i < chars.count, chars[i] != "{" { i += 1 }
                out += String(chars[start ..< i])
                continue
            }
            var j = i + 1
            while j < chars.count, chars[j] != "}", chars[j] != "{" { j += 1 }
            if j >= chars.count || chars[j] == "{" {
                out += "{"
                i += 1
                continue
            }
            let key = String(chars[i ... j])
            if let value = sampleVars[key] {
                out += formEscape ? formEncode(value) : jsonEscape(value)
            } else {
                out += key
            }
            i = j + 1
        }
        return out
    }

    /// 无模板（custom 预设）时的信封原文。
    static func envelopePreview() -> String {
        let v = sampleVars
        return """
        {
          "schemaVersion": 1,
          "event": "\(jsonEscape(v["{event}"] ?? ""))",
          "deliveryId": "5f2a91c7-8b3e-4d10-a6f4-c2d90b7e13aa",
          "timestamp": "\(jsonEscape(v["{timestamp}"] ?? ""))",
          "instance": {
            "app": "\(jsonEscape(v["{instance.app}"] ?? ""))",
            "version": "\(jsonEscape(v["{instance.version}"] ?? ""))",
            "host": "\(jsonEscape(v["{instance.host}"] ?? ""))"
          },
          "queue": {
            "id": "\(jsonEscape(v["{queue.id}"] ?? ""))",
            "name": "\(jsonEscape(v["{queue.name}"] ?? ""))"
          },
          "task": {
            "id": "\(jsonEscape(v["{task.id}"] ?? ""))",
            "fileName": "\(jsonEscape(v["{task.fileName}"] ?? ""))",
            "url": "\(jsonEscape(v["{task.url}"] ?? ""))",
            "saveDir": "\(jsonEscape(v["{task.saveDir}"] ?? ""))",
            "totalBytes": 6442450944,
            "status": 3,
            "errorMessage": ""
          }
        }
        """
    }

    /// URL 内联校验错误的文案键；nil = 通过。空 URL 由保存按钮禁用兜底。
    public static func urlErrorKey(_ rawURL: String, allowHttp: Bool) -> String? {
        let raw = rawURL.trimmingCharacters(in: .whitespacesAndNewlines)
        if raw.isEmpty { return nil }
        guard let sep = raw.range(of: "://") else { return "webhookUrlInvalid" }
        let scheme = raw[..<sep.lowerBound].lowercased()
        let host = raw[sep.upperBound...].prefix { $0 != "/" && $0 != "?" && $0 != "#" }
        if host.isEmpty { return "webhookUrlInvalid" }
        if scheme == "https" { return nil }
        if scheme == "http" { return allowHttp ? nil : "webhookUrlWarnHttp" }
        return "webhookUrlInvalid"
    }

    /// HMAC 密钥起点（`whsec_` + 32 位十六进制）。
    public static func generateSecret() -> String {
        var hex = ""
        for _ in 0 ..< 16 { hex += String(format: "%02x", Int(UInt8.random(in: 0 ... 255))) }
        return "whsec_" + hex
    }

    /// 请求体预览：自带模板优先，其次预设默认模板，都没有则是信封原文。
    public static func previewBody(ownTemplate: String, preset: WebhookPreset?) -> String {
        let template = ownTemplate.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? (preset?.defaultTemplate ?? "") : ownTemplate
        if template.isEmpty { return envelopePreview() }
        let isForm = preset?.contentType.hasPrefix("application/x-www-form") ?? false
        let rendered = renderPreview(template, formEscape: isForm)
        if isForm { return rendered }
        // 渲染结果若是合法 JSON 就美化一下，方便扫读；不是就原样显示。
        return prettyJSON(rendered) ?? rendered
    }

    /// 右栏「实时请求预览」全文；`firstEvent` = 按规范顺序第一个已订阅事件。
    public static func previewRequest(url: String, firstEvent: String?, signEnabled: Bool, template: String, preset: WebhookPreset?) -> String {
        let trimmed = url.trimmingCharacters(in: .whitespacesAndNewlines)
        let shownURL = trimmed.isEmpty ? (preset?.urlPlaceholder ?? "") : trimmed
        var lines = [
            "POST \(shownURL)",
            "Content-Type: \(preset?.contentType ?? "application/json")",
            "X-FluxDown-Event: \(firstEvent ?? "task.completed")",
            "X-FluxDown-Delivery: 5f2a91c7-…",
        ]
        if signEnabled { lines.append("X-FluxDown-Signature: t=1789647128,v1=9c41f2…") }
        lines.append(String(repeating: "─", count: 28))
        lines.append(contentsOf: previewBody(ownTemplate: template, preset: preset).components(separatedBy: "\n"))
        return lines.joined(separator: "\n")
    }

    /// 合法 JSON 文本 → 两空格缩进（保留键序）；非法返回 nil。
    public static func prettyJSON(_ text: String) -> String? {
        guard let data = text.data(using: .utf8),
              (try? JSONSerialization.jsonObject(with: data, options: [.fragmentsAllowed])) != nil
        else { return nil }
        let chars = Array(text)
        var out = ""
        var depth = 0
        var i = 0
        func newline() { out += "\n" + String(repeating: "  ", count: depth) }
        while i < chars.count {
            let ch = chars[i]
            switch ch {
            case "\"":
                var j = i + 1
                while j < chars.count {
                    if chars[j] == "\\" { j += 2; continue }
                    if chars[j] == "\"" { break }
                    j += 1
                }
                out += String(chars[i ..< min(j + 1, chars.count)])
                i = j
            case "{", "[":
                var k = i + 1
                while k < chars.count, chars[k].isWhitespace { k += 1 }
                if k < chars.count, chars[k] == (ch == "{" ? "}" : "]") {
                    out += String(ch) + String(chars[k])
                    i = k
                } else {
                    out += String(ch)
                    depth += 1
                    newline()
                }
            case "}", "]":
                depth -= 1
                newline()
                out += String(ch)
            case ",":
                out += ","
                newline()
            case ":":
                out += ": "
            default:
                if !ch.isWhitespace { out += String(ch) }
            }
            i += 1
        }
        return out
    }
}
