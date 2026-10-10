package com.fluxdown.core.protocol

import java.security.SecureRandom

/**
 * 端点编辑器的纯逻辑：占位符预览、URL 校验、签名密钥生成。
 * 语义与 GPUI `crates/settings/src/sections/webhook_dialog.rs`、Web `pages/webhooks/template.ts`
 * 及引擎 `render_template` 逐条对齐（同 iOS `WebhookTemplate.swift`）。
 */
object WebhookTemplate {
    /** 预览用样例变量——与引擎 `WebhookEvent::sample()` 对齐，仅用于预览。 */
    private val sampleVars: Map<String, String> = mapOf(
        "{event}" to "task.completed",
        "{event.title}" to "Download completed",
        "{event.summary}" to "ubuntu-24.04.2-desktop-amd64.iso · 6.0 GB",
        "{timestamp}" to "2026-07-17T12:34:56Z",
        "{instance.app}" to "fluxdown",
        "{instance.version}" to "0.1.44",
        "{instance.host}" to "DESKTOP",
        "{task.id}" to "00000000-0000-4000-8000-000000000000",
        "{task.fileName}" to "ubuntu-24.04.2-desktop-amd64.iso",
        "{task.url}" to "https://releases.ubuntu.com/24.04/ubuntu.iso",
        "{task.saveDir}" to "/downloads",
        "{task.totalBytes}" to "6442450944",
        "{task.totalBytesHuman}" to "6.0 GB",
        "{task.status}" to "3",
        "{task.errorMessage}" to "",
        "{queue.id}" to "main",
        "{queue.name}" to "Main",
        "{ntfy.topic}" to "my-topic",
    )

    private val securityRandom = SecureRandom()

    /** `application/x-www-form-urlencoded` 组件编码。 */
    fun formEncode(value: String): String {
        val out = StringBuilder()
        for (b in value.toByteArray(Charsets.UTF_8)) {
            val c = b.toInt() and 0xFF
            when {
                c in 'A'.code..'Z'.code || c in 'a'.code..'z'.code || c in '0'.code..'9'.code -> out.append(c.toChar())
                c == '-'.code || c == '_'.code || c == '.'.code || c == '!'.code || c == '~'.code ||
                    c == '*'.code || c == '\''.code || c == '('.code || c == ')'.code -> out.append(c.toChar())
                c == 0x20 -> out.append('+')
                else -> out.append('%').append(HEX_UPPER[c shr 4]).append(HEX_UPPER[c and 0xF])
            }
        }
        return out.toString()
    }

    /** JSON 字符串上下文转义（不含外层引号）。 */
    internal fun jsonEscape(value: String): String {
        val out = StringBuilder()
        for (ch in value) {
            when (ch) {
                '"' -> out.append("\\\"")
                '\\' -> out.append("\\\\")
                '\n' -> out.append("\\n")
                '\r' -> out.append("\\r")
                '\t' -> out.append("\\t")
                '\b' -> out.append("\\b")
                '\u000C' -> out.append("\\f")
                else -> if (ch.code < 0x20) out.append("\\u%04x".format(ch.code)) else out.append(ch)
            }
        }
        return out.toString()
    }

    /**
     * 占位符替换——与引擎 `render_template` 同规则：占位符是不含嵌套 `{` 的 `{…}` 段，未知段原样保留，
     * 因此 JSON 字面量不会被破坏。
     */
    fun renderPreview(template: String, formEscape: Boolean): String {
        val out = StringBuilder()
        var i = 0
        while (i < template.length) {
            if (template[i] != '{') {
                val start = i
                while (i < template.length && template[i] != '{') i++
                out.append(template, start, i)
                continue
            }
            var j = i + 1
            while (j < template.length && template[j] != '}' && template[j] != '{') j++
            if (j >= template.length || template[j] == '{') {
                out.append('{')
                i += 1
                continue
            }
            val key = template.substring(i, j + 1)
            val value = sampleVars[key]
            out.append(if (value != null) (if (formEscape) formEncode(value) else jsonEscape(value)) else key)
            i = j + 1
        }
        return out.toString()
    }

    /** 无模板（custom 预设）时的信封原文。 */
    internal fun envelopePreview(): String {
        fun v(key: String) = jsonEscape(sampleVars[key].orEmpty())
        return """
            {
              "schemaVersion": 1,
              "event": "${v("{event}")}",
              "deliveryId": "5f2a91c7-8b3e-4d10-a6f4-c2d90b7e13aa",
              "timestamp": "${v("{timestamp}")}",
              "instance": {
                "app": "${v("{instance.app}")}",
                "version": "${v("{instance.version}")}",
                "host": "${v("{instance.host}")}"
              },
              "queue": {
                "id": "${v("{queue.id}")}",
                "name": "${v("{queue.name}")}"
              },
              "task": {
                "id": "${v("{task.id}")}",
                "fileName": "${v("{task.fileName}")}",
                "url": "${v("{task.url}")}",
                "saveDir": "${v("{task.saveDir}")}",
                "totalBytes": 6442450944,
                "status": 3,
                "errorMessage": ""
              }
            }
        """.trimIndent()
    }

    /** URL 内联校验错误：[UrlError.Invalid] / [UrlError.WarnHttp]；null = 通过。空 URL 由保存按钮禁用兜底。 */
    fun urlError(rawUrl: String, allowHttp: Boolean): UrlError? {
        val raw = rawUrl.trim()
        if (raw.isEmpty()) return null
        val sep = raw.indexOf("://")
        if (sep < 0) return UrlError.Invalid
        val scheme = raw.substring(0, sep).lowercase()
        val host = raw.substring(sep + 3).takeWhile { it != '/' && it != '?' && it != '#' }
        if (host.isEmpty()) return UrlError.Invalid
        return when (scheme) {
            "https" -> null
            "http" -> if (allowHttp) null else UrlError.WarnHttp
            else -> UrlError.Invalid
        }
    }

    enum class UrlError {
        /** 非 http(s) 或缺主机（`webhookUrlInvalid`）。 */
        Invalid,

        /** http 明文但未勾选允许（`webhookUrlWarnHttp`）。 */
        WarnHttp,
    }

    /** HMAC 密钥起点（`whsec_` + 32 位十六进制）。 */
    fun generateSecret(): String {
        val bytes = ByteArray(16).also { securityRandom.nextBytes(it) }
        val hex = StringBuilder("whsec_")
        for (b in bytes) {
            val c = b.toInt() and 0xFF
            hex.append(HEX_LOWER[c shr 4]).append(HEX_LOWER[c and 0xF])
        }
        return hex.toString()
    }

    /** 请求体预览：自带模板优先，其次预设默认模板，都没有则是信封原文。 */
    fun previewBody(ownTemplate: String, preset: WebhookPreset?): String {
        val template = if (ownTemplate.isBlank()) preset?.defaultTemplate.orEmpty() else ownTemplate
        if (template.isEmpty()) return envelopePreview()
        val isForm = preset?.contentType?.startsWith("application/x-www-form") == true
        val rendered = renderPreview(template, formEscape = isForm)
        if (isForm) return rendered
        // 渲染结果若是合法 JSON 就美化一下，方便扫读；不是就原样显示。
        return prettyJson(rendered) ?: rendered
    }

    /** 右栏「实时请求预览」全文；[firstEvent] = 按规范顺序第一个已订阅事件。 */
    fun previewRequest(url: String, firstEvent: String?, signEnabled: Boolean, template: String, preset: WebhookPreset?): String {
        val trimmed = url.trim()
        val shownUrl = if (trimmed.isEmpty()) preset?.urlPlaceholder.orEmpty() else trimmed
        val lines = mutableListOf(
            "POST $shownUrl",
            "Content-Type: ${preset?.contentType ?: "application/json"}",
            "X-FluxDown-Event: ${firstEvent ?: "task.completed"}",
            "X-FluxDown-Delivery: 5f2a91c7-…",
        )
        if (signEnabled) lines += "X-FluxDown-Signature: t=1789647128,v1=9c41f2…"
        lines += "─".repeat(28)
        lines += previewBody(template, preset).split("\n")
        return lines.joinToString("\n")
    }

    /** 合法 JSON 文本 → 两空格缩进（保留键序）；非法返回 null。 */
    fun prettyJson(text: String): String? {
        if (Json.parseOrNull(text) == null) return null
        val out = StringBuilder()
        var depth = 0
        var i = 0
        fun newline() {
            out.append('\n')
            repeat(depth) { out.append("  ") }
        }
        while (i < text.length) {
            val ch = text[i]
            when (ch) {
                '"' -> {
                    var j = i + 1
                    while (j < text.length) {
                        if (text[j] == '\\') {
                            j += 2
                            continue
                        }
                        if (text[j] == '"') break
                        j++
                    }
                    out.append(text, i, minOf(j + 1, text.length))
                    i = j
                }
                '{', '[' -> {
                    var k = i + 1
                    while (k < text.length && text[k].isWhitespace()) k++
                    if (k < text.length && text[k] == (if (ch == '{') '}' else ']')) {
                        out.append(ch).append(text[k])
                        i = k
                    } else {
                        out.append(ch)
                        depth += 1
                        newline()
                    }
                }
                '}', ']' -> {
                    depth -= 1
                    newline()
                    out.append(ch)
                }
                ',' -> {
                    out.append(',')
                    newline()
                }
                ':' -> out.append(": ")
                else -> if (!ch.isWhitespace()) out.append(ch)
            }
            i++
        }
        return out.toString()
    }

    private const val HEX_UPPER = "0123456789ABCDEF"
    private const val HEX_LOWER = "0123456789abcdef"
}
