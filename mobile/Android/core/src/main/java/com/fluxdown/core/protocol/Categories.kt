package com.fluxdown.core.protocol

import com.fluxdown.core.format.UrlText

/**
 * 自定义分类（偏好键 `custom_categories`，与 `native/protocol/src/agent.rs::CustomCategoryDto`、
 * GPUI / Web 同 JSON 形状；对应 iOS `Preferences.swift` 的 `CustomCategoryDto`）。
 *
 * 与 `core.model.Category`（主机快照下发、只用于匹配的投影）不同，这里保留编辑所需的全部字段
 * （匹配模式 / 保存目录 / 内置标记）。
 */
data class CustomCategoryDto(
    val id: String,
    val name: String,
    val icon: String = "file",
    /** `extension` | `regex`。 */
    val matchMode: String = "extension",
    val extensions: List<String> = emptyList(),
    val regexPattern: String = "",
    val position: Long = 0,
    val visible: Boolean = true,
    val isBuiltin: Boolean = false,
    val builtinType: String? = null,
    val saveDir: String = "",
) {
    val isAll: Boolean get() = isBuiltin && builtinType == "all"
    val isOther: Boolean get() = isBuiltin && builtinType == "other"

    /** `all` 完全锁定；`other` 用排除逻辑匹配——两者都不展示匹配规则区。 */
    val hasMatchRules: Boolean get() = !(isAll || isOther)
    val isRegex: Boolean get() = matchMode == "regex"

    /** 与 Web 同形：`builtinType` 为空时写显式 `null`。 */
    fun toJson(): JsonValue = jsonObject(
        "id" to id,
        "name" to name,
        "icon" to icon,
        "matchMode" to matchMode,
        "extensions" to extensions,
        "regexPattern" to regexPattern,
        "position" to position,
        "visible" to visible,
        "isBuiltin" to isBuiltin,
        "builtinType" to builtinType,
        "saveDir" to saveDir,
    )

    companion object {
        const val PREFERENCE_KEY = "custom_categories"

        /** 宽松解码（serde `#[serde(default)]`）：缺失字段取默认值；`id` / `name` 缺失视为损坏（null）。 */
        fun fromJson(v: JsonValue?): CustomCategoryDto? {
            if (v !is JsonValue.Obj) return null
            val id = v.strOrNull("id") ?: return null
            val name = v.strOrNull("name") ?: return null
            return CustomCategoryDto(
                id = id,
                name = name,
                icon = v.str("icon", "file"),
                matchMode = v.str("matchMode", "extension"),
                extensions = v.strings("extensions"),
                regexPattern = v.str("regexPattern"),
                position = v.long("position", 0),
                visible = v.bool("visible", true),
                isBuiltin = v.bool("isBuiltin", false),
                builtinType = v.strOrNull("builtinType"),
                saveDir = v.str("saveDir"),
            )
        }

        /** 内置分类基线（与 `CustomCategoryDto::builtin_defaults` 同序同扩展名）。 */
        val builtinDefaults: List<CustomCategoryDto> = listOf(
            builtin("all", "folders", emptyList(), 0),
            builtin("video", "film", listOf("mp4", "mkv", "avi", "mov", "wmv", "flv", "webm", "m4v", "ts", "m3u8"), 1),
            builtin("audio", "music", listOf("mp3", "flac", "wav", "aac", "ogg", "m4a", "wma", "opus"), 2),
            builtin("document", "fileText", listOf("pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "txt", "epub", "md"), 3),
            builtin("image", "image", listOf("jpg", "jpeg", "png", "gif", "webp", "bmp", "svg", "heic", "avif"), 4),
            builtin("program", "cpu", listOf("exe", "msi", "dmg", "pkg", "deb", "rpm", "apk", "appimage"), 5),
            builtin("archive", "archive", listOf("zip", "rar", "7z", "tar", "gz", "bz2", "xz", "iso"), 6),
            builtin("other", "file", emptyList(), 7),
        )

        private fun builtin(type: String, icon: String, exts: List<String>, position: Long) = CustomCategoryDto(
            id = "builtin_$type", name = "", icon = icon, extensions = exts, position = position,
            isBuiltin = true, builtinType = type,
        )

        /**
         * 从偏好值解析分类列表（数组或 JSON 数组字符串）；空 / 损坏（含任一项缺 `id` / `name`）回退内置基线，
         * 按 `position` 稳定排序（同 `CustomCategoryDto::from_preference`）。
         */
        fun fromPreference(value: JsonValue?): List<CustomCategoryDto> {
            val array = when (value) {
                is JsonValue.Str -> Json.parseOrNull(value.value)
                else -> value
            }?.arrayOrNull
            val parsed = array?.map { fromJson(it) ?: return builtinDefaults }
            val list = parsed?.takeIf { it.isNotEmpty() } ?: builtinDefaults
            return list.sortedBy { it.position }
        }

        /** 写回偏好用的 JSON 值（对象数组；`position` 已按下标重排）。 */
        fun preferenceValue(list: List<CustomCategoryDto>): JsonValue =
            JsonValue.Arr(CategoryRules.reindexed(list).map { it.toJson() })
    }
}

/** 分类编辑的校验失败原因（[i18nKey] 为 `assets/i18n` 文案键）。 */
enum class CategoryValidationError(val i18nKey: String) {
    NameRequired("categoryNameRequired"),
    ExtensionsRequired("extensionsRequired"),
    RegexInvalid("regexInvalid"),
}

/** 分类编辑器的草稿（表单）；[existing] = null 为新建。 */
data class CategoryDraft(
    val existing: CustomCategoryDto? = null,
    val name: String = existing?.name.orEmpty(),
    val icon: String = existing?.icon ?: "file",
    /** `extension` | `regex`。 */
    val matchMode: String = if (existing?.matchMode == "regex") "regex" else "extension",
    val extensionsText: String = existing?.extensions?.joinToString(", ").orEmpty(),
    val regexText: String = existing?.regexPattern.orEmpty(),
    val saveDir: String = existing?.saveDir.orEmpty(),
)

/** [CategoryRules.build] 的结果。 */
sealed interface CategoryBuild {
    data class Success(val entry: CustomCategoryDto) : CategoryBuild
    data class Failure(val error: CategoryValidationError) : CategoryBuild
}

/** 分类列表的编辑规则（校验 / 目录名 / 排序 / 一键分类目录），对应 iOS `CategoryRules`。 */
object CategoryRules {
    /** 25 个图标的 wire 名（顺序即选择网格顺序）。 */
    val iconNames: List<String> = listOf(
        "folders", "film", "music", "fileText", "image", "archive", "file", "code", "database", "gamepad",
        "globe", "bookmark", "box", "cpu", "disc", "font", "hardDrive", "library", "package2", "pen",
        "printer", "smartphone", "subtitles", "type", "zap",
    )

    /** 新建分类的 `position`（保存时由 [reindexed] 按下标重写）。 */
    const val NEW_POSITION = 999L

    /** 内置分类的目录名基线（英文，与 `assets/i18n/en.json` 的 `categoryXxx` 逐字一致）。 */
    fun builtinDirLabel(builtinType: String): String = when (builtinType) {
        "video" -> "Video"
        "audio" -> "Audio"
        "document" -> "Document"
        "image" -> "Image"
        "program" -> "Programs"
        "archive" -> "Archive"
        else -> "Other"
    }

    private val invalidDirChars = setOf('\\', '/', ':', '*', '?', '"', '<', '>', '|')

    /**
     * 分类显示名 → 目录名：非法字符（`\ / : * ? " < > |` 与控制字符）换空格、压缩空白、去掉 Windows 会丢弃的
     * 结尾点 / 空格（`sanitizeCategoryDirName`，GPUI / Web 同规）。
     */
    fun sanitizeDirName(label: String): String {
        val replaced = buildString(label.length) {
            for (ch in label) {
                append(if (ch in invalidDirChars || Character.getType(ch) == Character.CONTROL.toInt()) ' ' else ch)
            }
        }
        val out = replaced.split(Regex("[\\s\\p{Z}]+")).filter { it.isNotEmpty() }.joinToString(" ")
        return out.trimEnd('.', ' ')
    }

    /** 目标机器的路径分隔符：宿主可能是 Linux / Windows 服务器，只能从目录本身反推（Web `separatorOf`）。 */
    internal fun separator(base: String): Char {
        if (base.length >= 3 && base[0].let { it in 'a'..'z' || it in 'A'..'Z' } && base[1] == ':' &&
            (base[2] == '\\' || base[2] == '/')
        ) {
            return '\\'
        }
        return if ('\\' in base && '/' !in base) '\\' else '/'
    }

    /** 「默认下载目录 / 分类名」。目录为空或分类名净化后为空时返回空串（调用方跳过）。 */
    fun dirUnder(base: String, label: String): String {
        var root = base.trim()
        if (root.isEmpty()) return ""
        val folder = sanitizeDirName(label)
        if (folder.isEmpty()) return ""
        val sep = separator(root)
        while (root.length > 1 && (root.endsWith('/') || root.endsWith('\\'))) root = root.dropLast(1)
        if (root.endsWith('/') || root.endsWith('\\')) return root + folder
        return "$root$sep$folder"
    }

    /**
     * 「一键分类目录」：每个分类（「全部」除外）指向默认下载目录下的同名子目录；默认目录为空返回 null。
     * 内置分类用英文基线目录名，自定义分类用其名称。
     */
    fun autoDirs(list: List<CustomCategoryDto>, baseDir: String): List<CustomCategoryDto>? {
        if (baseDir.isBlank()) return null
        return list.map { entry ->
            if (entry.builtinType == "all") {
                entry
            } else {
                val label = if (entry.isBuiltin) builtinDirLabel(entry.builtinType ?: "other") else entry.name
                entry.copy(saveDir = dirUnder(baseDir, label))
            }
        }
    }

    /** 被「一键分类目录」改写（目录发生变化）的分类数。 */
    fun autoDirsChangeCount(list: List<CustomCategoryDto>, baseDir: String): Int {
        val updated = autoDirs(list, baseDir) ?: return 0
        return list.zip(updated).count { (old, new) -> old.saveDir != new.saveDir }
    }

    /** 扩展名文本 → 规范化列表：逗号 / 中文逗号 / 空白分隔，去点、转小写、去空。 */
    fun parseExtensions(text: String): List<String> =
        text.split(Regex("[,\\uFF0C\\s\\p{Z}]+"))
            .map { it.replace(".", "").lowercase() }
            .filter { it.isNotEmpty() }

    /** 无引擎依赖的最小正则健全性检查：圆括号 / 方括号配对与尾部悬挂转义（同 `regex_looks_valid`）。 */
    fun regexLooksValid(pattern: String): Boolean {
        var depth = 0
        var inClass = false
        var i = 0
        while (i < pattern.length) {
            when (pattern[i]) {
                '\\' -> {
                    if (i + 1 >= pattern.length) return false
                    i++
                }
                '[' -> if (!inClass) inClass = true
                ']' -> if (inClass) inClass = false
                '(' -> if (!inClass) depth++
                ')' -> if (!inClass) {
                    depth--
                    if (depth < 0) return false
                }
            }
            i++
        }
        return depth == 0 && !inClass
    }

    /** `position` 按下标重写（保存前调用，同 `write_categories`）。 */
    fun reindexed(list: List<CustomCategoryDto>): List<CustomCategoryDto> =
        list.mapIndexed { index, entry -> entry.copy(position = index.toLong()) }

    /** 把 [from] 移到 [to] 当前所在位置（先移除再插入：向下落在目标之后，向上落在目标之前）；无变化返回 null。 */
    fun reorder(list: List<CustomCategoryDto>, from: String, to: String): List<CustomCategoryDto>? {
        val fromIndex = list.indexOfFirst { it.id == from }
        val toIndex = list.indexOfFirst { it.id == to }
        if (fromIndex < 0 || toIndex < 0 || fromIndex == toIndex) return null
        val next = list.toMutableList()
        val moved = next.removeAt(fromIndex)
        next.add(toIndex, moved)
        return next
    }

    /** 新建分类 id：`custom_<unix 毫秒>`。 */
    fun newId(nowMs: Long): String = "custom_$nowMs"

    /** 校验草稿并生成分类条目（规则同 GPUI `category_dialog.rs::save` / Web `CategoryDialog.tsx`）。 */
    fun build(draft: CategoryDraft, nowMs: Long): CategoryBuild {
        val existing = draft.existing
        val isBuiltin = existing?.isBuiltin ?: false
        val name = draft.name.trim()
        if (name.isEmpty() && !isBuiltin) return CategoryBuild.Failure(CategoryValidationError.NameRequired)
        var extensions = existing?.extensions.orEmpty()
        var regex = existing?.regexPattern.orEmpty()
        val mode = if (draft.matchMode == "regex") "regex" else "extension"
        if (existing?.hasMatchRules ?: true) {
            if (mode == "extension") {
                extensions = parseExtensions(draft.extensionsText)
                if (extensions.isEmpty() && !isBuiltin) return CategoryBuild.Failure(CategoryValidationError.ExtensionsRequired)
                regex = ""
            } else {
                regex = draft.regexText.trim()
                if (regex.isNotEmpty() && !regexLooksValid(regex)) return CategoryBuild.Failure(CategoryValidationError.RegexInvalid)
                extensions = emptyList()
            }
        }
        val saveDir = draft.saveDir.trim()
        if (existing != null) {
            return CategoryBuild.Success(
                existing.copy(
                    name = name, icon = draft.icon, matchMode = mode, extensions = extensions,
                    regexPattern = regex, saveDir = saveDir,
                ),
            )
        }
        return CategoryBuild.Success(
            CustomCategoryDto(
                id = newId(nowMs), name = name, icon = draft.icon, matchMode = mode, extensions = extensions,
                regexPattern = regex, position = NEW_POSITION, visible = true, isBuiltin = false, builtinType = null,
                saveDir = saveDir,
            ),
        )
    }

    /** 写入列表：同 id 替换，否则追加。 */
    fun upserting(entry: CustomCategoryDto, list: List<CustomCategoryDto>): List<CustomCategoryDto> {
        val index = list.indexOfFirst { it.id == entry.id }
        return if (index >= 0) list.toMutableList().also { it[index] = entry } else list + entry
    }

    /**
     * 外部唤起的分类保存目录（同 agent `category_dir.rs::category_save_dir`）：
     * 只看可见分类、按 [list] 顺序（调用方传 [CustomCategoryDto.fromPreference] 的已排序结果）；先取首个
     * 配置了目录且命中的普通分类（非 all / other），否则文件不命中任何普通分类时取 other 的目录。
     * [fileName] 不含 `.` 时用 URL 路径末段（百分号解码后含 `.`）参与匹配；无命中 / 未配置目录返回 null。
     */
    fun saveDirFor(list: List<CustomCategoryDto>, fileName: String, url: String): String? {
        val name = if ('.' in fileName) fileName else fileNameFromUrl(url) ?: fileName
        if (name.isEmpty()) return null
        val visible = list.filter { it.visible }
        val normals = visible.filter { it.builtinType != "all" && it.builtinType != "other" }
        normals.firstOrNull { it.saveDir.isNotEmpty() && matchesName(it, name) }?.let { return it.saveDir }
        val other = visible.firstOrNull { it.builtinType == "other" } ?: return null
        if (other.saveDir.isEmpty() || normals.any { matchesName(it, name) }) return null
        return other.saveDir
    }

    /** 正则不区分大小写、任意位置命中，非法正则视为不命中；扩展名比较不区分大小写（同 `category_dir.rs::matches`）。 */
    private fun matchesName(category: CustomCategoryDto, name: String): Boolean {
        if (category.matchMode == "regex") {
            if (category.regexPattern.isEmpty()) return false
            val regex = try {
                Regex(category.regexPattern, RegexOption.IGNORE_CASE)
            } catch (_: IllegalArgumentException) {
                return false
            }
            return regex.containsMatchIn(name)
        }
        val dot = name.lastIndexOf('.')
        if (dot < 0) return false
        val extension = name.substring(dot + 1)
        return extension.isNotEmpty() && category.extensions.any { it.trimStart('.').equals(extension, ignoreCase = true) }
    }

    /** URL 路径末段（百分号解码）含 `.` 时作为文件名。 */
    private fun fileNameFromUrl(url: String): String? =
        UrlText.lastPathSegment(url)?.let { UrlText.percentDecode(it) }?.takeIf { '.' in it }
}

/**
 * 下载页筛选区各部分的显隐（云同步偏好 `ui.show_sidebar_status|queues|category`，通用设置「下载页显示」）。
 * 缺省全部显示（对应 iOS `FilterBarVisibility`）。
 */
data class FilterBarVisibility(
    val status: Boolean = true,
    val queues: Boolean = true,
    val categories: Boolean = true,
) {
    /**
     * 整个筛选区无内容可显示（状态条被隐藏，且没有可见的范围 / 分类芯片）。
     * [hasScopeChip]：队列芯片当前是否会显示（已含 [queues] 开关）。
     */
    fun isEmpty(hasCategories: Boolean, hasScopeChip: Boolean): Boolean =
        !status && !(categories && hasCategories) && !hasScopeChip

    companion object {
        const val STATUS_KEY = "ui.show_sidebar_status"
        const val QUEUES_KEY = "ui.show_sidebar_queues"
        const val CATEGORY_KEY = "ui.show_sidebar_category"

        fun of(preferences: AgentPreferences) = FilterBarVisibility(
            status = preferences.bool(STATUS_KEY, true),
            queues = preferences.bool(QUEUES_KEY, true),
            categories = preferences.bool(CATEGORY_KEY, true),
        )
    }
}
