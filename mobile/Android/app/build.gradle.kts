import groovy.json.JsonSlurper

plugins {
    alias(libs.plugins.android.application)
    alias(libs.plugins.kotlin.compose)
}

/**
 * 文案单一事实源 = 仓库根 `assets/i18n/{en,zh}.json`（App / GPUI / Web 共用，AGENTS.md §5）。
 * 构建期生成 `values/strings.xml`（en，默认）与 `values-zh/strings.xml`，键名原样（camelCase），
 * 占位符 `{name}` 原样保留，由 `:app` 的 `fill()` 运行期替换。禁止在 res/ 里手写同名文案。
 */
abstract class GenerateI18nResources : DefaultTask() {
    @get:InputDirectory
    @get:PathSensitive(PathSensitivity.RELATIVE)
    abstract val i18nDir: DirectoryProperty

    @get:OutputDirectory
    abstract val outputDir: DirectoryProperty

    @TaskAction
    fun generate() {
        val out = outputDir.get().asFile
        out.deleteRecursively()
        mapOf("en" to "values", "zh" to "values-zh").forEach { (lang, dir) ->
            val src = i18nDir.file("$lang.json").get().asFile
            @Suppress("UNCHECKED_CAST")
            val map = JsonSlurper().parse(src) as Map<String, Any?>
            val xml = buildString {
                append("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<resources>\n")
                for ((k, v) in map) {
                    if (v !is String) continue
                    append("    <string name=\"").append(k).append("\" formatted=\"false\">")
                        .append(escape(v)).append("</string>\n")
                }
                append("</resources>\n")
            }
            File(out, dir).apply { mkdirs() }.resolve("strings_i18n.xml").writeText(xml)
        }
    }

    private fun escape(s: String): String {
        val b = StringBuilder(s.length + 8)
        s.forEachIndexed { i, c ->
            when (c) {
                '&' -> b.append("&amp;")
                '<' -> b.append("&lt;")
                '>' -> b.append("&gt;")
                '\\' -> b.append("\\\\")
                '\'' -> b.append("\\'")
                '"' -> b.append("\\\"")
                '\n' -> b.append("\\n")
                '\t' -> b.append("\\t")
                '@', '?' -> if (i == 0) b.append('\\').append(c) else b.append(c)
                else -> b.append(c)
            }
        }
        return b.toString()
    }
}

val generateI18n = tasks.register<GenerateI18nResources>("generateI18nResources") {
    i18nDir.set(rootProject.layout.projectDirectory.dir("../../assets/i18n"))
    outputDir.set(layout.buildDirectory.dir("generated/i18n/res"))
}

androidComponents {
    onVariants { variant ->
        variant.sources.res?.addGeneratedSourceDirectory(generateI18n, GenerateI18nResources::outputDir)
    }
}

/**
 * 发布参数（均可选；`scripts/package.sh` 与 CI 经此注入，本地开发构建无需设置）：
 *  - Gradle 属性 `fluxdown.version`：versionName（可带 `-rc.1` 之类后缀）；versionCode 由前三段派生
 *    `MAJ*1000000 + MIN*10000 + PAT*100`，与 Flutter 版 Android 同一公式，同一 applicationId 覆盖安装时单调递增；
 *  - 环境变量 `FLUXDOWN_ANDROID_KEYSTORE`（.jks 路径）/ `FLUXDOWN_ANDROID_KEYSTORE_PASSWORD` /
 *    `FLUXDOWN_ANDROID_KEY_ALIAS` / `FLUXDOWN_ANDROID_KEY_PASSWORD`：齐全时 release 用该密钥签名；全缺则 release 不签名；
 *    只给一部分直接失败，避免静默产出未签名包；
 *  - Gradle 属性 `fluxdown.splitAbi=true`：在 universal 包之外再按 ABI 拆出单 ABI 包。
 */
object Release {
    private val VERSION = Regex("""^(\d+)\.(\d+)\.(\d+)(?:-[0-9A-Za-z.]+)?$""")

    fun versionCode(versionName: String): Int {
        val match = VERSION.matchEntire(versionName)
            ?: throw GradleException("fluxdown.version 须为 X.Y.Z[-后缀]（得到 '$versionName'）")
        val (major, minor, patch) = match.destructured.toList().map(String::toInt)
        if (major > 2000 || minor > 99 || patch > 99) {
            throw GradleException("fluxdown.version 超出 versionCode 编码范围（major ≤ 2000，minor / patch ≤ 99）：$versionName")
        }
        val code = major * 1_000_000 + minor * 10_000 + patch * 100
        // Android 要求 versionCode 为正整数；0.0.0 会产出无法覆盖安装任何正式包的 APK。
        if (code <= 0) {
            throw GradleException("fluxdown.version 须大于 0.0.0（versionCode 必须为正）：$versionName")
        }
        return code
    }
}

val appVersionName: String = providers.gradleProperty("fluxdown.version").orElse("0.1.0").get()

val abiList: List<String> = providers.gradleProperty("fluxdown.abis").orElse("arm64-v8a,x86_64").get()
    .split(',').map { it.trim() }.filter { it.isNotEmpty() }

val signingEnvNames = listOf(
    "FLUXDOWN_ANDROID_KEYSTORE",
    "FLUXDOWN_ANDROID_KEYSTORE_PASSWORD",
    "FLUXDOWN_ANDROID_KEY_ALIAS",
    "FLUXDOWN_ANDROID_KEY_PASSWORD",
)

/** 齐全时为变量名 → 值；全缺为 null。 */
val releaseSigning: Map<String, String>? = run {
    val present = signingEnvNames.mapNotNull { name ->
        providers.environmentVariable(name).orNull?.takeIf { it.isNotBlank() }?.let { name to it }
    }.toMap()
    when (present.size) {
        0 -> null
        signingEnvNames.size -> present
        else -> throw GradleException("release 签名环境变量不完整，缺少：${(signingEnvNames - present.keys).joinToString()}")
    }
}

android {
    namespace = "com.fluxdown.app"
    compileSdk {
        version = release(37)
    }

    defaultConfig {
        applicationId = "com.fluxdown.app"
        // API 31：RenderEffect 背景模糊为材质基线（docs/mobile-ui README §0），无低于 31 的分支
        minSdk = 31
        targetSdk = 37
        versionCode = Release.versionCode(appVersionName)
        versionName = appVersionName
        // 只打包 :bridge 实际编译了 libfluxdown_mobile.so 的 ABI（JNA aar 自带 armeabi / mips 等无对应引擎库的 ABI，
        // 留着会让这些设备装得上却在加载引擎时崩溃）。与 :bridge 同源：Gradle 属性 fluxdown.abis。
        ndk {
            abiFilters += abiList
        }
    }

    splits {
        abi {
            isEnable = providers.gradleProperty("fluxdown.splitAbi").map(String::toBoolean).getOrElse(false)
            reset()
            include(*abiList.toTypedArray())
            isUniversalApk = true
        }
    }

    signingConfigs {
        releaseSigning?.let { env ->
            create("release") {
                storeFile = file(env.getValue("FLUXDOWN_ANDROID_KEYSTORE"))
                storePassword = env.getValue("FLUXDOWN_ANDROID_KEYSTORE_PASSWORD")
                keyAlias = env.getValue("FLUXDOWN_ANDROID_KEY_ALIAS")
                keyPassword = env.getValue("FLUXDOWN_ANDROID_KEY_PASSWORD")
            }
        }
    }

    buildTypes {
        release {
            signingConfig = signingConfigs.findByName("release")
            // 全量 R8（不用实验性的 packageScope）：packageScope 下被范围外代码引用的 kotlin 多文件门面父类
            // （如 SequencesKt__SequenceBuilderKt）保持包私有，子类却被重打包进默认包，启动即 IllegalAccessError。
            // JNA / UniFFI 绑定的保留规则在 :bridge 的 consumer-rules.pro。
            optimization {
                enable = true
            }
        }
    }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
    buildFeatures {
        compose = true
    }
    androidResources {
        // 仅打包有基线翻译的语言（assets/i18n 的 en / zh）；生成 localeConfig 供系统“应用语言”设置列出
        localeFilters += listOf("en", "zh")
        generateLocaleConfig = true
    }
}

dependencies {
    implementation(project(":core"))
    implementation(project(":fluxui"))
    implementation(project(":bridge"))
    implementation(platform(libs.androidx.compose.bom))
    implementation(libs.androidx.activity.compose)
    implementation(libs.androidx.core.ktx)
    implementation(libs.androidx.lifecycle.runtime.ktx)
    implementation(libs.androidx.lifecycle.runtime.compose)
    implementation(libs.androidx.datastore.preferences)
    implementation(libs.kotlinx.coroutines.android)
    implementation(libs.androidx.compose.ui.tooling.preview)
    debugImplementation(libs.androidx.compose.ui.tooling)
}
