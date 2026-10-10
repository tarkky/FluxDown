package com.fluxdown.core.update

import kotlinx.coroutines.flow.Flow

/*
 * 应用自更新的领域模型与端口。状态机在 Rust（`native/mobile/src/update.rs`，复用桌面 agent 的更新状态机），
 * Kotlin 只读状态、发起动作；生成的 UniFFI 类型不出 `:bridge`。
 */

/** 更新阶段（同 Rust `UpdatePhase`）。 */
enum class UpdatePhase { Idle, Checking, UpToDate, Available, Downloading, Ready, Installing, Failed }

/** 不可一键自更新的原因（同 Rust `UpdateManualReason`）。 */
enum class UpdateManualReason {
    ManagedPackage, NotWritable, NoAsset, ElevationUnavailable, ReadOnlyLocation, UnofficialBuild, Unsupported, Unknown,
}

/** 失败分类（同 Rust `UpdateFailure`；Android 上 [ElevationCancelled] = 用户在系统安装确认中取消）。 */
enum class UpdateFailure { Network, Verify, Storage, Install, ElevationCancelled, InstallIncomplete, Unknown }

/** 一个版本的更新说明。 */
data class ReleaseNote(val version: String, val publishedAt: String, val body: String)

/** 更新状态快照（尺寸与时间戳为 Long；字符串缺省为空串，与 Rust 一致）。 */
data class AppUpdateStatus(
    val phase: UpdatePhase = UpdatePhase.Idle,
    val currentVersion: String = "",
    val channel: String = "",
    val latestVersion: String = "",
    val hasUpdate: Boolean = false,
    val manualReason: UpdateManualReason? = null,
    val assetName: String = "",
    val assetSize: Long = 0,
    val downloadedBytes: Long = 0,
    val installPending: Boolean = false,
    val downloadUrl: String = "",
    val releasePageUrl: String = "",
    val notes: List<ReleaseNote> = emptyList(),
    val failure: UpdateFailure? = null,
    val errorDetail: String = "",
    val checkedAtMs: Long = 0,
)

/** 更新器信号：状态变化，或「已校验的安装包就绪且用户已确认安装」。 */
sealed interface AppUpdateSignal {
    data class Status(val status: AppUpdateStatus) : AppUpdateSignal
    data class InstallRequested(val packagePath: String, val version: String) : AppUpdateSignal
}

/** 更新器端口（`:bridge` 的 Rust 实现；所有失败为 `HostException`）。 */
interface AppUpdatePort {
    /** 拉取式信号流：首个信号 = 当前状态，安装请求优先。 */
    val signals: Flow<AppUpdateSignal>

    fun status(): AppUpdateStatus

    /** 对账上次安装：返回刚升级完成的版本（用于「已更新到 vX」提示），否则 null。 */
    suspend fun reconcile(): String?

    /** [channel] = `"stable"` / `"frontier"`。 */
    suspend fun check(channel: String): AppUpdateStatus
    suspend fun download(): AppUpdateStatus

    /** 用户确认安装：包未就绪先下载，就绪后发 [AppUpdateSignal.InstallRequested]。 */
    suspend fun install(): AppUpdateStatus
    fun cancel(): AppUpdateStatus

    /** 仅 Installing 阶段生效；[UpdateFailure.Verify] 会删除该安装包。 */
    suspend fun reportInstallFailure(failure: UpdateFailure, detail: String): AppUpdateStatus
}

/** 更新器启动参数：[manualReason] 是 App 侧判定的不可自更新原因（商店安装 / 非官方构建）。 */
data class AppUpdateConfig(
    val dataDir: String,
    val currentVersion: String,
    val supportedAbis: List<String>,
    val manualReason: UpdateManualReason?,
)
