package com.fluxdown.bridge

import com.fluxdown.core.update.AppUpdatePort
import com.fluxdown.core.update.AppUpdateSignal
import com.fluxdown.core.update.AppUpdateStatus
import com.fluxdown.core.update.UpdateFailure
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

/**
 * `:core` 的 [AppUpdatePort] 在 UniFFI `AppUpdater` 上的实现。信号流是“拉”模型：只在被收集时逐个拉取，
 * 收集被取消即停止；更新器本身进程内唯一、随进程存活，无需 close。
 */
internal class RustAppUpdater(private val updater: AppUpdater) : AppUpdatePort {
    override val signals: Flow<AppUpdateSignal> = flow {
        while (true) {
            val dto = guarded { updater.nextSignal() } ?: break
            emit(dto.toCore())
        }
    }

    override fun status(): AppUpdateStatus = updater.status().toCore()

    override suspend fun reconcile(): String? = guarded { updater.reconcile() }

    override suspend fun check(channel: String): AppUpdateStatus = guarded { updater.check(channel) }.toCore()

    override suspend fun download(): AppUpdateStatus = guarded { updater.download() }.toCore()

    override suspend fun install(): AppUpdateStatus = guarded { updater.install() }.toCore()

    override fun cancel(): AppUpdateStatus = updater.cancel().toCore()

    override suspend fun reportInstallFailure(failure: UpdateFailure, detail: String): AppUpdateStatus =
        guarded { updater.reportInstallFailure(failure.toDto(), detail) }.toCore()
}
