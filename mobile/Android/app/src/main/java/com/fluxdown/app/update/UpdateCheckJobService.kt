package com.fluxdown.app.update

import android.app.job.JobInfo
import android.app.job.JobParameters
import android.app.job.JobScheduler
import android.app.job.JobService
import android.content.ComponentName
import android.content.Context
import com.fluxdown.app.FluxApplication
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch

/**
 * 周期后台检查（JobScheduler，24h；不用 WorkManager）：检查 → 条件允许时下载 → 通知。从不安装。
 * 不持久化（`setPersisted(false)`，免 RECEIVE_BOOT_COMPLETED）：进程冷启动时由 [sync] 重新登记（幂等）。
 */
class UpdateCheckJobService : JobService() {
    private var running: Job? = null

    override fun onStartJob(params: JobParameters): Boolean {
        val container = (application as FluxApplication).container
        running = container.appScope.launch {
            try {
                container.updates.backgroundCheck()
            } finally {
                jobFinished(params, false)
            }
        }
        return true
    }

    /** 系统中止（约束不再满足 / 超时）：取消本次并要求重试；已下载的 `.part` 下次续传。 */
    override fun onStopJob(params: JobParameters): Boolean {
        running?.cancel()
        return true
    }

    companion object {
        private const val JOB_ID = 3101
        private const val PERIOD_MS = 24L * 60 * 60 * 1000

        /** 按「自动检查」开关登记 / 取消周期任务（幂等：已登记且开启时不重排，避免重置周期）。 */
        fun sync(context: Context, autoCheck: Boolean) {
            val scheduler = context.getSystemService(JobScheduler::class.java)
            if (!autoCheck) {
                scheduler.cancel(JOB_ID)
                return
            }
            if (scheduler.getPendingJob(JOB_ID) != null) return
            val job = JobInfo.Builder(JOB_ID, ComponentName(context, UpdateCheckJobService::class.java))
                .setPeriodic(PERIOD_MS)
                .setRequiredNetworkType(JobInfo.NETWORK_TYPE_ANY)
                .setRequiresBatteryNotLow(true)
                .setPersisted(false)
                .build()
            scheduler.schedule(job)
        }
    }
}
