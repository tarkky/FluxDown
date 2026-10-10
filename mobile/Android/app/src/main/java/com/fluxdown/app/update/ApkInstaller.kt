package com.fluxdown.app.update

import android.app.PendingIntent
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.pm.PackageInstaller
import android.content.pm.PackageManager
import android.os.Build
import android.util.Log
import com.fluxdown.app.FluxApplication
import com.fluxdown.core.update.UpdateFailure
import com.fluxdown.core.update.UpdatePolicy
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.io.File
import java.io.IOException

/** [ApkInstaller.install] 的结果。 */
internal sealed interface InstallOutcome {
    /** 会话已提交；最终结果经 [InstallResultReceiver] 异步回报（成功时系统替换并结束进程）。 */
    data object Submitted : InstallOutcome

    /** 未授予「安装未知应用」：未提交，UI 引导授权后重试。 */
    data object PermissionMissing : InstallOutcome

    /** 未提交：包校验 / 写入失败，应回报给 Rust（[UpdateFailure.Verify] 会让 Rust 删除坏包）。 */
    data class Rejected(val failure: UpdateFailure, val detail: String) : InstallOutcome
}

/**
 * 经 `PackageInstaller` 会话安装已校验的更新包。用户必须点按确认系统安装界面
 * （本类从不绕过系统确认；`USER_ACTION_NOT_REQUIRED` 只在系统判定符合静默更新条件时才免确认）。
 */
internal class ApkInstaller(private val context: Context, private val env: InstallEnvironment) {
    fun canRequestInstalls(): Boolean = context.packageManager.canRequestPackageInstalls()

    /** 预检 → 流式写入会话 → 提交。所有 IO 在 [Dispatchers.IO]。 */
    suspend fun install(packagePath: String): InstallOutcome = withContext(Dispatchers.IO) {
        if (!canRequestInstalls()) return@withContext InstallOutcome.PermissionMissing
        val file = File(packagePath)
        precheck(file)?.let { return@withContext it }
        try {
            commit(file)
            InstallOutcome.Submitted
        } catch (e: IOException) {
            Log.w(TAG, "write install session failed: ${e.message}")
            InstallOutcome.Rejected(UpdateFailure.Storage, e.message.orEmpty())
        } catch (e: SecurityException) {
            Log.w(TAG, "commit install session denied: ${e.message}")
            InstallOutcome.Rejected(UpdateFailure.Install, e.message.orEmpty())
        } catch (e: IllegalStateException) {
            Log.w(TAG, "install session state error: ${e.message}")
            InstallOutcome.Rejected(UpdateFailure.Install, e.message.orEmpty())
        }
    }

    /** 包名必须是自身、签名证书与已安装一致（允许轮换链）、版本不得低于已安装；否则按 Verify 拒绝。 */
    private fun precheck(file: File): InstallOutcome.Rejected? {
        val archive = env.archiveInfo(file.path) ?: return InstallOutcome.Rejected(UpdateFailure.Verify, "unreadable package")
        if (archive.packageName != context.packageName) {
            return InstallOutcome.Rejected(UpdateFailure.Verify, "package name mismatch: ${archive.packageName}")
        }
        val installed = try {
            env.installedInfo()
        } catch (e: PackageManager.NameNotFoundException) {
            Log.w(TAG, "installed package info unavailable, skip precheck: ${e.message}")
            return null
        }
        val installedSigners = InstallEnvironment.signerDigests(installed)
        val archiveSigners = InstallEnvironment.signerDigests(archive)
        val sameSigners = archiveSigners == installedSigners ||
            (installedSigners.isNotEmpty() && InstallEnvironment.signerHistory(archive).containsAll(installedSigners))
        if (!sameSigners) {
            return InstallOutcome.Rejected(UpdateFailure.Verify, "${UpdatePolicy.SIGNATURE_MISMATCH}: installed=$installedSigners package=$archiveSigners")
        }
        if (archive.longVersionCode < installed.longVersionCode) {
            return InstallOutcome.Rejected(UpdateFailure.Verify, "version downgrade: ${archive.longVersionCode} < ${installed.longVersionCode}")
        }
        return null
    }

    private fun commit(file: File) {
        val installer = context.packageManager.packageInstaller
        val params = PackageInstaller.SessionParams(PackageInstaller.SessionParams.MODE_FULL_INSTALL).apply {
            setAppPackageName(context.packageName)
            setSize(file.length())
            setInstallReason(PackageManager.INSTALL_REASON_USER)
            setRequireUserAction(PackageInstaller.SessionParams.USER_ACTION_NOT_REQUIRED)
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                setPackageSource(PackageInstaller.PACKAGE_SOURCE_DOWNLOADED_FILE)
            }
        }
        val sessionId = installer.createSession(params)
        try {
            installer.openSession(sessionId).use { session ->
                file.inputStream().use { input ->
                    session.openWrite(APK_NAME, 0, file.length()).use { out ->
                        input.copyTo(out, COPY_BUFFER)
                        session.fsync(out)
                    }
                }
                session.commit(resultSender(sessionId))
            }
        } catch (e: Exception) {
            runCatching { installer.abandonSession(sessionId) }
            throw e
        }
    }

    /** 结果回到 [InstallResultReceiver]：必须可变（系统要往里填状态 extras），显式组件满足 Android 14+ 的限制。 */
    private fun resultSender(sessionId: Int) = PendingIntent.getBroadcast(
        context,
        sessionId,
        Intent(context, InstallResultReceiver::class.java).setAction(ACTION_RESULT),
        PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_MUTABLE,
    ).intentSender

    private companion object {
        const val TAG = "FluxDown.update"
        const val APK_NAME = "base.apk"
        const val COPY_BUFFER = 1 shl 20
        const val ACTION_RESULT = "com.fluxdown.app.action.INSTALL_RESULT"
    }
}

/**
 * 安装会话结果（non-exported）：待确认 → 拉起系统安装确认；成功时进程已被系统替换（不会走到这里）；
 * 失败 / 取消 → 回报给更新控制器（Rust 状态机转 Failed）。
 */
class InstallResultReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent) {
        val status = intent.getIntExtra(PackageInstaller.EXTRA_STATUS, PackageInstaller.STATUS_FAILURE)
        val message = intent.getStringExtra(PackageInstaller.EXTRA_STATUS_MESSAGE).orEmpty()
        when (status) {
            PackageInstaller.STATUS_PENDING_USER_ACTION -> {
                @Suppress("DEPRECATION")
                val confirm = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                    intent.getParcelableExtra(Intent.EXTRA_INTENT, Intent::class.java)
                } else {
                    intent.getParcelableExtra(Intent.EXTRA_INTENT)
                }
                if (confirm == null) {
                    report(context, status, "missing confirmation intent")
                } else {
                    try {
                        context.startActivity(confirm.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK))
                    } catch (e: RuntimeException) {
                        Log.w(TAG, "launch install confirmation failed: ${e.message}")
                        report(context, PackageInstaller.STATUS_FAILURE, e.message.orEmpty())
                    }
                }
            }
            PackageInstaller.STATUS_SUCCESS -> Unit
            else -> report(context, status, message)
        }
    }

    private fun report(context: Context, status: Int, message: String) {
        val failure = UpdatePolicy.installFailureFor(status) ?: UpdateFailure.Install
        val container = (context.applicationContext as FluxApplication).container
        container.appScope.launch { container.updates.onInstallFailed(failure, message) }
    }

    private companion object {
        const val TAG = "FluxDown.update"
    }
}
