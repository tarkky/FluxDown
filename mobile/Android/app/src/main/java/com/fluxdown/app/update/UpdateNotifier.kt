package com.fluxdown.app.update

import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import androidx.core.app.NotificationChannelCompat
import androidx.core.app.NotificationCompat
import androidx.core.app.NotificationManagerCompat
import com.fluxdown.app.HomeActivity
import com.fluxdown.app.R
import com.fluxdown.app.i18n.str
import com.fluxdown.app.nav.AppNavigator
import com.fluxdown.app.nav.AppTab
import com.fluxdown.app.nav.Route
import com.fluxdown.app.nav.SettingsPage
import com.fluxdown.app.service.NotificationTint
import com.fluxdown.core.update.AppUpdateStatus
import com.fluxdown.core.update.UpdatePhase

/**
 * 应用更新通知：独立渠道「应用更新」；「新版本可用」与「已下载待安装」共用一个通知 id（后者覆盖前者）。
 * 点按打开应用并进入 关于 › 软件更新（[handle]）。系统通知被关闭 / 未授权时静默跳过。
 */
internal object UpdateNotifier {
    const val ACTION_OPEN = "com.fluxdown.app.action.OPEN_UPDATE"
    private const val CHANNEL = "fluxdown_update"
    private const val NOTIFICATION_ID = 3001

    fun notify(context: Context, status: AppUpdateStatus) {
        val app = context.applicationContext
        val manager = NotificationManagerCompat.from(app)
        if (!manager.areNotificationsEnabled()) return
        manager.createNotificationChannel(
            NotificationChannelCompat.Builder(CHANNEL, NotificationManagerCompat.IMPORTANCE_DEFAULT)
                .setName(app.str(R.string.mobileUpdateChannelName))
                .setDescription(app.str(R.string.mobileUpdateChannelDesc))
                .build(),
        )
        val text = if (status.phase == UpdatePhase.Ready) {
            app.str(R.string.mobileUpdateReady, "v" to status.latestVersion)
        } else {
            app.str(R.string.newVersionFound, "v" to status.latestVersion)
        }
        val notification = NotificationCompat.Builder(app, CHANNEL)
            .setSmallIcon(R.drawable.ic_stat_download)
            .setColor(NotificationTint)
            .setContentTitle(app.str(R.string.softwareUpdate))
            .setContentText(text)
            .setContentIntent(openIntent(app))
            .setAutoCancel(true)
            .setOnlyAlertOnce(true)
            .build()
        // 授权已由 areNotificationsEnabled 确认；系统仍可能因权限变化拒绝，此时静默放弃。
        try {
            manager.notify(NOTIFICATION_ID, notification)
        } catch (e: SecurityException) {
            android.util.Log.w("FluxDown.update", "update notification denied: ${e.message}")
        }
    }

    fun cancel(context: Context) {
        NotificationManagerCompat.from(context.applicationContext).cancel(NOTIFICATION_ID)
    }

    private fun openIntent(context: Context): PendingIntent {
        val intent = Intent(context, HomeActivity::class.java)
            .setAction(ACTION_OPEN)
            .addFlags(Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TOP)
        return PendingIntent.getActivity(context, NOTIFICATION_ID, intent, PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT)
    }

    /** @return true = 这是更新通知意图（已处理）：导航到 关于 页。 */
    fun handle(nav: AppNavigator, intent: Intent?): Boolean {
        if (intent?.action != ACTION_OPEN) return false
        nav.selectTab(AppTab.Settings)
        val about = Route.Settings(SettingsPage.About)
        if (nav.stack.lastOrNull() != about) nav.push(about)
        return true
    }
}
