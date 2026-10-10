package com.fluxdown.app

import android.app.Activity
import android.content.Intent
import android.os.Bundle

/**
 * 对外公开的稳定入口：类名 `com.fluxdown.app.MainActivity` 不可改——桌面图标与固定快捷方式指向它，部分浏览器
 * （X / Via 等）还按 Flutter 版保存的组件名把下载 intent 显式发给它。
 *
 * 自身无界面（`Theme.NoDisplay`）：下载 intent（VIEW / SEND / SEND_MULTIPLE）转交透明的
 * [ExternalDownloadActivity]，其余转交主界面 [HomeActivity]，随即结束。
 */
class MainActivity : Activity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        forward(intent)
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        setIntent(intent)
        forward(intent)
    }

    private fun forward(source: Intent) {
        val target = when (source.action) {
            Intent.ACTION_SEND, Intent.ACTION_SEND_MULTIPLE, Intent.ACTION_VIEW -> ExternalDownloadActivity::class.java
            else -> HomeActivity::class.java
        }
        startActivity(Intent(source).setClass(this, target))
        finish()
    }
}
