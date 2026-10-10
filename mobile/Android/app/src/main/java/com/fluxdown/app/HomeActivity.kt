package com.fluxdown.app

import android.animation.ValueAnimator
import android.content.Intent
import android.content.res.Resources
import android.graphics.drawable.ColorDrawable
import android.os.Build
import android.os.Bundle
import android.window.SplashScreenView
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.compose.ui.geometry.Offset
import androidx.compose.ui.geometry.Rect
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.unit.dp
import androidx.lifecycle.lifecycleScope
import com.fluxdown.app.data.ThemeMode
import com.fluxdown.app.nav.AppNavigator
import com.fluxdown.app.service.NotificationIntents
import com.fluxdown.app.update.UpdateNotifier
import com.fluxdown.app.shell.AppShell
import com.fluxdown.app.shell.FluxAppRoot
import com.fluxdown.fluxui.material.FluxLaunchReveal
import com.fluxdown.fluxui.material.LaunchRevealState
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
import kotlin.math.min

/**
 * 主界面（Compose 宿主）。不对外导出：桌面图标经 [MainActivity] 路由进来，系统通知 / 前台服务通知直接指向这里；
 * 外部下载唤起由透明的 [ExternalDownloadActivity] 承接。
 */
class HomeActivity : ComponentActivity() {
    private val navigator = AppNavigator()

    override fun onCreate(savedInstanceState: Bundle?) {
        enableEdgeToEdge()
        super.onCreate(savedInstanceState)
        val container = (application as FluxApplication).container
        // 启动揭幕只在新建界面时播放（恢复实例 / 系统关闭动画时直接呈现，系统画面按默认方式退场）。
        val reveal = if (savedInstanceState == null && ValueAnimator.areAnimatorsEnabled()) LaunchRevealState() else null
        if (reveal != null) splashScreen.setOnExitAnimationListener { splash -> handOff(splash, reveal) }
        if (savedInstanceState == null) handleIntent(intent)
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) followThemeModeInSplash(container)
        setContent {
            FluxAppRoot(container, navigator) {
                FluxLaunchReveal(reveal, SPLASH_MARK_SIZE) {
                    AppShell()
                }
            }
        }
    }

    /**
     * 系统启动画面只认静态主题：App 内强制明 / 暗时把对应启动主题持久化给系统（下次冷启动生效），
     * 跟随系统时清除，回到 `Theme.FluxDown` 的随系统资源。强调色无法这样传递，由揭幕吸气段过渡。
     */
    private fun followThemeModeInSplash(container: AppContainer) {
        lifecycleScope.launch {
            container.appearance.state.map { it.mode }.distinctUntilChanged().collect { mode ->
                splashScreen.setSplashScreenTheme(
                    when (mode) {
                        ThemeMode.System -> Resources.ID_NULL
                        ThemeMode.Light -> R.style.Theme_FluxDown_Splash_Light
                        ThemeMode.Dark -> R.style.Theme_FluxDown_Splash_Dark
                    },
                )
            }
        }
    }

    /** 系统启动画面在首帧绘制后回调：读出其中 logo 画框的窗口坐标与底色交给揭幕，由揭幕决定何时移除系统画面。 */
    private fun handOff(splash: SplashScreenView, reveal: LaunchRevealState) {
        val icon = splash.iconView
        val mark = if (icon != null && icon.width > 0 && icon.height > 0) {
            val at = IntArray(2)
            icon.getLocationInWindow(at)
            val side = min(icon.width, icon.height) * SPLASH_MARK_FRACTION
            Rect(Offset(at[0] + icon.width / 2f, at[1] + icon.height / 2f), side / 2f)
        } else {
            null
        }
        val background = (splash.background as? ColorDrawable)?.color ?: getColor(R.color.flux_canvas)
        reveal.handOff(mark, Color(background)) { splash.remove() }
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        handleIntent(intent)
    }

    /** 点按系统通知 → 切到通知所属主机并打开任务详情；其余（桌面图标启动）无需处理。 */
    private fun handleIntent(intent: Intent?) {
        if (UpdateNotifier.handle(navigator, intent)) return
        NotificationIntents.handle((application as FluxApplication).container, navigator, intent)
    }
}

/** `drawable/splash_mark.xml` 中 logo 画框（400 单位）占图标画布的比例。 */
private const val SPLASH_MARK_FRACTION = 0.4f

/** 无系统画面可交接时的 logo 画框边长 = 系统无底图标尺寸 288dp × [SPLASH_MARK_FRACTION]。 */
private val SPLASH_MARK_SIZE = 115.2.dp
