//! 应用菜单（macOS 原生 / Windows·Linux 标题栏菜单栏）、全局键位与应用级动作处理。

use fluxdown_ui_downloads::actions as dl;
use fluxdown_ui_i18n::Translator;
use gpui::{App, Entity, KeyBinding, Menu, MenuItem, SharedString, Window};
use gpui_component::{GlobalState, menu::AppMenuBar};

use crate::{
    actions::{
        About, BringAllToFront, CheckUpdate, CloseWindow, Hide, HideOthers, MinimizeWindow,
        OpenLogsFolder, OpenSettings, OpenWebsite, Quit, ShowAll, ShowMainWindow,
        ToggleCommandPalette, ToggleFullScreen, ZoomWindow,
    },
    app::Desktop,
    windows::{WindowKey, WindowRegistry, confirm_active_tasks},
};

const WEBSITE_URL: &str = "https://fluxdown.zerx.dev";

fn primary() -> &'static str {
    if cfg!(target_os = "macos") {
        "cmd"
    } else {
        "ctrl"
    }
}

/// 注册全局键位。字母键只在下载页上下文生效（页面内再按聚焦输入框二次拦截）。
///
/// macOS 的 ⌘W / ⌘M / ⌘H / ⌃⌘F 由 AppKit 经菜单 key equivalent 分发，gpui 不会自动
/// 提供；须像 Zed 一样显式绑定并在菜单树里给出对应项。
pub fn bind_keys(cx: &mut App) {
    let p = primary();
    let dl_ctx = Some(dl::KEY_CONTEXT);
    let mut bindings = vec![
        KeyBinding::new(&format!("{p}-n"), dl::NewDownload, None),
        KeyBinding::new(&format!("{p}-,"), OpenSettings, None),
        KeyBinding::new(&format!("{p}-k"), ToggleCommandPalette, None),
        KeyBinding::new(&format!("{p}-f"), dl::FocusSearch, dl_ctx),
        KeyBinding::new(&format!("{p}-a"), dl::SelectAllTasks, dl_ctx),
        KeyBinding::new("delete", dl::DeleteSelected, dl_ctx),
        KeyBinding::new("backspace", dl::DeleteSelected, dl_ctx),
        KeyBinding::new(&format!("{p}-backspace"), dl::DeleteSelected, dl_ctx),
        KeyBinding::new("shift-delete", dl::DeleteSelectedWithFiles, dl_ctx),
        KeyBinding::new("space", dl::TogglePauseSelected, dl_ctx),
        KeyBinding::new("shift-d", dl::CycleDensity, dl_ctx),
        KeyBinding::new("g", dl::CycleGroupBy, dl_ctx),
        KeyBinding::new("s", dl::CycleSort, dl_ctx),
        KeyBinding::new(&format!("{p}-i"), dl::ToggleDetailPanel, dl_ctx),
        KeyBinding::new("escape", dl::ClearSelection, dl_ctx),
        KeyBinding::new(
            "space",
            gpui_component::dialog::Confirm { secondary: false },
            Some(fluxdown_ui_components::DIALOG_PRIMARY_KEY_CONTEXT),
        ),
    ];
    if cfg!(target_os = "macos") {
        bindings.extend([
            KeyBinding::new("cmd-q", Quit, None),
            KeyBinding::new("cmd-w", CloseWindow, None),
            KeyBinding::new("cmd-m", MinimizeWindow, None),
            KeyBinding::new("ctrl-cmd-f", ToggleFullScreen, None),
            KeyBinding::new("cmd-h", Hide, None),
            KeyBinding::new("alt-cmd-h", HideOthers, None),
        ]);
    }
    cx.bind_keys(bindings);
    fluxdown_ui_command_palette::bind_keys(cx);
}

fn t(translator: &Translator, key: &str) -> SharedString {
    SharedString::from(translator.text(key).to_owned())
}

/// 菜单树（macOS 原生菜单与标题栏菜单栏共用一份）。
pub fn build_menus(translator: &Translator) -> Vec<Menu> {
    let mut menus = Vec::with_capacity(7);
    if cfg!(target_os = "macos") {
        menus.push(Menu::new("FluxDown").items([
            MenuItem::action(t(translator, "menuAbout"), About),
            MenuItem::separator(),
            MenuItem::action(t(translator, "menuSettings"), OpenSettings),
            MenuItem::separator(),
            MenuItem::action(t(translator, "menuHide"), Hide),
            MenuItem::action(t(translator, "menuHideOthers"), HideOthers),
            MenuItem::action(t(translator, "menuShowAll"), ShowAll),
            MenuItem::separator(),
            MenuItem::action(t(translator, "menuQuit"), Quit),
        ]));
    }
    let mut file = vec![
        MenuItem::action(t(translator, "menuNewDownload"), dl::NewDownload),
        MenuItem::action(t(translator, "openTorrentFile"), dl::OpenTorrentFile),
    ];
    file.push(MenuItem::separator());
    if cfg!(target_os = "macos") {
        file.push(MenuItem::action(
            t(translator, "menuCloseWindow"),
            CloseWindow,
        ));
    } else {
        file.push(MenuItem::action(t(translator, "menuQuit"), Quit));
    }
    menus.push(Menu::new(t(translator, "menuFile")).items(file));
    menus.push(Menu::new(t(translator, "menuTasks")).items([
        MenuItem::action(t(translator, "pause"), dl::PauseSelected),
        MenuItem::action(t(translator, "resume"), dl::ResumeSelected),
        MenuItem::action(t(translator, "deleteTask"), dl::DeleteSelected),
        MenuItem::action(
            t(translator, "deleteTaskAndFile"),
            dl::DeleteSelectedWithFiles,
        ),
        MenuItem::separator(),
        MenuItem::action(t(translator, "selectAll"), dl::SelectAllTasks),
        MenuItem::action(t(translator, "pauseAll"), dl::PauseAll),
        MenuItem::action(t(translator, "resumeAll"), dl::ResumeAll),
        MenuItem::action(t(translator, "menuClearFinished"), dl::ClearFinished),
        MenuItem::separator(),
        MenuItem::action(t(translator, "openFile"), dl::OpenSelected),
        MenuItem::action(t(translator, "openFolder"), dl::RevealSelected),
        MenuItem::action(t(translator, "copyUrl"), dl::CopySelectedUrl),
        MenuItem::action(t(translator, "renameTask"), dl::RenameSelected),
        MenuItem::action(t(translator, "redownloadTask"), dl::RedownloadSelected),
        MenuItem::action(t(translator, "boostDownload"), dl::ToggleBoostSelected),
        MenuItem::action(t(translator, "menuOpenInWindow"), dl::OpenSelectedInWindow),
    ]));
    let mut view = vec![
        MenuItem::action(t(translator, "menuCycleDensity"), dl::CycleDensity),
        MenuItem::action(t(translator, "menuCycleGroupBy"), dl::CycleGroupBy),
        MenuItem::action(t(translator, "menuCycleSort"), dl::CycleSort),
        MenuItem::separator(),
        MenuItem::action(t(translator, "menuDetailPanel"), dl::ToggleDetailPanel),
    ];
    if cfg!(target_os = "macos") {
        view.push(MenuItem::separator());
        view.push(MenuItem::action(
            t(translator, "menuToggleFullScreen"),
            ToggleFullScreen,
        ));
    }
    menus.push(Menu::new(t(translator, "menuView")).items(view));
    let mut tools = Vec::with_capacity(4);
    if !cfg!(target_os = "macos") {
        tools.push(MenuItem::action(
            t(translator, "menuSettings"),
            OpenSettings,
        ));
    }
    tools.extend([
        MenuItem::action(t(translator, "manageQueueAction"), dl::OpenQueueManager),
        MenuItem::action(t(translator, "menuCheckForUpdates"), CheckUpdate),
        MenuItem::action(t(translator, "menuOpenLogsFolder"), OpenLogsFolder),
    ]);
    menus.push(Menu::new(t(translator, "menuTools")).items(tools));
    if cfg!(target_os = "macos") {
        menus.push(Menu::new(t(translator, "menuWindow")).items([
            MenuItem::action(t(translator, "menuMinimize"), MinimizeWindow),
            MenuItem::action(t(translator, "menuZoom"), ZoomWindow),
            MenuItem::separator(),
            MenuItem::action(t(translator, "menuBringAllToFront"), BringAllToFront),
        ]));
    }
    let mut help = vec![MenuItem::action(t(translator, "menuWebsite"), OpenWebsite)];
    if !cfg!(target_os = "macos") {
        help.push(MenuItem::action(t(translator, "menuAbout"), About));
    }
    menus.push(Menu::new(t(translator, "menuHelp")).items(help));
    menus
}

/// 装配菜单：原生菜单 + 标题栏菜单栏；翻译变化时重建。
pub fn install(cx: &mut App, translator: &Entity<Translator>) -> Entity<AppMenuBar> {
    apply_menus(translator, cx);
    let menu_bar = AppMenuBar::new(cx);
    let observed_bar = menu_bar.clone();
    cx.observe(translator, move |translator, cx| {
        apply_menus(&translator, cx);
        observed_bar.update(cx, |bar, cx| bar.reload(cx));
    })
    .detach();
    menu_bar
}

fn apply_menus(translator: &Entity<Translator>, cx: &mut App) {
    let translator = translator.read(cx).clone();
    cx.set_menus(build_menus(&translator));
    GlobalState::global_mut(cx).set_app_menus(
        build_menus(&translator)
            .into_iter()
            .map(Menu::owned)
            .collect(),
    );
}

/// 应用级动作（不依赖具体窗口）。窗口级动作作用于当前活动窗口。
pub fn install_global_actions(cx: &mut App) {
    cx.on_action(|_: &OpenSettings, cx| crate::windows::settings::open(cx));
    cx.on_action(|_: &ShowMainWindow, cx| {
        crate::windows::main::open(cx);
        cx.activate(true);
    });
    cx.on_action(|_: &Quit, cx| request_quit(cx));
    cx.on_action(|_: &CloseWindow, cx| WindowRegistry::close_active_window(cx));
    cx.on_action(|_: &MinimizeWindow, cx| with_active_window(cx, |w| w.minimize_window()));
    cx.on_action(|_: &ZoomWindow, cx| with_active_window(cx, |w| w.zoom_window()));
    cx.on_action(|_: &ToggleFullScreen, cx| {
        with_active_window(cx, |w| w.toggle_fullscreen());
    });
    cx.on_action(|_: &Hide, cx| cx.hide());
    cx.on_action(|_: &HideOthers, cx| cx.hide_other_apps());
    cx.on_action(|_: &ShowAll, cx| cx.unhide_other_apps());
    cx.on_action(|_: &BringAllToFront, cx| cx.activate(true));
    cx.on_action(|_: &OpenWebsite, cx| cx.open_url(WEBSITE_URL));
    cx.on_action(|_: &dl::NewDownload, cx| crate::windows::new_download::open_default(cx));
    cx.on_action(|_: &dl::OpenQueueManager, cx| crate::windows::queue_manager::open(cx));
    cx.on_action(|_: &CheckUpdate, cx| crate::update_notices::check_now(cx));
    cx.on_action(|_: &OpenLogsFolder, cx| open_logs_folder(cx));
    cx.on_action(|_: &About, cx| {
        crate::windows::settings::reveal(
            cx,
            fluxdown_ui_settings::SettingsTarget {
                page: "about",
                tab: "",
                row: None,
            },
        );
    });
    cx.on_action(|_: &ToggleCommandPalette, cx| crate::command_palette::toggle(cx));
}

/// 全局动作里操作活动窗口必须 defer：有活动窗口时，菜单点击与键盘一样经
/// `App::dispatch_action` 在该窗口的 update 栈内分发，同步 `handle.update` 拿不到窗口会静默失败。
fn with_active_window(cx: &mut App, f: impl FnOnce(&Window) + 'static) {
    cx.defer(move |cx| {
        if let Some(window) = WindowRegistry::focused_window(cx)
            && let Err(error) = window.update(cx, |_, window, _| f(window))
        {
            log::debug!("view or window released before lifecycle update: {error:#}");
        }
    });
}

/// 退出（菜单 / ⌘Q）：完全退出，后台 agent 与 daemon 一起停止。有活跃任务时先在当前窗口
/// 确认「下载将暂停」。只退出界面、保留后台，走关闭窗口（托盘驻留时）。
pub fn request_quit(cx: &mut App) {
    if Desktop::active_task_count(cx) == 0 {
        crate::lifecycle::quit_everything(cx);
        return;
    }
    cx.defer(|cx| {
        let Some(window) = WindowRegistry::focused_window(cx)
            .or_else(|| WindowRegistry::handle(cx, &WindowKey::Main))
        else {
            crate::lifecycle::quit_everything(cx);
            return;
        };
        if let Err(error) = window.update(cx, |_, window, cx| {
            confirm_active_tasks(window, cx, |_, cx| crate::lifecycle::quit_everything(cx));
        }) {
            log::debug!("view or window released before lifecycle update: {error:#}");
        }
    });
}

fn open_logs_folder(cx: &mut App) {
    // 桌面端自身日志目录本地已知；agent 不可达时日志入口恰是最需要的。
    let dir = crate::app::agent_data_dir().join("logs");
    #[cfg(not(windows))]
    cx.reveal_path(&dir);
    #[cfg(windows)]
    cx.spawn(async move |_| {
        if let Err(error) = crate::shell_open::directory(dir.to_string_lossy().into_owned()).await {
            log::warn!("could not open desktop log directory: {error:?}");
        }
    })
    .detach();
}
