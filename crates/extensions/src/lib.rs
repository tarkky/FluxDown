//! 插件、插件市场与受管组件（ffmpeg / yt-dlp）capability。

mod components;
mod controller;
mod market;
mod pages;
mod ui;

use std::{future::Future, pin::Pin, sync::Arc};

use fluxdown_protocol::{
    AgentSnapshot, ApplicationErrorCode, ErrorReason, RpcErrorData, ServiceEvent,
};
use fluxdown_ui_components::segmented_tabs;
use fluxdown_ui_i18n::Translator;
use fluxdown_ui_theme::active_theme;
use gpui::{
    Context, Entity, IntoElement, ParentElement, Render, SharedString, Styled, Window, div,
    prelude::FluentBuilder as _,
};
use gpui_component::{WindowExt as _, h_flex, notification::Notification, v_flex};

use controller::{COMPONENT_KINDS, component_slot};
pub use controller::{ExtensionsController, ExtensionsSignal};
use pages::{managed_components::ComponentUi, plugins::PluginsUi};

pub type PortFuture<T> = Pin<Box<dyn Future<Output = Result<T, RpcErrorData>> + Send + 'static>>;

pub trait ExtensionsPort: Send + Sync {
    fn call(
        &self,
        method: &'static str,
        params: serde_json::Value,
    ) -> PortFuture<serde_json::Value>;
}

/// 扩展分类的子页：插件与受管组件。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExtensionsTab {
    Plugins,
    Components,
}

impl ExtensionsTab {
    const ALL: [Self; 2] = [Self::Plugins, Self::Components];

    fn index(self) -> usize {
        match self {
            Self::Plugins => 0,
            Self::Components => 1,
        }
    }
}

pub struct ExtensionsView {
    translator: Entity<Translator>,
    controller: ExtensionsController,
    tab: ExtensionsTab,
    last_error: Option<String>,
    plugins: PluginsUi,
    components: [ComponentUi; 2],
}

impl ExtensionsView {
    pub fn new(
        translator: Entity<Translator>,
        port: Arc<dyn ExtensionsPort>,
        cx: &mut Context<Self>,
    ) -> Self {
        cx.observe(&translator, |_, _, cx| cx.notify()).detach();
        Self {
            translator,
            controller: ExtensionsController::new(port),
            tab: ExtensionsTab::Plugins,
            last_error: None,
            plugins: PluginsUi::default(),
            components: [ComponentUi::default(), ComponentUi::default()],
        }
    }

    pub fn replace_snapshot(&mut self, snapshot: &AgentSnapshot, cx: &mut Context<Self>) {
        self.last_error = None;
        self.controller.replace_snapshot(snapshot);
        cx.notify();
    }

    pub fn apply_event(&mut self, event: &ServiceEvent, cx: &mut Context<Self>) {
        let relevant = ExtensionsController::affects_view(event);
        match self.controller.apply_event(event) {
            Some(ExtensionsSignal::ComponentProgress {
                kind,
                downloaded_bytes,
                total_bytes,
            }) => {
                let ui = &mut self.components[component_slot(kind)];
                ui.installing = true;
                ui.downloaded_bytes = downloaded_bytes;
                ui.total_bytes = total_bytes;
            }
            Some(ExtensionsSignal::ComponentResult { kind, ok, message }) => {
                let ui = &mut self.components[component_slot(kind)];
                ui.last_result = Some((ok, message));
                // 非本视图发起的安装（如 Web UI）：结果到达即结束进度展示。
                if !ui.install_pending {
                    ui.installing = false;
                }
            }
            // 熔断提示由 app 层全局弹出；本视图只刷新列表（控制器已更新本地状态）。
            Some(ExtensionsSignal::PluginAutoDisabled { .. }) => {}
            None => {}
        }
        let cleared_error = !self.controller.is_stale() && self.last_error.take().is_some();
        // 无关事件（如下载进度帧）不重绘，保住 retained 渲染的复用。
        if relevant || cleared_error {
            cx.notify();
        }
    }

    pub fn mark_stale(&mut self, cx: &mut Context<Self>) {
        self.last_error = Some(
            self.translator
                .read(cx)
                .text("localServiceDisconnected")
                .to_owned(),
        );
        self.controller.mark_stale();
        cx.notify();
    }

    pub(crate) fn show_tab(&mut self, tab: ExtensionsTab, cx: &mut Context<Self>) {
        if self.tab != tab {
            self.tab = tab;
            cx.notify();
        }
    }

    pub(crate) fn toast_success(
        &self,
        message: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        window.push_notification(Notification::success(message), cx);
    }

    pub(crate) fn toast_error(&self, message: String, window: &mut Window, cx: &mut Context<Self>) {
        window.push_notification(Notification::error(message).autohide(false), cx);
    }
}

/// agent 端口不透传服务端 message：有细分原因时按原因给出可操作文案，否则按码映射通用文案。
pub(crate) fn error_text(translator: &Translator, error: &RpcErrorData) -> String {
    let reason_key = error.reason.and_then(|reason| match reason {
        ErrorReason::MarketUnreachable => Some("pluginErrorMarketUnreachable"),
        ErrorReason::MarketIndexInvalid => Some("pluginErrorMarketIndexInvalid"),
        ErrorReason::MarketIndexRollback => Some("pluginErrorMarketIndexRollback"),
        ErrorReason::PluginNotInMarket => Some("pluginErrorNotInMarket"),
        ErrorReason::PluginYanked => Some("pluginErrorYanked"),
        ErrorReason::PluginDownloadFailed => Some("pluginErrorDownloadFailed"),
        ErrorReason::PluginPackageTooLarge => Some("pluginErrorPackageTooLarge"),
        ErrorReason::PluginPackageInvalid => Some("pluginErrorPackageInvalid"),
        ErrorReason::MarketVersionChanged => Some("pluginErrorMarketVersionChanged"),
        // 账户 / 远程任务 / 局域网配对等其他能力的原因不在插件页展示，退回按码的通用文案。
        _ => None,
    });
    let key = reason_key.unwrap_or(match error.code {
        ApplicationErrorCode::Unavailable | ApplicationErrorCode::Timeout => {
            "localServiceDisconnected"
        }
        ApplicationErrorCode::InvalidArgument | ApplicationErrorCode::NotFound => {
            "localServiceInvalidArgument"
        }
        ApplicationErrorCode::Conflict => "localServiceConflict",
        ApplicationErrorCode::Unsupported => "settingsUnsupportedOnPlatform",
        ApplicationErrorCode::ProtocolIncompatible
        | ApplicationErrorCode::Unauthorized
        | ApplicationErrorCode::Cancelled
        | ApplicationErrorCode::Internal => "localServiceActionFailed",
    });
    translator.text(key).to_owned()
}

impl Render for ExtensionsView {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let tab = self.tab;
        let body = match tab {
            ExtensionsTab::Plugins => self.render_plugins(window, cx).into_any_element(),
            ExtensionsTab::Components => self.render_components(window, cx).into_any_element(),
        };
        let translator = self.translator.read(cx);
        let labels = [
            SharedString::from(translator.text("settingsCatPlugins").to_owned()),
            SharedString::from(translator.text("settingsCatComponents").to_owned()),
        ];
        let tokens = active_theme(cx).tokens();
        let (spacing, typography, destructive) = (
            tokens.spacing,
            tokens.typography.xs,
            tokens.colors.destructive,
        );
        let view = cx.entity().downgrade();
        v_flex()
            .w_full()
            .gap(spacing.md)
            .when_some(self.last_error.clone(), |this, error| {
                this.child(
                    div()
                        .text_size(typography.size)
                        .line_height(typography.line_height)
                        .text_color(destructive)
                        .child(error),
                )
            })
            // 标签条左对齐、保持自然宽度（外层行吸收纵向容器的横向拉伸）。
            .child(h_flex().w_full().child(segmented_tabs(
                "extensions-tabs",
                labels,
                tab.index(),
                move |index, _, cx| {
                    if let Some(tab) = ExtensionsTab::ALL.get(index) {
                        let Ok(()) = view.update(cx, |this, cx| this.show_tab(*tab, cx)) else {
                            // 扩展视图已释放，结束回调，不再更新状态。
                            return;
                        };
                    }
                },
                cx,
            )))
            .child(body)
    }
}

/// 组件 UI 状态数组的固定顺序与 [`COMPONENT_KINDS`] 对齐。
const _: () = assert!(COMPONENT_KINDS.len() == 2);
