//! 服务器地址分组（仅调试构建）：FluxCloud 地址输入框 + 恢复默认按钮，
//! 失焦/回车提交，成功与否以通知提示。

use fluxdown_protocol::CloudEndpointDto;
use fluxdown_ui_components::{ControlExt as _, card, input_with_action};
use fluxdown_ui_i18n::Translator;
use fluxdown_ui_theme::SemanticThemeTokens;
use gpui::{ClickEvent, Context, Entity, IntoElement, ParentElement, Styled, div};
use gpui_component::{
    Disableable as _,
    button::Button,
    input::{Input, InputState},
};

use crate::view::AccountView;
use crate::{t, ui};

pub(crate) fn render(
    translator: &Translator,
    tokens: &SemanticThemeTokens,
    endpoint: &CloudEndpointDto,
    input: &Entity<InputState>,
    disabled: bool,
    cx: &mut Context<AccountView>,
) -> impl IntoElement {
    let heading = t(translator, "accountServerAddress");
    let desc = t(translator, "accountServerAddressDesc");
    let reset_label = t(translator, "accountServerAddressReset");
    let is_custom = endpoint.base_url != endpoint.default_base_url;

    div()
        .flex()
        .flex_col()
        .gap(tokens.spacing.sm)
        .child(ui::group_heading(heading, Some(desc), cx))
        .child(
            card(cx)
                .w_full()
                .px(tokens.spacing.md)
                .py(tokens.spacing.md)
                .child(input_with_action(
                    Input::new(input).control(cx).w_full().disabled(disabled),
                    Button::new("account-server-address-reset")
                        .outline()
                        .label(reset_label)
                        .control(cx)
                        .disabled(disabled || !is_custom)
                        .on_click(cx.listener(|view, _: &ClickEvent, window, cx| {
                            view.set_endpoint(String::new(), window, cx);
                        })),
                    cx,
                )),
        )
}
