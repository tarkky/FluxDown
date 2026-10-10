//! 注册对话框：邮箱 + 密码 + 可选昵称；完成后转入邮箱验证码步骤（有效期倒计时、
//! 重新发送、返回）。
//! 登录时服务端报告「注册未完成」会经 [`open_resume`] 直接进入验证步骤。

use std::sync::Arc;

use fluxdown_ui_components::{
    BusyExt as _, ControlExt as _, dialog_scroll_body, field_error, field_hint, form, form_field,
};
use fluxdown_ui_i18n::Translator;
use fluxdown_ui_theme::active_theme;
use gpui::{
    App, AppContext as _, ClickEvent, Context, Entity, FontWeight, IntoElement, ParentElement,
    Render, SharedString, Styled, Window, div, prelude::FluentBuilder as _,
};
use gpui_component::{
    Disableable as _, WindowExt as _,
    button::{Button, ButtonVariants as _},
    h_flex,
    input::{Input, InputContentType, InputEvent, InputState},
    v_flex,
};

use crate::dialogs::{code_step, password_field};
use crate::errors::{ErrorContext, error_text};
use crate::verification::{CodeChallenge, spawn_ticker};
use crate::{AccountCommand, AccountPort, t, t_with};

pub(crate) struct RegisterDialog {
    translator: Entity<Translator>,
    port: Arc<dyn AccountPort>,
    email_input: Entity<InputState>,
    password_input: Entity<InputState>,
    nickname_input: Entity<InputState>,
    code_input: Entity<InputState>,
    verification_required: bool,
    challenge: Option<CodeChallenge>,
    /// 验证码计时器代际：新一轮验证码 / 返回时作废旧计时器。
    challenge_generation: u64,
    busy: bool,
    resending: bool,
    error: Option<SharedString>,
}

/// 打开注册对话框；聚焦邮箱输入框。
pub(crate) fn open(
    translator: Entity<Translator>,
    port: Arc<dyn AccountPort>,
    window: &mut Window,
    cx: &mut App,
) {
    let title = t(translator.read(cx), "accountRegisterDialogTitle");
    let view = cx.new(|cx| RegisterDialog::new(translator, port, window, cx));
    let email_input = view.read(cx).email_input.clone();
    window.open_dialog(cx, move |dialog, _, cx| {
        let view = view.clone();
        dialog
            .title(fluxdown_ui_components::dialog_title(title.clone(), cx))
            .w(active_theme(cx).text_extent(520.))
            // 点遮罩不关闭：误触关闭会丢掉已输入内容并迫使重新发码。
            .overlay_closable(false)
            // 回车由输入框 `PressEnter` 提交；默认 Confirm 会抢先关窗。
            .on_ok(|_, _, _| false)
            .content(move |content, _, _| content.min_h_0().child(view.clone()))
    });
    email_input.update(cx, |input, cx| input.focus(window, cx));
}

/// 登录发现注册未完成：打开注册对话框并立即用同一组邮箱 / 密码重新注册（云端重发验证码），
/// 进入验证步骤。
pub(crate) fn open_resume(
    translator: Entity<Translator>,
    port: Arc<dyn AccountPort>,
    email: String,
    password: String,
    window: &mut Window,
    cx: &mut App,
) {
    let title = t(translator.read(cx), "accountRegisterDialogTitle");
    let view = cx.new(|cx| RegisterDialog::new(translator, port, window, cx));
    view.update(cx, |this, cx| {
        this.email_input
            .update(cx, |input, cx| input.set_value(email, window, cx));
        this.password_input
            .update(cx, |input, cx| input.set_value(password, window, cx));
        this.submit_register(window, cx);
    });
    window.open_dialog(cx, move |dialog, _, cx| {
        let view = view.clone();
        dialog
            .title(fluxdown_ui_components::dialog_title(title.clone(), cx))
            .w(active_theme(cx).text_extent(520.))
            .overlay_closable(false)
            .on_ok(|_, _, _| false)
            .content(move |content, _, _| content.min_h_0().child(view.clone()))
    });
}

impl RegisterDialog {
    fn new(
        translator: Entity<Translator>,
        port: Arc<dyn AccountPort>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let email_placeholder = t(translator.read(cx), "accountEmailPlaceholder");
        let password_placeholder = t(translator.read(cx), "accountPasswordHint");
        let nickname_placeholder = t(translator.read(cx), "accountNicknamePlaceholder");
        let code_placeholder = t(translator.read(cx), "accountCodePlaceholder");
        let email_input = cx.new(|cx| InputState::new(window, cx).placeholder(email_placeholder));
        let password_input = cx.new(|cx| {
            InputState::new(window, cx)
                .masked(true)
                .placeholder(password_placeholder)
        });
        password_field::restrict_to_ascii(&password_input, window, cx);
        let nickname_input =
            cx.new(|cx| InputState::new(window, cx).placeholder(nickname_placeholder));
        let code_input = cx.new(|cx| InputState::new(window, cx).placeholder(code_placeholder));
        for input in [&email_input, &password_input, &nickname_input, &code_input] {
            cx.subscribe_in(input, window, |this, _, event, window, cx| {
                if matches!(event, InputEvent::PressEnter { .. }) {
                    if this.verification_required {
                        this.submit_verify(window, cx);
                    } else {
                        this.submit_register(window, cx);
                    }
                }
            })
            .detach();
        }
        Self {
            translator,
            port,
            email_input,
            password_input,
            nickname_input,
            code_input,
            verification_required: false,
            challenge: None,
            challenge_generation: 0,
            busy: false,
            resending: false,
            error: None,
        }
    }

    fn t(&self, key: &str, cx: &App) -> SharedString {
        t(self.translator.read(cx), key)
    }

    fn email(&self, cx: &App) -> String {
        self.email_input.read(cx).value().trim().to_owned()
    }

    fn register_params(&self, cx: &App) -> Option<serde_json::Value> {
        let email = self.email(cx);
        let password = self.password_input.read(cx).value().to_string();
        let nickname = self.nickname_input.read(cx).value().trim().to_owned();
        if email.is_empty() || password.is_empty() {
            return None;
        }
        let mut params = serde_json::json!({"email": email, "password": password});
        if !nickname.is_empty() {
            params["nickname"] = serde_json::Value::String(nickname);
        }
        Some(params)
    }

    fn submit_register(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.busy {
            return;
        }
        let Some(params) = self.register_params(cx) else {
            return;
        };
        self.run(
            fluxdown_protocol::method::AGENT_AUTH_REGISTER,
            params,
            ErrorContext::Register,
            false,
            window,
            cx,
        );
    }

    fn submit_verify(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.busy {
            return;
        }
        let email = self.email(cx);
        let code = self.code_input.read(cx).value().trim().to_owned();
        if code.is_empty() {
            return;
        }
        let params = serde_json::json!({"email": email, "code": code});
        self.run(
            fluxdown_protocol::method::AGENT_AUTH_REGISTER_VERIFY,
            params,
            ErrorContext::Code,
            false,
            window,
            cx,
        );
    }

    /// 重新发送验证码：以同一组信息再次注册（云端在限频窗口内沿用原验证步骤）。
    fn resend(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.busy || self.resending {
            return;
        }
        let Some(params) = self.register_params(cx) else {
            return;
        };
        self.run(
            fluxdown_protocol::method::AGENT_AUTH_REGISTER,
            params,
            ErrorContext::Register,
            true,
            window,
            cx,
        );
    }

    fn start_challenge(
        &mut self,
        challenge: CodeChallenge,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.verification_required = true;
        self.challenge = Some(challenge);
        self.challenge_generation = self.challenge_generation.wrapping_add(1);
        let generation = self.challenge_generation;
        spawn_ticker(window, cx, move |this: &mut Self, _, cx| {
            if this.challenge_generation != generation {
                return false;
            }
            let Some(challenge) = this.challenge.as_mut() else {
                return false;
            };
            if challenge.tick() {
                cx.notify();
            }
            !challenge.is_idle()
        });
        self.code_input
            .update(cx, |input, cx| input.focus(window, cx));
    }

    fn back(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.busy {
            return;
        }
        self.verification_required = false;
        self.challenge = None;
        self.challenge_generation = self.challenge_generation.wrapping_add(1);
        self.error = None;
        self.code_input
            .update(cx, |input, cx| input.set_value("", window, cx));
        cx.notify();
    }

    fn run(
        &mut self,
        method: &'static str,
        params: serde_json::Value,
        context: ErrorContext,
        resend: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if resend {
            self.resending = true;
        } else {
            self.busy = true;
        }
        self.error = None;
        cx.notify();
        let future = self.port.execute(AccountCommand::Auth { method, params });
        cx.spawn_in(window, async move |this, cx| {
            let result = future.await;
            let Ok(()) = this.update_in(cx, |this, window, cx| {
                this.busy = false;
                this.resending = false;
                match result {
                    Ok(value) => {
                        match serde_json::from_value::<fluxdown_protocol::AgentLoginResult>(value) {
                            Ok(fluxdown_protocol::AgentLoginResult::Ok { .. }) => {
                                window.close_dialog(cx);
                            }
                            Ok(
                                fluxdown_protocol::AgentLoginResult::DeviceVerificationRequired {
                                    ttl_seconds,
                                    will_replace_devices,
                                },
                            ) => {
                                this.start_challenge(
                                    CodeChallenge::new(ttl_seconds, will_replace_devices),
                                    window,
                                    cx,
                                );
                            }
                            Err(_) => {
                                this.error = Some(this.t("accountErrorUnknown", cx));
                            }
                        }
                    }
                    Err(error) => {
                        this.error = Some(error_text(this.translator.read(cx), &error, context));
                    }
                }
                cx.notify();
            }) else {
                // 对话框或窗口已释放，停止回写异步结果。
                return;
            };
        })
        .detach();
    }

    fn render_footer(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let submit_label = if self.verification_required {
            self.t("accountVerifySubmit", cx)
        } else {
            self.t("accountRegister", cx)
        };
        // 底栏：返回(ghost) 靠左；取消(outline) 在左、主操作(primary) 在右并右对齐。
        let verifying = self.verification_required;
        h_flex()
            .w_full()
            .pt(active_theme(cx).tokens().spacing.sm)
            .justify_between()
            .gap(active_theme(cx).tokens().spacing.sm)
            .child(div().when(verifying, |this| {
                this.child(
                    Button::new("account-register-back")
                        .ghost()
                        .label(self.t("back", cx))
                        .control(cx)
                        .disabled(self.busy)
                        .on_click(cx.listener(|this, _: &ClickEvent, window, cx| {
                            this.back(window, cx);
                        })),
                )
            }))
            .child(
                h_flex()
                    .gap(active_theme(cx).tokens().spacing.sm)
                    .child(
                        Button::new("account-register-cancel")
                            .outline()
                            .label(self.t("cancel", cx))
                            .control(cx)
                            .disabled(self.busy)
                            .on_click(|_, window, cx| window.close_dialog(cx)),
                    )
                    .child(
                        Button::new("account-register-submit")
                            .primary()
                            .label(submit_label)
                            .control(cx)
                            .busy(self.busy)
                            .disabled(self.busy)
                            .on_click(cx.listener(|this, _: &ClickEvent, window, cx| {
                                if this.verification_required {
                                    this.submit_verify(window, cx);
                                } else {
                                    this.submit_register(window, cx);
                                }
                            })),
                    ),
            )
    }
}

impl Render for RegisterDialog {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let tokens = active_theme(cx).tokens().clone();
        let translator = self.translator.read(cx).clone();
        let body = if self.verification_required {
            let subtitle = t_with(
                &translator,
                "accountRegisterVerifySubtitle",
                &[("email", &self.email(cx))],
            );
            form(cx)
                .child(
                    v_flex()
                        .gap(tokens.spacing.xxs)
                        .child(
                            div()
                                .text_size(tokens.typography.sm.size)
                                .line_height(tokens.typography.sm.line_height)
                                .font_weight(FontWeight::MEDIUM)
                                .text_color(tokens.colors.foreground)
                                .child(self.t("accountRegisterVerifyTitle", cx)),
                        )
                        .child(field_hint(subtitle, cx)),
                )
                .child(form_field(
                    self.t("accountFieldCode", cx),
                    Input::new(&self.code_input).control(cx).w_full(),
                    None,
                    cx,
                ))
                .when_some(self.challenge, |form, challenge| {
                    form.child(code_step::render::<Self>(
                        "account-register-resend",
                        &translator,
                        &challenge,
                        self.resending,
                        |this, window, cx| this.resend(window, cx),
                        cx,
                    ))
                })
        } else {
            form(cx)
                .child(form_field(
                    self.t("accountEmailPlaceholder", cx),
                    Input::new(&self.email_input).control(cx).w_full(),
                    None,
                    cx,
                ))
                .child(form_field(
                    self.t("accountPasswordPlaceholder", cx),
                    Input::new(&self.password_input)
                        .control(cx)
                        .w_full()
                        .content_type(InputContentType::Password)
                        .mask_toggle(),
                    Some(self.t("accountPasswordHint", cx)),
                    cx,
                ))
                .child(form_field(
                    self.t("accountFieldNickname", cx),
                    Input::new(&self.nickname_input).control(cx).w_full(),
                    None,
                    cx,
                ))
        };
        v_flex()
            .w_full()
            .min_h_0()
            .gap(tokens.spacing.lg)
            .child(dialog_scroll_body("account-register-body", None, body, cx))
            .when_some(self.error.clone(), |column, error| {
                column.child(field_error(error, cx))
            })
            .child(self.render_footer(cx))
    }
}
