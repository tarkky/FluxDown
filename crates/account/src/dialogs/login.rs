//! 登录对话框：密码 / 邮箱验证码两种方式；新设备需要设备验证时切换到验证步骤
//! （替换设备提示、有效期倒计时、重新发送、返回）。

use std::sync::Arc;

use fluxdown_protocol::{AgentLoginResult, RpcErrorData, method};
use fluxdown_ui_components::{
    BusyExt as _, ControlExt as _, dialog_scroll_body, field_error, field_hint, form, form_field,
    input_with_action, segmented_tabs,
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

use crate::dialogs::{code_step, email, password_field, password_reset, register};
use crate::errors::{ErrorContext, error_text, is_registration_incomplete};
use crate::verification::{CodeChallenge, spawn_ticker};
use crate::{AccountCommand, AccountPort, PortFuture, t};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LoginMethod {
    Password,
    Code,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Step {
    Credentials,
    DeviceVerify,
}

pub(crate) struct LoginDialog {
    translator: Entity<Translator>,
    port: Arc<dyn AccountPort>,
    method: LoginMethod,
    step: Step,
    account_input: Entity<InputState>,
    password_input: Entity<InputState>,
    email_input: Entity<InputState>,
    code_input: Entity<InputState>,
    challenge: Option<CodeChallenge>,
    /// 验证码计时器代际：新一轮验证码 / 返回时作废旧计时器。
    challenge_generation: u64,
    busy: bool,
    resending: bool,
    error: Option<SharedString>,
}

/// 登录请求返回后的走向。
enum Outcome {
    LoggedIn,
    Verify(CodeChallenge),
    Invalid,
}

fn parse_outcome(value: serde_json::Value) -> Outcome {
    match serde_json::from_value::<AgentLoginResult>(value) {
        Ok(AgentLoginResult::Ok { .. }) => Outcome::LoggedIn,
        Ok(AgentLoginResult::DeviceVerificationRequired {
            ttl_seconds,
            will_replace_devices,
        }) => Outcome::Verify(CodeChallenge::new(ttl_seconds, will_replace_devices)),
        Err(_) => Outcome::Invalid,
    }
}

/// `agent.auth.sendCode` 返回 `{ttlSeconds}`。
pub(crate) fn parse_send_code(value: &serde_json::Value) -> Option<CodeChallenge> {
    value
        .get("ttlSeconds")
        .and_then(serde_json::Value::as_u64)
        .map(|ttl| CodeChallenge::new(ttl, false))
}

/// 打开登录对话框；聚焦账号输入框。
pub(crate) fn open(
    translator: Entity<Translator>,
    port: Arc<dyn AccountPort>,
    window: &mut Window,
    cx: &mut App,
) {
    open_with_account(translator, port, String::new(), window, cx);
}

/// 打开登录对话框并预填账号（重置密码成功后回到这里）；预填时聚焦密码框。
pub(crate) fn open_with_account(
    translator: Entity<Translator>,
    port: Arc<dyn AccountPort>,
    account: String,
    window: &mut Window,
    cx: &mut App,
) {
    let title = t(translator.read(cx), "accountLoginDialogTitle");
    let view = cx.new(|cx| LoginDialog::new(translator, port, window, cx));
    let account_input = view.read(cx).account_input.clone();
    let focus = if account.is_empty() {
        account_input
    } else {
        account_input.update(cx, |input, cx| {
            input.set_value(account, window, cx);
        });
        view.read(cx).password_input.clone()
    };
    window.open_dialog(cx, move |dialog, _, cx| {
        let view = view.clone();
        dialog
            .title(fluxdown_ui_components::dialog_title(title.clone(), cx))
            .w(active_theme(cx).text_extent(520.))
            // 点遮罩不关闭：登录/验证码流程误触关闭会丢掉已输入内容并迫使重新发码。
            .overlay_closable(false)
            // 回车由输入框 `PressEnter` 走 `submit`（与主按钮同一路径）；默认 Confirm 会抢先关窗。
            .on_ok(|_, _, _| false)
            .content(move |content, _, _| content.min_h_0().child(view.clone()))
    });
    focus.update(cx, |input, cx| input.focus(window, cx));
}

impl LoginDialog {
    fn new(
        translator: Entity<Translator>,
        port: Arc<dyn AccountPort>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let account_placeholder = t(translator.read(cx), "accountLoginAccountPlaceholder");
        let password_placeholder = t(translator.read(cx), "accountPasswordPlaceholder");
        let email_placeholder = t(translator.read(cx), "accountEmailPlaceholder");
        let code_placeholder = t(translator.read(cx), "accountCodePlaceholder");
        let account_input =
            cx.new(|cx| InputState::new(window, cx).placeholder(account_placeholder));
        let password_input = cx.new(|cx| {
            InputState::new(window, cx)
                .masked(true)
                .placeholder(password_placeholder)
        });
        password_field::restrict_to_ascii(&password_input, window, cx);
        let email_input = cx.new(|cx| InputState::new(window, cx).placeholder(email_placeholder));
        let code_input = cx.new(|cx| InputState::new(window, cx).placeholder(code_placeholder));
        for input in [&account_input, &password_input, &email_input, &code_input] {
            cx.subscribe_in(input, window, |this, _, event, window, cx| {
                if matches!(event, InputEvent::PressEnter { .. }) {
                    this.submit(window, cx);
                }
            })
            .detach();
        }
        Self {
            translator,
            port,
            method: LoginMethod::Password,
            step: Step::Credentials,
            account_input,
            password_input,
            email_input,
            code_input,
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

    fn fail(&mut self, error: &RpcErrorData, context: ErrorContext, cx: &App) {
        self.error = Some(error_text(self.translator.read(cx), error, context));
    }

    fn select_method(&mut self, method: LoginMethod, cx: &mut Context<Self>) {
        if self.busy || self.method == method || self.step != Step::Credentials {
            return;
        }
        self.method = method;
        self.error = None;
        cx.notify();
    }

    /// 开始新一轮验证码：重置计时并起一个新的秒级计时器。
    fn start_challenge(
        &mut self,
        challenge: CodeChallenge,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
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
        self.step = Step::Credentials;
        self.challenge = None;
        self.challenge_generation = self.challenge_generation.wrapping_add(1);
        self.error = None;
        self.code_input
            .update(cx, |input, cx| input.set_value("", window, cx));
        cx.notify();
    }

    fn credentials(&self, cx: &App) -> (String, String) {
        (
            self.account_input.read(cx).value().trim().to_owned(),
            self.password_input.read(cx).value().to_string(),
        )
    }

    fn submit(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.busy {
            return;
        }
        let code = self.code_input.read(cx).value().trim().to_owned();
        match (self.method, self.step) {
            (LoginMethod::Password, Step::Credentials) => {
                let (account, password) = self.credentials(cx);
                if account.is_empty() || password.is_empty() {
                    return;
                }
                let future = self.port.execute(AccountCommand::Auth {
                    method: method::AGENT_AUTH_LOGIN,
                    params: serde_json::json!({"account": account, "password": password}),
                });
                self.run(
                    future,
                    ErrorContext::Login,
                    Some((account, password)),
                    window,
                    cx,
                );
            }
            (LoginMethod::Password, Step::DeviceVerify) => {
                let (account, password) = self.credentials(cx);
                if code.is_empty() {
                    return;
                }
                let future = self.port.execute(AccountCommand::Auth {
                    method: method::AGENT_AUTH_LOGIN_VERIFY,
                    params: serde_json::json!({
                        "account": account, "password": password, "code": code
                    }),
                });
                self.run(future, ErrorContext::Code, None, window, cx);
            }
            (LoginMethod::Code, _) => {
                let email = self.email_input.read(cx).value().trim().to_owned();
                if email.is_empty() || code.is_empty() {
                    return;
                }
                let future = self.port.execute(AccountCommand::Auth {
                    method: method::AGENT_AUTH_VERIFY_CODE,
                    params: serde_json::json!({"email": email, "code": code}),
                });
                self.run(future, ErrorContext::Code, None, window, cx);
            }
        }
    }

    /// 发起登录类请求并按结果推进：成功关窗 / 进入设备验证 / 显示错误。
    /// `resume` 为密码登录的账号与密码：服务端报告「注册未完成」时据此转入注册验证。
    fn run(
        &mut self,
        future: PortFuture<serde_json::Value>,
        context: ErrorContext,
        resume: Option<(String, String)>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.busy = true;
        self.error = None;
        cx.notify();
        cx.spawn_in(window, async move |this, cx| {
            let result = future.await;
            let Ok(()) = this.update_in(cx, |this, window, cx| {
                this.busy = false;
                match result {
                    Ok(value) => match parse_outcome(value) {
                        Outcome::LoggedIn => window.close_dialog(cx),
                        Outcome::Verify(challenge) => {
                            this.step = Step::DeviceVerify;
                            this.start_challenge(challenge, window, cx);
                        }
                        Outcome::Invalid => {
                            this.error = Some(this.t("accountErrorUnknown", cx));
                        }
                    },
                    Err(error) => {
                        if let Some((email, password)) =
                            resume.filter(|_| is_registration_incomplete(&error))
                        {
                            // 注册没有完成验证：转到注册验证步骤（重发验证码）。
                            let translator = this.translator.clone();
                            let port = this.port.clone();
                            window.close_dialog(cx);
                            register::open_resume(translator, port, email, password, window, cx);
                        } else {
                            this.fail(&error, context, cx);
                        }
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

    /// 重新发送验证码：设备验证重新登录（云端限频窗口内会沿用原验证步骤），
    /// 邮箱验证码登录重新 `sendCode`。
    fn resend(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.busy || self.resending {
            return;
        }
        let (future, context) = match (self.method, self.step) {
            (LoginMethod::Password, Step::DeviceVerify) => {
                let (account, password) = self.credentials(cx);
                (
                    self.port.execute(AccountCommand::Auth {
                        method: method::AGENT_AUTH_LOGIN,
                        params: serde_json::json!({"account": account, "password": password}),
                    }),
                    ErrorContext::Login,
                )
            }
            (LoginMethod::Code, _) => {
                let email = self.email_input.read(cx).value().trim().to_owned();
                if email.is_empty() {
                    return;
                }
                (
                    self.port.execute(AccountCommand::Auth {
                        method: method::AGENT_AUTH_SEND_CODE,
                        params: serde_json::json!({"email": email}),
                    }),
                    ErrorContext::Login,
                )
            }
            (LoginMethod::Password, Step::Credentials) => return,
        };
        let is_send_code = self.method == LoginMethod::Code;
        self.resending = true;
        self.error = None;
        cx.notify();
        cx.spawn_in(window, async move |this, cx| {
            let result = future.await;
            let Ok(()) = this.update_in(cx, |this, window, cx| {
                this.resending = false;
                match result {
                    Ok(value) if is_send_code => match parse_send_code(&value) {
                        Some(challenge) => this.start_challenge(challenge, window, cx),
                        None => this.error = Some(this.t("accountErrorUnknown", cx)),
                    },
                    Ok(value) => match parse_outcome(value) {
                        Outcome::LoggedIn => window.close_dialog(cx),
                        Outcome::Verify(challenge) => this.start_challenge(challenge, window, cx),
                        Outcome::Invalid => {
                            this.error = Some(this.t("accountErrorUnknown", cx));
                        }
                    },
                    Err(error) => this.fail(&error, context, cx),
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
        let verifying = self.step == Step::DeviceVerify;
        let submit_label = if verifying {
            self.t("accountVerifySubmit", cx)
        } else {
            self.t("accountLogin", cx)
        };
        // 底栏：返回(ghost) 靠左；取消(outline) 在左、主操作(primary) 在右并右对齐。
        h_flex()
            .w_full()
            .pt(active_theme(cx).tokens().spacing.sm)
            .justify_between()
            .gap(active_theme(cx).tokens().spacing.sm)
            .child(div().when(verifying, |this| {
                this.child(
                    Button::new("account-login-back")
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
                        Button::new("account-login-cancel")
                            .outline()
                            .label(self.t("cancel", cx))
                            .control(cx)
                            .disabled(self.busy)
                            .on_click(|_, window, cx| window.close_dialog(cx)),
                    )
                    .child(
                        Button::new("account-login-submit")
                            .primary()
                            .label(submit_label)
                            .control(cx)
                            .busy(self.busy)
                            .disabled(self.busy)
                            .on_click(cx.listener(|this, _: &ClickEvent, window, cx| {
                                this.submit(window, cx);
                            })),
                    ),
            )
    }

    fn render_device_verify(&self, cx: &mut Context<Self>) -> gpui::AnyElement {
        let tokens = active_theme(cx).tokens().clone();
        let translator = self.translator.read(cx).clone();
        let account = self.account_input.read(cx).value().trim().to_owned();
        let subtitle = if account.contains('@') {
            crate::t_with(
                &translator,
                "accountDeviceVerifySubtitle",
                &[("email", &account)],
            )
        } else {
            t(&translator, "accountDeviceVerifySubtitleGeneric")
        };
        let challenge = self.challenge;
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
                            .child(t(&translator, "accountDeviceVerifyTitle")),
                    )
                    .child(field_hint(subtitle, cx)),
            )
            .child(form_field(
                t(&translator, "accountFieldCode"),
                Input::new(&self.code_input).control(cx).w_full(),
                None,
                cx,
            ))
            .when_some(challenge, |form, challenge| {
                form.child(code_step::render::<Self>(
                    "account-login-resend",
                    &translator,
                    &challenge,
                    self.resending,
                    |this, window, cx| this.resend(window, cx),
                    cx,
                ))
            })
            .into_any_element()
    }

    fn render_code_login(&self, cx: &mut Context<Self>) -> gpui::AnyElement {
        let translator = self.translator.read(cx).clone();
        let email_empty = self.email_input.read(cx).value().trim().is_empty();
        let challenge = self.challenge;
        // 发送按钮：冷却中显示剩余秒数；未发送 / 可重发时可点。
        let (send_label, send_enabled) = match &challenge {
            Some(challenge) => (
                code_step::resend_label(&translator, challenge),
                challenge.can_resend(),
            ),
            None => (t(&translator, "accountSendCode"), true),
        };
        form(cx)
            .child(form_field(
                t(&translator, "accountEmailPlaceholder"),
                Input::new(&self.email_input).control(cx).w_full(),
                None,
                cx,
            ))
            .child(form_field(
                t(&translator, "accountFieldCode"),
                input_with_action(
                    Input::new(&self.code_input).control(cx).w_full(),
                    Button::new("account-login-send-code")
                        .outline()
                        .label(send_label)
                        .control(cx)
                        .busy(self.resending)
                        .disabled(!send_enabled || self.resending || self.busy || email_empty)
                        .on_click(cx.listener(|this, _: &ClickEvent, window, cx| {
                            this.resend(window, cx);
                        })),
                    cx,
                ),
                challenge
                    .as_ref()
                    .map(|challenge| code_step::countdown_text(&translator, challenge)),
                cx,
            ))
            .into_any_element()
    }

    fn render_password_login(&self, cx: &mut Context<Self>) -> gpui::AnyElement {
        form(cx)
            .child(form_field(
                self.t("accountFieldAccount", cx),
                Input::new(&self.account_input).control(cx).w_full(),
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
                None,
                cx,
            ))
            .child(
                h_flex().w_full().justify_end().child(
                    Button::new("account-login-forgot-password")
                        .ghost()
                        .label(self.t("accountForgotPassword", cx))
                        .control(cx)
                        .disabled(self.busy)
                        .on_click(cx.listener(|this, _: &ClickEvent, window, cx| {
                            this.forgot_password(window, cx);
                        })),
                ),
            )
            .into_any_element()
    }

    /// 「忘记密码？」：关闭登录框并打开重置密码对话框，账号框里像邮箱的内容预填为邮箱。
    fn forgot_password(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.busy {
            return;
        }
        let account = self.account_input.read(cx).value().trim().to_owned();
        let email = if email::is_valid_email(&account) {
            account
        } else {
            String::new()
        };
        let translator = self.translator.clone();
        let port = self.port.clone();
        window.close_dialog(cx);
        password_reset::open(translator, port, email, window, cx);
    }
}

impl Render for LoginDialog {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let tokens = active_theme(cx).tokens().clone();
        let body = match (self.step, self.method) {
            (Step::DeviceVerify, _) => self.render_device_verify(cx),
            (Step::Credentials, LoginMethod::Password) => self.render_password_login(cx),
            (Step::Credentials, LoginMethod::Code) => self.render_code_login(cx),
        };
        let tabs = (self.step == Step::Credentials).then(|| {
            let view = cx.entity();
            segmented_tabs(
                "account-login-tabs",
                [
                    self.t("accountLoginTabPassword", cx),
                    self.t("accountLoginTabCode", cx),
                ],
                usize::from(self.method == LoginMethod::Code),
                move |index, _, cx| {
                    let method = if index == 0 {
                        LoginMethod::Password
                    } else {
                        LoginMethod::Code
                    };
                    view.update(cx, |this, cx| this.select_method(method, cx));
                },
                cx,
            )
        });
        v_flex()
            .w_full()
            .min_h_0()
            .gap(tokens.spacing.lg)
            .when_some(tabs, |column, tabs| column.child(tabs))
            .child(dialog_scroll_body("account-login-body", None, body, cx))
            .when_some(self.error.clone(), |column, error| {
                column.child(field_error(error, cx))
            })
            .child(self.render_footer(cx))
    }
}
