//! 分类新增 / 编辑对话框：字段、校验与保存。

use fluxdown_ui_components::{
    ControlExt as _, DialogIntent, category_icon, dialog_scroll_body, dialog_title, field_error,
    form, form_field, input_with_action, segmented_tabs,
};
use fluxdown_ui_i18n::Translator;
use fluxdown_ui_theme::active_theme;
use gpui::{
    App, AppContext as _, ClickEvent, Context, Div, Entity, InteractiveElement as _, IntoElement,
    ParentElement, Render, SharedString, StatefulInteractiveElement as _, Styled, Window, div,
    prelude::FluentBuilder as _, px,
};
use gpui_component::{
    Disableable as _, Icon, WindowExt as _,
    button::{Button, ButtonVariants as _},
    h_flex,
    input::{Input, InputState},
};

use super::categories::{CategoryEntry, read_categories, write_categories};
use crate::store::SettingsStore;
use crate::ui::{danger_ghost_button, dialog_footer};

/// 图标选择器可选的图标名（持久化的 wire 值，顺序即网格顺序）。
///
/// 渲染统一经 `fluxdown_ui_components::category_icon` 映射，与下载侧栏分类子项
/// 显示一致（存储值保留原始图标名）。
pub(crate) const CATEGORY_ICONS: &[&str] = &[
    "folders",
    "film",
    "music",
    "fileText",
    "image",
    "archive",
    "file",
    "code",
    "database",
    "gamepad",
    "globe",
    "bookmark",
    "box",
    "cpu",
    "disc",
    "font",
    "hardDrive",
    "library",
    "package2",
    "pen",
    "printer",
    "smartphone",
    "subtitles",
    "type",
    "zap",
];

/// 扩展名文本 → 规范化列表：逗号 / 中文逗号 / 空白分隔，去点、转小写、去空。
pub(crate) fn parse_extensions(text: &str) -> Vec<String> {
    text.split(|ch: char| ch == ',' || ch == '，' || ch.is_whitespace())
        .map(|part| part.trim().replace('.', "").to_ascii_lowercase())
        .filter(|part| !part.is_empty())
        .collect()
}

/// 无 `regex` 依赖的最小正则健全性检查：圆括号 / 方括号配对与尾部悬挂转义。
/// 能拦住最常见的手误；语法级校验由引擎在匹配时兜底。
pub(crate) fn regex_looks_valid(pattern: &str) -> bool {
    let mut depth = 0i32;
    let mut in_class = false;
    let mut chars = pattern.chars();
    while let Some(ch) = chars.next() {
        match ch {
            '\\' => {
                if chars.next().is_none() {
                    return false;
                }
            }
            '[' if !in_class => in_class = true,
            ']' if in_class => in_class = false,
            '(' if !in_class => depth += 1,
            ')' if !in_class => {
                depth -= 1;
                if depth < 0 {
                    return false;
                }
            }
            _ => {}
        }
    }
    depth == 0 && !in_class
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum MatchMode {
    Extension,
    Regex,
}

impl MatchMode {
    fn wire(self) -> &'static str {
        match self {
            Self::Extension => "extension",
            Self::Regex => "regex",
        }
    }
}

pub(crate) struct CategoryDialog {
    store: Entity<SettingsStore>,
    translator: Translator,
    existing: Option<CategoryEntry>,
    name: Entity<InputState>,
    extensions: Entity<InputState>,
    regex: Entity<InputState>,
    save_dir: Entity<InputState>,
    icon: String,
    match_mode: MatchMode,
    error: Option<SharedString>,
    picking_dir: bool,
}

/// 打开新增（`existing = None`）或编辑对话框。
pub(crate) fn open(
    store: Entity<SettingsStore>,
    translator: Translator,
    existing: Option<CategoryEntry>,
    window: &mut Window,
    cx: &mut App,
) {
    let title = SharedString::from(
        translator
            .text(if existing.is_some() {
                "editCategory"
            } else {
                "addCategory"
            })
            .to_owned(),
    );
    let view = cx.new(|cx| CategoryDialog::new(store, translator, existing, window, cx));
    let name = view.read(cx).name.clone();
    window.open_dialog(cx, move |dialog, _, cx| {
        let view = view.clone();
        dialog
            .title(dialog_title(title.clone(), cx))
            .w(active_theme(cx).text_extent(520.))
            .content(move |content, _, _| content.min_h_0().child(view.clone()))
    });
    name.update(cx, |input, cx| input.focus(window, cx));
}

impl CategoryDialog {
    fn new(
        store: Entity<SettingsStore>,
        translator: Translator,
        existing: Option<CategoryEntry>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let entry = existing.as_ref();
        let name_hint = SharedString::from(translator.text("categoryNameHint").to_owned());
        let ext_hint = SharedString::from(translator.text("extensionsHint").to_owned());
        let regex_hint = SharedString::from(translator.text("regexHint").to_owned());
        let dir_hint = SharedString::from(translator.text("selectSaveDir").to_owned());
        let name = cx.new(|cx| {
            InputState::new(window, cx)
                .default_value(entry.map(|entry| entry.name.clone()).unwrap_or_default())
                .placeholder(name_hint)
        });
        let extensions = cx.new(|cx| {
            InputState::new(window, cx)
                .default_value(
                    entry
                        .map(|entry| entry.extensions.join(", "))
                        .unwrap_or_default(),
                )
                .placeholder(ext_hint)
        });
        let regex = cx.new(|cx| {
            InputState::new(window, cx)
                .default_value(
                    entry
                        .map(|entry| entry.regex_pattern.clone())
                        .unwrap_or_default(),
                )
                .placeholder(regex_hint)
        });
        let save_dir = cx.new(|cx| {
            InputState::new(window, cx)
                .default_value(
                    entry
                        .map(|entry| entry.save_dir.clone())
                        .unwrap_or_default(),
                )
                .placeholder(dir_hint)
        });
        // 目录输入变化要刷新「恢复默认」按钮的可见性。
        cx.observe(&save_dir, |_, _, cx| cx.notify()).detach();
        let match_mode = match entry.map(|entry| entry.match_mode.as_str()) {
            Some("regex") => MatchMode::Regex,
            _ => MatchMode::Extension,
        };
        Self {
            store,
            translator,
            icon: entry.map_or_else(|| "file".to_owned(), |entry| entry.icon.clone()),
            existing,
            name,
            extensions,
            regex,
            save_dir,
            match_mode,
            error: None,
            picking_dir: false,
        }
    }

    fn t(&self, key: &str) -> SharedString {
        SharedString::from(self.translator.text(key).to_owned())
    }

    fn is_builtin(&self) -> bool {
        self.existing.as_ref().is_some_and(|entry| entry.is_builtin)
    }

    fn builtin_type(&self) -> Option<&str> {
        self.existing
            .as_ref()
            .filter(|entry| entry.is_builtin)
            .and_then(|entry| entry.builtin_type.as_deref())
    }

    /// `all` 完全锁定；`other` 用排除逻辑匹配——两者都不展示匹配规则区。
    fn is_special_builtin(&self) -> bool {
        matches!(self.builtin_type(), Some("all" | "other"))
    }

    fn can_delete(&self) -> bool {
        self.existing
            .as_ref()
            .is_some_and(|entry| !entry.is_builtin)
    }

    fn fail(&mut self, key: &str, cx: &mut Context<Self>) {
        self.error = Some(self.t(key));
        cx.notify();
    }

    fn save(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let name = self.name.read(cx).value().trim().to_owned();
        if name.is_empty() && !self.is_builtin() {
            self.fail("categoryNameRequired", cx);
            return;
        }
        let mut extensions = self
            .existing
            .as_ref()
            .map(|entry| entry.extensions.clone())
            .unwrap_or_default();
        let mut regex_pattern = self
            .existing
            .as_ref()
            .map(|entry| entry.regex_pattern.clone())
            .unwrap_or_default();
        if !self.is_special_builtin() {
            match self.match_mode {
                MatchMode::Extension => {
                    extensions = parse_extensions(&self.extensions.read(cx).value());
                    if extensions.is_empty() && !self.is_builtin() {
                        self.fail("extensionsRequired", cx);
                        return;
                    }
                    regex_pattern = String::new();
                }
                MatchMode::Regex => {
                    regex_pattern = self.regex.read(cx).value().trim().to_owned();
                    if !regex_pattern.is_empty() && !regex_looks_valid(&regex_pattern) {
                        self.fail("regexInvalid", cx);
                        return;
                    }
                    extensions = Vec::new();
                }
            }
        }
        let save_dir = self.save_dir.read(cx).value().trim().to_owned();
        let entry = match &self.existing {
            Some(existing) => CategoryEntry {
                name,
                icon: self.icon.clone(),
                match_mode: self.match_mode.wire().to_owned(),
                extensions,
                regex_pattern,
                save_dir,
                ..existing.clone()
            },
            None => CategoryEntry {
                id: format!("custom_{}", unix_ms()),
                name,
                icon: self.icon.clone(),
                match_mode: self.match_mode.wire().to_owned(),
                extensions,
                regex_pattern,
                position: 999,
                visible: true,
                is_builtin: false,
                builtin_type: None,
                save_dir,
            },
        };
        self.store.update(cx, |store, cx| {
            let mut list = read_categories(store);
            match list.iter_mut().find(|item| item.id == entry.id) {
                Some(slot) => *slot = entry,
                None => list.push(entry),
            }
            write_categories(store, list, cx);
        });
        window.close_dialog(cx);
    }

    fn confirm_delete(&self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(id) = self.existing.as_ref().map(|entry| entry.id.clone()) else {
            return;
        };
        let store = self.store.clone();
        let title = self.t("deleteCategory");
        let description = self.t("deleteCategoryConfirm");
        let cancel = self.t("cancel");
        window.open_alert_dialog(cx, move |alert, _, cx| {
            let store = store.clone();
            let id = id.clone();
            alert
                .title(dialog_title(title.clone(), cx))
                .description(description.clone())
                .footer(fluxdown_ui_components::dialog_footer(
                    Some(cancel.clone()),
                    title.clone(),
                    DialogIntent::Destructive,
                    cx,
                ))
                .on_ok(move |_, window, cx| {
                    let id = id.clone();
                    store.update(cx, |store, cx| {
                        let list = read_categories(store)
                            .into_iter()
                            .filter(|entry| entry.id != id)
                            .collect();
                        write_categories(store, list, cx);
                    });
                    // 确认框与编辑框一起关掉；返回 false 避免基座再弹出一层。
                    window.close_all_dialogs(cx);
                    false
                })
        });
    }

    fn pick_dir(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.picking_dir {
            return;
        }
        self.picking_dir = true;
        cx.notify();
        let receiver = cx.prompt_for_paths(gpui::PathPromptOptions {
            files: false,
            directories: true,
            multiple: false,
            prompt: None,
        });
        cx.spawn_in(window, async move |this, cx| {
            let picked = match receiver.await {
                Ok(Ok(Some(paths))) => paths.first().map(|path| path.display().to_string()),
                _ => None,
            };

            let Ok(()) = this.update_in(cx, |this, window, cx| {
                this.picking_dir = false;
                if let Some(path) = picked {
                    this.save_dir
                        .update(cx, |input, cx| input.set_value(path, window, cx));
                }
                cx.notify();
            }) else {
                // 编辑器或窗口已释放，停止回写异步结果。
                return;
            };
        })
        .detach();
    }

    fn render_icon_grid(&self, cx: &mut Context<Self>) -> Div {
        let theme = active_theme(cx);
        let tokens = theme.tokens().clone();
        let extended = theme.extended().clone();
        let colors = tokens.colors;
        let mut grid = h_flex()
            .flex_wrap()
            .gap(tokens.spacing.xs + tokens.spacing.xxs);
        for name in CATEGORY_ICONS {
            let selected = self.icon == *name;
            let name = *name;
            // 本格是自建 div，悬停只在这里设置一次（未选中时）。
            grid = grid.child(
                div()
                    .id(SharedString::from(format!("category-icon-{name}")))
                    .size(theme.density().control)
                    .flex()
                    .items_center()
                    .justify_center()
                    .rounded(tokens.radius.md)
                    .border_1()
                    .border_color(if selected {
                        colors.primary
                    } else {
                        colors.border
                    })
                    .when(selected, |this| this.bg(colors.accent))
                    .text_color(if selected {
                        colors.primary
                    } else {
                        colors.muted_foreground
                    })
                    .cursor_pointer()
                    .when(!selected, |this| {
                        this.hover(move |style| style.bg(extended.colors.row_hover))
                    })
                    .child(Icon::new(category_icon(name)).size(extended.icon.lg))
                    .on_click(cx.listener(move |this, _: &ClickEvent, _, cx| {
                        this.icon = name.to_owned();
                        cx.notify();
                    })),
            );
        }
        grid
    }

    fn render_match_mode(&self, cx: &mut Context<Self>) -> Div {
        let view = cx.entity();
        form_field(
            self.t("matchMode"),
            h_flex().child(segmented_tabs(
                "category-match-mode",
                [self.t("matchByExtension"), self.t("matchByRegex")],
                match self.match_mode {
                    MatchMode::Extension => 0,
                    MatchMode::Regex => 1,
                },
                move |index, _, cx| {
                    view.update(cx, |this, cx| {
                        this.match_mode = if index == 0 {
                            MatchMode::Extension
                        } else {
                            MatchMode::Regex
                        };
                        this.error = None;
                        cx.notify();
                    });
                },
                cx,
            )),
            None,
            cx,
        )
    }

    fn render_match_input(&self, cx: &mut Context<Self>) -> Div {
        let (label, input) = match self.match_mode {
            MatchMode::Extension => ("extensionsLabel", &self.extensions),
            MatchMode::Regex => ("regexLabel", &self.regex),
        };
        form_field(
            self.t(label),
            Input::new(input).control(cx).w_full(),
            None,
            cx,
        )
    }

    fn render_save_dir(&self, cx: &mut Context<Self>) -> Div {
        let tokens = active_theme(cx).tokens().clone();
        let has_value = !self.save_dir.read(cx).value().trim().is_empty();
        let actions = h_flex()
            .gap(tokens.spacing.sm)
            .child(
                Button::new("category-pick-dir")
                    .outline()
                    .label(self.t("browse"))
                    .control(cx)
                    .disabled(self.picking_dir)
                    .on_click(cx.listener(|this, _: &ClickEvent, window, cx| {
                        this.pick_dir(window, cx);
                    })),
            )
            .when(has_value, |this| {
                this.child(
                    Button::new("category-clear-dir")
                        .ghost()
                        .label(self.t("restoreDefaultPath"))
                        .control(cx)
                        .on_click(cx.listener(|this, _: &ClickEvent, window, cx| {
                            this.save_dir
                                .update(cx, |input, cx| input.set_value("", window, cx));
                            cx.notify();
                        })),
                )
            });
        form_field(
            self.t("categorySaveDir"),
            input_with_action(Input::new(&self.save_dir).control(cx).w_full(), actions, cx),
            Some(self.t("categorySaveDirDesc")),
            cx,
        )
    }

    fn render_footer(&self, cx: &mut Context<Self>) -> Div {
        let delete = self.can_delete().then(|| {
            danger_ghost_button("category-dialog-delete", self.t("deleteCategory"), cx)
                .on_click(cx.listener(|this, _: &ClickEvent, window, cx| {
                    this.confirm_delete(window, cx);
                }))
                .into_any_element()
        });
        dialog_footer(
            delete,
            [
                Button::new("category-dialog-cancel")
                    .outline()
                    .label(self.t("cancel"))
                    .control(cx)
                    .on_click(|_, window, cx| window.close_dialog(cx))
                    .into_any_element(),
                Button::new("category-dialog-save")
                    .primary()
                    .label(self.t("confirm"))
                    .control(cx)
                    .on_click(cx.listener(|this, _: &ClickEvent, window, cx| this.save(window, cx)))
                    .into_any_element(),
            ],
            cx,
        )
    }
}

impl Render for CategoryDialog {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let tokens = active_theme(cx).tokens().clone();
        let show_rules = !self.is_special_builtin();
        let show_dir = self.builtin_type() != Some("all");
        let mut body = form(cx)
            .child(form_field(
                self.t("categoryName"),
                Input::new(&self.name).control(cx).w_full(),
                None,
                cx,
            ))
            .child(form_field(
                self.t("categoryIcon"),
                self.render_icon_grid(cx),
                None,
                cx,
            ));
        if show_rules {
            body = body
                .child(self.render_match_mode(cx))
                .child(self.render_match_input(cx));
        }
        if show_dir {
            body = body.child(self.render_save_dir(cx));
        }
        div()
            .flex()
            .flex_col()
            .w_full()
            .min_h_0()
            .gap(tokens.spacing.lg)
            .child(dialog_scroll_body(
                "category-dialog-body",
                Some(px(480.)),
                body,
                cx,
            ))
            .when_some(self.error.clone(), |this, error| {
                this.child(field_error(error, cx))
            })
            .child(self.render_footer(cx).pt(tokens.spacing.sm))
    }
}

/// 当前 Unix 毫秒；时钟异常时退化为 0（仅用于生成 id）。
pub(crate) fn unix_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_extensions_like_dart() {
        assert_eq!(
            parse_extensions(" .EPUB, mobi，azw3  txt "),
            vec!["epub", "mobi", "azw3", "txt"]
        );
        assert!(parse_extensions(" , ").is_empty());
    }

    #[test]
    fn regex_sanity_check() {
        assert!(regex_looks_valid(r".*\.(epub|mobi)$"));
        assert!(regex_looks_valid(r"[a-z)]+\{2}"));
        assert!(!regex_looks_valid(r"(abc"));
        assert!(!regex_looks_valid(r"abc)"));
        assert!(!regex_looks_valid(r"[abc"));
        assert!(!regex_looks_valid(r"abc\"));
    }
}
