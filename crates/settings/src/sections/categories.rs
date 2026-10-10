//! 自定义分类：持久化 JSON 模型与列表分区。

use fluxdown_ui_components::{
    ButtonVariant, DialogIntent, FluxIcon, button, category_icon, dialog_title,
};
use fluxdown_ui_i18n::Translator;
use fluxdown_ui_theme::active_theme;
use gpui::{
    App, AppContext as _, Context, InteractiveElement as _, IntoElement, ParentElement, Render,
    SharedString, StatefulInteractiveElement as _, Styled, Window, div,
    prelude::FluentBuilder as _,
};
use gpui_component::{Icon, WindowExt as _, h_flex, v_flex};

use super::{SectionContext, category_dialog};
use crate::store::SettingsStore;
use crate::ui::{
    SettingsRow, SettingsSection, body_text, meta_text, row_button, row_danger_button,
};

pub(crate) use fluxdown_protocol::CUSTOM_CATEGORIES_PREF_KEY as CATEGORIES_KEY;
/// 分类模型：wire 形状归 protocol，设置页只读写偏好。
pub(crate) type CategoryEntry = fluxdown_protocol::CustomCategoryDto;

/// 读取分类列表；未设置或损坏时回退内置基线。
pub(crate) fn read_categories(store: &SettingsStore) -> Vec<CategoryEntry> {
    CategoryEntry::from_preference(store.pref(CATEGORIES_KEY))
}

pub(crate) fn write_categories(
    store: &mut SettingsStore,
    mut list: Vec<CategoryEntry>,
    cx: &mut Context<SettingsStore>,
) {
    for (index, entry) in list.iter_mut().enumerate() {
        entry.position = index as i64;
    }
    let value = serde_json::to_value(&list).unwrap_or(serde_json::Value::Array(Vec::new()));
    store.set_pref(CATEGORIES_KEY, value, cx);
}

/// 内置分类的显示名文案键。
pub(crate) fn builtin_label_key(builtin_type: &str) -> &'static str {
    match builtin_type {
        "all" => "categoryAll",
        "video" => "categoryVideo",
        "audio" => "categoryAudio",
        "document" => "categoryDocument",
        "image" => "categoryImage",
        "program" => "categoryProgram",
        "archive" => "categoryArchive",
        _ => "categoryOther",
    }
}

pub(crate) fn display_name(translator: &Translator, entry: &CategoryEntry) -> SharedString {
    match entry.builtin_type.as_deref() {
        Some(kind) if entry.is_builtin => {
            SharedString::from(translator.text(builtin_label_key(kind)).to_owned())
        }
        _ => SharedString::from(entry.name.clone()),
    }
}

/// 拖拽中的分类行：载荷是分类 id，预览显示图标 + 名称。
#[derive(Clone)]
struct DraggedCategory {
    id: String,
    label: SharedString,
    icon: String,
}

impl Render for DraggedCategory {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = active_theme(cx);
        let tokens = theme.tokens();
        let extended = theme.extended();
        h_flex()
            .px(tokens.spacing.md)
            .py(tokens.spacing.xs)
            .gap(tokens.spacing.sm)
            .items_center()
            .rounded(tokens.radius.sm)
            .border_1()
            .border_color(tokens.colors.border)
            .bg(tokens.colors.surface)
            .shadow_sm()
            .text_color(tokens.colors.surface_foreground)
            .child(Icon::new(FluxIcon::GripVertical).size(extended.icon.md))
            .child(Icon::new(category_icon(&self.icon)).size(extended.icon.md))
            .child(self.label.clone())
    }
}

pub(crate) fn group(ctx: &SectionContext, _cx: &mut App) -> SettingsSection {
    SettingsSection::new()
        .title(ctx.t("customCategories"))
        .subtitle(ctx.t("categoryPriorityDragNote"))
        .row(list_item(ctx))
}

fn list_item(ctx: &SectionContext) -> SettingsRow {
    let store = ctx.store();
    let translator = ctx.translator.clone();
    let builtin = ctx.t("builtinCategory");
    let custom = ctx.t("customCategory");
    let edit = ctx.t("editCategory");
    let delete = ctx.t("delete");
    let add = ctx.t("addCategory");
    let reset = ctx.t("resetBuiltinCategories");
    let auto_dirs = ctx.t("autoCategoryDirs");
    let regex_label = ctx.t("regexLabel");
    SettingsRow::custom(
        move |disabled: bool, _key: &SharedString, _window: &mut gpui::Window, cx: &mut App| {
            let theme = active_theme(cx);
            let tokens = theme.tokens();
            let extended = theme.extended();
            let list = read_categories(store.read(cx));
            let mut column = v_flex().w_full().gap(tokens.spacing.xxs);
            for entry in &list {
                let name = display_name(&translator, entry);
                let details = if entry.match_mode == "regex" {
                    format!("{regex_label}: {}", entry.regex_pattern)
                } else if entry.extensions.is_empty() {
                    String::new()
                } else {
                    entry
                        .extensions
                        .iter()
                        .map(|ext| format!(".{ext}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                let save_dir = entry.save_dir.clone();
                let drop_store = store.clone();
                let edit_store = store.clone();
                let edit_translator = translator.clone();
                let edit_entry = entry.clone();
                let delete_store = store.clone();
                let id_drop = entry.id.clone();
                let id_delete = entry.id.clone();
                let drag = DraggedCategory {
                    id: entry.id.clone(),
                    label: name.clone(),
                    icon: entry.icon.clone(),
                };
                let accent = tokens.colors.primary;
                column = column.child(
                    h_flex()
                        .id(SharedString::from(format!("category-row-{}", entry.id)))
                        .w_full()
                        .items_center()
                        .gap(tokens.spacing.sm)
                        .px(tokens.spacing.sm)
                        .py(tokens.spacing.xs)
                        .rounded(tokens.radius.md)
                        .border_1()
                        .border_color(gpui::transparent_black())
                        .hover(|style| style.bg(extended.colors.row_hover))
                        // 整行是落点；只有左侧抓手可发起拖拽。禁用态不可拖。
                        .when(!disabled, |row| {
                            row.drag_over::<DraggedCategory>(move |style, _, _, _| {
                                style.border_color(accent)
                            })
                            .on_drop(
                                move |drag: &DraggedCategory, _, cx| {
                                    let target = id_drop.clone();
                                    drop_store.update(cx, |store, cx| {
                                        reorder_category(store, &drag.id, &target, cx);
                                    });
                                },
                            )
                        })
                        .child(
                            div()
                                .id(SharedString::from(format!("category-grip-{}", entry.id)))
                                .flex_none()
                                .p(tokens.spacing.xxs)
                                .rounded(tokens.radius.sm)
                                .text_color(extended.colors.text_tertiary)
                                .when(!disabled, |grip| {
                                    grip.cursor_grab()
                                        .hover(|style| {
                                            style
                                                .bg(tokens.colors.muted)
                                                .text_color(tokens.colors.foreground)
                                        })
                                        .on_drag(drag, |drag, _, _, cx| cx.new(|_| drag.clone()))
                                })
                                .child(Icon::new(FluxIcon::GripVertical).size(extended.icon.md)),
                        )
                        .child(
                            Icon::new(category_icon(&entry.icon))
                                .size(extended.icon.lg)
                                .text_color(tokens.colors.muted_foreground),
                        )
                        .child(
                            v_flex()
                                .flex_1()
                                .min_w_0()
                                .gap(tokens.spacing.xxs)
                                .child(
                                    h_flex()
                                        .gap(tokens.spacing.sm)
                                        .items_center()
                                        .child(body_text(cx).child(name))
                                        .child(
                                            div()
                                                .px(tokens.spacing.xs)
                                                .rounded(tokens.radius.sm)
                                                .bg(tokens.colors.muted)
                                                .text_color(tokens.colors.muted_foreground)
                                                .text_size(extended.caption.size)
                                                .line_height(extended.caption.line_height)
                                                .child(if entry.is_builtin {
                                                    builtin.clone()
                                                } else {
                                                    custom.clone()
                                                }),
                                        ),
                                )
                                .when(!details.is_empty(), |this| {
                                    this.child(
                                        meta_text(cx).truncate().child(SharedString::from(details)),
                                    )
                                })
                                .when(!save_dir.is_empty(), |this| {
                                    this.child(
                                        meta_text(cx)
                                            .truncate()
                                            .text_color(extended.colors.text_tertiary)
                                            .child(SharedString::from(save_dir)),
                                    )
                                }),
                        )
                        .child(
                            row_button(
                                SharedString::from(format!("category-edit-{}", entry.id)),
                                edit.clone(),
                                ButtonVariant::Secondary,
                                cx,
                            )
                            .disabled(disabled)
                            .on_click(move |_, window, cx| {
                                category_dialog::open(
                                    edit_store.clone(),
                                    edit_translator.clone(),
                                    Some(edit_entry.clone()),
                                    window,
                                    cx,
                                );
                            }),
                        )
                        .child(
                            row_danger_button(
                                SharedString::from(format!("category-delete-{}", entry.id)),
                                delete.clone(),
                                cx,
                            )
                            .disabled(disabled || entry.is_builtin)
                            .on_click(move |_, _, cx| {
                                let id = id_delete.clone();
                                delete_store.update(cx, |store, cx| {
                                    let list = read_categories(store)
                                        .into_iter()
                                        .filter(|entry| entry.id != id)
                                        .collect();
                                    write_categories(store, list, cx);
                                });
                            }),
                        ),
                );
            }
            let reset_store = store.clone();
            let auto_store = store.clone();
            let add_store = store.clone();
            let add_translator = translator.clone();
            column
                .child(
                    h_flex()
                        .w_full()
                        .pt(tokens.spacing.sm)
                        .justify_end()
                        .gap(tokens.spacing.sm)
                        .child(
                            button(
                                "category-auto-dirs",
                                auto_dirs.clone(),
                                ButtonVariant::Secondary,
                                cx,
                            )
                            .disabled(disabled)
                            .on_click(move |_, _, cx| {
                                auto_store.update(cx, apply_auto_dirs);
                            }),
                        )
                        .child(
                            button(
                                "category-reset",
                                reset.clone(),
                                ButtonVariant::Secondary,
                                cx,
                            )
                            .disabled(disabled)
                            .on_click({
                                let reset_translator = translator.clone();
                                move |_, window, cx| {
                                    let store = reset_store.clone();
                                    let title = SharedString::from(
                                        reset_translator.text("resetBuiltinCategories").to_owned(),
                                    );
                                    let description = SharedString::from(
                                        reset_translator
                                            .text("resetAllCategoriesConfirm")
                                            .to_owned(),
                                    );
                                    let cancel = SharedString::from(
                                        reset_translator.text("cancel").to_owned(),
                                    );
                                    window.open_alert_dialog(cx, move |alert, _, cx| {
                                        let store = store.clone();
                                        alert
                                            .title(dialog_title(title.clone(), cx))
                                            .description(description.clone())
                                            .footer(fluxdown_ui_components::dialog_footer(
                                                Some(cancel.clone()),
                                                title.clone(),
                                                DialogIntent::Destructive,
                                                cx,
                                            ))
                                            .on_ok(move |_, _, cx| {
                                                store.update(cx, |store, cx| {
                                                    write_categories(
                                                        store,
                                                        CategoryEntry::builtin_defaults(),
                                                        cx,
                                                    );
                                                });
                                                true
                                            })
                                    });
                                }
                            }),
                        )
                        .child(
                            button("category-add", add.clone(), ButtonVariant::Primary, cx)
                                .disabled(disabled)
                                .on_click(move |_, window, cx| {
                                    category_dialog::open(
                                        add_store.clone(),
                                        add_translator.clone(),
                                        None,
                                        window,
                                        cx,
                                    );
                                }),
                        ),
                )
                .into_any_element()
        },
    )
    .keywords([ctx.t("customCategories"), ctx.t("customCategory")])
}

/// 把 `from` 移到 `to` 当前所在位置（先移除再插入：向下拖落在目标之后，向上拖落在目标之前）。
pub(crate) fn reorder_category(
    store: &mut SettingsStore,
    from: &str,
    to: &str,
    cx: &mut Context<SettingsStore>,
) {
    let mut list = read_categories(store);
    if !reorder(&mut list, from, to) {
        return;
    }
    write_categories(store, list, cx);
}

/// 纯列表重排；无变化（同一项 / id 不存在）返回 false。
fn reorder(list: &mut Vec<CategoryEntry>, from: &str, to: &str) -> bool {
    let (Some(from), Some(to)) = (
        list.iter().position(|entry| entry.id == from),
        list.iter().position(|entry| entry.id == to),
    ) else {
        return false;
    };
    if from == to {
        return false;
    }
    let entry = list.remove(from);
    list.insert(to, entry);
    true
}

/// 「一键分类目录」：把每个分类的保存目录设为默认下载目录下的同名子目录。
/// 目录名净化规则与 `sanitizeCategoryDirName` 一致。
pub(crate) fn apply_auto_dirs(store: &mut SettingsStore, cx: &mut Context<SettingsStore>) {
    let base = store.daemon_str("default_save_dir");
    if base.trim().is_empty() {
        return;
    }
    let mut list = read_categories(store);
    for entry in &mut list {
        if entry.builtin_type.as_deref() == Some("all") {
            continue;
        }
        let label = match entry.builtin_type.as_deref() {
            Some(kind) if entry.is_builtin => builtin_dir_label(kind).to_owned(),
            _ => entry.name.clone(),
        };
        entry.save_dir = category_dir_under(&base, &label);
    }
    write_categories(store, list, cx);
}

/// 内置分类的目录名基线（英文，与 App `assets/i18n/en.json` 的 `categoryXxx` 逐字一致）。
fn builtin_dir_label(kind: &str) -> &'static str {
    match kind {
        "video" => "Video",
        "audio" => "Audio",
        "document" => "Document",
        "image" => "Image",
        "program" => "Programs",
        "archive" => "Archive",
        _ => "Other",
    }
}

#[must_use]
pub(crate) fn sanitize_category_dir_name(label: &str) -> String {
    let mut out: String = label
        .chars()
        .map(|ch| {
            if matches!(ch, '\\' | '/' | ':' | '*' | '?' | '"' | '<' | '>' | '|') || ch.is_control()
            {
                ' '
            } else {
                ch
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    while out.ends_with('.') || out.ends_with(' ') {
        out.pop();
    }
    out
}

#[must_use]
pub(crate) fn category_dir_under(base_dir: &str, label: &str) -> String {
    let mut root = base_dir.trim().to_owned();
    if root.is_empty() {
        return String::new();
    }
    let folder = sanitize_category_dir_name(label);
    if folder.is_empty() {
        return String::new();
    }
    while root.len() > 1 && (root.ends_with('/') || root.ends_with('\\')) {
        root.pop();
    }
    if root.ends_with('/') || root.ends_with('\\') {
        return format!("{root}{folder}");
    }
    format!("{root}{}{folder}", std::path::MAIN_SEPARATOR)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(list: &[CategoryEntry]) -> Vec<&str> {
        list.iter().map(|entry| entry.id.as_str()).collect()
    }

    #[test]
    fn drag_reorder_moves_to_target_slot_both_directions() {
        let mut list = CategoryEntry::builtin_defaults();
        let before: Vec<String> = list.iter().map(|entry| entry.id.clone()).collect();
        let (a, c) = (before[0].clone(), before[2].clone());

        // 向下拖：a 落到 c 的位置（c 之后）。
        assert!(reorder(&mut list, &a, &c));
        assert_eq!(ids(&list)[..3], [&*before[1], &*before[2], &*before[0]]);

        // 向上拖回原位。
        assert!(reorder(&mut list, &a, &before[1]));
        assert_eq!(
            ids(&list),
            before.iter().map(String::as_str).collect::<Vec<_>>()
        );

        assert!(!reorder(&mut list, &a, &a));
        assert!(!reorder(&mut list, "missing", &a));
    }

    #[test]
    fn sanitizes_dir_names_like_dart() {
        assert_eq!(sanitize_category_dir_name("a/b:c "), "a b c");
        assert_eq!(sanitize_category_dir_name("name..."), "name");
        assert_eq!(sanitize_category_dir_name("  "), "");
    }

    #[test]
    fn joins_under_base() {
        assert_eq!(category_dir_under("", "Video"), "");
        assert_eq!(category_dir_under("/", "Video"), "/Video");
        assert_eq!(
            category_dir_under("/tmp/dl/", "Video"),
            format!("/tmp/dl{}Video", std::path::MAIN_SEPARATOR)
        );
    }

    #[test]
    fn json_shape_matches_dart() {
        let json = r#"[{"id":"x","name":"eBooks","icon":"library","matchMode":"extension","extensions":["epub"],"regexPattern":"","position":3,"visible":true,"isBuiltin":false,"builtinType":null,"saveDir":""}]"#;
        let list: Vec<CategoryEntry> = serde_json::from_str(json).expect("parse");
        assert_eq!(list[0].extensions, vec!["epub"]);
        let back = serde_json::to_string(&list).expect("serialize");
        assert!(back.contains("\"matchMode\":\"extension\""));
    }
}
