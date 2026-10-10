//! 账户能力自带的图标（gpui-component 内置 lucide 子集缺少的两枚）。

use std::borrow::Cow;

use gpui::{AssetSource, Result, SharedString};

/// 套餐徽标皇冠。
pub const CROWN_ICON_PATH: &str = "fluxdown/icons/crown.svg";
/// 头像无可用首字符时的回退云图标。
pub const CLOUD_ICON_PATH: &str = "fluxdown/icons/cloud.svg";
/// 刷新云端信息。
pub const REFRESH_ICON_PATH: &str = "fluxdown/icons/refresh-cw.svg";

const ICONS: &[(&str, &[u8])] = &[
    (CROWN_ICON_PATH, include_bytes!("../assets/crown.svg")),
    (CLOUD_ICON_PATH, include_bytes!("../assets/cloud.svg")),
    (
        REFRESH_ICON_PATH,
        include_bytes!("../assets/refresh-cw.svg"),
    ),
];

/// 账户能力拥有的嵌入资源。
pub struct AccountAssets;

impl AssetSource for AccountAssets {
    fn load(&self, path: &str) -> Result<Option<Cow<'static, [u8]>>> {
        Ok(ICONS
            .iter()
            .find(|(icon_path, _)| *icon_path == path)
            .map(|(_, bytes)| Cow::Borrowed(*bytes)))
    }

    fn list(&self, path: &str) -> Result<Vec<SharedString>> {
        Ok(ICONS
            .iter()
            .filter(|(icon_path, _)| icon_path.starts_with(path))
            .map(|(icon_path, _)| SharedString::from(*icon_path))
            .collect())
    }
}
