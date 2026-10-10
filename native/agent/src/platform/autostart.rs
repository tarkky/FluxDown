//! 开机自启：登录时以 `--autostart` 拉起同级 `fluxdown-agent`，由 agent 按托盘偏好决定
//! 只驻留托盘还是再拉起 `fluxdown-desktop --minimized`。
//!
//! - Windows：`HKCU\Software\Microsoft\Windows\CurrentVersion\Run` 的 `FluxDown`
//!   值（与 Flutter 时代 `launch_at_startup` 及 `installer/windows/setup.iss`
//!   的 `RemoveAutostartRunValue` 使用同一值名）。
//! - macOS：`~/Library/LaunchAgents/dev.zerx.fluxdown.desktop.plist`（RunAtLoad；沿用旧文件名，
//!   升级后原条目被原地改写而不是遗留两份）。
//! - Linux：`~/.config/autostart/fluxdown.desktop`（XDG autostart）。
//!
//! “已注册”要求条目以 `--autostart` 指向当前 agent；程序移动/升级后旧条目视为未注册，
//! 用户重新开启即覆盖为新路径。“已启用”= 已注册且未被系统级开关禁用（Windows
//! `Explorer\StartupApproved\Run` 首字节为奇数；XDG `Hidden=true` /
//! `X-GNOME-Autostart-enabled=false`；macOS launchd 用户域禁用表把标签列为 disabled）。
//!
//! 保留旧客户端的自启契约：
//! [`enable`] 是用户在应用内的明确开启，写入条目并清除系统级禁用标记；
//! [`retarget`] 只供启动时的自动迁移使用，只改写启动目标，**绝不**改变系统级启用状态——
//! 用户在系统设置 / 任务管理器 / 桌面环境里关掉的自启不能被应用重新打开。
//! [`targets`] 只判断条目是否指向某个可执行文件，用于识别并迁移旧版直接拉起桌面程序的条目。

/// Windows `StartupApproved\Run` 值：缺失或为空 = 启用；首字节奇数 = 用户在系统设置 /
/// 任务管理器里禁用。
#[cfg(any(target_os = "windows", test))]
fn startup_approved(value: Option<&[u8]>) -> bool {
    value
        .and_then(|bytes| bytes.first())
        .is_none_or(|flag| flag % 2 == 0)
}

/// `[Desktop Entry]` 组内的 `key=value` 行；`=` 两侧空白忽略。
#[cfg(any(target_os = "linux", test))]
fn desktop_entry_lines(content: &str) -> impl Iterator<Item = (usize, &str, &str)> {
    let mut in_entry = false;
    content
        .split('\n')
        .enumerate()
        .filter_map(move |(index, raw)| {
            let line = raw.trim();
            if line.starts_with('[') {
                in_entry = line == "[Desktop Entry]";
                return None;
            }
            if !in_entry {
                return None;
            }
            let (key, value) = line.split_once('=')?;
            let key = key.trim();
            (!key.is_empty()).then(|| (index, key, value.trim()))
        })
}

/// 条目被桌面环境禁用：`Hidden=true`（XDG 规范）或 `X-GNOME-Autostart-enabled=false`。
#[cfg(any(target_os = "linux", test))]
fn xdg_user_disabled(content: &str) -> bool {
    desktop_entry_lines(content).any(|(_, key, value)| {
        (key == "Hidden" && value == "true")
            || (key == "X-GNOME-Autostart-enabled" && value == "false")
    })
}

/// 把 `[Desktop Entry]` 组的 `Exec` 行换成 `exec_line`（缺失则插在组头之后），其余行
/// 逐字保留；无变化或没有 `[Desktop Entry]` 组时返回 `None`。
#[cfg(any(target_os = "linux", test))]
fn xdg_retarget_exec(content: &str, exec_line: &str) -> Option<String> {
    let mut lines: Vec<&str> = content.split('\n').collect();
    if let Some((index, _, _)) = desktop_entry_lines(content).find(|(_, key, _)| *key == "Exec") {
        if lines[index] == exec_line {
            return None;
        }
        lines[index] = exec_line;
        return Some(lines.join("\n"));
    }
    let header = lines
        .iter()
        .position(|line| line.trim() == "[Desktop Entry]")?;
    lines.insert(header + 1, exec_line);
    Some(lines.join("\n"))
}

/// `launchctl print-disabled` 输出里 `label` 是否被禁用：新系统为 `"label" => disabled`，
/// 旧系统为 `"label" => true`。
#[cfg(any(target_os = "macos", test))]
fn launchd_label_disabled(output: &str, label: &str) -> bool {
    let quoted = format!("\"{label}\"");
    output.lines().any(|line| {
        line.trim()
            .strip_prefix(&quoted)
            .and_then(|rest| rest.trim_start().strip_prefix("=>"))
            .is_some_and(|state| matches!(state.trim(), "disabled" | "true"))
    })
}

#[cfg(target_os = "windows")]
mod inner {
    use std::path::Path;

    use winreg::enums::{HKEY_CURRENT_USER, KEY_READ, KEY_WRITE, RegType};
    use winreg::{RegKey, RegValue};

    use crate::platform::{AUTOSTART_ARG, PlatformError, registry_executable};

    const RUN_KEY: &str = "Software\\Microsoft\\Windows\\CurrentVersion\\Run";
    const APPROVED_KEY: &str =
        "Software\\Microsoft\\Windows\\CurrentVersion\\Explorer\\StartupApproved\\Run";
    const VALUE_NAME: &str = "FluxDown";
    /// 首字节 2 = 启用；与 Windows 设置、任务管理器及 Flutter `launch_at_startup` 写入格式一致。
    const APPROVED_ENABLED: [u8; 12] = [2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

    pub fn supported(desktop: Option<&Path>) -> bool {
        desktop.is_some()
    }

    fn command_line(exe: &str) -> String {
        format!("\"{exe}\" {AUTOSTART_ARG}")
    }

    fn registered_value() -> Option<String> {
        let hkcu = RegKey::predef(HKEY_CURRENT_USER);
        let key = hkcu.open_subkey_with_flags(RUN_KEY, KEY_READ).ok()?;
        key.get_value::<String, _>(VALUE_NAME).ok()
    }

    fn approved() -> bool {
        let value = RegKey::predef(HKEY_CURRENT_USER)
            .open_subkey_with_flags(APPROVED_KEY, KEY_READ)
            .ok()
            .and_then(|key| key.get_raw_value(VALUE_NAME).ok());
        super::startup_approved(value.as_ref().map(|value| value.bytes.as_slice()))
    }

    pub fn is_registered(agent: &Path) -> bool {
        let Ok(exe) = registry_executable(Some(agent)) else {
            return false;
        };
        registered_value().is_some_and(|value| value.eq_ignore_ascii_case(&command_line(&exe)))
    }

    pub fn is_enabled(agent: &Path) -> bool {
        is_registered(agent) && approved()
    }

    pub fn targets(executable: &Path) -> bool {
        let Ok(exe) = registry_executable(Some(executable)) else {
            return false;
        };
        let quoted = format!("\"{exe}\"").to_ascii_lowercase();
        registered_value().is_some_and(|value| value.to_ascii_lowercase().starts_with(&quoted))
    }

    pub fn retarget(agent: &Path) -> Result<(), PlatformError> {
        let exe = registry_executable(Some(agent))?;
        let hkcu = RegKey::predef(HKEY_CURRENT_USER);
        let (key, _) = hkcu.create_subkey_with_flags(RUN_KEY, KEY_WRITE)?;
        key.set_value(VALUE_NAME, &command_line(&exe))?;
        tracing::info!(exe, "retargeted autostart entry");
        Ok(())
    }

    pub fn enable(agent: &Path) -> Result<(), PlatformError> {
        retarget(agent)?;
        let hkcu = RegKey::predef(HKEY_CURRENT_USER);
        let (key, _) = hkcu.create_subkey_with_flags(APPROVED_KEY, KEY_WRITE)?;
        key.set_raw_value(
            VALUE_NAME,
            &RegValue {
                bytes: APPROVED_ENABLED.to_vec(),
                vtype: RegType::REG_BINARY,
            },
        )?;
        tracing::info!("enabled autostart");
        Ok(())
    }

    fn delete_value(path: &str) -> Result<(), PlatformError> {
        let hkcu = RegKey::predef(HKEY_CURRENT_USER);
        let key = match hkcu.open_subkey_with_flags(path, KEY_WRITE) {
            Ok(key) => key,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        match key.delete_value(VALUE_NAME) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub fn disable() -> Result<(), PlatformError> {
        delete_value(RUN_KEY)?;
        delete_value(APPROVED_KEY)?;
        tracing::info!("disabled autostart");
        Ok(())
    }
}

#[cfg(target_os = "macos")]
mod inner {
    use std::path::{Path, PathBuf};

    use crate::platform::{AUTOSTART_ARG, PlatformError};

    const LABEL: &str = "dev.zerx.fluxdown.desktop";

    pub fn supported(desktop: Option<&Path>) -> bool {
        desktop.is_some()
    }

    fn plist_path() -> Result<PathBuf, PlatformError> {
        let base = directories::BaseDirs::new()
            .ok_or(PlatformError::Unsupported("home directory unavailable"))?;
        Ok(base
            .home_dir()
            .join("Library")
            .join("LaunchAgents")
            .join(format!("{LABEL}.plist")))
    }

    fn xml_escape(value: &str) -> String {
        let mut out = String::with_capacity(value.len());
        for ch in value.chars() {
            match ch {
                '&' => out.push_str("&amp;"),
                '<' => out.push_str("&lt;"),
                '>' => out.push_str("&gt;"),
                other => out.push(other),
            }
        }
        out
    }

    fn program_element(executable: &Path) -> String {
        format!(
            "<string>{}</string>",
            xml_escape(&executable.display().to_string())
        )
    }

    fn plist_body(agent: &Path) -> String {
        let program = program_element(agent);
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \
             \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n\
             <plist version=\"1.0\">\n\
             <dict>\n\
             \t<key>Label</key>\n\
             \t<string>{LABEL}</string>\n\
             \t<key>ProgramArguments</key>\n\
             \t<array>\n\
             \t\t{program}\n\
             \t\t<string>{AUTOSTART_ARG}</string>\n\
             \t</array>\n\
             \t<key>RunAtLoad</key>\n\
             \t<true/>\n\
             \t<key>ProcessType</key>\n\
             \t<string>Interactive</string>\n\
             </dict>\n\
             </plist>\n"
        )
    }

    fn content() -> Option<String> {
        std::fs::read_to_string(plist_path().ok()?).ok()
    }

    /// 「系统设置 → 登录项 → 允许在后台」关闭的状态由 Background Task Management 维护，
    /// 没有公开 API 可读；这里只以 plist 是否指向当前 agent 判断注册。
    pub fn is_registered(agent: &Path) -> bool {
        content().is_some_and(|content| {
            content.contains(&program_element(agent))
                && content.contains(&format!("<string>{AUTOSTART_ARG}</string>"))
        })
    }

    /// 已注册且 launchd 的用户域禁用表里没有把该标签标为禁用。
    pub fn is_enabled(agent: &Path) -> bool {
        is_registered(agent) && !launchd_disabled()
    }

    /// `launchctl print-disabled gui/<uid>` 是否把本标签列为禁用；读不到时按未禁用处理。
    fn launchd_disabled() -> bool {
        let Some(uid) = launchctl(&["manageruid"]) else {
            return false;
        };
        let domain = format!("gui/{}", uid.trim());
        launchctl(&["print-disabled", &domain])
            .is_some_and(|output| super::launchd_label_disabled(&output, LABEL))
    }

    fn launchctl(args: &[&str]) -> Option<String> {
        let output = std::process::Command::new("/bin/launchctl")
            .args(args)
            .stdin(std::process::Stdio::null())
            .output()
            .ok()?;
        output
            .status
            .success()
            .then(|| String::from_utf8_lossy(&output.stdout).into_owned())
    }

    pub fn targets(executable: &Path) -> bool {
        content().is_some_and(|content| content.contains(&program_element(executable)))
    }

    /// plist 只有启动目标，迁移即整份重写；launchd 禁用表与登录项后台许可不在文件里，不受影响。
    pub fn retarget(agent: &Path) -> Result<(), PlatformError> {
        write_plist(agent)?;
        tracing::info!("retargeted autostart entry");
        Ok(())
    }

    /// 用户在应用内明确开启：写入 plist 并清除 launchd 用户域禁用标记（尽力而为）。
    pub fn enable(agent: &Path) -> Result<(), PlatformError> {
        write_plist(agent)?;
        if launchd_disabled() {
            let target =
                launchctl(&["manageruid"]).map(|uid| format!("gui/{}/{LABEL}", uid.trim()));
            if target
                .and_then(|target| launchctl(&["enable", &target]))
                .is_none()
            {
                tracing::warn!("could not clear the launchd disabled flag for autostart");
            }
        }
        tracing::info!("enabled autostart");
        Ok(())
    }

    fn write_plist(agent: &Path) -> Result<(), PlatformError> {
        let path = plist_path()?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, plist_body(agent))?;
        Ok(())
    }

    pub fn disable() -> Result<(), PlatformError> {
        let path = plist_path()?;
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        tracing::info!(path = %path.display(), "disabled autostart");
        Ok(())
    }
}

#[cfg(target_os = "linux")]
mod inner {
    use std::path::{Path, PathBuf};

    use crate::platform::{AUTOSTART_ARG, PlatformError};

    pub fn supported(desktop: Option<&Path>) -> bool {
        desktop.is_some()
    }

    fn entry_path() -> Result<PathBuf, PlatformError> {
        let base = directories::BaseDirs::new()
            .ok_or(PlatformError::Unsupported("home directory unavailable"))?;
        Ok(base.config_dir().join("autostart").join("fluxdown.desktop"))
    }

    /// 按 Desktop Entry 规范为 `Exec` 引用并转义参数。
    fn exec_quote(value: &str) -> String {
        let mut out = String::with_capacity(value.len() + 2);
        out.push('"');
        for ch in value.chars() {
            if matches!(ch, '"' | '`' | '$' | '\\') {
                out.push('\\');
            }
            out.push(ch);
        }
        out.push('"');
        out
    }

    fn exec_line(agent: &Path) -> String {
        format!(
            "Exec={} {AUTOSTART_ARG}\n",
            exec_quote(&agent.display().to_string())
        )
    }

    fn entry_body(agent: &Path) -> String {
        let exec = exec_line(agent);
        format!(
            "[Desktop Entry]\n\
             Type=Application\n\
             Name=FluxDown\n\
             Comment=Free IDM-alternative download manager\n\
             {exec}\
             Icon=com.fluxdown.app\n\
             Terminal=false\n\
             X-GNOME-Autostart-enabled=true\n"
        )
    }

    fn content() -> Option<String> {
        std::fs::read_to_string(entry_path().ok()?).ok()
    }

    pub fn is_registered(agent: &Path) -> bool {
        content().is_some_and(|content| content.contains(&exec_line(agent)))
    }

    pub fn is_enabled(agent: &Path) -> bool {
        content().is_some_and(|content| {
            content.contains(&exec_line(agent)) && !super::xdg_user_disabled(&content)
        })
    }

    pub fn targets(executable: &Path) -> bool {
        let exec = format!("Exec={}", exec_quote(&executable.display().to_string()));
        content().is_some_and(|content| content.contains(&exec))
    }

    /// 只替换 `Exec` 行，保留 `Hidden` / `X-GNOME-Autostart-enabled` 等用户禁用标记。
    pub fn retarget(agent: &Path) -> Result<(), PlatformError> {
        let path = entry_path()?;
        let content = std::fs::read_to_string(&path)?;
        let exec = exec_line(agent);
        let Some(updated) = super::xdg_retarget_exec(&content, exec.trim_end()) else {
            return Ok(());
        };
        std::fs::write(&path, updated)?;
        tracing::info!(path = %path.display(), "retargeted autostart entry");
        Ok(())
    }

    /// 整份重写：用户在应用内明确开启，连同禁用标记一起清掉。
    pub fn enable(agent: &Path) -> Result<(), PlatformError> {
        let path = entry_path()?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, entry_body(agent))?;
        tracing::info!(path = %path.display(), "enabled autostart");
        Ok(())
    }

    pub fn disable() -> Result<(), PlatformError> {
        let path = entry_path()?;
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        tracing::info!(path = %path.display(), "disabled autostart");
        Ok(())
    }
}

#[cfg(not(any(target_os = "windows", target_os = "linux", target_os = "macos")))]
mod inner {
    use std::path::Path;

    use crate::platform::PlatformError;

    pub fn supported(_desktop: Option<&Path>) -> bool {
        false
    }

    pub fn is_registered(_agent: &Path) -> bool {
        false
    }

    pub fn is_enabled(_agent: &Path) -> bool {
        false
    }

    pub fn targets(_executable: &Path) -> bool {
        false
    }

    pub fn retarget(_agent: &Path) -> Result<(), PlatformError> {
        Err(PlatformError::Unsupported(
            "autostart is not supported on this platform",
        ))
    }

    pub fn enable(_agent: &Path) -> Result<(), PlatformError> {
        Err(PlatformError::Unsupported(
            "autostart is not supported on this platform",
        ))
    }

    pub fn disable() -> Result<(), PlatformError> {
        Err(PlatformError::Unsupported(
            "autostart is not supported on this platform",
        ))
    }
}

pub use inner::{disable, enable, is_enabled, is_registered, retarget, supported, targets};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_approved_follows_first_byte_parity() {
        assert!(startup_approved(None));
        assert!(startup_approved(Some(&[])));
        assert!(startup_approved(Some(&[2, 0, 0])));
        assert!(!startup_approved(Some(&[3, 0, 0])));
    }

    #[test]
    fn launchd_disabled_table_matches_exact_label_in_both_formats() {
        let modern = "disabled services = {\n\t\t\"dev.zerx.fluxdown.desktop\" => disabled\n\t\t\"other\" => enabled\n\t}\n";
        assert!(launchd_label_disabled(modern, "dev.zerx.fluxdown.desktop"));
        assert!(!launchd_label_disabled(modern, "other"));
        assert!(!launchd_label_disabled(modern, "dev.zerx.fluxdown"));
        let legacy = "\t\"dev.zerx.fluxdown.desktop\" => true\n\t\"other\" => false\n";
        assert!(launchd_label_disabled(legacy, "dev.zerx.fluxdown.desktop"));
        assert!(!launchd_label_disabled(legacy, "other"));
        assert!(!launchd_label_disabled("", "dev.zerx.fluxdown.desktop"));
    }

    #[test]
    fn xdg_disable_markers_only_count_inside_desktop_entry() {
        assert!(xdg_user_disabled(
            "[Desktop Entry]\nExec=a\nHidden = true\n"
        ));
        assert!(xdg_user_disabled(
            "[Desktop Entry]\nX-GNOME-Autostart-enabled=false\n"
        ));
        assert!(!xdg_user_disabled(
            "[Desktop Entry]\nX-GNOME-Autostart-enabled=true\n"
        ));
        assert!(!xdg_user_disabled(
            "[Desktop Entry]\nExec=a\n[Desktop Action x]\nHidden=true\n"
        ));
    }

    #[test]
    fn xdg_retarget_replaces_exec_and_keeps_disable_markers() {
        let content = "[Desktop Entry]\nExec=\"/old\" --autostart\nHidden=true\n";
        assert_eq!(
            xdg_retarget_exec(content, "Exec=\"/new\" --autostart").as_deref(),
            Some("[Desktop Entry]\nExec=\"/new\" --autostart\nHidden=true\n")
        );
        assert_eq!(
            xdg_retarget_exec(content, "Exec=\"/old\" --autostart"),
            None
        );
        assert_eq!(
            xdg_retarget_exec("[Desktop Entry]\nName=x\n", "Exec=y").as_deref(),
            Some("[Desktop Entry]\nExec=y\nName=x\n")
        );
        assert_eq!(xdg_retarget_exec("Name=x\n", "Exec=y"), None);
    }
}
