//! 构建脚本：确定应用版本号并注入 `FLUXDOWN_APP_VERSION` 编译期环境变量，供
//! downloader.rs 拼出 aria2 风格的默认 UA（`FluxDown/<版本>`）。
//!
//! 取值顺序：构建环境变量 `FLUXDOWN_APP_VERSION`（发布流水线按 tag 导出）→
//! 当前 engine crate 的 `CARGO_PKG_VERSION`（本地构建与独立打包）。

fn main() {
    println!("cargo:rerun-if-env-changed=FLUXDOWN_APP_VERSION");
    let from_env = std::env::var("FLUXDOWN_APP_VERSION")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
    if let Some(version) = from_env {
        println!("cargo:rustc-env=FLUXDOWN_APP_VERSION={version}");
        return;
    }

    let version = env!("CARGO_PKG_VERSION");
    println!("cargo:rustc-env=FLUXDOWN_APP_VERSION={version}");
}
