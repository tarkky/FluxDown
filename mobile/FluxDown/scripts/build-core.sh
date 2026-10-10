#!/usr/bin/env bash
# 构建 iOS 用的 Rust 核心（crate `fluxdown_mobile`，UniFFI）并生成 Swift 绑定：
#   FluxKit/Artifacts/fluxdown_mobileFFI.xcframework   静态库（真机 arm64 + 模拟器 arm64）+ C 头 + modulemap
#   FluxKit/Sources/FluxRustBindings/fluxdown_mobile.swift      UniFFI 生成的 Swift 绑定（独立模块，只被 FluxBridge 内部导入）
# 两者都是构建产物（已 gitignore），改了 native/ 下任何 Rust 源码后重跑本脚本，再回 Xcode 构建。
#
# 用法：mobile/FluxDown/scripts/build-core.sh [--release] [--cloud-url URL] [--run]
#   --release        发布 / 性能测试用；默认 debug（编译快，引擎 BT 数据面 crate 仍按 profile.dev.package 开 O3）。
#   --cloud-url URL  FluxCloud 地址，编译期写入核心（等价于导出 FLUXCLOUD_BASE_URL；参数优先）。
#                    都未提供时核心回退到 http://127.0.0.1:8720（本机 FluxCloud 开发服务），脚本会提示。
#   --run            核心构建完后顺带构建 App，安装并重启到已启动的模拟器（无需再回 Xcode 点运行）。
# 可选 FLUXDOWN_APP_VERSION：编译期产品版本（协议握手 / 云端 / UA）；testflight.sh 按 --version 注入，未设置回落 crate 版本。
# 前置：rustup target add aarch64-apple-ios aarch64-apple-ios-sim；Xcode 命令行工具。
set -euo pipefail

PROFILE_FLAG=""
PROFILE_DIR="debug"
RUN_APP=0
while [ $# -gt 0 ]; do
  case "$1" in
    --release) PROFILE_FLAG="--release"; PROFILE_DIR="release" ;;
    --run) RUN_APP=1 ;;
    --cloud-url)
      [ $# -ge 2 ] || { echo "--cloud-url needs a value" >&2; exit 64; }
      export FLUXCLOUD_BASE_URL="$2"
      shift
      ;;
    --cloud-url=*) export FLUXCLOUD_BASE_URL="${1#--cloud-url=}" ;;
    *) echo "unknown argument: $1 (expected --release, --cloud-url URL, --run)" >&2; exit 64 ;;
  esac
  shift
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IOS_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$IOS_ROOT/../.." && pwd)"
KIT="$IOS_ROOT/FluxKit"
CRATE="fluxdown_mobile"
FFI_MODULE="${CRATE}FFI"

# Xcode / IDE 启动的 shell 常不带 ~/.cargo/bin。
export PATH="${CARGO_HOME:-$HOME/.cargo}/bin:$PATH"
# 与 Xcode 工程的最低部署版本一致，避免链接期 "built for newer iOS" 警告。
export IPHONEOS_DEPLOYMENT_TARGET="${IPHONEOS_DEPLOYMENT_TARGET:-26.1}"

# agent 用 option_env!("FLUXCLOUD_BASE_URL") 读取：空串会被当成「已设置」写进二进制，必须清掉。
# 变量变化时 cargo 按 env-dep 自动重编 fluxdown_agent，无需 clean。
if [ -n "${FLUXCLOUD_BASE_URL:-}" ]; then
  echo "==> FluxCloud: $FLUXCLOUD_BASE_URL"
else
  unset FLUXCLOUD_BASE_URL
  echo "==> FluxCloud: http://127.0.0.1:8720 (default; pass --cloud-url URL for a real server)"
fi

cd "$REPO_ROOT"
TARGET_DIR="$(cargo metadata --format-version 1 --no-deps | /usr/bin/python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"

echo "==> cargo build ($PROFILE_DIR) for iOS device + simulator"
for target in aarch64-apple-ios aarch64-apple-ios-sim; do
  cargo build -p "$CRATE" --lib --target "$target" $PROFILE_FLAG
done

# 绑定从宿主机 debug 库的 proc-macro 元数据生成（同一份源码 → 校验和一致；release 会 strip 掉元数据）。
echo "==> uniffi-bindgen (swift)"
cargo build -p "$CRATE" --lib
case "$(uname -s)" in
  Darwin) HOST_LIB="$TARGET_DIR/debug/lib$CRATE.dylib" ;;
  *) HOST_LIB="$TARGET_DIR/debug/lib$CRATE.so" ;;
esac
GEN_DIR="$(mktemp -d)"
trap 'rm -rf "$GEN_DIR"' EXIT
cargo run -q -p "$CRATE" --features bindgen --bin uniffi-bindgen -- \
  generate --library "$HOST_LIB" --language swift --no-format --out-dir "$GEN_DIR"

HEADERS="$GEN_DIR/headers"
mkdir -p "$HEADERS"
cp "$GEN_DIR/$FFI_MODULE.h" "$HEADERS/"
cp "$GEN_DIR/$FFI_MODULE.modulemap" "$HEADERS/module.modulemap"

echo "==> xcframework"
mkdir -p "$KIT/Artifacts"
rm -rf "$KIT/Artifacts/$FFI_MODULE.xcframework"
xcodebuild -create-xcframework \
  -library "$TARGET_DIR/aarch64-apple-ios/$PROFILE_DIR/lib$CRATE.a" -headers "$HEADERS" \
  -library "$TARGET_DIR/aarch64-apple-ios-sim/$PROFILE_DIR/lib$CRATE.a" -headers "$HEADERS" \
  -output "$KIT/Artifacts/$FFI_MODULE.xcframework" >/dev/null

mkdir -p "$KIT/Sources/FluxRustBindings"
cp "$GEN_DIR/$CRATE.swift" "$KIT/Sources/FluxRustBindings/$CRATE.swift"

echo "==> done: $KIT/Artifacts/$FFI_MODULE.xcframework ($PROFILE_DIR)"

[ "$RUN_APP" = 1 ] || exit 0

# ── 构建 App 并刷新到已启动的模拟器 ──
# 不带 -derivedDataPath：与 Xcode 共用同一份 DerivedData，之后在 Xcode 里增量构建不会重来。
UDID="$(xcrun simctl list devices booted -j | /usr/bin/python3 -c '
import json, sys
devices = [d for runtime in json.load(sys.stdin)["devices"].values() for d in runtime if d.get("state") == "Booted"]
print(devices[0]["udid"] if devices else "")')"
if [ -z "$UDID" ]; then
  echo "no booted simulator; start one (open -a Simulator) and rerun with --run" >&2
  exit 1
fi
CONFIG="Debug"
[ "$PROFILE_DIR" = "release" ] && CONFIG="Release"

echo "==> xcodebuild FluxDown ($CONFIG) for simulator $UDID"
cd "$IOS_ROOT"
xcodebuild build -quiet -project FluxDown.xcodeproj -scheme FluxDown -configuration "$CONFIG" \
  -destination "id=$UDID"
SETTINGS="$(xcodebuild -showBuildSettings -project FluxDown.xcodeproj -scheme FluxDown -configuration "$CONFIG" \
  -destination "id=$UDID" 2>/dev/null)"
setting() { printf '%s\n' "$SETTINGS" | awk -F' = ' -v key="$1" '$1 ~ "^ +" key "$" { print $2; exit }'; }
APP="$(setting TARGET_BUILD_DIR)/$(setting WRAPPER_NAME)"
BUNDLE_ID="$(setting PRODUCT_BUNDLE_IDENTIFIER)"

echo "==> install + relaunch $BUNDLE_ID"
xcrun simctl terminate "$UDID" "$BUNDLE_ID" >/dev/null 2>&1 || true # 未在运行时 terminate 失败属正常
xcrun simctl install "$UDID" "$APP"
xcrun simctl launch "$UDID" "$BUNDLE_ID" >/dev/null
echo "==> launched on simulator"
