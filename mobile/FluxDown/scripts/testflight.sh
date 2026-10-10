#!/usr/bin/env bash
# 打包原生 iOS App（mobile/FluxDown）为 App Store 签名的 IPA，并校验 / 上传到 TestFlight。
# 本机与 CI（.github/workflows/ios-testflight.yml）共用这一份流程。
#
# 用法：mobile/FluxDown/scripts/testflight.sh --version X.Y.Z --build N [--upload] [--profile PATH] [--p12 PATH]
#                                              [--group NAME]... [--whats-new TEXT] [--skip-core]
#   --version    CFBundleShortVersionString（不带 -rc 后缀）
#   --build      CFBundleVersion（1–3 段整数，如 42 或 20261009.123045）；同一 --version 下不能重复，否则上传被拒
#   --upload     上传并分发；缺省只做 altool --validate-app（不消耗构建号）
#   --profile    App Store 描述文件（.mobileprovision）；给出则先安装到本机描述文件目录
#   --p12        Apple Distribution 证书 + 私钥（.p12，密码取 IOS_DIST_CERT_PASSWORD）；导入临时钥匙串，退出即删除（CI 用）。
#                缺省则用登录钥匙串里已有的证书（本机）
#   --group      上传后把构建加入的 TestFlight 组（可重复；含外部组时自动提交 Beta 审核）
#   --whats-new  TestFlight「测试内容」
#   --skip-core  跳过 build-core.sh --release（核心 xcframework 已是 release 构建时；须以同一 FLUXDOWN_APP_VERSION 构建）
# 前置：钥匙串里有 Apple Distribution 证书与私钥（或传 --p12）；
#       环境变量 APPLE_API_KEY_ID / APPLE_API_ISSUER_ID / APPLE_API_KEY_PATH（.p8）；
#       可选 FLUXCLOUD_BASE_URL（编译期写入核心，见 build-core.sh）；FLUXDOWN_APP_VERSION 缺省取 --version，
#       写入核心的协议握手 / 云端 / UA 版本（与 Android package.sh、桌面打包一致）。
set -euo pipefail

TEAM_ID="KD4N89AAF5"
BUNDLE_ID="com.fluxdown.app"
PROFILE_NAME="FluxDown App Store"

VERSION=""
BUILD=""
UPLOAD=0
PROFILE=""
P12=""
SKIP_CORE=0
WHATS_NEW=""
TF_GROUPS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --version) VERSION="${2:?--version needs a value}"; shift ;;
    --build) BUILD="${2:?--build needs a value}"; shift ;;
    --upload) UPLOAD=1 ;;
    --profile) PROFILE="${2:?--profile needs a value}"; shift ;;
    --p12) P12="${2:?--p12 needs a value}"; shift ;;
    --group) TF_GROUPS+=("${2:?--group needs a value}"); shift ;;
    --whats-new) WHATS_NEW="${2:?--whats-new needs a value}"; shift ;;
    --skip-core) SKIP_CORE=1 ;;
    *) echo "unknown argument: $1" >&2; exit 64 ;;
  esac
  shift
done
[[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "--version must be X.Y.Z (got '$VERSION')" >&2; exit 64; }
[[ "$BUILD" =~ ^[0-9]+(\.[0-9]+){0,2}$ ]] || { echo "--build must be 1-3 dot-separated integers (got '$BUILD')" >&2; exit 64; }
for var in APPLE_API_KEY_ID APPLE_API_ISSUER_ID APPLE_API_KEY_PATH; do
  [ -n "${!var:-}" ] || { echo "missing environment variable $var" >&2; exit 64; }
done
[ -f "$APPLE_API_KEY_PATH" ] || { echo "APPLE_API_KEY_PATH not found: $APPLE_API_KEY_PATH" >&2; exit 64; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IOS_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
OUT="$IOS_ROOT/build/testflight"
WORK="$(mktemp -d)"
KC=""
ORIG_KCS=()
cleanup() {
  if [ -n "$KC" ]; then
    security list-keychains -d user -s ${ORIG_KCS[@]+"${ORIG_KCS[@]}"}
    security delete-keychain "$KC"
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

if [ -n "$P12" ]; then
  KC="$WORK/signing.keychain-db"
  KC_PASS="$(uuidgen)"
  security create-keychain -p "$KC_PASS" "$KC"
  security set-keychain-settings "$KC"
  security unlock-keychain -p "$KC_PASS" "$KC"
  security import "$P12" -k "$KC" -P "${IOS_DIST_CERT_PASSWORD:?IOS_DIST_CERT_PASSWORD required with --p12}" \
    -T /usr/bin/codesign >/dev/null
  security set-key-partition-list -S apple-tool:,apple:,codesign: -s -k "$KC_PASS" "$KC" >/dev/null
  # codesign 经用户钥匙串搜索列表找证书链与私钥，临时钥匙串必须在列表里。
  while IFS= read -r line; do
    line="${line//\"/}"
    line="${line#"${line%%[![:space:]]*}"}"
    [ -n "$line" ] && ORIG_KCS+=("$line")
  done < <(security list-keychains -d user)
  security list-keychains -d user -s "$KC" ${ORIG_KCS[@]+"${ORIG_KCS[@]}"}
  security find-identity -v -p codesigning "$KC" | grep -q '"Apple Distribution: ' \
    || { echo "no Apple Distribution identity in $P12" >&2; exit 1; }
  echo "==> imported Apple Distribution identity into temporary keychain"
fi

if [ -n "$PROFILE" ]; then
  # Xcode 16+ 读 UserData/Provisioning Profiles，旧位置仍被 xcodebuild 兼容读取；两处都放。
  UUID="$(security cms -D -i "$PROFILE" | plutil -extract UUID raw -o - -)"
  for dir in "$HOME/Library/Developer/Xcode/UserData/Provisioning Profiles" "$HOME/Library/MobileDevice/Provisioning Profiles"; do
    mkdir -p "$dir"
    cp "$PROFILE" "$dir/$UUID.mobileprovision"
  done
  echo "==> installed provisioning profile $UUID"
fi

if [ "$SKIP_CORE" = 0 ]; then
  FLUXDOWN_APP_VERSION="${FLUXDOWN_APP_VERSION:-$VERSION}" "$SCRIPT_DIR/build-core.sh" --release
fi

rm -rf "$OUT"
mkdir -p "$OUT"

# 归档不签名：工程是自动签名，命令行强改手动签名会波及 SPM 包目标；签名统一在导出阶段按描述文件完成。
echo "==> xcodebuild archive $VERSION ($BUILD)"
xcodebuild archive -quiet \
  -project "$IOS_ROOT/FluxDown.xcodeproj" -scheme FluxDown -configuration Release \
  -destination "generic/platform=iOS" -archivePath "$OUT/FluxDown.xcarchive" \
  MARKETING_VERSION="$VERSION" CURRENT_PROJECT_VERSION="$BUILD" \
  CODE_SIGNING_ALLOWED=NO

cat > "$WORK/ExportOptions.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>method</key><string>app-store-connect</string>
	<key>destination</key><string>export</string>
	<key>teamID</key><string>$TEAM_ID</string>
	<key>signingStyle</key><string>manual</string>
	<key>signingCertificate</key><string>Apple Distribution</string>
	<key>provisioningProfiles</key>
	<dict><key>$BUNDLE_ID</key><string>$PROFILE_NAME</string></dict>
	<key>uploadSymbols</key><true/>
	<key>manageAppVersionAndBuildNumber</key><false/>
</dict>
</plist>
PLIST

echo "==> xcodebuild -exportArchive"
xcodebuild -exportArchive -quiet \
  -archivePath "$OUT/FluxDown.xcarchive" -exportPath "$OUT" \
  -exportOptionsPlist "$WORK/ExportOptions.plist"
IPA="$OUT/FluxDown.ipa"
[ -f "$IPA" ] || { echo "export produced no $IPA" >&2; exit 1; }

# altool 只从 API_PRIVATE_KEYS_DIR（或 ~/.appstoreconnect/private_keys）按 AuthKey_<ID>.p8 查找私钥。
mkdir -p "$WORK/keys"
cp "$APPLE_API_KEY_PATH" "$WORK/keys/AuthKey_${APPLE_API_KEY_ID}.p8"
export API_PRIVATE_KEYS_DIR="$WORK/keys"
ALTOOL_AUTH=(--apiKey "$APPLE_API_KEY_ID" --apiIssuer "$APPLE_API_ISSUER_ID")

if [ "$UPLOAD" = 0 ]; then
  echo "==> altool --validate-app (no upload)"
  xcrun altool --validate-app -f "$IPA" -t ios "${ALTOOL_AUTH[@]}"
  echo "==> validated: $IPA"
  exit 0
fi

echo "==> altool --upload-app"
xcrun altool --upload-app -f "$IPA" -t ios "${ALTOOL_AUTH[@]}"

DIST_ARGS=(distribute --bundle-id "$BUNDLE_ID" --version "$VERSION" --build "$BUILD")
for group in ${TF_GROUPS[@]+"${TF_GROUPS[@]}"}; do DIST_ARGS+=(--group "$group"); done
[ -n "$WHATS_NEW" ] && DIST_ARGS+=(--whats-new "$WHATS_NEW")
python3 "$SCRIPT_DIR/asc.py" "${DIST_ARGS[@]}"
echo "==> uploaded $VERSION ($BUILD) to TestFlight"
