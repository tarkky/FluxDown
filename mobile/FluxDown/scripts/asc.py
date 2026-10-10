#!/usr/bin/env python3
"""App Store Connect API 小工具（零第三方依赖：JWT 的 ES256 签名走系统 openssl）。

TestFlight 上传后的分发步骤，供 scripts/testflight.sh 与 CI 调用：
  等待构建处理完成 → 写「测试内容」→ 加入外部测试组 → 提交 Beta 审核。
内部测试组建议在 App Store Connect 勾选「自动分发」（hasAccessToAllBuilds），无需本工具。

鉴权环境变量：
  APPLE_API_KEY_ID      App Store Connect API 密钥 ID
  APPLE_API_ISSUER_ID   Issuer ID
  APPLE_API_KEY_PATH    .p8 私钥文件路径

用法：
  asc.py distribute --bundle-id com.fluxdown.app --version 0.5.6 --build 42 \
      [--group "Public Beta"]... [--whats-new "..."] [--timeout 3600]
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

API = "https://api.appstoreconnect.apple.com"


class AscError(RuntimeError):
    pass


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def der_to_raw(sig: bytes) -> bytes:
    """openssl 输出 DER 编码的 ECDSA 签名；JWT ES256 要 r||s 各 32 字节。"""
    if len(sig) < 8 or sig[0] != 0x30:
        raise AscError("unexpected ECDSA signature encoding")
    idx = 2 if sig[1] < 0x80 else 2 + (sig[1] & 0x7F)
    parts = []
    for _ in range(2):
        if sig[idx] != 0x02:
            raise AscError("unexpected ECDSA integer tag")
        length = sig[idx + 1]
        value = sig[idx + 2 : idx + 2 + length].lstrip(b"\x00")
        if len(value) > 32:
            raise AscError("ECDSA integer longer than 32 bytes")
        parts.append(value.rjust(32, b"\x00"))
        idx += 2 + length
    return parts[0] + parts[1]


def make_token() -> str:
    try:
        key_id = os.environ["APPLE_API_KEY_ID"]
        issuer = os.environ["APPLE_API_ISSUER_ID"]
        key_path = os.environ["APPLE_API_KEY_PATH"]
    except KeyError as missing:
        raise AscError(f"missing environment variable {missing}") from None
    now = int(time.time())
    header = b64url(json.dumps({"alg": "ES256", "kid": key_id, "typ": "JWT"}).encode())
    payload = b64url(
        json.dumps({"iss": issuer, "iat": now, "exp": now + 1100, "aud": "appstoreconnect-v1"}).encode()
    )
    signing_input = f"{header}.{payload}".encode()
    proc = subprocess.run(
        ["openssl", "dgst", "-sha256", "-sign", key_path],
        input=signing_input,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        raise AscError(f"openssl sign failed: {proc.stderr.decode(errors='replace').strip()}")
    return f"{header}.{payload}.{b64url(der_to_raw(proc.stdout))}"


class Client:
    def __init__(self) -> None:
        self._token = ""
        self._token_at = 0.0

    def _auth(self) -> str:
        # 令牌有效期 ~18 分钟；轮询构建可能更久，提前刷新。
        if time.time() - self._token_at > 900:
            self._token = make_token()
            self._token_at = time.time()
        return self._token

    def call(self, method: str, path: str, body: dict | None = None) -> dict:
        req = urllib.request.Request(
            API + path,
            method=method,
            data=json.dumps(body).encode() if body is not None else None,
            headers={"Authorization": f"Bearer {self._auth()}", "Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as err:
            detail = err.read().decode(errors="replace")
            raise AscError(f"{method} {path} -> HTTP {err.code}: {detail}") from None
        return json.loads(raw) if raw else {}


def query(params: dict[str, str]) -> str:
    return "?" + urllib.parse.urlencode(params)


def find_app(client: Client, bundle_id: str) -> str:
    data = client.call("GET", "/v1/apps" + query({"filter[bundleId]": bundle_id}))["data"]
    if not data:
        raise AscError(f"no App Store Connect app with bundle id {bundle_id}")
    return data[0]["id"]


def wait_build(client: Client, app_id: str, version: str, build: str, timeout: int) -> str:
    deadline = time.time() + timeout
    path = "/v1/builds" + query(
        {"filter[app]": app_id, "filter[version]": build, "filter[preReleaseVersion.version]": version}
    )
    while True:
        data = client.call("GET", path)["data"]
        state = data[0]["attributes"]["processingState"] if data else "NOT_FOUND"
        print(f"[asc] build {version} ({build}): {state}", flush=True)
        if state == "VALID":
            return data[0]["id"]
        if state in ("FAILED", "INVALID"):
            raise AscError(f"build {version} ({build}) processing {state}")
        if time.time() > deadline:
            raise AscError(f"timed out waiting for build {version} ({build}); last state {state}")
        time.sleep(30)


def set_whats_new(client: Client, build_id: str, text: str) -> None:
    existing = client.call("GET", f"/v1/builds/{build_id}/betaBuildLocalizations")["data"]
    by_locale = {loc["attributes"]["locale"]: loc["id"] for loc in existing}
    for locale in ("en-US", "zh-Hans"):
        if locale in by_locale:
            client.call(
                "PATCH",
                f"/v1/betaBuildLocalizations/{by_locale[locale]}",
                {"data": {"type": "betaBuildLocalizations", "id": by_locale[locale], "attributes": {"whatsNew": text}}},
            )
        else:
            client.call(
                "POST",
                "/v1/betaBuildLocalizations",
                {
                    "data": {
                        "type": "betaBuildLocalizations",
                        "attributes": {"locale": locale, "whatsNew": text},
                        "relationships": {"build": {"data": {"type": "builds", "id": build_id}}},
                    }
                },
            )


def add_to_groups(client: Client, app_id: str, build_id: str, names: list[str]) -> bool:
    """返回是否含外部组（外部组需要提交 Beta 审核）。"""
    groups = client.call("GET", f"/v1/apps/{app_id}/betaGroups" + query({"limit": "200"}))["data"]
    by_name = {g["attributes"]["name"]: g for g in groups}
    external = False
    for name in names:
        group = by_name.get(name)
        if group is None:
            raise AscError(f"TestFlight group {name!r} not found (have: {sorted(by_name)})")
        client.call(
            "POST",
            f"/v1/betaGroups/{group['id']}/relationships/builds",
            {"data": [{"type": "builds", "id": build_id}]},
        )
        print(f"[asc] added build to group {name!r}", flush=True)
        external = external or not group["attributes"]["isInternalGroup"]
    return external


def submit_review(client: Client, build_id: str) -> None:
    try:
        client.call(
            "POST",
            "/v1/betaAppReviewSubmissions",
            {"data": {"type": "betaAppReviewSubmissions", "relationships": {"build": {"data": {"type": "builds", "id": build_id}}}}},
        )
    except AscError as err:
        # 同一版本号已过审的后续构建无需再提交，Apple 以 409 拒收重复提交。
        if "HTTP 409" in str(err):
            print(f"[asc] beta review submission skipped: {err}", flush=True)
            return
        raise
    print("[asc] submitted for beta app review", flush=True)


def distribute(args: argparse.Namespace) -> None:
    client = Client()
    app_id = find_app(client, args.bundle_id)
    build_id = wait_build(client, app_id, args.version, args.build, args.timeout)
    if args.whats_new:
        set_whats_new(client, build_id, args.whats_new)
    if args.group and add_to_groups(client, app_id, build_id, args.group):
        submit_review(client, build_id)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    dist = sub.add_parser("distribute", help="wait for processing, then hand the build to TestFlight groups")
    dist.add_argument("--bundle-id", required=True)
    dist.add_argument("--version", required=True, help="CFBundleShortVersionString, e.g. 0.5.6")
    dist.add_argument("--build", required=True, help="CFBundleVersion, e.g. 42")
    dist.add_argument("--group", action="append", default=[], help="TestFlight group name (repeatable)")
    dist.add_argument("--whats-new", default="", help="TestFlight 'What to Test' text")
    dist.add_argument("--timeout", type=int, default=3600, help="seconds to wait for processing")
    args = parser.parse_args()
    try:
        distribute(args)
    except AscError as err:
        print(f"[asc] error: {err}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
