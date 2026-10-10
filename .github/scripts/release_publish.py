#!/usr/bin/env python3
"""统一 vX.Y.Z release 的收尾（release.yml 的 publish-release job 调用；tag 推送与手动补发都跑，幂等）。

1. 以 `SHA256SUMS-<组件>.txt` 完成哨兵判定已上传完整的组件（契约见 .github/actions/release-upload）。
2. 把各组件哨兵合并为总校验文件 `SHA256SUMS.txt`。
3. 刷新 release 说明头部：已发布组件清单 + 服务器 / CLI / Android 安装说明。头部位于第一个
   `<!-- fluxdown:lang:* -->` 标记之前，官网 / App 的双语解析只取标记区块，不会把安装说明
   混进更新日志；组件清单变化会让补发产生 release `edited` 事件，官网据此清缓存。
4. 草稿 → 发布（至少一个组件完整）。稳定版且桌面端完整、且是最高稳定版本时才标 latest。
5. 本次应发布但未完成的组件写入 job summary 并以非零退出，提示补发。

演练（`--rehearsal-dir <dir>`，release.yml rehearsal 模式）：release 不存在，改从该目录读取各组件
`SHA256SUMS-<组件>.txt` 与 `RELEASE_NOTES.md`，按草稿状态走同一套判定，把 `SHA256SUMS.txt`、
`release-body.md` 写回该目录并打印计划中的 gh 操作；不调用任何 gh 命令。
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

COMPONENTS = ("app", "extension", "server", "cli", "mobile")
LABELS = {
    "app": "桌面客户端",
    "extension": "浏览器扩展",
    "server": "Headless 服务器",
    "cli": "命令行 CLI",
    "mobile": "Android",
}
HEADER_RE = re.compile(
    r"<!-- fluxdown:release:begin[^>]*-->.*?<!-- fluxdown:release:end -->\n*", re.S
)
STABLE_TAG_RE = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$")


def gh(*args: str) -> str:
    return subprocess.run(
        ["gh", *args], check=True, text=True, capture_output=True
    ).stdout


def merge_checksums(texts: list[str]) -> str:
    """合并多个 `sha256sum` 输出，按文件名去重排序。"""
    entries: dict[str, str] = {}
    for text in texts:
        for line in text.splitlines():
            parts = line.split()
            if len(parts) == 2:
                entries[parts[1].lstrip("*")] = parts[0]
    return "".join(f"{digest}  {name}\n" for name, digest in sorted(entries.items()))


def install_sections(present: list[str], version: str, repo: str, owner: str) -> str:
    sections: list[str] = []
    if "server" in present:
        sections.append(
            f"""## 📦 Headless 服务器：直接下载运行（免 Docker）

| 平台 | 资产 |
|---|---|
| Windows x64 / ARM64 | `FluxDown-Server-{version}-windows-{{x64,arm64}}.zip` |
| macOS Intel / Apple Silicon | `FluxDown-Server-{version}-macos-{{x64,arm64}}.tar.gz` |
| Linux x64 / ARM64（musl 静态链接，任意发行版可用） | `FluxDown-Server-{version}-linux-{{x64,arm64}}.tar.gz` |
| OpenWrt 软路由（x86_64 / aarch64 各子架构） | `fluxdown-server_{version}_<arch>.ipk` + `luci-app-fluxdown_{version}_all.ipk` |
| QNAP NAS（QTS x86_64 / ARM64） | `FluxDown-Server-{version}-qnap-{{x64,arm64}}.qpkg` |
| 群晖 NAS（DSM 7 / DSM 6，x86_64 / ARM64） | `FluxDown-Server-{version}-synology-{{dsm7,dsm6}}-{{x64,arm64}}.spk` |

解压后运行 `fluxdown-agent --server`（Windows 为 `fluxdown-agent.exe --server`），浏览器访问 `http://127.0.0.1:17800`。
**包内两个可执行文件必须放在同一目录**：`fluxdown-agent`（含 Web 控制台，已编译进二进制）与 `fluxdownd`（下载 daemon，由 agent 自动拉起、退出时一并关停），无需附带任何静态资源目录。
首次启动请在浏览器里完成「初始化 FluxDown Server」向导设置访问密钥（无人值守部署可用 `FLUXDOWN_TOKEN` 预置）。

**OpenWrt**：`opkg install fluxdown-server_*.ipk luci-app-fluxdown_*.ipk`，LuCI「服务 → FluxDown」页管理，Web 界面在 `http://路由器IP:17800`。agent 二进制含完整 Web UI，另附 fluxdownd，需 overlay 空间充足的软路由设备。
**QNAP**：App Center → 手动安装 `.qpkg`（需在设置中允许安装未经验证的应用），装完点应用图标直达 Web 界面。
**群晖**：套件中心 → 手动安装 `.spk`（需在「设置 → 信任来源」允许任何发行者）。DSM 7.x 装 `dsm7` 包（以套件专属用户运行），DSM 6.x 装 `dsm6` 包；Intel/AMD 机型选 x64，ARM 机型（rtd1296/rtd1619b 等）选 arm64。装完在套件中心点「打开」直达 Web 界面（端口 17800）。

## 🐳 Headless 服务器：Docker（linux/amd64 · linux/arm64）

```bash
docker pull ghcr.io/{owner}/fluxdown-server:{version}
docker run -d -p 17800:17800 -v fluxdown-data:/data \\
  ghcr.io/{owner}/fluxdown-server:{version}
```

Compose 部署见仓库 `docker/docker-compose.yml`。首次打开 Web 界面时按向导设置访问密钥，或用 `-e FLUXDOWN_TOKEN=...` 预置。
"""
        )
    if "cli" in present:
        sections.append(
            f"""## 📦 命令行 CLI

| 平台 | 资产 |
|---|---|
| Windows x64 / ARM64 | `FluxDown-CLI-{version}-windows-{{x64,arm64}}.zip` |
| macOS Intel / Apple Silicon | `FluxDown-CLI-{version}-macos-{{x64,arm64}}.tar.gz` |
| Linux x64 / ARM64（musl 静态链接，任意发行版可用） | `FluxDown-CLI-{version}-linux-{{x64,arm64}}.tar.gz` |

解压后得到 `fluxdown`（Windows 为 `fluxdown.exe`），放入 PATH 即可使用；也可从源码安装：
`cargo install --git https://github.com/{repo} fluxdown_cli`。

```bash
fluxdown ping                         # 探活（需 FluxDown App / server 运行中）
fluxdown add https://example.com/f.zip
fluxdown ls
```

服务地址经 `--url` 或 `FLUXDOWN_URL`（默认 `http://127.0.0.1:17800`），管理 token 经 `--token` 或 `FLUXDOWN_TOKEN`。
"""
        )
    if "mobile" in present:
        sections.append(
            f"""## 📱 Android

| 设备 | 资产 |
|---|---|
| 不确定架构（通用包） | `FluxDown-{version}-android-universal.apk` |
| 64 位 ARM（绝大多数手机） | `FluxDown-{version}-android-arm64-v8a.apk` |
| 32 位 ARM | `FluxDown-{version}-android-armeabi-v7a.apk` |
| x86_64（模拟器 / Chromebook） | `FluxDown-{version}-android-x86_64.apk` |

⚠️ **从旧版（Flutter 版）升级**：新版 Android 客户端换用了新的签名证书，不能直接覆盖安装旧版（系统会提示「应用未安装」或签名冲突）。
请先在旧版中记下需要保留的下载任务与设置，卸载旧版后再安装新版；卸载会清除旧版的本地数据，已下载到公共目录的文件不受影响。
"""
        )
    return "\n".join(sections)


def compose_body(body: str, present: list[str], version: str, repo: str, owner: str) -> str:
    """去掉旧头部，按当前已发布组件重建头部，其后接原有更新说明。"""
    notes = HEADER_RE.sub("", body).lstrip("\n")
    header = f"<!-- fluxdown:release:begin components={','.join(present)} -->\n"
    sections = install_sections(present, version, repo, owner)
    if sections:
        header += (
            "<details>\n<summary>📦 下载与安装说明（服务器 / CLI / Android）· 文件校验见 SHA256SUMS.txt</summary>\n\n"
            f"{sections}\n</details>\n"
        )
    header += "<!-- fluxdown:release:end -->\n\n"
    return header + notes


def is_highest_stable(tag: str, tags: list[str]) -> bool:
    current = STABLE_TAG_RE.match(tag)
    if not current:
        return False
    versions = [tuple(map(int, m.groups())) for t in tags if (m := STABLE_TAG_RE.match(t))]
    return tuple(map(int, current.groups())) == max(versions, default=())


def write_summary(
    tag: str, expected: list[str], present: list[str], failures: list[str], rehearsal: bool
) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    rows = [
        f"| {LABELS[c]} (`{c}`) | {'✔' if c in expected else ''} | {'✅' if c in present else '—'} |"
        for c in COMPONENTS
    ]
    lines = [
        f"## Release {tag}（演练，未发布）" if rehearsal else f"## Release {tag}",
        "",
        "| 组件 | 本次应发布 | 已发布（完成哨兵） |",
        "|---|---|---|",
        *rows,
    ]
    if failures and rehearsal:
        lines += ["", f"**演练发现未完成：{', '.join(failures)}**。真实发布时这些组件会缺失，需先修复。"]
    elif failures:
        lines += [
            "",
            f"**未完成：{', '.join(failures)}**。修复后补发（源码默认取 tag，修了代码时用 `source_ref` 指定包含修复的分支或提交）：",
            "",
            "```bash",
            *(
                f"gh workflow run release.yml --ref main -f tag={tag} -f component={c.split('(')[0]} [-f source_ref=<branch|sha>]"
                for c in failures
            ),
            "```",
            "",
            "构建源码未变的偶发失败也可直接在本次运行页点 “Re-run failed jobs”。",
        ]
    with open(path, "a", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tag", required=True)
    parser.add_argument("--expected", default="", help="本次应发布的组件，逗号分隔")
    parser.add_argument("--docker-result", default="", help="build-server-docker 的 job result")
    parser.add_argument(
        "--rehearsal-dir", default="", help="演练：从该目录读哨兵与说明，只输出预览，不调用 gh"
    )
    args = parser.parse_args()

    tag: str = args.tag
    version = tag.removeprefix("v")
    repo = os.environ["GITHUB_REPOSITORY"]
    owner = os.environ["GITHUB_REPOSITORY_OWNER"]
    expected = [c for c in args.expected.split(",") if c]
    rehearsal_dir = Path(args.rehearsal_dir) if args.rehearsal_dir else None

    if rehearsal_dir:
        # 演练：prepare-release 未建草稿，按「草稿 + 本次生成的说明 + 本次产出的哨兵」模拟
        rehearsal_dir.mkdir(parents=True, exist_ok=True)
        notes_path = rehearsal_dir / "RELEASE_NOTES.md"
        info = {
            "assets": [{"name": p.name} for p in rehearsal_dir.glob("SHA256SUMS-*.txt")],
            "body": notes_path.read_text(encoding="utf-8") if notes_path.exists() else "",
            "isDraft": True,
        }
    else:
        info = json.loads(gh("release", "view", tag, "--json", "assets,body,isDraft"))
    names = {a["name"] for a in info["assets"]}
    present = [c for c in COMPONENTS if f"SHA256SUMS-{c}.txt" in names]

    if present and rehearsal_dir:
        merged = merge_checksums(
            [p.read_text(encoding="utf-8") for p in sorted(rehearsal_dir.glob("SHA256SUMS-*.txt"))]
        )
        (rehearsal_dir / "SHA256SUMS.txt").write_text(merged, encoding="utf-8")
    elif present:
        with tempfile.TemporaryDirectory() as tmp:
            gh("release", "download", tag, "-p", "SHA256SUMS-*.txt", "-D", tmp, "--clobber")
            merged = merge_checksums(
                [p.read_text(encoding="utf-8") for p in sorted(Path(tmp).glob("SHA256SUMS-*.txt"))]
            )
            total = Path(tmp) / "SHA256SUMS.txt"
            total.write_text(merged, encoding="utf-8")
            gh("release", "upload", tag, str(total), "--clobber")

    edit_args: list[str] = []
    old_body = info.get("body") or ""
    new_body = compose_body(old_body, present, version, repo, owner)
    notes_dir = rehearsal_dir or Path(os.environ.get("RUNNER_TEMP", tempfile.gettempdir()))
    notes_file = notes_dir / "release-body.md"
    if new_body != old_body:
        notes_file.write_text(new_body, encoding="utf-8")
        edit_args += ["--notes-file", str(notes_file)]

    tags = subprocess.run(
        ["git", "tag", "-l", "v*"], check=True, text=True, capture_output=True
    ).stdout.split()
    if tag not in tags:
        tags.append(tag)  # 演练的 tag 尚不存在；真实发布时它已在列表中，此处无变化
    should_latest = "app" in present and is_highest_stable(tag, tags)
    if info["isDraft"]:
        if present:
            # 草稿首次发布时 GitHub 默认抢占 latest，必须显式给值
            edit_args += ["--draft=false", f"--latest={'true' if should_latest else 'false'}"]
    elif should_latest:
        edit_args += ["--latest"]

    if edit_args and rehearsal_dir:
        print(f"🧪 rehearsal: would run gh release edit {tag} {' '.join(edit_args)}")
    elif edit_args:
        gh("release", "edit", tag, *edit_args)

    failures = [c for c in expected if c not in present]
    if "server" in expected and args.docker_result not in ("success", "skipped", ""):
        failures.append("server(docker)")
    write_summary(tag, expected, present, failures, rehearsal_dir is not None)

    state = "draft" if info["isDraft"] and not present else "published"
    if rehearsal_dir:
        state = f"rehearsal (would be {state})"
    print(f"{tag}: {state}; components={','.join(present) or '<none>'}; latest={should_latest}")
    if not present:
        print(f"::error::{tag} 没有任何组件上传完成，release 保持草稿")
        return 1
    if failures:
        hint = "演练结果见 job summary" if rehearsal_dir else "见 job summary 的补发命令"
        print(f"::error::{tag} 未完成的组件：{', '.join(failures)}（{hint}）")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
