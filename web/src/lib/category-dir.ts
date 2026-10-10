// 分类保存目录名净化与拼接（纯函数，无 i18n / 传输依赖）。
//
// 桌面镜像：`crates/settings/src/sections/categories.rs` 的 sanitizeCategoryDirName /
// categoryDirUnder 同名函数。
// 同一台机器上桌面与 Web 一键出来的目录必须逐字一致，改一处就要改另一处。

// 控制字符是有意匹配的：文件系统不接受它们，与桌面同规剔除。
// eslint-disable-next-line no-control-regex
const INVALID_DIR_CHARS = /[\\/:*?"<>|\u0000-\u001f]/g

/** 分类显示名 → 目录名：非法字符换空格、压缩空白、去掉 Windows 会丢弃的结尾点/空格。 */
export function sanitizeCategoryDirName(label: string): string {
  let out = label.replace(INVALID_DIR_CHARS, ' ').replace(/\s+/g, ' ').trim()
  while (out !== '' && (out.endsWith('.') || out.endsWith(' '))) out = out.slice(0, -1)
  return out
}

/** 目标机器的路径分隔符。宿主可能是 Linux 服务器而浏览器在 Windows，只能从目录本身反推。 */
function separatorOf(base: string): string {
  return /^[a-zA-Z]:[\\/]/.test(base) || (base.includes('\\') && !base.includes('/')) ? '\\' : '/'
}

/** 「默认下载目录 / 分类名」。目录为空或分类名净化后为空时返回 ''（调用方跳过）。 */
export function categoryDirUnder(baseDir: string, label: string): string {
  let root = baseDir.trim()
  if (root === '') return ''
  const folder = sanitizeCategoryDirName(label)
  if (folder === '') return ''
  const sep = separatorOf(root)
  while (root.length > 1 && (root.endsWith('/') || root.endsWith('\\'))) root = root.slice(0, -1)
  // 根目录（"/" 或 "\"）本身就带分隔符，直接拼名字。
  if (root.endsWith('/') || root.endsWith('\\')) return `${root}${folder}`
  return `${root}${sep}${folder}`
}
