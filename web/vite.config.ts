import { fileURLToPath, URL } from 'node:url'
import { defineConfig } from 'vite'
import react, { reactCompilerPreset } from '@vitejs/plugin-react'
import babel from '@rolldown/plugin-babel'
import tailwindcss from '@tailwindcss/vite'

const repoRoot = fileURLToPath(new URL('..', import.meta.url))
const AGENT = 'http://127.0.0.1:17800'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), babel({ presets: [reactCompilerPreset()] }), tailwindcss()],
  resolve: {
    alias: {
      // 与 GPUI 共享的资源与主题实现：直接从仓库根引用，不复制。
      '@i18n-assets': fileURLToPath(new URL('../assets/i18n', import.meta.url)),
      '@gpui-theme': fileURLToPath(new URL('../website-v2/src/lib/gpui-theme', import.meta.url)),
      // 内置文件图标包与扩展名 → 大类表（GPUI `crates/icon_pack` 编译期嵌入同一批文件）。
      '@icon-packs': fileURLToPath(new URL('../assets/icon-packs', import.meta.url)),
    },
  },
  server: {
    fs: { allow: [repoRoot] },
    // dev 同源代理到 fluxdown-agent（server 模式，默认 17800）。
    proxy: {
      '/rpc': { target: AGENT, changeOrigin: false, ws: true },
      '/api': { target: AGENT, changeOrigin: false },
      '/ping': { target: AGENT, changeOrigin: false },
      '/demo': { target: AGENT, changeOrigin: false },
    },
  },
})
