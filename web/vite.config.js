import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

// Shared by the entry and the lazy vendors — see manualChunks below.
const EAGER_SHARED = new Set(['clsx', 'use-sync-external-store'])
const MARKDOWN = new Set([
  'react-markdown', 'remark-gfm', 'remark-math', 'remark-cjk-friendly',
  'rehype-katex', 'rehype-raw', 'katex',
])
const CHARTS = new Set(['recharts', 'lightweight-charts'])

// Emits dist/version.json holding this build's entry chunk filename — the identity
// the running app polls to notice it is a build the server no longer serves.
//
// Content-derived on purpose: the value is the entry's content hash, so a rebuild
// that changes nothing produces the same id and raises no spurious "new version"
// prompt. A timestamp or git sha would fire on every rebuild.
//
// generateBundle, not writeBundle: the hashed fileName is final by this hook, and
// emitFile puts the result through Rollup's own output pipeline. Selecting by
// `isEntry` and not by name is the load-bearing part — manualChunks below also emits
// vendor-* chunks, and `index` is a name a chunking change could quietly move.
function emitVersionManifest() {
  return {
    name: 'la-version-manifest',
    apply: 'build',
    generateBundle(_options, bundle) {
      const entries = Object.values(bundle).filter((c) => c.type === 'chunk' && c.isEntry)
      // Zero means the selector went stale; more than one means a second entry
      // appeared and "the build" is no longer a single identity. Either way the
      // manifest would be wrong, and a wrong build id is worse than none: the
      // client would prompt for a reload that changes nothing.
      if (entries.length !== 1) {
        this.error(
          `version.json needs exactly 1 entry chunk, found ${entries.length}` +
            `${entries.length ? `: ${entries.map((c) => c.fileName).join(', ')}` : ''}`,
        )
      }
      this.emitFile({
        type: 'asset',
        fileName: 'version.json',
        source: `${JSON.stringify({ build: entries[0].fileName.split('/').pop() })}\n`,
      })
    },
  }
}

// https://vitejs.dev/config/
export default defineConfig(({ mode }) => {
  // Load VITE_-prefixed vars from .env files (.env, .env.local, …) so they can
  // be seeded on disk instead of passed inline. loadEnv also merges matching
  // process.env entries, so `VITE_FOO=bar pnpm dev` keeps working too.
  const env = loadEnv(mode, process.cwd())
  const backendTarget = env.VITE_PROXY_BACKEND || 'http://localhost:8000'
  // Only honor a well-formed port; a non-numeric VITE_HMR_CLIENT_PORT would
  // otherwise yield NaN and silently break the HMR socket.
  const hmrClientPort = Number(env.VITE_HMR_CLIENT_PORT)
  const hasHmrClientPort = Number.isFinite(hmrClientPort) && hmrClientPort > 0
  // Extra Host headers the dev server accepts, for tunnels (ngrok, Cloudflare)
  // that front it under their own hostname. Comma-separated — kept in .env so a
  // tunnel host never needs a local edit to this file.
  const allowedHosts = (env.VITE_DEV_ALLOWED_HOSTS || '')
    .split(',')
    .map((host) => host.trim())
    .filter(Boolean)

  return {
    base: env.VITE_CDN_BASE || '/',
    plugins: [react(), emitVersionManifest()],
    resolve: {
      alias: {
        '@': path.resolve(__dirname, './src'),
        // Fixtures a unit test shares with the Playwright specs. Kept in step
        // with vitest.config.ts; nothing in the app graph imports it.
        '@e2e': path.resolve(__dirname, './e2e'),
      },
    },
    build: {
      rollupOptions: {
        // Explicit single entry: dev-only harness pages (e.g. intro-preview.html)
        // must never ship in the production build, even if a future Vite version
        // or multi-page config change starts picking up root .html files.
        input: path.resolve(__dirname, 'index.html'),
        output: {
          // Vendors get a pinned chunk so app deploys don't re-invalidate them.
          //
          // The order below is load-bearing. `clsx` and `use-sync-external-store`
          // are imported by both the entry and the lazy vendors; left to Rollup
          // they land in a lazy vendor chunk, and the entry then has to preload
          // that whole chunk to reach 3 kB — which is how 170 kB of charts sat on
          // the critical path for five months. Claiming them for an already-eager
          // chunk first keeps the lazy vendors genuinely lazy.
          // Enforced by scripts/check-critical-path.mjs.
          manualChunks(id) {
            if (!id.includes('node_modules')) return
            const tail = id.split(/[\\/]node_modules[\\/]/).pop().split(/[\\/]/)
            const pkg = tail[0].startsWith('@') ? `${tail[0]}/${tail[1]}` : tail[0]
            if (EAGER_SHARED.has(pkg)) return 'vendor-react'
            if (['react', 'react-dom', 'react-router-dom'].includes(pkg)) return 'vendor-react'
            if (pkg === 'framer-motion') return 'vendor-motion'
            if (pkg.startsWith('@dnd-kit')) return 'vendor-dnd'
            if (MARKDOWN.has(pkg)) return 'vendor-markdown'
            if (CHARTS.has(pkg)) return 'vendor-charts'
          },
        },
      },
    },
    server: {
      host: '127.0.0.1',
      // Unset leaves Vite's default host checking in place.
      allowedHosts: allowedHosts.length ? allowedHosts : undefined,
      // In Docker on macOS the bind mount doesn't forward fsevents, so Vite's
      // watcher silently dies and HMR stops (edits don't hot-reload; a reload
      // can even serve the stale transform). Enable polling ONLY when
      // CHOKIDAR_USEPOLLING=true (set by docker-compose for the containerized
      // dev server) — native `pnpm dev` stays event-based with no CPU overhead.
      watch: process.env.CHOKIDAR_USEPOLLING === 'true'
        ? { usePolling: true, interval: 100 }
        : undefined,
      // When served behind the nginx dev proxy (oss.localhost etc.), the HMR
      // WebSocket must dial the proxy port, not the Vite port. Seed
      // VITE_HMR_CLIENT_PORT (e.g. =80) in .env.local, or pass it inline.
      // Unset leaves Vite's default HMR behavior untouched (dev-only; ignored
      // by `vite build`).
      //
      // `path` moves the HMR socket off "/" so it doesn't collide with the
      // proxy's `location = /` session redirect (/ → /home|/app), which would
      // 302 the upgrade and surface as "WebSocket closed without opened". The
      // non-root path falls through nginx's `location /` to this Vite server.
      hmr: hasHmrClientPort
        ? { clientPort: hmrClientPort, path: '/vite-hmr' }
        : undefined,
      proxy: {
        '/api/v1': {
          target: backendTarget,
          changeOrigin: true,
        },
        '/ws/v1': {
          target: backendTarget.replace(/^http/, 'ws'),
          ws: true,
        },
      },
      cors: true,
    },
  }
})
