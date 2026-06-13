import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

// https://vitejs.dev/config/
export default defineConfig(({ mode }) => {
  // Load VITE_-prefixed vars from .env files (.env, .env.local, …) so they can
  // be seeded on disk instead of passed inline. loadEnv also merges matching
  // process.env entries, so `VITE_FOO=bar pnpm dev` keeps working too.
  const env = loadEnv(mode, process.cwd())
  const backendTarget = env.VITE_PROXY_BACKEND || 'http://localhost:8000'

  return {
    base: env.VITE_CDN_BASE || '/',
    plugins: [react()],
    resolve: {
      alias: {
        '@': path.resolve(__dirname, './src'),
      },
    },
    build: {
      rollupOptions: {
        output: {
          manualChunks: {
            'vendor-react': ['react', 'react-dom', 'react-router-dom'],
            'vendor-markdown': ['react-markdown', 'remark-gfm', 'remark-math', 'remark-cjk-friendly', 'rehype-katex', 'rehype-raw', 'katex'],
            'vendor-charts': ['recharts', 'lightweight-charts'],
            'vendor-motion': ['framer-motion'],
            'vendor-dnd': ['@dnd-kit/core', '@dnd-kit/sortable', '@dnd-kit/utilities'],
          },
        },
      },
    },
    server: {
      host: '127.0.0.1',
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
      hmr: env.VITE_HMR_CLIENT_PORT
        ? { clientPort: Number(env.VITE_HMR_CLIENT_PORT), path: '/vite-hmr' }
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
