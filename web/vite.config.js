import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

const backendTarget = process.env.VITE_PROXY_BACKEND || 'http://localhost:8000'

// https://vitejs.dev/config/
export default defineConfig({
  base: process.env.VITE_CDN_BASE || '/',
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
})
