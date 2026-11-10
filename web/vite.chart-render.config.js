import { defineConfig } from 'vite'
import path from 'path'
import { reactPlugins } from './scripts/react-plugins.ts'

/**
 * The chart render page: the app's MarketChart alone, which chartkit loads in
 * the sandbox's headless Chromium to snapshot a chart before it is published
 * (`pnpm build:chart-render`). A build of its own, so nothing here reaches the
 * app's bundle or its first-load budget.
 *
 * It lands beside chartkit's Python (gitignored), which ships it into every
 * sandbox with the internal packages. Relative asset paths, because the driver
 * serves it from disk under an origin of its own. `.env` files are not read:
 * the page talks to no backend (every request is answered by the driver), and
 * oss mode keeps the auth client out.
 */
export default defineConfig({
  base: './',
  // The app's public/ files (logos, headers) have no use in a chart.
  publicDir: false,
  envDir: false,
  define: {
    'import.meta.env.VITE_HOST_MODE': JSON.stringify('oss'),
    'import.meta.env.VITE_API_BASE_URL': JSON.stringify(''),
  },
  plugins: [...reactPlugins()],
  resolve: { tsconfigPaths: true },
  build: {
    outDir: process.env.CHART_RENDER_OUT_DIR
      || path.resolve(import.meta.dirname, '../libs/chartkit/chartkit/render'),
    emptyOutDir: true,
    // Loaded from disk into one page: a source map would only add weight to every sandbox.
    sourcemap: false,
    reportCompressedSize: false,
    // One page loaded from disk, never over a network: chunk size is not a cost here.
    chunkSizeWarningLimit: 2000,
    rolldownOptions: {
      input: path.resolve(import.meta.dirname, 'chart-render.html'),
    },
  },
})
