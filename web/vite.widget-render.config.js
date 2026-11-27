import { defineConfig } from 'vite'
import path from 'path'

/**
 * The widget render page: one ShowWidget widget alone, framed as the chat
 * frames it, which the backend loads in headless Chromium to tell the agent
 * what its widget did and show it a screenshot (`pnpm build:widget-render`).
 * A build of its own, so nothing here reaches the app's bundle or its
 * first-load budget.
 *
 * It lands beside the backend service that drives it (gitignored); the
 * backend images build it in a node stage. Relative asset paths, because the
 * driver serves it from disk under an origin of its own. No React: the page
 * only builds the widget's document and frames it. `.env` files are not read,
 * since the page talks to no backend.
 */
export default defineConfig({
  base: './',
  publicDir: false,
  envDir: false,
  resolve: { tsconfigPaths: true },
  build: {
    outDir: process.env.WIDGET_RENDER_OUT_DIR
      || path.resolve(import.meta.dirname, '../src/server/services/widget_render_page'),
    emptyOutDir: true,
    sourcemap: false,
    reportCompressedSize: false,
    chunkSizeWarningLimit: 2000,
    rolldownOptions: {
      input: path.resolve(import.meta.dirname, 'widget-render.html'),
    },
  },
})
