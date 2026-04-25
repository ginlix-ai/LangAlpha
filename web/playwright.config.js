import { defineConfig } from '@playwright/test';

// Use a dedicated port so E2E tests never collide with the user's dev server
// on :5173 (which might have real Supabase env vars) or other local services.
const E2E_PORT = 5176;

export default defineConfig({
  testDir: './e2e',
  timeout: 30_000,
  retries: process.env.CI ? 1 : 0,
  // Serial execution: the mock SSE server is shared state, so parallel
  // workers would clobber each other's scenarios via resetMockServer().
  workers: 1,
  use: {
    baseURL: `http://127.0.0.1:${E2E_PORT}`,
    trace: 'on-first-retry',
  },
  webServer: [
    {
      command: `npm run dev -- --port ${E2E_PORT}`,
      port: E2E_PORT,
      // Always start fresh: the env block below is load-bearing (forces OSS mode
      // and clears Supabase vars). If we reused an existing server, a developer's
      // pre-running `pnpm dev` with personal .env would silently override these.
      reuseExistingServer: false,
      env: {
        // Force OSS mode so the Supabase auth branch never runs, regardless of
        // what's in the developer's local .env. VITE_HOST_MODE is the single
        // source of truth for mode selection (see web/src/config/hostMode.ts).
        VITE_HOST_MODE: 'oss',
        VITE_SUPABASE_URL: '',
        VITE_SUPABASE_PUBLISHABLE_KEY: '',
        VITE_API_BASE_URL: 'http://127.0.0.1:4100',
      },
    },
    {
      command: 'node e2e/mock-sse-server.js',
      port: 4100,
      reuseExistingServer: !process.env.CI,
    },
  ],
  projects: [
    { name: 'chromium', use: { browserName: 'chromium' } },
  ],
});
