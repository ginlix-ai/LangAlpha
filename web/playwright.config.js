import { defineConfig } from '@playwright/test';

// Use a dedicated port so E2E tests never collide with the user's dev server
// on :5173 (which might have real Supabase env vars) or other local services.
const E2E_PORT = 5176;
// The auth surface only exists in platform mode, and the mode is fixed when the
// dev server boots, not per test. So it gets a server of its own rather than a
// flag some spec could flip.
const E2E_AUTH_PORT = 5177;
// PERF_BUILD only means anything under PERF: the benchmarks are the one caller
// that wants a production build, and a PERF_BUILD left in the shell must not
// make an ordinary e2e run wait four minutes for a vite build first.
const PERF_BUILD = !!process.env.PERF && !!process.env.PERF_BUILD;

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
      // PERF=1 PERF_BUILD=1 serves a production build instead of the dev server,
      // so the smoothness benchmark (e2e/perf) measures shipped code rather than
      // the dev JSX runtime and HMR client. Everything else is identical.
      command: PERF_BUILD
        ? `npx vite build --outDir dist-perf && npx vite preview --port ${E2E_PORT} --outDir dist-perf`
        : `npm run dev -- --port ${E2E_PORT}`,
      port: E2E_PORT,
      timeout: PERF_BUILD ? 240_000 : 60_000,
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
      command: `npm run dev -- --port ${E2E_AUTH_PORT}`,
      port: E2E_AUTH_PORT,
      reuseExistingServer: false,
      env: {
        VITE_HOST_MODE: 'platform',
        // Non-empty so lib/supabase.ts constructs its client and the login page
        // renders. It resolves to nothing: the one spec that submits answers
        // the send with a route handler, so no request leaves the browser.
        VITE_SUPABASE_URL: 'https://example.supabase.co',
        VITE_SUPABASE_PUBLISHABLE_KEY: 'e2e-placeholder-key',
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
    {
      name: 'chromium',
      testIgnore: /auth-surface\..*\.spec\.js/,
      use: { browserName: 'chromium' },
    },
    {
      name: 'auth-surface',
      testMatch: /auth-surface\..*\.spec\.js/,
      use: { browserName: 'chromium', baseURL: `http://127.0.0.1:${E2E_AUTH_PORT}` },
    },
  ],
});
