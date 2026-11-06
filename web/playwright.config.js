import { defineConfig } from '@playwright/test';

// Use a dedicated port so E2E tests never collide with the user's dev server
// on :5173 (which might have real Supabase env vars) or other local services.
// Each port takes an env override so two checkouts can run the suite at once.
const E2E_PORT = Number(process.env.E2E_PORT) || 5176;
// The auth surface only exists in platform mode, and the mode is fixed when the
// app is served, not per test. So it gets a server of its own rather than a
// flag some spec could flip.
const E2E_AUTH_PORT = Number(process.env.E2E_AUTH_PORT) || 5177;
// The API origin both apps bake in. Nothing listens on it: each worker starts
// its own mock server on the next ports up (E2E_MOCK_PORT + 1 + its parallel
// index) and e2e/fixtures.js routes the page's API traffic there, so a checkout
// needs E2E_MOCK_PORT through E2E_MOCK_PORT + workers free.
const E2E_MOCK_PORT = Number(process.env.E2E_MOCK_PORT) || 4100;
// Written back resolved so e2e/fixtures.js, read in each worker, gets the same port.
process.env.E2E_MOCK_PORT = String(E2E_MOCK_PORT);
const CI = !!process.env.CI;
const PERF = !!process.env.PERF;
// PERF_BUILD only means anything under PERF: the benchmarks are the one caller
// that files its numbers under the kind of bundle it measured, and a PERF_BUILD
// left in the shell must not change what an ordinary run serves.
const PERF_BUILD = PERF && !!process.env.PERF_BUILD;
// CI serves both apps from production builds. A dev server compiles each module
// the first time a page asks for it, so a cold page waits on the compile and
// every worker competes with the server for the same CPUs: on the CI image at
// 4 CPUs the suite took 15.4 min on dev servers and 8.5 on builds. Locally the
// dev server stays the default, since a spec being written runs against the
// code as edited with no build in between; E2E_BUILD=1 opts in, E2E_BUILD=0
// opts CI out. The benchmarks keep to PERF_BUILD.
const E2E_BUILD = !PERF && (process.env.E2E_BUILD ? process.env.E2E_BUILD !== '0' : CI);

/**
 * Serves the app on `port`: the dev server, or a production build into
 * `outDir` served by `vite preview`. Each app builds into its own directory,
 * since the env it bakes in differs.
 *
 * @param {number} port
 * @param {string | null} outDir
 * @param {string} [buildArgs]
 */
function serve(port, outDir, buildArgs = '') {
  if (!outDir) return { command: `npm run dev -- --port ${port}`, timeout: 60_000 };
  return {
    command: `npx vite build --outDir ${outDir} --logLevel error${buildArgs}`
      + ` && npx vite preview --strictPort --port ${port} --outDir ${outDir}`,
    // Playwright starts the servers one after the other, so each build has
    // its own budget, and a 4-vCPU runner builds several times slower than
    // a laptop.
    timeout: 300_000,
  };
}

const appServer = PERF_BUILD
  // So the smoothness benchmark (e2e/perf) measures shipped code rather than
  // the dev JSX runtime and HMR client. Everything else is identical.
  ? serve(E2E_PORT, 'dist-perf', process.env.PERF_UNMINIFIED ? ' --minify false' : '')
  : serve(E2E_PORT, E2E_BUILD ? 'dist-e2e/oss' : null);
const authServer = serve(E2E_AUTH_PORT, E2E_BUILD ? 'dist-e2e/auth' : null);

/** Origins a dev server answers, which e2e/global-setup.js warms. */
const devServerOrigins = [
  !PERF_BUILD && !E2E_BUILD && `http://127.0.0.1:${E2E_PORT}`,
  !E2E_BUILD && `http://127.0.0.1:${E2E_AUTH_PORT}`,
].filter(Boolean);

export default defineConfig({
  testDir: './e2e',
  // Runs after the web servers are up: compiles the app once on each dev server
  // before workers race to. A build has nothing left to compile.
  globalSetup: './e2e/global-setup.js',
  metadata: { devServerOrigins },
  timeout: 30_000,
  retries: CI ? 1 : 0,
  // Workers never share scenario state (each owns a mock server, and every
  // test sets the scenario it reads), so tests run in parallel one by one, not
  // a file at a time: a few long files would otherwise finish the run alone.
  fullyParallel: true,
  // CI pins the count rather than take Playwright's half of the cores, which
  // in a container is half of the host's, not of the CPUs it may use. On the
  // CI image held to 4 CPUs, four workers ran the suite in about two thirds
  // of the time two took (3 runs each, retries off, none failing on four).
  // The benchmarks stay alone: a second browser on the same CPUs would move
  // the frame timings they measure.
  workers: PERF ? 1 : CI ? 4 : undefined,
  // CI keeps its console output and also writes the report the workflow uploads.
  reporter: CI ? [['dot'], ['html', { open: 'never' }]] : 'list',
  use: {
    baseURL: `http://127.0.0.1:${E2E_PORT}`,
    trace: 'on-first-retry',
  },
  webServer: [
    {
      ...appServer,
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
        VITE_API_BASE_URL: `http://127.0.0.1:${E2E_MOCK_PORT}`,
      },
    },
    {
      ...authServer,
      port: E2E_AUTH_PORT,
      reuseExistingServer: false,
      env: {
        VITE_HOST_MODE: 'platform',
        // Non-empty so lib/supabase.ts constructs its client and the login page
        // renders. It resolves to nothing: the one spec that submits answers
        // the send with a route handler, so no request leaves the browser.
        VITE_SUPABASE_URL: 'https://example.supabase.co',
        VITE_SUPABASE_PUBLISHABLE_KEY: 'e2e-placeholder-key',
        // Set, as a deployment that publishes a privacy policy sets it, so the
        // sign-in sentence carries both of its links and the tab walk lands on
        // each. Nothing follows it.
        VITE_PRIVACY_URL: 'https://example.com/privacy',
        VITE_API_BASE_URL: `http://127.0.0.1:${E2E_MOCK_PORT}`,
      },
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
