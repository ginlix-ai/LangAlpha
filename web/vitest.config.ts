import { defineConfig } from 'vitest/config';
import { reactPlugins } from './scripts/react-plugins.ts';
import { monacoVersionDefine } from './scripts/monaco-version.ts';

// Date tests pin local-day boundaries, and on a UTC host (CI) the local and the
// UTC date agree, so a bug that mixes them up passes there. A zone west of UTC
// splits them. An explicit TZ still wins, to try another zone.
process.env.TZ ||= 'America/Los_Angeles';

const TEST_FILES = ['src/**/*.test.{ts,tsx}'];
const EXCLUDE = ['e2e/**', 'node_modules/**'];

// jsdom files a vm pool cannot run. There the global is a real jsdom Window:
// `window` and `location` cannot be redefined on it, and it lacks Node globals
// such as ReadableStream that forks keep beside jsdom.
const FORKS_FILES = [
  // Replace window.location to watch a full-page navigation.
  'src/pages/Login/__tests__/AuthConfirm.handoff.test.tsx',
  'src/pages/Plugins/__tests__/McpServers.test.tsx',
  'src/pages/Plugins/__tests__/brokerageSurface.test.tsx',
  'src/pages/ChatAgent/components/mcp/__tests__/McpTab.test.tsx',
  'src/lib/__tests__/setupReturn.test.ts',
  'src/pages/Setup/__tests__/SetupWizard.return.test.tsx',
  'src/pages/Setup/steps/__tests__/connectReturn.test.tsx',
  'src/components/__tests__/HostedSignInGate.test.tsx',
  'src/pages/Onboarding/__tests__/GettingStarted.desktop.test.tsx',
  'src/pages/Login/__tests__/AuthConfirm.hostedSignIn.test.tsx',
  // Replaces `window` itself to install the desktop bridge.
  'src/lib/__tests__/desktop.test.ts',
  // Need ReadableStream: two load jsdom themselves (its undici reads it at
  // import), the other builds an SSE body from one.
  'src/lib/__tests__/staleBuildPreBoot.test.ts',
  'src/lib/__tests__/localePreload.test.ts',
  'src/pages/ChatAgent/hooks/__tests__/useWarmWorkspaceSandbox.test.tsx',
];

export default defineConfig({
  plugins: reactPlugins(),
  define: monacoVersionDefine(import.meta.dirname),
  test: {
    globals: true,
    // A vm worker keeps every file's module graph until its heap reaches this,
    // then restarts. The default is total memory / workers, which on a 16 GB,
    // 4-CPU CI runner lets three workers grow toward all 16 GB. At 1 GB a
    // 3-worker run peaks near 4 GB (7 GB uncapped) at the same wall time.
    // Read from the root config only.
    vmMemoryLimit: '1GB',
    projects: [
      {
        extends: true,
        test: {
          // A vm worker loads jsdom and compiles each module once, then gives
          // every file a fresh context. Forks pay both again per file.
          name: 'jsdom',
          environment: 'jsdom',
          pool: 'vmThreads',
          include: TEST_FILES,
          exclude: [...EXCLUDE, ...FORKS_FILES],
          setupFiles: ['./src/test/setup.ts'],
        },
      },
      {
        extends: true,
        test: {
          // Named to sort first: Vitest queues projects by name, and started
          // last these files ran up to 5x slower and set the run's tail.
          name: 'forks',
          environment: 'jsdom',
          pool: 'forks',
          include: FORKS_FILES,
          setupFiles: ['./src/test/setup.ts'],
        },
      },
    ],
  },
  resolve: {
    tsconfigPaths: true,
  },
});
