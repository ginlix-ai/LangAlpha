import { defineConfig } from 'vitest/config';
import react from '@vitejs/plugin-react';
import path from 'path';

export default defineConfig({
  plugins: [react()],
  test: {
    environment: 'jsdom',
    setupFiles: ['./src/test/setup.ts'],
    globals: true,
    exclude: ['e2e/**', 'node_modules/**'],
    // 10s default. Worker pool is CPU-contended when tests that use real
    // react-query retry timers (up to 6s wall clock in useWorkspaceFiles)
    // land on the same thread as Testing Library render()s — the default 5s
    // starved the settings-atoms tests into spurious timeouts.
    testTimeout: 10_000,
  },
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
});
