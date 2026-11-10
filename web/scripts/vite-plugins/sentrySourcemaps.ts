import fs from 'node:fs';
import path from 'node:path';
import { sentryVitePlugin } from '@sentry/vite-plugin';
import type { PluginOption } from 'vite';

// Gives a build that reports to Sentry readable stack traces. Every such build
// (VITE_SENTRY_DSN set) carries debug IDs, which the plugin writes into each
// chunk from its content; one with SENTRY_AUTH_TOKEN also uploads the source
// maps they point at. The IDs go in either way so the token never changes the
// bundle: a build without it would otherwise get another entry hash for the
// same code, and the stale-build check would offer open tabs a reload for it.
//
// The maps are built hidden (no sourceMappingURL), uploaded, then deleted from
// the output here rather than by the plugin, which leaves them when its glob
// misses (another --outDir, a Windows path) and only warns when a delete fails.
// A map still there fails the build. A failed upload only warns: Sentry being
// down must not block a deploy.
//
// The app names its release from the entry's hash at runtime, so the plugin's
// own release (the git HEAD, or "undefined" in an export with no .git) only
// labels the upload, and nothing release-shaped goes into the bundle.
export function sentrySourcemaps(env: Record<string, string>, command: string): PluginOption[] {
  if (command !== 'build' || !env.VITE_SENTRY_DSN) return [];
  const authToken = env.SENTRY_AUTH_TOKEN;
  const org = env.SENTRY_ORG;
  const project = env.SENTRY_PROJECT;
  if (authToken && (!org || !project)) {
    throw new Error('SENTRY_AUTH_TOKEN is set without SENTRY_ORG and SENTRY_PROJECT: the source maps have nowhere to go');
  }
  let outDir = '';
  return [
    {
      name: 'la-sentry-sourcemaps',
      apply: 'build',
      config: () => (authToken ? { build: { sourcemap: 'hidden' } } : undefined),
      configResolved(config) {
        outDir = path.resolve(config.root, config.build.outDir);
      },
      // After every writeBundle, the plugin's upload included.
      closeBundle() {
        if (!authToken) return;
        const left = mapFiles(outDir).filter((file) => {
          try {
            fs.rmSync(file);
            return false;
          } catch {
            return true;
          }
        });
        if (left.length) {
          this.error(`${left.length} source map(s) could not be removed from ${outDir}, e.g. ${left[0]}`);
        }
      },
    },
    ...sentryVitePlugin({
      authToken,
      org,
      project,
      telemetry: false,
      release: { inject: false, create: false, finalize: false, setCommits: false, deploy: false },
      sourcemaps: authToken ? {} : { disable: 'disable-upload' },
      errorHandler: (error) => {
        console.warn(`[sentry] ${error.message}`);
      },
    }),
  ];
}

function mapFiles(dir: string): string[] {
  if (!fs.existsSync(dir)) return [];
  return fs
    .readdirSync(dir, { recursive: true, withFileTypes: true })
    .filter((entry) => entry.isFile() && entry.name.endsWith('.map'))
    .map((entry) => path.join(entry.parentPath, entry.name));
}
