import type { Plugin } from 'vite';
import { entryChunks } from './entryChunks.ts';

// Keeps the entry chunk out of every lazy import's preload list. The `$initial`
// group in chunking.ts lives in the entry, so most route chunks import from it
// and Vite lists it among their deps. Its preload helper skips a dep that
// already has a <link>, but the entry arrived by <script>, so the helper appends
// a modulepreload for a module that is already running. Firefox 146 fires `error`
// on that link, and index.html's stale-build listener reads it as a dead build.
export function skipEntryPreload(): Plugin {
  const entries = new Set<string>();
  return {
    name: 'la-skip-entry-preload',
    apply: 'build',
    config: () => ({
      build: {
        modulePreload: {
          resolveDependencies: (_file, deps) => deps.filter((dep) => !entries.has(dep)),
        },
      },
    }),
    // Ahead of vite:build-import-analysis, whose generateBundle is what calls
    // resolveDependencies.
    generateBundle: {
      order: 'pre',
      handler(_options, bundle) {
        entries.clear();
        for (const chunk of entryChunks(bundle)) entries.add(chunk.fileName);
      },
    },
  };
}
