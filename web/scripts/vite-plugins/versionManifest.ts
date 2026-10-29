import type { Plugin } from 'vite';
import { entryChunks } from './entryChunks.ts';

// Emits dist/version.json holding this build's entry chunk filename — the identity
// the running app polls to notice it is a build the server no longer serves.
//
// Content-derived on purpose: the value is the entry's content hash, so a rebuild
// that changes nothing produces the same id and raises no spurious "new version"
// prompt. A timestamp or git sha would fire on every rebuild.
//
// generateBundle, not writeBundle: the hashed fileName is final by this hook, and
// emitFile puts the result through the bundler's own output pipeline. Selecting by
// `isEntry` and not by name is the load-bearing part — codeSplitting also emits
// vendor-* chunks, and `index` is a name a chunking change could quietly move.
export function versionManifest(): Plugin {
  return {
    name: 'la-version-manifest',
    apply: 'build',
    generateBundle(_options, bundle) {
      const entries = entryChunks(bundle);
      // Zero means the selector went stale; more than one means a second entry
      // appeared and "the build" is no longer a single identity. Either way the
      // manifest would be wrong, and a wrong build id is worse than none: the
      // client would prompt for a reload that changes nothing.
      if (entries.length !== 1) {
        this.error(
          `version.json needs exactly 1 entry chunk, found ${entries.length}` +
            `${entries.length ? `: ${entries.map((c) => c.fileName).join(', ')}` : ''}`,
        );
      }
      this.emitFile({
        type: 'asset',
        fileName: 'version.json',
        source: `${JSON.stringify({ build: entries[0].fileName.split('/').pop() })}\n`,
      });
    },
  };
}
