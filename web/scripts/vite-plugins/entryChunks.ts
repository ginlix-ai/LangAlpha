import type { Rollup } from 'vite';

/** The chunks the browser runs on load, selected by `isEntry` and not by name. */
export function entryChunks(bundle: Rollup.OutputBundle): Rollup.OutputChunk[] {
  return Object.values(bundle).filter((c): c is Rollup.OutputChunk => c.type === 'chunk' && c.isEntry);
}
