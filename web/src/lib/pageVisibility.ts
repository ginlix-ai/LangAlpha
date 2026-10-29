/**
 * Whether what the page applies now happens in front of the reader.
 *
 * A hidden page is not watched, and neither is what it held back while hidden
 * and applies on its return. Both land settled: framer skips its animations
 * (lib/framer) and the typewriter shows the text at once (animated-text). Work
 * held back while hidden runs from `onPageReturn`, while the page still counts
 * as unseen, so the first frame back already shows what it brings, settled.
 */
const returns = new Set<() => void>();
const changes = new Set<() => void>();
let returning = false;

function onVisibilityChange(): void {
  if (!document.hidden) {
    returning = true;
    try {
      for (const run of [...returns]) run();
    } finally {
      returning = false;
    }
  }
  for (const change of [...changes]) change();
}

if (typeof document !== 'undefined') document.addEventListener('visibilitychange', onVisibilityChange);

export function isPageUnseen(): boolean {
  return returning || document.hidden;
}

/** Runs `run` each time the page comes back, before it counts as seen. */
export function onPageReturn(run: () => void): () => void {
  returns.add(run);
  return () => {
    returns.delete(run);
  };
}

/** Runs `change` after every change to `isPageUnseen()`. */
export function onPageUnseenChange(change: () => void): () => void {
  changes.add(change);
  return () => {
    changes.delete(change);
  };
}
