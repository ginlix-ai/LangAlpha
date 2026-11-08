/**
 * Runs `run` once the main thread is idle, or after `timeout` ms whatever it
 * is doing. A browser with no idle callback (Safari) waits `fallbackMs`
 * instead. Returns its cancel.
 */
export function whenIdle(run: () => void, { timeout, fallbackMs }: { timeout: number; fallbackMs: number }): () => void {
  if (typeof window.requestIdleCallback === 'function') {
    const id = window.requestIdleCallback(run, { timeout });
    return () => window.cancelIdleCallback(id);
  }
  const id = window.setTimeout(run, fallbackMs);
  return () => window.clearTimeout(id);
}
