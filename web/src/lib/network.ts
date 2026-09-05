/**
 * Browser connectivity, for non-React callers.
 *
 * `navigator.onLine` is a soft signal: `false` is reliable, `true` only means
 * the OS sees a link, not that anything is reachable. That asymmetry is what
 * makes it worth acting on here — we use it to decide when NOT to bother
 * retrying, never to claim a request would have succeeded.
 *
 * SSR-safe: reports online when `navigator` is undefined, so a build-time
 * render never takes the offline branch.
 */
export function isOnline(): boolean {
  return typeof navigator === 'undefined' ? true : navigator.onLine;
}

/**
 * Resolve once the browser reports a link again, or `false` if `timeoutMs`
 * elapses first. Resolves immediately when already online.
 *
 * Bounded on purpose: an unbounded wait would hold a turn in "reconnecting"
 * forever for someone who closed their laptop lid and walked away.
 */
export function waitForOnline(timeoutMs: number): Promise<boolean> {
  if (isOnline()) return Promise.resolve(true);
  if (typeof window === 'undefined') return Promise.resolve(true);

  return new Promise((resolve) => {
    let done = false;
    const finish = (value: boolean) => {
      if (done) return;
      done = true;
      window.removeEventListener('online', onOnline);
      clearTimeout(timer);
      resolve(value);
    };
    const onOnline = () => finish(true);
    const timer = setTimeout(() => finish(false), Math.max(0, timeoutMs));
    window.addEventListener('online', onOnline);
  });
}
