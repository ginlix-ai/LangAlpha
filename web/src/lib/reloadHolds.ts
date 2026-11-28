/**
 * What the stale-build reload a section switch takes has to respect. A hold is
 * work no reload would keep and no short wait would see through (a download
 * being prepared, an upload, a dialog's state): the reload skips that switch.
 * A request is the page talking to the server, mostly for a moment: the reload
 * waits a little for those to settle, so a write the page being left still has
 * out lands first. Kept apart from `staleBuild` so the transfers and the API
 * client can report without importing the toast it raises.
 */
import { registerAuthReset } from './authResets';

const holds = new Set<symbol>();
const leaveHolds = new Set<symbol>();
const requests = new Map<symbol, number>();

// The last account's work no longer holds this tab: a transfer that stalls
// past sign-out would otherwise block the next account's reloads, and ask
// before it leaves the page, for good. Its late release then finds nothing
// to remove.
registerAuthReset(() => {
  holds.clear();
  leaveHolds.clear();
  window.removeEventListener('beforeunload', askBeforeLeaving);
  requests.clear();
});

/** Hold the reload until the returned release is called; a second call does nothing. */
export function holdReload(): () => void {
  const hold = Symbol('reload-hold');
  holds.add(hold);
  return () => {
    holds.delete(hold);
  };
}

function askBeforeLeaving(e: BeforeUnloadEvent): void {
  e.preventDefault();
  // Older browsers ask only when a value is set; its text is never shown.
  e.returnValue = '';
}

/**
 * Hold the reload and the page until the returned release is called; a second
 * call does nothing. For work a reload or a closed tab would lose: the
 * stale-build reload waits, and the browser asks before the page goes, while
 * any hold is out. In-app navigation keeps the work, so it is not asked about.
 */
export function holdPageLeave(): () => void {
  const releaseReload = holdReload();
  const hold = Symbol('leave-hold');
  leaveHolds.add(hold);
  window.addEventListener('beforeunload', askBeforeLeaving);
  return () => {
    releaseReload();
    if (leaveHolds.delete(hold) && leaveHolds.size === 0) {
      window.removeEventListener('beforeunload', askBeforeLeaving);
    }
  };
}

export function reloadHeld(): boolean {
  return holds.size > 0;
}

/**
 * Report a request, sent as the account of `generation` (`authGeneration`),
 * until the returned settle is called; a second call does nothing.
 */
export function trackRequest(generation: number): () => void {
  const request = Symbol('request');
  requests.set(request, generation);
  return () => {
    requests.delete(request);
  };
}

/**
 * Whether a request sent as the account of `generation` is out. One the last
 * account left behind waits on nobody: a save a sign-out flushed as its page
 * unmounted starts after the reset, and stalled, it would stop every reload
 * the next account's switches take.
 */
export function requestsInFlight(generation: number): boolean {
  for (const sentAs of requests.values()) if (sentAs === generation) return true;
  return false;
}
