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
const requests = new Map<symbol, number>();

// The last account's work no longer holds this tab: a transfer that stalls
// past sign-out would otherwise block the next account's reloads for good.
// Its late release then finds nothing to remove.
registerAuthReset(() => {
  holds.clear();
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
