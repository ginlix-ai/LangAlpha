/**
 * How long the GoTrue token endpoint is held closed after a rate limit.
 *
 * Its own module because two unrelated things need it and neither should have
 * to import the other: `authFetch` opens the window when a 429 arrives, and
 * `authToken` reads it to decide whether asking for a rotation is worth a
 * caller's time. Routing that read through the fetch wrapper made the token
 * cache depend on a module it otherwise has no business knowing about.
 *
 * Timed on `performance.now()`, deliberately, not on the wall clock. A window
 * is a duration, and the wall clock is the instrument this whole fix exists
 * because it cannot be trusted. The machines that reach a 429 are the skewed
 * ones, and the remedy we hand them is to turn on time synchronisation, which
 * walks `Date.now()` backwards by the size of the skew. Read literally, that
 * would stretch a 60s window into an hour of refusing to refresh, at exactly
 * the moment the user did the thing we asked. `performance.now()` only ever
 * moves forward, at one rate, so no correction can reach it.
 */

/** No `Retry-After` on the 429; Supabase's burst window is the sensible guess. */
const DEFAULT_COOLDOWN_MS = 60_000;
const MAX_COOLDOWN_MS = 300_000;

let closedUntil = 0;

/**
 * Hold the endpoint closed for the window the server asked for, clamped: a
 * hostile or mistaken `Retry-After` should not disable refresh for an afternoon.
 */
export function openTokenCooldown(retryAfter: string | null): void {
  const seconds = retryAfter ? Number.parseInt(retryAfter, 10) : Number.NaN;
  const ms = !Number.isFinite(seconds) || seconds <= 0
    ? DEFAULT_COOLDOWN_MS
    : Math.min(seconds * 1000, MAX_COOLDOWN_MS);
  closedUntil = performance.now() + ms;
}

/**
 * How long the token endpoint stays closed. Callers check this to avoid sitting
 * through auth-js's ~25s local retry ladder for a token they already hold.
 */
export function tokenEndpointCooldownRemainingMs(): number {
  return Math.max(0, closedUntil - performance.now());
}

/** Test seam: forget any cool-off. */
export function resetTokenEndpointCooldown(): void {
  closedUntil = 0;
}
