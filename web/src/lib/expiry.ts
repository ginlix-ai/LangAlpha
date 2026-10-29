/** An owner's file grant lasts 12h; renewing it with an hour left costs nothing. */
export const GRANT_RENEW_MARGIN_MS = 60 * 60_000;
/** A credential already inside its margin (or a skewed clock) must not turn into a tight loop. */
export const MIN_RENEW_INTERVAL_MS = 60_000;

/**
 * How long from `from` (now, by default) until a credential should be
 * replaced, leaving `marginMs` so whatever it is loaded into never sees it
 * lapse. The server sends a lifetime, `expiresIn` seconds, and it is anchored
 * at `receivedAt` on this clock: one clock measures both ends, so a skewed one
 * cannot put renewal past expiry. Zero when there is none, or it is already
 * inside the margin.
 */
export function msUntilRenewal(
  expiresIn: number | undefined,
  receivedAt: number,
  marginMs: number,
  from = Date.now(),
): number {
  if (expiresIn === undefined || !Number.isFinite(expiresIn)) return 0;
  return Math.max(receivedAt + expiresIn * 1000 - marginMs - from, 0);
}
