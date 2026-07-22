/**
 * Steering rollback boundary helpers.
 *
 * When the user sends a steering message mid-turn, the backend emits
 * `steering_accepted` carrying the cutoff `_eventId`. The frontend
 * snapshots that id and, on the subsequent `steering_delivered` event,
 * filters the assistant message's content segments to keep only those
 * whose `order <= boundary`.
 *
 * Two pure helpers extracted from `useChatMessages.ts` so the boundary
 * read and the destructive-filter guard can be regression-tested without
 * mounting the hook:
 *
 *   • `computeSteeringBoundary` — read `_eventId` (numeric or coerced
 *     numeric string), fall back to the local counter when absent.
 *   • `shouldSkipSteeringRollback` — true when a non-positive, NaN, or
 *     null boundary would wipe every segment if applied. Real segment
 *     orders are always positive integers, so any other boundary is a
 *     "no rollback" signal.
 *
 * Both are called from the primary stream's `steering_delivered` handler
 * AND the secondary stream's `steering_accepted` handler. Keeping them
 * pure means the two call sites cannot drift.
 */

/**
 * Compute the rollback boundary from a steering_accepted event.
 *
 * Numeric `_eventId` (typical Redis stream id parsed as number) wins.
 * String `_eventId` is coerced via `Number()` — non-numeric strings
 * yield `NaN`, which `shouldSkipSteeringRollback` then catches.
 * Missing `_eventId` falls back to the per-stream counter.
 */
export function computeSteeringBoundary(
  event: { _eventId?: number | string },
  counterFallback: number,
): number {
  if (event._eventId == null) return counterFallback;
  return typeof event._eventId === 'number' ? event._eventId : Number(event._eventId);
}

/**
 * Return true when the rollback filter should be SKIPPED — i.e. when the
 * boundary would wipe every visible segment.
 *
 * Skip cases:
 *   • `null` — steering arrived before any ordered content
 *   • `NaN` — `_eventId` was a non-numeric string fallback
 *   • `<= 0` — boundary predates the first segment (or is bogus)
 *
 * Real segment orders are always positive integers, so any other
 * boundary value would drop everything and leave the user with an
 * empty turn. The caller should finalize the message instead.
 */
export function shouldSkipSteeringRollback(boundary: number | null): boolean {
  return boundary === null || !Number.isFinite(boundary) || boundary <= 0;
}
