/**
 * Idempotency key for a logical send (v4 attempt chain).
 *
 * One UUID per logical send, REUSED when the same send is retransmitted after
 * a failure whose response never arrived: the server dedups on `request_key`
 * (HTTP 409 `duplicate_request`), so a lost-response resubmit adopts the
 * already-accepted run instead of opening a duplicate turn. The key is
 * consumed (cleared) as soon as response headers arrive — from then on an
 * identical message is a genuinely new logical send.
 */

export interface RequestKeyTracker {
  /** Key for this send; reuses the pending key iff the fingerprint matches (a retransmit). */
  take: (fingerprint: string) => string;
  /** The send reached the server (headers arrived) — the pending key is consumed. */
  clear: () => void;
}

export function createRequestKeyTracker(): RequestKeyTracker {
  let pending: { key: string; fingerprint: string } | null = null;

  function take(fingerprint: string): string {
    if (pending && pending.fingerprint === fingerprint) {
      return pending.key;
    }
    pending = { key: crypto.randomUUID(), fingerprint };
    return pending.key;
  }

  function clear(): void {
    pending = null;
  }

  return { take, clear };
}
