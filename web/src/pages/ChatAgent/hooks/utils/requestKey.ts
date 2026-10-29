/**
 * Idempotency key for a logical send (v4 attempt chain).
 *
 * One UUID per logical send, REUSED when the same send is retransmitted after
 * a failure whose response never arrived: the server dedups on `request_key`
 * (HTTP 409 `duplicate_request`), so a lost-response resubmit adopts the
 * already-accepted run instead of opening a duplicate turn. A send's key is
 * settled as soon as its response headers arrive; from then on an identical
 * message is a genuinely new logical send. Only that send's own key settles:
 * by then the pending key may be another send's.
 */

import { randomUUID } from '@/lib/randomUUID';

export interface RequestKeyTracker {
  /** Key for this send; reuses the pending key iff the fingerprint matches (a retransmit).
   *  A steer goes out `alongside` others still waiting for their answers, so
   *  it keeps their keys; any other send drops them. */
  take: (fingerprint: string, options?: { alongside?: boolean }) => string;
  /** The send under this key reached the server (headers arrived, a run
   *  delivered it, or the server named its run): the same text sent again
   *  is a new send, not its retransmit. */
  settle: (requestKey: string) => void;
}

/** The fingerprint of a plain send or a steer: one text, sent on one thread. */
export const sendFingerprint = (threadId: string, message: string): string => `send|${threadId}|${message}`;

export function createRequestKeyTracker(): RequestKeyTracker {
  /** Fingerprint to the key its send went out under, until that send settles. */
  const pending = new Map<string, string>();

  function take(fingerprint: string, { alongside = false }: { alongside?: boolean } = {}): string {
    const held = pending.get(fingerprint);
    if (held) return held;
    if (!alongside) pending.clear();
    const key = randomUUID();
    pending.set(fingerprint, key);
    return key;
  }

  function settle(requestKey: string): void {
    for (const [fingerprint, key] of pending) {
      if (key === requestKey) pending.delete(fingerprint);
    }
  }

  return { take, settle };
}
