/**
 * Idempotency key for a logical send (v4 attempt chain).
 *
 * One UUID per logical send, REUSED when the same send is retransmitted after
 * a failure whose response never arrived: the server dedups on `request_key`
 * (HTTP 409 `duplicate_request`), so a lost-response resubmit adopts the
 * already-accepted run instead of opening a duplicate turn. A send's key is
 * spent as soon as its response headers arrive; from then on an identical
 * message is a genuinely new logical send. Only that send's own key is spent:
 * by then the pending key may be another send's.
 *
 * The tracker also follows the file comments each request carries, from the
 * take that opens it to the outcome its exit settles, and tells whoever shows
 * them waiting (`watchComments`). A comment is held by the latest request to
 * carry it, so a retry or a parked send going out takes over from the one
 * that handed it off.
 */

import { randomUUID } from '@/lib/randomUUID';
import type { ArtifactCommentsShown, CommentSend } from '@/types/api';
import type { StructuredError } from '@/utils/rateLimitError';
import { commentSendKey } from '../../components/artifactComments/artifactCommentSend';

/** The comments as the send shows them, or only their ids (a resend of a request). */
export type CarriedComments = ArtifactCommentsShown | CommentSend;

/** How a request ended for the comments it carries. */
export type SendOutcome =
  /** The server has the send, or a copy of it under its key. */
  | 'accepted'
  /** Let go with no answer: the comments' rows say where they are. */
  | 'dropped'
  /** A scheduled retry or a parked send carries them on, and settles them in its turn. */
  | 'handedOff'
  /** Refused, or never had: why, as the chat said it. */
  | { refused: string | StructuredError | null };

/** A request took the comments, or how the one holding them ended. */
export type CommentSendListener = (comments: CarriedComments, outcome: 'began' | 'returned' | Exclude<SendOutcome, 'handedOff'>) => void;

export interface RequestKeyTracker {
  /** Key for this send; reuses the pending key iff the fingerprint matches (a retransmit).
   *  A steer goes out `alongside` others still waiting for their answers, so
   *  it keeps their keys; any other send drops them. `key` is a send going
   *  out again under the key it was sent with. The request holds `comments`
   *  until it settles. */
  take: (fingerprint: string, options?: { alongside?: boolean; key?: string; comments?: CarriedComments }) => string;
  /** The server has a copy of the send under this key (headers arrived, or a
   *  run delivered or returned it): the same text sent again is a new send. */
  spend: (requestKey: string) => void;
  /** How the request under this key ended for its comments. The first answer
   *  stands; `accepted` also spends the key. */
  settle: (requestKey: string, outcome: SendOutcome) => void;
  /** A stop handed back a steer the server had taken, and the comments it carried wait again. */
  returned: (comments: CarriedComments) => void;
  watchComments: (listener: CommentSendListener) => () => void;
}

/** The fingerprint of a plain send or a steer: one text, sent on one thread,
 *  with the file comments it carries, so the same note sent with other
 *  comments is another send. */
export const sendFingerprint = (threadId: string, message: string, comments?: CommentSend | null): string => {
  const base = `send|${threadId}|${message}`;
  const key = commentSendKey(comments);
  return key ? `${base}|comments:${key}` : base;
};

export const carriedIds = (comments: CarriedComments | null | undefined): string[] => {
  if (!comments) return [];
  return 'ids' in comments ? [...comments.ids] : comments.comments.map((c) => c.comment_id);
};

function onlyThese(comments: CarriedComments, ids: ReadonlySet<string>): CarriedComments {
  return 'ids' in comments
    ? { ...comments, ids: comments.ids.filter((id) => ids.has(id)) }
    : { ...comments, comments: comments.comments.filter((c) => ids.has(c.comment_id)) };
}

export function createRequestKeyTracker(): RequestKeyTracker {
  /** Fingerprint to the key its send went out under, until that send is spent. */
  const pending = new Map<string, string>();
  /** What each request still open carries, and which request holds each comment. */
  const carried = new Map<string, CarriedComments>();
  const holder = new Map<string, string>();
  const listeners = new Set<CommentSendListener>();
  const tell: CommentSendListener = (comments, outcome) => {
    for (const listener of [...listeners]) listener(comments, outcome);
  };

  function keyFor(fingerprint: string, alongside: boolean): string {
    const held = pending.get(fingerprint);
    if (!alongside) {
      for (const fp of [...pending.keys()]) if (fp !== fingerprint) pending.delete(fp);
    }
    if (held) return held;
    const key = randomUUID();
    pending.set(fingerprint, key);
    return key;
  }

  function take(
    fingerprint: string,
    { alongside = false, key, comments }: { alongside?: boolean; key?: string; comments?: CarriedComments } = {},
  ): string {
    const requestKey = key ?? keyFor(fingerprint, alongside);
    const ids = carriedIds(comments);
    if (!comments || ids.length === 0) return requestKey;
    const before = new Set<string>();
    for (const id of ids) {
      const was = holder.get(id);
      if (was && was !== requestKey) before.add(was);
      holder.set(id, requestKey);
    }
    // A request whose comments all went on to this one has nothing left to settle.
    const holding = new Set(holder.values());
    for (const was of before) if (!holding.has(was)) carried.delete(was);
    carried.set(requestKey, comments);
    tell(comments, 'began');
    return requestKey;
  }

  function spend(requestKey: string): void {
    for (const [fingerprint, key] of pending) {
      if (key === requestKey) pending.delete(fingerprint);
    }
  }

  function settle(requestKey: string, outcome: SendOutcome): void {
    if (outcome === 'accepted') spend(requestKey);
    if (outcome === 'handedOff') return;
    const comments = carried.get(requestKey);
    if (!comments) return;
    carried.delete(requestKey);
    const held = new Set(carriedIds(comments).filter((id) => holder.get(id) === requestKey));
    for (const id of held) holder.delete(id);
    if (held.size > 0) tell(onlyThese(comments, held), outcome);
  }

  function watchComments(listener: CommentSendListener): () => void {
    listeners.add(listener);
    return () => listeners.delete(listener);
  }

  return { take, spend, settle, returned: (comments) => tell(comments, 'returned'), watchComments };
}
