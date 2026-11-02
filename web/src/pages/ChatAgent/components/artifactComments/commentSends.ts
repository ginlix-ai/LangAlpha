import type { TFunction } from 'i18next';
import { CommentSendRefused, type CommentTarget } from '../../utils/api/artifacts';
import { MAX_COMMENTS_PER_SEND, type CommentInWorkspace } from './artifactCommentSend';
import type { CommentSendWriter } from './commentSendStore';
import { chatCarries, type ArtifactCommentSender } from './sendContext';
import { targetKey, type CommentDestinations } from './destinations';

/** Comments that go out in one send, to one thread or a new one. */
export interface SendBatch {
  to: CommentTarget;
  comments: readonly CommentInWorkspace[];
}

export interface BatchOutcome {
  batch: SendBatch;
  /** The thread the send went to; null when refused. */
  threadId: string | null;
  /** Why it was refused, as the cards say it; null when it went. */
  reason: string | null;
}

/**
 * A file's queued comments as the sends that carry them: one per thread they
 * go to (`targetFor`), in pin order, and another each time one is full. One
 * whose destination is not known yet waits for the next Send all.
 */
export function queuedBatches(
  comments: readonly CommentInWorkspace[],
  targetFor: CommentDestinations['targetFor'],
): SendBatch[] {
  const batches = new Map<string, SendBatch & { comments: CommentInWorkspace[] }>();
  for (const c of comments) {
    if (c.status !== 'open' || c.delivery !== 'queued') continue;
    const route = targetFor(c, 'send');
    if (route?.kind !== 'send') continue;
    const key = targetKey(route.to);
    const batch = batches.get(key);
    if (batch) batch.comments.push(c);
    else batches.set(key, { to: route.to, comments: [c] });
  }
  return [...batches.values()].flatMap(({ to, comments: all }) => {
    const full: SendBatch[] = [];
    for (let i = 0; i < all.length; i += MAX_COMMENTS_PER_SEND) {
      full.push({ to, comments: all.slice(i, i + MAX_COMMENTS_PER_SEND) });
    }
    return full;
  });
}

/** A refused send as a card says it, after "Not sent.": where the comments still are, unless the server said why. */
export function refusalReason(t: TFunction, err: unknown, queued: boolean): string {
  if (err instanceof CommentSendRefused) {
    if (err.code === 'comments_unavailable') return t('pageViewer.comments.notSentUnavailable');
    // A conflict's message is written for the reader (a busy agent, a move).
    if (err.status === 409 && err.message) return err.message;
  }
  return queued ? t('pageViewer.comments.notSentQueued') : t('pageViewer.comments.notSentNote');
}

/**
 * Send each destination's batches through the sender, destinations at once
 * and one's own in order, so pins reach the agent in order and a new thread
 * is made once: the batches after the first join it. A send to the chat
 * beside the panel is the chat's own: the chat marks its comments in the send
 * store as it goes, and the batch settles when the chat's send does, with the
 * chat's reason on a refusal. Any other is marked here: on its way, then
 * taken, or refused with the reason.
 */
export async function sendBatches(
  batches: readonly SendBatch[],
  { sender, sends, reasonOf }: {
    sender: ArtifactCommentSender;
    sends: CommentSendWriter;
    reasonOf: (err: unknown, batch: SendBatch) => string;
  },
): Promise<BatchOutcome[]> {
  const posted = batches.filter((b) => !chatCarries(sender, b.to));
  sends.sending(posted.flatMap((b) => b.comments));
  const sendOne = async (batch: SendBatch, to: CommentTarget): Promise<BatchOutcome> => {
    const marks = posted.includes(batch);
    try {
      const { threadId } = await sender.send(batch.comments, to);
      if (marks) sends.taken(batch.comments);
      return { batch, threadId, reason: null };
    } catch (err) {
      const reason = reasonOf(err, batch);
      if (marks) sends.refused(batch.comments.map((c) => c.comment_id), reason);
      return { batch, threadId: null, reason };
    }
  };
  const byDestination = new Map<string, SendBatch[]>();
  for (const batch of batches) {
    const key = targetKey(batch.to);
    byDestination.set(key, [...(byDestination.get(key) ?? []), batch]);
  }
  const outcomes = new Map<SendBatch, BatchOutcome>();
  await Promise.all([...byDestination.values()].map(async (own) => {
    let to = own[0].to;
    for (const batch of own) {
      const outcome = await sendOne(batch, to);
      outcomes.set(batch, outcome);
      if (outcome.threadId) to = { threadId: outcome.threadId };
    }
  }));
  return batches.map((batch) => outcomes.get(batch)!);
}
