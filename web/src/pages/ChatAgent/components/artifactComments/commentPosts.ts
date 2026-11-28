/**
 * Every post of comments that leaves the chat beside: a send from a view with
 * no chat, or from one to a thread other than the chat's own. Both senders
 * (`useArtifactCommentSender`, `useChatCommentSends`) post through here, so
 * what a post still out means is held in one place.
 *
 * Out, a post holds the page: a reload or a closed tab would cancel it before
 * the server has the comments. And a post to a new thread for a file's
 * comments that starts while another for the same file and place is out
 * joins the thread that one makes rather than making a second: the file's
 * default moves to that thread only once the server has it and the view has
 * read it again, so a second click would otherwise start another chat. A
 * post is out, for this, until that read is in; the posts are kept for the
 * page's life, so a box closed and opened again, or another file looked at
 * meanwhile, still joins.
 */
import type { QueryClient } from '@tanstack/react-query';
import { authSessionCheck, registerAuthReset } from '@/lib/authResets';
import { queryKeys } from '@/lib/queryKeys';
import { holdPageLeave } from '@/lib/reloadHolds';
import type { ArtifactReading } from '@/types/api';
import { CommentSendRefused, postArtifactComments, type CommentTarget } from '../../utils/api/artifacts';
import type { CommentInWorkspace } from './artifactCommentSend';
import { targetKey } from './destinations';

/** Posts to a new thread still out, by file and place, each resolving with the thread it made (null when refused). */
const making = new Map<string, Promise<string | null>>();

// The last account's posts are not the next one's to join.
registerAuthReset(() => making.clear());

const makingKey = (artifactId: string, to: CommentTarget): string | null =>
  'newThreadIn' in to ? `${artifactId}|${targetKey(to)}` : null;

/**
 * Resolves once every view showing where `artifactId`'s comments go has read
 * that since `since`, so the default it offers is the thread just made. The
 * read is asked for here, which drops one already out (it may predate the
 * thread), and watched in the cache rather than awaited, since another
 * refresh may ask again and drop this one. A view not on screen reads it
 * when it next shows. A read that failed leaves the view offering the old
 * default, so only one that answered counts; a view that leaves the screen
 * stops holding it.
 */
function destinationsRead(queryClient: QueryClient, artifactId: string, since: number): Promise<void> {
  const queryKey = queryKeys.artifacts.destinationsOf(artifactId);
  const cache = queryClient.getQueryCache();
  const behind = () => cache.findAll({ queryKey, type: 'active' })
    .some((query) => query.state.dataUpdatedAt < since);
  void queryClient.invalidateQueries({ queryKey });
  if (!behind()) return Promise.resolve();
  return new Promise((resolve) => {
    const stop = cache.subscribe(() => {
      if (behind()) return;
      stop();
      resolve();
    });
  });
}

export function postComments(
  comments: readonly CommentInWorkspace[],
  to: CommentTarget,
  { reading, queryClient }: { reading: ArtifactReading; queryClient: QueryClient },
): Promise<{ threadId: string }> {
  const files = 'newThreadIn' in to ? [...new Set(comments.map((c) => c.artifact_id))] : [];
  const keys = files.map((id) => makingKey(id, to)).filter((k): k is string => k !== null);
  const before = keys.map((k) => making.get(k)).find((p) => p !== undefined);
  const release = holdPageLeave();
  const sameSession = authSessionCheck();
  const posted = (async () => {
    try {
      const made = before ? await before : null;
      // One that waited through a sign-out is not the next account's to send.
      if (!sameSession()) throw new CommentSendRefused(null);
      return await postArtifactComments(comments.map((c) => c.comment_id), made ? { threadId: made } : to, { reading });
    } finally {
      release();
    }
  })();
  if (keys.length > 0) {
    // One that joined an earlier post's thread holds it even when refused:
    // the thread is there, and the earlier post's read may still be out.
    const thread = posted.then(({ threadId }) => threadId, () => before ?? null);
    for (const key of keys) making.set(key, thread);
    void thread
      .then(async (made) => {
        if (made) await Promise.all(files.map((id) => destinationsRead(queryClient, id, Date.now())));
      })
      .then(() => {
        for (const key of keys) if (making.get(key) === thread) making.delete(key);
      });
  }
  return posted;
}
