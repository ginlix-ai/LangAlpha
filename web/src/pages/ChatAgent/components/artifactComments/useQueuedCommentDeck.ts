/**
 * The composer's batch of file comments: what the thread's next send carries,
 * and why a send that carried them was refused. The queue itself is the
 * server's; the send store says which of it a send carries before the list
 * catches up, and which a send gave back with a reason, until they are sent
 * or kept.
 */
import { useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { apiErrorStatus } from '@/api/errors';
import { useQueuedArtifactComments } from '@/hooks/useArtifact';
import { refreshArtifactComments } from '@/lib/artifactCache';
import { queryKeys } from '@/lib/queryKeys';
import type { ArtifactCommentsShown, ArtifactReading, QueuedArtifactComment } from '@/types/api';
import { updateArtifactComment } from '../../utils/api/artifacts';
import { MAX_COMMENTS_PER_SEND, shownFromQueued } from './artifactCommentSend';
import { leftQueue, type CommentSendStore } from './commentSendStore';

const NONE: readonly QueuedArtifactComment[] = [];

export interface QueuedCommentDeck {
  /** The thread whose batch it is. */
  threadId: string | undefined;
  /** The thread's queue has been read once, so the deck can draw what it holds
   *  as already there rather than arriving. */
  loaded: boolean;
  /** Every queued comment, those a send has just taken included. */
  comments: readonly QueuedArtifactComment[];
  /** The comments the next send carries: the queue less what a send has taken. */
  ready: readonly QueuedArtifactComment[];
  /** One line on why the batch did not go, or why keeping one failed. */
  error: string | null;
  /** The composer's placeholder while the batch waits, else null. */
  placeholder: string | null;
  /** Hand the ready comments to a send with `note`, read where the person
   *  reads as it goes; undefined when none. */
  take: (note: string, reading: ArtifactReading) => ArtifactCommentsShown | undefined;
  /** Take comments out of the batch; they stay on their files as notes. */
  keep: (comments: readonly QueuedArtifactComment[]) => void;
}

export function useQueuedCommentDeck({ threadId, running, sends }: {
  /** The thread whose batch it is; undefined where there is none (no thread yet). */
  threadId: string | undefined;
  running: boolean;
  sends: CommentSendStore;
}): QueuedCommentDeck {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const { data } = useQueuedArtifactComments(threadId);
  const comments = data ?? NONE;
  const [keepError, setKeepError] = useState<{ threadId: string; message: string } | null>(null);

  const ready = comments.filter((c) => !leftQueue(sends.get(c.comment_id), c));
  // The deck speaks for its whole batch, so one reason stands for it: the
  // first comment still waiting that a send gave back.
  let refused: string | null = null;
  for (const c of ready) {
    const mark = sends.get(c.comment_id);
    if (mark?.phase === 'refused') {
      refused = mark.reason;
      break;
    }
  }
  const error = (keepError && keepError.threadId === threadId ? keepError.message : null) ?? refused;

  let placeholder: string | null = null;
  if (ready.length > 0) {
    placeholder = running
      ? t('chat.artifactComments.deck.placeholderRunning')
      : t('chat.artifactComments.deck.placeholder', { count: ready.length });
  }

  // The send that carries them marks them on their way as the chat takes or
  // parks it (`watchCommentSends`), in the same call as this take.
  const take = (note: string, reading: ArtifactReading): ArtifactCommentsShown | undefined => {
    if (!threadId || ready.length === 0) return undefined;
    setKeepError(null);
    return shownFromQueued(note, reading, ready.slice(0, MAX_COMMENTS_PER_SEND));
  };

  const keep = (targets: readonly QueuedArtifactComment[]) => {
    if (!threadId || targets.length === 0) return;
    sends.clear(targets.map((c) => c.comment_id));
    setKeepError(null);
    const key = queryKeys.artifacts.queued(threadId);
    const ids = new Set(targets.map((c) => c.comment_id));
    void queryClient.cancelQueries({ queryKey: key });
    queryClient.setQueryData<QueuedArtifactComment[]>(key, (old) => old?.filter((c) => !ids.has(c.comment_id)));
    void Promise.allSettled(
      targets.map((c) => updateArtifactComment(c.artifact_id, c.comment_id, { delivery: 'note' })),
    ).then((results) => {
      // 409 is a comment a send took first: nothing is left to hold back.
      const failed = results.some((r) => r.status === 'rejected' && apiErrorStatus(r.reason) !== 409);
      if (failed) setKeepError({ threadId, message: t('chat.artifactComments.deck.keepFailed') });
      const files = new Map(targets.map((c) => [c.artifact_id, c.workspace_id]));
      refreshArtifactComments(
        queryClient,
        [...files].map(([artifactId, workspaceId]) => ({ artifactId, workspaceId })),
        threadId,
      );
    });
  };

  return { threadId, loaded: data !== undefined, comments, ready, error, placeholder, take, keep };
}
