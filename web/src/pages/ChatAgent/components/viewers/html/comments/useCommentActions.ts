import { useCallback, useMemo } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { useEnsureArtifact } from '@/hooks/useArtifact';
import { ALL, refreshArtifactComments } from '@/lib/artifactCache';
import { queryKeys } from '@/lib/queryKeys';
import {
  addArtifactCommentMessage,
  createArtifactComment,
  deleteArtifactComment,
  updateArtifactComment,
  type CommentTarget,
} from '@/pages/ChatAgent/utils/api/artifacts';
import type { ArtifactComment, ArtifactCommentAnchor } from '@/types/api';
import { sendLook, type CommentSendStore } from '../../../artifactComments/commentSendStore';
import type { ArtifactCommentSender } from '../../../artifactComments/sendContext';
import type { SendLook } from '../../../artifactComments/commentSendStore';
import { refusalReason, sendBatches } from '../../../artifactComments/commentSends';
import type { CommentRoute } from '../../../artifactComments/destinations';

/** A route that leaves the words on the file: kept, or waiting for a thread. */
type HoldRoute = Extract<CommentRoute, { kind: 'note' | 'queue' }>;

export interface CommentActions {
  /**
   * A new comment on the file at `path`, made with its record if the file has
   * none. `fileSha` is the live file's hash as the view loaded it.
   */
  create: (input: {
    path: string;
    anchor: ArtifactCommentAnchor;
    body: string;
    route: CommentRoute;
    fileSha: string | null;
    /** The document's title when picked, kept with the comment. */
    title: string | null;
  }) => Promise<ArtifactComment>;
  reply: (comment: ArtifactComment, body: string, route: CommentRoute) => Promise<ArtifactComment>;
  setDelivery: (comment: ArtifactComment, route: HoldRoute) => Promise<void>;
  /** Sends the comment alone, now, to `to`. */
  send: (comment: ArtifactComment, to: CommentTarget) => void;
  setStatus: (comment: ArtifactComment, status: 'open' | 'resolved') => Promise<void>;
  remove: (comment: ArtifactComment) => Promise<void>;
  /** Where a send stands for the comment, which `describeComment` draws. */
  sendState: (comment: ArtifactComment) => SendLook;
}

interface CommentActionsOptions {
  workspaceId: string;
  sender: ArtifactCommentSender;
  /** The sends every view of these comments marks and reads. */
  sends: CommentSendStore;
}

const holdFields = (route: HoldRoute) =>
  route.kind === 'queue' ? { delivery: 'queued' as const, thread_id: route.threadId } : { delivery: 'note' as const };

/**
 * Every write the comment layer makes, each to the route its caller decided
 * (`useCommentDestinations`). Each lands the server's answer in the file's
 * list at once, so the pin moves on the click, then reads again what that
 * answer does not hold: the record's counts and every queue, since a comment
 * can move between threads and the chat composer shows its thread's.
 *
 * Sending is the sender's: beside the chat it posts the comment as the
 * owner's own message, which steers a running turn the way a typed one does;
 * elsewhere it starts a turn on the destination. A comment is written as a
 * note first, so a refused send leaves it on the file rather than lost, with
 * the reason on its card.
 */
export function useCommentActions({ workspaceId, sender, sends }: CommentActionsOptions): CommentActions {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const ensure = useEnsureArtifact();

  const settle = useCallback((id: string, comment: ArtifactComment | null, removedId?: string) => {
    queryClient.setQueryData<ArtifactComment[]>(queryKeys.artifacts.comments(id), (prev) => {
      if (removedId) return prev?.filter((c) => c.comment_id !== removedId);
      if (!comment) return prev;
      if (!prev) return [comment];
      const at = prev.findIndex((c) => c.comment_id === comment.comment_id);
      if (at < 0) return [...prev, comment].sort((a, b) => a.number - b.number);
      const next = prev.slice();
      next[at] = comment;
      return next;
    });
    // A comment can move between threads, so every queue is read again.
    refreshArtifactComments(queryClient, [{ artifactId: id, workspaceId }], ALL, { listed: true });
  }, [queryClient, workspaceId]);

  const send = useCallback<CommentActions['send']>((comment, to) => {
    void sendBatches([{ to, comments: [{ ...comment, workspace_id: workspaceId }] }], {
      sender,
      sends,
      reasonOf: (err) => refusalReason(t, err, comment.delivery === 'queued'),
    }).then(() => refreshArtifactComments(queryClient, [{ artifactId: comment.artifact_id, workspaceId }], ALL));
  }, [sender, sends, t, queryClient, workspaceId]);

  const create = useCallback<CommentActions['create']>(async ({ path, anchor, body, route, fileSha, title }) => {
    const id = (await ensure(workspaceId, path)).artifact_id;
    const comment = await createArtifactComment(id, {
      anchor,
      body,
      ...holdFields(route.kind === 'send' ? { kind: 'note' } : route),
      ...(fileSha && { file_sha256: fileSha }),
      ...(title && { title }),
    });
    settle(id, comment);
    if (route.kind === 'send') send(comment, route.to);
    return comment;
  }, [ensure, workspaceId, settle, send]);

  const reply = useCallback<CommentActions['reply']>(async (comment, body, route) => {
    const next = await addArtifactCommentMessage(comment.artifact_id, comment.comment_id, {
      body,
      ...holdFields(route.kind === 'send' ? { kind: 'note' } : route),
    });
    settle(comment.artifact_id, next);
    if (route.kind === 'send') send(next, route.to);
    return next;
  }, [settle, send]);

  const setDelivery = useCallback<CommentActions['setDelivery']>(async (comment, route) => {
    settle(comment.artifact_id, await updateArtifactComment(comment.artifact_id, comment.comment_id, holdFields(route)));
    // A refusal was about the send that queue was waiting for.
    sends.clear([comment.comment_id]);
  }, [settle, sends]);

  const setStatus = useCallback<CommentActions['setStatus']>(async (comment, status) => {
    settle(comment.artifact_id, await updateArtifactComment(comment.artifact_id, comment.comment_id, { status }));
  }, [settle]);

  const remove = useCallback<CommentActions['remove']>(async (comment) => {
    await deleteArtifactComment(comment.artifact_id, comment.comment_id);
    settle(comment.artifact_id, null, comment.comment_id);
  }, [settle]);

  const sendState = useCallback(
    (comment: ArtifactComment): SendLook => sendLook(sends.get(comment.comment_id), comment),
    [sends],
  );

  return useMemo(
    () => ({ create, reply, setDelivery, send, setStatus, remove, sendState }),
    [create, reply, setDelivery, send, setStatus, remove, sendState],
  );
}
