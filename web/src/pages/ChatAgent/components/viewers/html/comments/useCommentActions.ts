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
  editArtifactCommentMessage,
  updateArtifactComment,
  type CommentTarget,
} from '@/pages/ChatAgent/utils/api/artifacts';
import type { ArtifactComment, ArtifactCommentAnchor, ArtifactKind, ArtifactTarget } from '@/types/api';
import { sendLook, type CommentSendStore } from '../../../artifactComments/commentSendStore';
import type { ArtifactCommentSender } from '../../../artifactComments/sendContext';
import type { SendLook } from '../../../artifactComments/commentSendStore';
import { toast } from '@/components/ui/use-toast';
import { refusalNotice, refusalReason, sendBatches } from '../../../artifactComments/commentSends';
import { sameTarget, type CommentRoute } from '../../../artifactComments/destinations';

/** A route that leaves the words on the file: kept, or waiting for a thread. */
type HoldRoute = Extract<CommentRoute, { kind: 'note' | 'queue' }>;

export interface CommentActions {
  /**
   * A new comment on `target` (a file or a chart, as `targetFor` keys it),
   * made with its record if it has none. `fileSha` is the live file's hash as
   * the view loaded it.
   */
  create: (input: {
    target: ArtifactTarget;
    anchor: ArtifactCommentAnchor;
    body: string;
    route: CommentRoute;
    fileSha: string | null;
    /** The document's title when picked, kept with the comment. */
    title: string | null;
  }) => Promise<ArtifactComment>;
  reply: (comment: ArtifactComment, body: string, route: CommentRoute) => Promise<ArtifactComment>;
  setDelivery: (comment: ArtifactComment, route: HoldRoute) => Promise<void>;
  /** Rewrites the owner's words in `messageId`, which no send has taken; refused once one has. */
  edit: (comment: ArtifactComment, messageId: string, body: string) => Promise<void>;
  /** Sends the comment alone, now, to `to`, from its card, which stays on screen and says a refusal itself. */
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
  /**
   * The Drive file these comments are on, where they are on one. A comment
   * row holds only the path it was made at, so a send names the file, and
   * its chip in the chat opens that file wherever it has moved since.
   */
  entryId?: string | null;
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
export function useCommentActions({ workspaceId, sender, sends, entryId = null }: CommentActionsOptions): CommentActions {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const ensure = useEnsureArtifact();

  const settle = useCallback((id: string, kind: ArtifactKind, comment: ArtifactComment | null, removedId?: string) => {
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
    refreshArtifactComments(queryClient, [{ artifactId: id, workspaceId, kind }], ALL, { listed: true });
  }, [queryClient, workspaceId]);

  /**
   * `boxClosed`: the send came from the box, which closed on the click, so
   * the card that holds a refusal's reason is not on screen and the refusal
   * is said where the person is. The chat beside says its own.
   */
  const post = useCallback((comment: ArtifactComment, to: CommentTarget, boxClosed: boolean) => {
    const queued = comment.delivery === 'queued';
    const sent = { ...comment, workspace_id: workspaceId, ...(entryId && comment.kind === 'drive' ? { entry_id: entryId } : {}) };
    void sendBatches([{ to, comments: [sent] }], {
      sender,
      sends,
      reasonOf: (err) => refusalReason(t, err, queued),
    }).then(([outcome]) => {
      refreshArtifactComments(queryClient, [{ artifactId: comment.artifact_id, workspaceId, kind: comment.kind }], ALL);
      if (boxClosed && outcome.error !== null && !sameTarget(sender.beside, to)) {
        toast({ description: refusalNotice(t, outcome.error, queued), variant: 'destructive' });
      }
    });
  }, [sender, sends, t, queryClient, workspaceId, entryId]);

  const send = useCallback<CommentActions['send']>((comment, to) => post(comment, to, false), [post]);

  const create = useCallback<CommentActions['create']>(async ({ target, anchor, body, route, fileSha, title }) => {
    const id = (await ensure(workspaceId, target)).artifact_id;
    const comment = await createArtifactComment(id, {
      anchor,
      body,
      ...holdFields(route.kind === 'send' ? { kind: 'note' } : route),
      ...(fileSha && { file_sha256: fileSha }),
      ...(title && { title }),
    });
    settle(id, comment.kind, comment);
    if (route.kind === 'send') post(comment, route.to, true);
    return comment;
  }, [ensure, workspaceId, settle, post]);

  const reply = useCallback<CommentActions['reply']>(async (comment, body, route) => {
    const next = await addArtifactCommentMessage(comment.artifact_id, comment.comment_id, {
      body,
      ...holdFields(route.kind === 'send' ? { kind: 'note' } : route),
    });
    settle(comment.artifact_id, next.kind, next);
    if (route.kind === 'send') post(next, route.to, true);
    return next;
  }, [settle, post]);

  const setDelivery = useCallback<CommentActions['setDelivery']>(async (comment, route) => {
    settle(comment.artifact_id, comment.kind, await updateArtifactComment(comment.artifact_id, comment.comment_id, holdFields(route)));
    // A refusal was about the send that queue was waiting for.
    sends.clear([comment.comment_id]);
  }, [settle, sends]);

  const edit = useCallback<CommentActions['edit']>(async (comment, messageId, body) => {
    settle(comment.artifact_id, comment.kind, await editArtifactCommentMessage(comment.artifact_id, comment.comment_id, messageId, body));
  }, [settle]);

  const setStatus = useCallback<CommentActions['setStatus']>(async (comment, status) => {
    settle(comment.artifact_id, comment.kind, await updateArtifactComment(comment.artifact_id, comment.comment_id, { status }));
  }, [settle]);

  const remove = useCallback<CommentActions['remove']>(async (comment) => {
    await deleteArtifactComment(comment.artifact_id, comment.comment_id);
    settle(comment.artifact_id, comment.kind, null, comment.comment_id);
  }, [settle]);

  const sendState = useCallback(
    (comment: ArtifactComment): SendLook => sendLook(sends.get(comment.comment_id), comment),
    [sends],
  );

  return useMemo(
    () => ({ create, reply, setDelivery, edit, send, setStatus, remove, sendState }),
    [create, reply, setDelivery, edit, send, setStatus, remove, sendState],
  );
}
