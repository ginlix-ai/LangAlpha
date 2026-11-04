/**
 * The chat's side of file comments: the send store every view beside the
 * chat reads, written from the chat's own account of each send that carries
 * comments; the composer's deck of comments queued for this thread; the
 * sender a file panel beside the chat sends through; the reading each send
 * carries; and what a send's answer or a turn's end makes stale.
 */
import { createElement, useEffect, useMemo, useRef, type ReactNode } from 'react';
import { useQueryClient, type QueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { fetchArtifactComments } from '@/hooks/useArtifact';
import { usePreferences } from '@/hooks/usePreferences';
import { useStableHandler } from '@/hooks/useStableHandler';
import { ALL, refreshArtifactComments, type ArtifactRef } from '@/lib/artifactCache';
import { readArtifactAnswers, readingFor } from '@/lib/artifactPreferences';
import type { ArtifactCommentsShown } from '@/types/api';
import { carriedIds, type CommentSendListener } from '../../hooks/utils/requestKey';
import { CommentSendRefused, postArtifactComments } from '../../utils/api/artifacts';
import { shownFromQueued, type CommentInWorkspace } from './artifactCommentSend';
import { CommentSendsContext, useCommentSendStore, type CommentSendStore } from './commentSendStore';
import { ArtifactCommentSendContext, chatCarries, type ArtifactCommentSender } from './sendContext';
import { useQueuedCommentDeck, type QueuedCommentDeck } from './useQueuedCommentDeck';

export interface ChatCommentSends {
  deck: QueuedCommentDeck;
  /** Where each comment's send stands, for every view beside the chat. */
  sends: CommentSendStore;
  /** What every file view beside the chat sends through. */
  sender: ArtifactCommentSender;
  /** The queued comments for the composer's next send, with `note`; undefined when none wait. */
  take: (note: string) => ArtifactCommentsShown | undefined;
}

/** How the chat answered for a send of comments a file view handed it. */
type ChatAnswer = { kind: 'taken' } | { kind: 'refused'; reason: string } | { kind: 'dropped' };
type Waiter = (ids: readonly string[], outcome: Exclude<Parameters<CommentSendListener>[1], 'returned'>, reason: string) => void;

export function useChatCommentSends({ threadId, running, watch, chatCovered, sendToChat }: {
  /** The thread beside the panel, if it is a real one. */
  threadId: string | undefined;
  /** A turn runs on it. */
  running: boolean;
  /** The chat's account of each send's comments (`watchCommentSends`). */
  watch: (listener: CommentSendListener) => () => void;
  /** Whether the file panel covers the chat now, read as a send goes. */
  chatCovered: () => boolean;
  /** Sends comments as the owner's own message in this chat, with no note. */
  sendToChat: (artifactComments: ArtifactCommentsShown) => void;
}): ChatCommentSends {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const answers = readArtifactAnswers(usePreferences().preferences);
  const sends = useCommentSendStore();
  const deck = useQueuedCommentDeck({ threadId, running, sends });
  // The file each comment the chat carried is on, for a resend that knows it by id.
  const files = useRef(new Map<string, ArtifactRef>());
  // File views' sends waiting on the chat's answer.
  const waiters = useRef(new Set<Waiter>());

  // The chat is the one writer of its sends' marks. An answer reads again
  // the files the comments are on and this thread's queue.
  const hear = useStableHandler<Parameters<CommentSendListener>, void>((comments, outcome) => {
    const ids = carriedIds(comments);
    if ('comments' in comments) {
      for (const c of comments.comments) files.current.set(c.comment_id, { artifactId: c.artifact_id, workspaceId: c.workspace_id || null });
    }
    let reason = '';
    let moved = true;
    if (outcome === 'began') {
      sends.sending(ids.map((comment_id) => ({ comment_id })));
      moved = false;
    } else if (outcome === 'accepted') {
      sends.taken(ids.map((comment_id) => ({ comment_id })));
    } else if (outcome === 'dropped') {
      moved = sends.release(ids);
    } else if (outcome === 'returned') {
      sends.clear(ids);
    } else {
      const said = typeof outcome.refused === 'string' ? outcome.refused : outcome.refused?.message;
      reason = said || t('chat.artifactComments.sendFailed');
      sends.refused(ids, reason);
    }
    if (moved) {
      const known = new Map(ids.flatMap((id) => {
        const file = files.current.get(id);
        return file ? [[file.artifactId, file] as const] : [];
      }));
      refreshArtifactComments(queryClient, [...known.values()], threadId);
    }
    // A send a stop hands back was answered long ago.
    if (outcome !== 'returned') for (const waiter of [...waiters.current]) waiter(ids, outcome, reason);
  });
  useEffect(() => watch(hear), [watch, hear]);

  // With the chat covered, the agent answers each comment in full, unless the
  // owner picked one place for answers whatever is on screen.
  const take = useStableHandler((note: string) => deck.take(note, readingFor(answers, chatCovered())));

  // A file viewer's "Send now": its comments alone, with no note, leaving the
  // draft and the queued batch where they are. To this chat's thread it is
  // this chat's send, so it steers a running turn and a refusal shows here,
  // and the chat says at once whether it took the comments. Anywhere else it
  // goes out on its own and nobody here reads its stream, so the person reads
  // the file.
  const send = useStableHandler<Parameters<ArtifactCommentSender['send']>, ReturnType<ArtifactCommentSender['send']>>(
    async (comments, to) => {
      if (!threadId || !chatCarries({ besideThreadId: threadId }, to)) {
        return postArtifactComments(comments.map((c) => c.comment_id), to, { reading: readingFor(answers, true) });
      }
      const left = new Set(comments.map((c) => c.comment_id));
      let carried = false;
      let answered!: (answer: ChatAnswer) => void;
      const answer = new Promise<ChatAnswer>((resolve) => { answered = resolve; });
      const waiter: Waiter = (ids, outcome, reason) => {
        if (!ids.some((id) => left.has(id))) return;
        if (outcome === 'began') {
          carried = true;
          return;
        }
        if (outcome === 'accepted') {
          for (const id of ids) left.delete(id);
          if (left.size > 0) return;
        }
        waiters.current.delete(waiter);
        answered(outcome === 'accepted' ? { kind: 'taken' } : outcome === 'dropped' ? { kind: 'dropped' } : { kind: 'refused', reason });
      };
      waiters.current.add(waiter);
      sendToChat(shownFromQueued('', readingFor(answers, chatCovered()), comments));
      if (!carried) {
        waiters.current.delete(waiter);
        throw new CommentSendRefused('The comments were not sent.', null, null);
      }
      const outcome = await answer;
      if (outcome.kind === 'taken') return { threadId };
      // The chat's own words for it, which the card says after "Not sent."
      if (outcome.kind === 'refused') throw new CommentSendRefused(outcome.reason, null, 409);
      return sentByRows(queryClient, comments, threadId);
    },
  );
  // Read by every comment card in the panel, so a fresh object per render
  // would redraw them all.
  const sender = useMemo<ArtifactCommentSender>(
    () => ({ besideThreadId: threadId ?? null, running, send }),
    [threadId, running, send],
  );

  // A turn's end puts back what it never read and leaves the agent's replies
  // and resolves, and unread counts, on files this view may not know of.
  const wasRunning = useRef(running);
  useEffect(() => {
    const ended = wasRunning.current && !running;
    wasRunning.current = running;
    if (ended) refreshArtifactComments(queryClient, ALL, threadId);
  }, [running, queryClient, threadId]);

  return { deck, sends, sender, take };
}

/** The chat's send store and sender, for every file view beside the chat. */
export function ChatCommentSendsProvider({ value, children }: { value: ChatCommentSends; children: ReactNode }) {
  return createElement(
    CommentSendsContext,
    { value: value.sends },
    createElement(ArtifactCommentSendContext, { value: value.sender }, children),
  );
}

/**
 * How a send the chat let go with no answer went, by the comments' rows: it
 * went if the server shows them all sent, and otherwise they are where they
 * were.
 */
async function sentByRows(
  queryClient: QueryClient,
  comments: readonly CommentInWorkspace[],
  threadId: string,
): Promise<{ threadId: string }> {
  const files = [...new Set(comments.map((c) => c.artifact_id))];
  const lists = await Promise.all(files.map((id) => fetchArtifactComments(queryClient, id))).catch(() => null);
  const rows = new Map((lists ?? []).flat().map((c) => [c.comment_id, c]));
  if (lists && comments.every((c) => rows.get(c.comment_id)?.delivery === 'sent')) {
    return { threadId: rows.get(comments[0].comment_id)?.thread_id ?? threadId };
  }
  throw new CommentSendRefused('The comments were not sent.', null, null);
}
