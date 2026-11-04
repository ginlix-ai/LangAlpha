import { createContext, useContext, useMemo } from 'react';
import { usePreferences } from '@/hooks/usePreferences';
import { readArtifactAnswers, readingFor } from '@/lib/artifactPreferences';
import { postArtifactComments, type CommentTarget } from '../../utils/api/artifacts';
import type { CommentInWorkspace } from './artifactCommentSend';

/**
 * How a file viewer sends comments to the agent. The chat beside the panel
 * provides it (`useChatCommentSends`); a view with none posts each send itself.
 */
export interface ArtifactCommentSender {
  /** The thread beside the panel, if it is a real one; null in a view with no chat. */
  besideThreadId: string | null;
  /** A turn runs on the thread beside, so a send there steers it. */
  running: boolean;
  /**
   * Send these comments now, alone, with no note. To the thread beside it goes
   * through the chat, as the owner's own message, so it steers a running turn
   * and a refusal shows there; the composer's draft and its queued batch stay
   * put, and the chat marks the comments' send as it goes
   * ({@link chatCarries}). Anywhere else it is posted with
   * {@link postArtifactComments}. Either way it resolves once the server has
   * them, with the thread they went to, and rejects with a
   * `CommentSendRefused` saying why they did not go.
   */
  send: (comments: readonly CommentInWorkspace[], to: CommentTarget) => Promise<{ threadId: string }>;
}

/** A send to `to` is the chat's own, made beside the panel, which marks its comments itself. */
export function chatCarries(sender: Pick<ArtifactCommentSender, 'besideThreadId'>, to: CommentTarget): boolean {
  return sender.besideThreadId !== null && 'threadId' in to && to.threadId === sender.besideThreadId;
}

export const ArtifactCommentSendContext = createContext<ArtifactCommentSender | null>(null);

/** The chat's sender beside the panel, else one that posts each send itself. */
export function useArtifactCommentSender(): ArtifactCommentSender {
  const beside = useContext(ArtifactCommentSendContext);
  // No chat is on screen, so the person reads the file.
  const reading = readingFor(readArtifactAnswers(usePreferences().preferences), true);
  // Read by every comment card, so one object per reading rather than per render.
  const posting = useMemo<ArtifactCommentSender>(() => ({
    besideThreadId: null,
    running: false,
    send: (comments, to) => postArtifactComments(comments.map((c) => c.comment_id), to, { reading }),
  }), [reading]);
  return beside ?? posting;
}
