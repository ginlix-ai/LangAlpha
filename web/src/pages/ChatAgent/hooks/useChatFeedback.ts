/**
 * Per-thread message feedback (thumbs up/down) — load, submit, and lookup.
 * Addressed by BACKEND TURN INDEX throughout: MessageList already projects
 * every bubble to its turn (see turnProjection), so this hook never touches
 * the messages array — which also removes the child-first effect-ordering trap
 * the old `messagesRef` mirror created.
 */

import { useCallback, useState } from 'react';
import { submitFeedback, removeFeedback, getThreadFeedback } from '../utils/api';

type FeedbackEntry = { rating: string | null; [key: string]: unknown };

export function useChatFeedback(threadId: string) {
  // Feedback state: { [turnIndex]: { rating, ... } }. State, not a ref: a
  // resolved load or submit must re-render the rated bubble so it re-seeds
  // from the new entry.
  const [feedbackByTurn, setFeedbackByTurn] = useState<Record<number, FeedbackEntry>>({});

  /** Fetch feedback state for the thread. Best-effort — display-only data. */
  const loadFeedback = useCallback(async (targetThreadId?: string) => {
    const tid = targetThreadId ?? threadId;
    if (!tid) return;
    try {
      const feedbackList = await getThreadFeedback(tid);
      const map: Record<number, FeedbackEntry> = {};
      feedbackList.forEach((fb: Record<string, unknown>) => { map[fb.turn_index as number] = fb as FeedbackEntry; });
      setFeedbackByTurn(map);
    } catch (e) {
      // Non-critical — feedback display is best-effort
      console.warn('[History] Failed to load feedback:', e);
    }
  }, [threadId]);

  const handleThumbUp = useCallback(async (turnIndex: number) => {
    if (turnIndex < 0) return null;

    const existing = feedbackByTurn[turnIndex];
    try {
      if (existing?.rating === 'thumbs_up') {
        await removeFeedback(threadId, turnIndex);
        setFeedbackByTurn((prev) => {
          const next = { ...prev };
          delete next[turnIndex];
          return next;
        });
        return { rating: null };
      } else {
        const result = await submitFeedback(threadId, turnIndex, 'thumbs_up');
        setFeedbackByTurn((prev) => ({ ...prev, [turnIndex]: result }));
        return { rating: 'thumbs_up' };
      }
    } catch (e) {
      console.error('[Feedback] Error:', e);
      return null;
    }
  }, [threadId, feedbackByTurn]);

  const handleThumbDown = useCallback(async (turnIndex: number, issueCategories: string[], comment: string | null, consentHumanReview: boolean) => {
    if (turnIndex < 0) return null;

    try {
      const result = await submitFeedback(threadId, turnIndex, 'thumbs_down', issueCategories, comment, consentHumanReview);
      setFeedbackByTurn((prev) => ({ ...prev, [turnIndex]: result }));
      return { rating: 'thumbs_down' };
    } catch (e) {
      console.error('[Feedback] Error:', e);
      return null;
    }
  }, [threadId]);

  return { handleThumbUp, handleThumbDown, feedbackByTurn, loadFeedback };
}
