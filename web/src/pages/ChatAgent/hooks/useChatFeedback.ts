/**
 * Per-thread message feedback (thumbs up/down) — load, submit, and lookup.
 * Turn index is derived positionally from the messages array (positional
 * assistant count = backend turn_index; see the turn-bubble invariant).
 */

import { useRef, useCallback } from 'react';
import { submitFeedback, removeFeedback, getThreadFeedback } from '../utils/api';
import type { ChatMessage } from '@/types/chat';

export function useChatFeedback(threadId: string, messages: ChatMessage[]) {
  // Feedback state: { [turnIndex]: { rating, ... } }
  const feedbackMapRef = useRef<Record<number, { rating: string | null; [key: string]: unknown }>>({});

  /** Fetch feedback state for the thread. Best-effort — display-only data. */
  const loadFeedback = useCallback(async (targetThreadId?: string) => {
    const tid = targetThreadId ?? threadId;
    if (!tid) return;
    try {
      const feedbackList = await getThreadFeedback(tid);
      const map: Record<number, { rating: string | null; [key: string]: unknown }> = {};
      feedbackList.forEach((fb: Record<string, unknown>) => { map[fb.turn_index as number] = fb as { rating: string | null; [key: string]: unknown }; });
      feedbackMapRef.current = map;
    } catch (e) {
      // Non-critical — feedback display is best-effort
      console.warn('[History] Failed to load feedback:', e);
    }
  }, [threadId]);

  const deriveTurnIndex = useCallback((messageId: string): number => {
    const msgIndex = messages.findIndex(m => m.id === messageId);
    if (msgIndex === -1) return -1;
    return messages.slice(0, msgIndex + 1).filter(m => m.role === 'assistant' && !m.isSteering).length - 1;
  }, [messages]);

  const handleThumbUp = useCallback(async (messageId: string) => {
    const turnIndex = deriveTurnIndex(messageId);
    if (turnIndex === -1) return null;

    const existing = feedbackMapRef.current[turnIndex];
    try {
      if (existing?.rating === 'thumbs_up') {
        await removeFeedback(threadId, turnIndex);
        delete feedbackMapRef.current[turnIndex];
        return { rating: null };
      } else {
        const result = await submitFeedback(threadId, turnIndex, 'thumbs_up');
        feedbackMapRef.current[turnIndex] = result;
        return { rating: 'thumbs_up' };
      }
    } catch (e) {
      console.error('[Feedback] Error:', e);
      return null;
    }
  }, [deriveTurnIndex, threadId]);

  const handleThumbDown = useCallback(async (messageId: string, issueCategories: string[], comment: string | null, consentHumanReview: boolean) => {
    const turnIndex = deriveTurnIndex(messageId);
    if (turnIndex === -1) return null;

    try {
      const result = await submitFeedback(threadId, turnIndex, 'thumbs_down', issueCategories, comment, consentHumanReview);
      feedbackMapRef.current[turnIndex] = result;
      return { rating: 'thumbs_down' };
    } catch (e) {
      console.error('[Feedback] Error:', e);
      return null;
    }
  }, [deriveTurnIndex, threadId]);

  const getFeedbackForMessage = useCallback((messageId: string) => {
    const turnIndex = deriveTurnIndex(messageId);
    if (turnIndex === -1) return null;
    return feedbackMapRef.current[turnIndex] || null;
  }, [deriveTurnIndex]);

  return { handleThumbUp, handleThumbDown, getFeedbackForMessage, loadFeedback };
}
