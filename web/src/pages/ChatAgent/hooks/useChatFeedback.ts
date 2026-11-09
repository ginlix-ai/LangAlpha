/**
 * Per-thread message feedback (thumbs up/down) — load, submit, and lookup.
 * Addressed by BACKEND TURN INDEX throughout: MessageList already projects
 * every bubble to its turn (see turnProjection), so this hook never touches
 * the messages array — which also removes the child-first effect-ordering trap
 * the old `messagesRef` mirror created.
 */

import { useCallback, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import { reportRefusal, type ReportRefusal } from '@/api/reports';
import { apiErrorObject } from '@/api/errors';
import { refusalMessage } from '@/components/report/refusalMessage';
import { submitFeedback, removeFeedback, getThreadFeedback, followUpFeedback, type ThreadFeedback } from '../utils/api';

/** More for a sent thumbs-down: a note and/or a review, or withdrawing it. */
export type ThumbDownFollowUp =
  | { kind: 'add'; note: { id: string; text: string } | null; humanReview: boolean }
  | { kind: 'withdraw' };

export type ThumbDownFollowUpOutcome = { status: 'ok' } | ReportRefusal;

export function useChatFeedback(threadId: string) {
  // Feedback state: { [turnIndex]: { rating, ... } }. State, not a ref: a
  // resolved load or submit must re-render the rated bubble so it re-seeds
  // from the new entry.
  const [feedbackByTurn, setFeedbackByTurn] = useState<Record<number, ThreadFeedback>>({});
  // While the thread's ratings are on their way, a rated turn looks unrated,
  // and a thumbs-down dialog opened then would offer the form for a report the
  // team already has.
  const [feedbackLoading, setFeedbackLoading] = useState(false);
  const { t } = useTranslation();

  /** Fetch feedback state for the thread. Best-effort — display-only data. */
  const loadFeedback = useCallback(async (targetThreadId?: string) => {
    const tid = targetThreadId ?? threadId;
    if (!tid) return;
    setFeedbackLoading(true);
    try {
      const feedbackList = await getThreadFeedback(tid);
      const map: Record<number, ThreadFeedback> = {};
      feedbackList.forEach((fb) => { map[fb.turn_index] = fb; });
      setFeedbackByTurn(map);
    } catch (e) {
      // Non-critical — feedback display is best-effort
      console.warn('[History] Failed to load feedback:', e);
    } finally {
      setFeedbackLoading(false);
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
      // Over a thumbs-down, the rating changes only once its report is
      // withdrawn; say why it did not.
      if (String(apiErrorObject(e)?.error ?? '').startsWith('report_')) {
        toast({ description: refusalMessage(t, reportRefusal(e)) });
      }
      return null;
    }
  }, [threadId, feedbackByTurn, t]);

  const handleThumbDown = useCallback(async (turnIndex: number, issueCategories: string[], comment: string | null, consentHumanReview: boolean) => {
    if (turnIndex < 0) return null;

    try {
      const result = await submitFeedback(threadId, turnIndex, 'thumbs_down', issueCategories, comment, consentHumanReview);
      setFeedbackByTurn((prev) => ({ ...prev, [turnIndex]: result }));
      // The rating is saved either way; say whether the team has it too.
      if (result.report_status === 'accepted' || result.report_status === 'duplicate') {
        toast({ description: consentHumanReview ? t('report.thumbsSentReview') : t('report.thumbsSent') });
      } else if (result.report_status) {
        toast({ description: t('report.followUp.notSent') });
      }
      return { rating: 'thumbs_down' };
    } catch (e) {
      console.error('[Feedback] Error:', e);
      return null;
    }
  }, [threadId, t]);

  const handleThumbDownFollowUp = useCallback(async (turnIndex: number, action: ThumbDownFollowUp): Promise<ThumbDownFollowUpOutcome> => {
    try {
      if (action.kind === 'withdraw') {
        await removeFeedback(threadId, turnIndex);
        setFeedbackByTurn((prev) => {
          const next = { ...prev };
          delete next[turnIndex];
          return next;
        });
      } else {
        const feedback = await followUpFeedback(threadId, turnIndex, action.note, action.humanReview);
        setFeedbackByTurn((prev) => ({ ...prev, [turnIndex]: feedback }));
      }
      return { status: 'ok' };
    } catch (err) {
      return reportRefusal(err);
    }
  }, [threadId]);

  return { handleThumbUp, handleThumbDown, handleThumbDownFollowUp, feedbackByTurn, feedbackLoading, loadFeedback };
}
