/**
 * Turn feedback endpoints.
 */
import { api } from '@/api/client';

export async function submitFeedback(
  threadId: string,
  turnIndex: number,
  rating: string,
  issueCategories: string[] | null = null,
  comment: string | null = null,
  consentHumanReview: boolean = false
) {
  const { data } = await api.post(`/api/v1/threads/${threadId}/feedback`, {
    turn_index: turnIndex,
    rating,
    issue_categories: issueCategories,
    comment: comment || null,
    consent_human_review: consentHumanReview,
  });
  return data;
}

export async function removeFeedback(threadId: string, turnIndex: number) {
  const { data } = await api.delete(`/api/v1/threads/${threadId}/feedback`, {
    params: { turn_index: turnIndex },
  });
  return data;
}

export async function getThreadFeedback(threadId: string) {
  const { data } = await api.get(`/api/v1/threads/${threadId}/feedback`);
  return data;
}

// --- File uploads ---
