/**
 * Turn feedback endpoints.
 */
import { api } from '@/api/client';
import { REPORT_REQUEST_TIMEOUT_MS, reportClient } from '@/api/reports';

/** A turn's rating as the server returns it (`FeedbackResponse`). */
export interface ThreadFeedback {
  conversation_feedback_id: string;
  turn_index: number;
  rating: string;
  issue_categories: string[] | null;
  comment: string | null;
  consent_human_review: boolean;
  review_status: string | null;
  created_at: string;
  /** The report a thumbs-down sends; null on a thumbs-up, and on a
   *  thumbs-down from before reports, which has none to add to. */
  report_id: string | null;
  /** When the team got the report this thumbs-down sent; null until it did. */
  reported_at: string | null;
  /** The notes the user added to that report since. */
  report_follow_ups: Array<{ note: string; at: string; id?: string | null }>;
  /** What became of the report this call sent; null when it sent none. */
  report_status: string | null;
}

export async function submitFeedback(
  threadId: string,
  turnIndex: number,
  rating: string,
  issueCategories: string[] | null = null,
  comment: string | null = null,
  consentHumanReview: boolean = false
): Promise<ThreadFeedback> {
  const { data } = await api.post<ThreadFeedback>(`/api/v1/threads/${threadId}/feedback`, {
    turn_index: turnIndex,
    rating,
    issue_categories: issueCategories,
    comment: comment || null,
    consent_human_review: consentHumanReview,
    // A thumbs-down is also sent to the team, and its post names the app.
    client: rating === 'thumbs_down' ? reportClient() : null,
  }, {
    // A thumbs-down waits on the platform, as does a thumbs-up over one.
    timeout: REPORT_REQUEST_TIMEOUT_MS,
  });
  return data;
}

/** Remove a rating. A thumbs-down's report to the team is withdrawn with it,
 *  and the rating stays when that could not be done now (`reportRefusal`). */
export async function removeFeedback(threadId: string, turnIndex: number) {
  const { data } = await api.delete(`/api/v1/threads/${threadId}/feedback`, {
    params: { turn_index: turnIndex },
    // Withdrawing a thumbs-down's report waits on the platform.
    timeout: REPORT_REQUEST_TIMEOUT_MS,
  });
  return data;
}

export async function getThreadFeedback(threadId: string): Promise<ThreadFeedback[]> {
  const { data } = await api.get<ThreadFeedback[]>(`/api/v1/threads/${threadId}/feedback`);
  return data;
}

/** Add a note to the report a thumbs-down sent, ask for a human review, or
 *  with neither, send the report again if the team never got it. */
export async function followUpFeedback(
  threadId: string,
  turnIndex: number,
  /** `id` is made once per note and kept across its retries, so the note
   *  is posted once. */
  note: { id: string; text: string } | null,
  humanReview: boolean,
): Promise<ThreadFeedback> {
  const { data } = await api.post<ThreadFeedback>(`/api/v1/threads/${threadId}/feedback/follow-up`, {
    turn_index: turnIndex,
    note: note?.text ?? null,
    note_id: note?.id ?? null,
    human_review: humanReview,
    client: reportClient(),
  }, { timeout: REPORT_REQUEST_TIMEOUT_MS });
  return data;
}

// --- File uploads ---
