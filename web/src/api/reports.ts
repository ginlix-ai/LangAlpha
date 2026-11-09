/**
 * Reports to the team: a problem or feedback from the account menu, or a
 * failed turn from its error banner. The server adds who and where and the
 * links our engineers open; the conversation itself is never sent.
 */
import { api } from '@/api/client';
import { apiErrorObject, apiErrorStatus } from '@/api/errors';
import { isDesktopShell } from '@/lib/desktop';
import i18n from '@/i18n';

export type ReportCategory = 'bug' | 'feedback' | 'billing' | 'other';

export interface ReportDraft {
  event: 'user_report' | 'error_report';
  source: 'button' | 'error_banner';
  category?: ReportCategory;
  note?: string;
  threadId?: string;
  /** The failed run the report is about, as its error frame named it. */
  runId?: string;
  error?: { type: string; message: string };
}

/** Why a report route did not take a call, as every report route answers. */
export type ReportRefusal =
  | { status: 'rate_limited' | 'unavailable'; retryAfter: number | null }
  | { status: 'gone' | 'closed' | 'note_limit' };

export type ReportSendOutcome = { status: 'accepted' } | { status: 'duplicate' } | ReportRefusal;

/** The refusal a report route's error carries (`report_refusal` on the server). */
export function reportRefusal(err: unknown): ReportRefusal {
  const status = apiErrorStatus(err);
  const detail = apiErrorObject(err);
  if (status === 409 && detail?.error === 'report_note_limit') return { status: 'note_limit' };
  if (status === 404) return { status: 'gone' };
  if (status === 409) return { status: 'closed' };
  const retryAfter = typeof detail?.retry_after === 'number' ? detail.retry_after : null;
  return { status: status === 429 ? 'rate_limited' : 'unavailable', retryAfter };
}

/** The app a report comes from, as the platform's post names it. */
export function reportClient(): { surface: 'web' | 'desktop'; app_version: null; locale: string | null } {
  return {
    surface: isDesktopShell() ? 'desktop' : 'web',
    app_version: null,
    locale: i18n.language || null,
  };
}

/** Past the server's 45s wait on the platform, so a stalled request ends as
 *  unavailable and the dialog that waits on it can close. Ids are kept, so a
 *  retry is not posted twice. */
export const REPORT_REQUEST_TIMEOUT_MS = 60_000;

export async function getReportsEnabled(): Promise<boolean> {
  const { data } = await api.get<{ enabled: boolean }>('/api/v1/reports/config');
  return data.enabled;
}

/**
 * `reportId` is made once per report by the caller and kept across retries,
 * so a retry after a timeout is not posted twice.
 */
export async function sendReport(reportId: string, draft: ReportDraft): Promise<ReportSendOutcome> {
  try {
    const { data } = await api.post<{ status: 'accepted' | 'duplicate' }>('/api/v1/reports', {
      report_id: reportId,
      event: draft.event,
      source: draft.source,
      category: draft.category ?? null,
      user_note: draft.note?.trim() || null,
      thread_id: draft.threadId ?? null,
      run_id: draft.runId ?? null,
      error: draft.error ?? null,
      client: reportClient(),
    }, { timeout: REPORT_REQUEST_TIMEOUT_MS });
    return { status: data.status };
  } catch (err) {
    return reportRefusal(err);
  }
}
