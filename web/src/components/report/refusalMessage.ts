import type { TFunction } from 'i18next';
import type { ReportRefusal } from '@/api/reports';

/** What the user is told when a report, or an addition to one, was not taken. */
export function refusalMessage(t: TFunction, refusal: ReportRefusal): string {
  switch (refusal.status) {
    case 'gone':
    case 'closed':
      return t('report.followUp.gone');
    case 'note_limit':
      return t('report.followUp.noteLimit');
    case 'rate_limited':
      return refusal.retryAfter
        ? t('report.rateLimitedIn', { minutes: Math.max(1, Math.ceil(refusal.retryAfter / 60)) })
        : t('report.rateLimited');
    case 'unavailable':
      return t('report.unavailable');
  }
}
