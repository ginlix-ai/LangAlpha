/**
 * One reader for a refused request.
 *
 * A 429 is the only refusal whose wording this client does not own: the quota
 * service knows the plan, the counts and the user's language, so its sentence
 * is relayed as it arrived. Everything else falls back to the structured
 * `detail` the server sent, and only then to a generic line.
 */
import type { useTranslation } from 'react-i18next';

import { formatApiErrorDetail } from './api';
import { buildRateLimitError, type RateLimitErrorInfo } from '@/utils/rateLimitError';

type Translate = ReturnType<typeof useTranslation>['t'];

export function denialMessage(err: unknown, t: Translate): string {
  const e = err as { status?: number; rateLimitInfo?: RateLimitErrorInfo };
  if (e?.status === 429 && e.rateLimitInfo) {
    const platformUrl =
      (import.meta.env.VITE_PLATFORM_URL as string | undefined) || '/account';
    return buildRateLimitError(e.rateLimitInfo, platformUrl).message;
  }
  const detail = formatApiErrorDetail(err);
  return detail || t('common.error', 'Error');
}
