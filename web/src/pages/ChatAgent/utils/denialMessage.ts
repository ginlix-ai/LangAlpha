/**
 * One reader for a refused request.
 *
 * A 429 is the only refusal whose wording this client does not own: the quota
 * service knows the plan, the counts and the user's language, so its sentence
 * is relayed as it arrived. A refused workspace name is read by its code,
 * since the sentence beside it is English. Everything else falls back to the
 * structured `detail` the server sent, and only then to a generic line.
 */
import type { useTranslation } from 'react-i18next';

import { formatApiErrorDetail } from './api';
import { apiErrorCoded } from '@/api/errors';
import { workspaceNameErrorMessage } from './workspaceName';
import { buildRateLimitError, type RateLimitErrorInfo } from '@/utils/rateLimitError';

type Translate = ReturnType<typeof useTranslation>['t'];

export function denialMessage(err: unknown, t: Translate): string {
  const e = err as { status?: number; rateLimitInfo?: RateLimitErrorInfo };
  if (e?.status === 429 && e.rateLimitInfo) {
    const platformUrl =
      (import.meta.env.VITE_PLATFORM_URL as string | undefined) || '/account';
    return buildRateLimitError(e.rateLimitInfo, platformUrl).message;
  }
  const nameRefusal = workspaceNameErrorMessage(err, t);
  if (nameRefusal) return nameRefusal;
  const detail = formatApiErrorDetail(err);
  return detail || t('common.error', 'Error');
}

/**
 * A refused workspace create. The target computer still being prepared is
 * read by its code, since the server's sentence for it is English.
 */
export function createWorkspaceDenialMessage(err: unknown, t: Translate): string {
  if (apiErrorCoded(err, 'computer_preparing')) {
    return t(
      'workspace.computerPreparing',
      "This computer is still being prepared. Try again once it's ready.",
    );
  }
  return denialMessage(err, t);
}
