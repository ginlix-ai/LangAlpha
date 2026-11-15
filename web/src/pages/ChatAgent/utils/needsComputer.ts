/**
 * A request refused because the user has no computer and none will be built
 * for them: they chose to work on their own machine, or the deployment builds
 * none. The server answers a send, a new thread, a new workspace and a start
 * with a 409 `needs_computer` before anything runs, and the way out is adding
 * a computer, so that is what the refusal offers. A computer they have with
 * its backup off can't hold Home either, and that refusal (`needs_backup`)
 * offers the computer, where backup is turned on.
 */
import type { useTranslation } from 'react-i18next';

import type { ErrorLinkSpec, StructuredError } from '@/utils/rateLimitError';

type Translate = ReturnType<typeof useTranslation>['t'];

export const NEEDS_COMPUTER = 'needs_computer';

/** The link that opens the way to add one (`AddComputerLink` words it for the device). */
export const ADD_COMPUTER_LINK: ErrorLinkSpec = {
  url: '/chat',
  label: 'Add a computer',
  labelKey: 'homePlacement.addComputer',
  addComputer: true,
};

/** Whether a refusal is `needs_computer`, as the stream transport or the API client carries it. */
export function isNeedsComputer(err: unknown): boolean {
  if (!err || typeof err !== 'object') return false;
  const { errorInfo, response } = err as {
    errorInfo?: { code?: unknown };
    response?: { status?: number; data?: { detail?: { code?: unknown } } };
  };
  if (errorInfo?.code === NEEDS_COMPUTER) return true;
  return response?.status === 409 && response.data?.detail?.code === NEEDS_COMPUTER;
}

export function needsComputerError(t: Translate): StructuredError {
  return { message: t('homePlacement.needsComputer'), links: [ADD_COMPUTER_LINK] };
}

export const NEEDS_BACKUP = 'needs_backup';

/** The computer a `needs_backup` refusal names. */
export interface NeedsBackup {
  computerId: string;
  name: string;
}

/**
 * The computer named by a refusal for its backup being off, where it is the
 * only one Home could live on: the same 409 as `needs_computer`, with its id
 * and name, since turning its backup on is the way out. Null for any other error.
 */
export function needsBackupOf(err: unknown): NeedsBackup | null {
  if (!err || typeof err !== 'object') return null;
  const { errorInfo, response } = err as {
    errorInfo?: Record<string, unknown>;
    response?: { status?: number; data?: { detail?: unknown } };
  };
  const detail = errorInfo?.code === NEEDS_BACKUP
    ? errorInfo
    : response?.status === 409 ? (response.data?.detail as Record<string, unknown> | undefined) : undefined;
  if (!detail || typeof detail !== 'object' || detail.code !== NEEDS_BACKUP) return null;
  const { computer_id: computerId, computer_name: name } = detail;
  if (typeof computerId !== 'string' || !computerId) return null;
  return { computerId, name: typeof name === 'string' ? name : '' };
}

export function needsBackupError(t: Translate, { computerId, name }: NeedsBackup): StructuredError {
  return {
    message: t('homePlacement.needsBackup', { name }),
    links: [{ url: '/chat', label: name, computerDetails: { computerId, name } }],
  };
}
