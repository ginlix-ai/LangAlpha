/**
 * A request refused because the user has no computer and none will be built
 * for them: they chose to work on their own machine, or the deployment builds
 * none. The server answers a send, a new thread, a new workspace and a start
 * with a 409 `needs_computer` before anything runs, and the way out is adding
 * a computer, so that is what the refusal offers. A computer they have with
 * its backup off can't hold Home either, and that refusal (`needs_backup`)
 * offers the computer, where backup is turned on.
 */
import { apiErrorCoded } from '@/api/errors';
import type { StructuredError } from '@/utils/rateLimitError';

type Translate = (key: string, options?: Record<string, unknown>) => string;

export const NEEDS_COMPUTER = 'needs_computer';

/**
 * The detail of a refusal carrying `code`, as either carrier holds it: the
 * stream transport puts it on `errorInfo`, the API client on the response.
 */
function refusalCoded(err: unknown, code: string): Record<string, unknown> | null {
  const info = (err as { errorInfo?: Record<string, unknown> } | null)?.errorInfo;
  return info?.code === code ? info : apiErrorCoded(err, code);
}

/** Whether a refusal is `needs_computer`. */
export function isNeedsComputer(err: unknown): boolean {
  return refusalCoded(err, NEEDS_COMPUTER) !== null;
}

export function needsComputerError(t: Translate): StructuredError {
  return { message: t('homePlacement.needsComputer'), links: [{ kind: 'needsComputer' }] };
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
  const detail = refusalCoded(err, NEEDS_BACKUP);
  const computerId = detail?.computer_id;
  if (typeof computerId !== 'string' || !computerId) return null;
  const name = detail?.computer_name;
  return { computerId, name: typeof name === 'string' ? name : '' };
}

export function needsBackupError(t: Translate, { computerId, name }: NeedsBackup): StructuredError {
  return {
    message: t('homePlacement.needsBackup', { name }),
    links: [{ kind: 'needsBackup', computerId, name }],
  };
}
