/**
 * The rules a workspace name answers to.
 *
 * A name is unique per user and also names the workspace's folder, so the
 * server refuses one that is taken or that folds to nothing usable. Its
 * refusal carries a `code` beside an English sentence, so the copy is chosen by
 * code here and read in the user's language.
 */
import type { useTranslation } from 'react-i18next';

import { apiErrorDetail } from '@/api/errors';

type Translate = ReturnType<typeof useTranslation>['t'];

/** The server's cap on create and rename; a longer name is a 422. */
export const WORKSPACE_NAME_MAX_LENGTH = 80;

/**
 * `value` cut to the server's cap in code points, which is what Python's `len`
 * counts. The native `maxLength` counts UTF-16 units instead, so an emoji spends
 * two and a field using it stops at 40 emoji the server would take 80 of.
 */
export function clampWorkspaceName(value: string): string {
  const chars = Array.from(value);
  return chars.length > WORKSPACE_NAME_MAX_LENGTH
    ? chars.slice(0, WORKSPACE_NAME_MAX_LENGTH).join('')
    : value;
}

/**
 * Copy for a refused workspace name, or null when `err` is any other failure.
 * Duplicate picks the copy's name itself, so a taken name there is a race lost
 * to another create, and the way out is to try again rather than to choose.
 */
export function workspaceNameErrorMessage(
  err: unknown,
  t: Translate,
  flow: 'name' | 'duplicate' = 'name',
): string | null {
  const detail = apiErrorDetail(err);
  if (!detail || typeof detail !== 'object' || Array.isArray(detail)) return null;
  const { code, name, reason } = detail as {
    code?: unknown;
    name?: unknown;
    reason?: unknown;
  };
  if (code === 'workspace_name_invalid') {
    if (reason === 'empty') return t('workspace.nameEmpty');
    if (reason === 'too_long') {
      return t('workspace.nameTooLong', { max: WORKSPACE_NAME_MAX_LENGTH });
    }
    if (reason === 'reserved' && typeof name === 'string') {
      return t('workspace.nameReserved', { name });
    }
    return t('workspace.nameInvalid');
  }
  if (code === 'workspace_folder_moving') return t('workspace.folderMoving');
  if (code !== 'workspace_name_taken' || typeof name !== 'string') return null;
  return flow === 'duplicate'
    ? t('workspace.copyNameTaken', { name })
    : t('workspace.nameTaken', { name });
}
