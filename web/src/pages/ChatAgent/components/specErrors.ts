/**
 * Copy for a spec change that did not happen: the refusal a request gets up
 * front (a 409, or a plan refusal) and the failure a change settles with
 * later. Both surfaces that show one, the change-spec dialog and the toast
 * for a change that settled while it was closed, read it from here.
 */
import type { useTranslation } from 'react-i18next';

import type { ComputerSpecChange, ResourceTier, SpecChangeErrorCode, UnsavedFile } from '@/types/api';

import { specConflictDetail, type SpecConflictDetail } from '../utils/api';
import { entitlementErrorMessage } from '../utils/entitlementErrors';
import { shownPath, unsavedReasonLabel } from './filePanel/unsavedReason';

type Translate = ReturnType<typeof useTranslation>['t'];

/** A spec change that did not happen, shaped for the dialog: a sentence and,
 *  for a backup that could not take some files, which ones. */
export interface SpecError {
  message: string;
  files?: UnsavedFile[];
}

type SpecErrorCode = SpecChangeErrorCode | SpecConflictDetail['code'];

function genericFailure(t: Translate): SpecError {
  return {
    message: t(
      'computer.spec.failed',
      'The spec change failed. Your files are safe and the computer stays at its previous spec.',
    ),
  };
}

/**
 * Copy for one code. Most codes carry wording written here; the ones whose
 * reason only the server can state (how much a downgrade would not fit, what
 * the plan forbids) relay its sentence, which always says what to do next.
 */
function specErrorForCode(
  t: Translate,
  code: SpecErrorCode,
  serverMessage: string,
  files: UnsavedFile[],
): SpecError {
  switch (code) {
    case 'turn_active':
      return {
        message: t(
          'computer.spec.turnActive',
          'An Agent is working on this computer. Wait for the running turn to finish, then try again.',
        ),
      };
    case 'backup_incomplete':
      // A project that failed outright names no files; saying which ones
      // would read as the whole problem.
      return files.length > 0
        ? {
            message: t(
              'computer.spec.backupIncomplete',
              'Nothing was changed. The spec change backs up every file first, and these files could not be backed up. Move or delete them, or ask the Agent to clean up, then try again.',
            ),
            files,
          }
        : {
            message: t(
              'computer.spec.backupIncompleteUnnamed',
              "Nothing was changed. This computer's files could not be backed up first. Try again in a moment.",
            ),
          };
    case 'spec_in_progress':
      return { message: t('computer.spec.inProgress', 'A spec change is already running on this computer.') };
    case 'busy':
      return {
        message: t(
          'computer.spec.busy',
          'The computer was busy with another operation, so the spec was not changed. Try again in a moment.',
        ),
      };
    case 'interrupted':
      // Where it stopped is not known, so the copy does not promise the old spec.
      return {
        message: t(
          'computer.spec.interrupted',
          "The spec change was interrupted before it finished. Your files are safe. Check the computer's current spec and try again.",
        ),
      };
    case 'disk_too_small':
    case 'not_allowed':
      return serverMessage ? { message: serverMessage } : genericFailure(t);
    default:
      return genericFailure(t);
  }
}

/** The refusal a spec request got up front. */
export function specErrorFrom(err: unknown, t: Translate, tier: ResourceTier): SpecError {
  const conflict = specConflictDetail(err);
  if (conflict) return specErrorForCode(t, conflict.code, conflict.message, conflict.files ?? []);
  return { message: entitlementErrorMessage(err, t, tier) };
}

/** The failure a change settled with. */
export function specErrorFromOutcome(change: ComputerSpecChange, t: Translate): SpecError {
  const error = change.error;
  return error ? specErrorForCode(t, error.code, error.message, error.files) : genericFailure(t);
}

/** One line per file: its path and why the backup could not take it. */
export function specErrorFiles(files: SpecError['files'], t: Translate, locale: string): string[] {
  return (files ?? []).map((f) => `${shownPath(f.path)}: ${unsavedReasonLabel(t, f, locale)}`);
}
