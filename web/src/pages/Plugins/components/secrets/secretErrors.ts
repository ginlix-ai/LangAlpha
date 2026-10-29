import type { TFunction } from 'i18next';
import {
  apiErrorCoded,
  apiErrorReason,
  apiErrorStatus,
  apiValidationErrors,
} from '@/api/errors';
import type { FormError } from './secretDraft';

/**
 * A refused save, placed on the field it is about. The server's sentences are
 * written for the API, so the two a form can act on (the name is taken, the
 * account is full) are said in the form's own words, and a validation message
 * lands under the field it names rather than as `body.value: …` over the page.
 */

function fieldError(err: unknown, t: TFunction): FormError | null {
  for (const { field, message } of apiValidationErrors(err)) {
    if (field === 'name') return { field: 'name', message: t('vault.nameInvalid') };
    if (field === 'value' || field === 'description') return { field, message };
  }
  return null;
}

function notSaved(err: unknown, t: TFunction): FormError {
  const reason = apiErrorReason(err);
  return {
    field: 'form',
    message: reason ? t('vault.errors.notSaved', { reason }) : t('vault.errors.notSavedRetry'),
  };
}

export function createError(
  err: unknown,
  t: TFunction,
  { name, max }: { name: string; max: number },
): FormError {
  if (apiErrorCoded(err, 'secret_exists')) {
    return { field: 'name', message: t('vault.nameTaken'), existing: name };
  }
  if (apiErrorCoded(err, 'secret_limit')) return { field: 'form', message: t('vault.atCapHint', { max }) };
  return fieldError(err, t) ?? notSaved(err, t);
}

export function updateError(err: unknown, t: TFunction): FormError {
  return fieldError(err, t) ?? notSaved(err, t);
}

export function deleteError(err: unknown, t: TFunction): string {
  const reason = apiErrorReason(err);
  return reason ? t('vault.errors.notDeleted', { reason }) : t('vault.errors.notDeletedRetry');
}

export function revealError(err: unknown, t: TFunction): string {
  const reason = apiErrorReason(err);
  return reason ? t('vault.errors.notRevealed', { reason }) : t('vault.errors.notRevealedRetry');
}

/** A delete or update the server answers with 404 found the key already gone. */
export function isAlreadyGone(err: unknown): boolean {
  return apiErrorStatus(err) === 404;
}
