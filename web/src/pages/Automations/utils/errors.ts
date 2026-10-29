import {
  apiErrorDetail,
  apiErrorDetailMessage,
  apiValidationErrors,
} from '@/api/errors';

/**
 * What a failed write says to the reader. A 422 names each field by its own
 * name rather than its `body.` path, since the reader filled a form, not a
 * request body.
 */
export function mutationErrorMessage(err: unknown, fallback: string): string {
  const invalid = apiValidationErrors(err);
  if (invalid.length) {
    return invalid.map((e) => (e.field ? `${e.field}: ${e.message}` : e.message)).join('; ');
  }
  const detail = apiErrorDetail(err);
  if (typeof detail === 'string' && detail) return detail;
  // A refusal that names each entry it refused (`problems`) says the whole
  // of it in `message`, or in a `detail` of its own.
  const nested = (detail as { detail?: unknown } | null)?.detail;
  const message =
    apiErrorDetailMessage(err) ?? (typeof nested === 'string' && nested ? nested : (err as { message?: unknown })?.message);
  return typeof message === 'string' && message ? message : fallback;
}
