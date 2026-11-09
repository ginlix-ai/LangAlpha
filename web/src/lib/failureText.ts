/**
 * The one reader of an API failure for a surface that leads with its own
 * sentence. `ApiError.message` restates the status ("Request failed with
 * status code 409"); the server's `detail` is the sentence a reader can act
 * on, and it lives on the error's response. Falls back to the surface's own
 * copy when the server wrote none.
 */
import { apiErrorReason } from '@/api/errors';

export function failureText(error: unknown, fallback: string): string | null {
  if (!error) return null;
  return apiErrorReason(error) ?? fallback;
}
