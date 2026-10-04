import { apiErrorStatus } from './api/errors';

/** Whether a failed thread lookup should evict the user back to the gallery.
 *
 * `needsThreadLookup` is the load-bearing term. ChatAgent's redirect effect
 * re-runs on every navigation (useNavigate's identity tracks the location), so
 * acting on an error from a lookup this route never asked for bounces the user
 * out of every /chat/* they click into. A 403 keeps them here for the
 * access-denied surface instead of silently dropping them at the gallery.
 * Once the thread has been read, only a 404 evicts: the same query refetches
 * on refocus and at every turn's end, and a blip there says nothing about the
 * thread the user is in.
 */
export function shouldLeaveThreadRoute(
  needsThreadLookup: boolean,
  threadError: unknown,
  accessDenied: boolean,
  threadResolved: boolean,
): boolean {
  if (!needsThreadLookup || !threadError || accessDenied) return false;
  return !threadResolved || apiErrorStatus(threadError) === 404;
}
