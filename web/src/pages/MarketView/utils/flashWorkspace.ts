import { api } from '@/api/client';

/**
 * Ensure the user's Flash workspace exists and return its id. Cached in
 * module scope so a successful call happens once per session. A failed
 * call clears the cache so the next caller retries instead of the
 * session being permanently stuck on a null id. Invalidated on logout via
 * {@link clearFlashWorkspaceCache} so a different user in the same tab does
 * not inherit the previous user's workspace id.
 */
let flashWorkspaceIdPromise: Promise<string | null> | null = null;

export function getOrFetchFlashWorkspaceId(): Promise<string | null> {
  if (flashWorkspaceIdPromise) return flashWorkspaceIdPromise;
  const pending = (async () => {
    try {
      const { data } = await api.post<{ workspace_id: string }>(
        '/api/v1/workspaces/flash',
      );
      const id = data?.workspace_id ?? null;
      if (!id) {
        // Empty response — treat as failure so we retry next time.
        flashWorkspaceIdPromise = null;
      }
      return id;
    } catch (err) {
      if (import.meta.env.DEV) {
        console.warn('[chart-annotation] flash workspace lookup failed', err);
      }
      flashWorkspaceIdPromise = null;
      return null;
    }
  })();
  flashWorkspaceIdPromise = pending;
  return pending;
}

/** Drop the cached Flash workspace id. Call on logout so the next user in the
 * same tab re-resolves their own workspace instead of inheriting this one. */
export function clearFlashWorkspaceCache(): void {
  flashWorkspaceIdPromise = null;
}
