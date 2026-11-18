import { noteIfSuspended } from '@/lib/accountSuspension';

/**
 * A thread's long-lived streams (the thread stream, a run's stream and the watch) refused on
 * reconnect: the server ends both, as a timed out stream ends, once the viewer
 * may no longer read a shared workspace's thread, and the reconnect then meets
 * the route's 403 or 404. The streams only report it here; the chat route
 * decides what it means, since only it knows whether the thread was shared.
 * The run stream a turn is followed on ends the same way, as a dropped one.
 * A suspended account's refusal goes to the app-wide notice instead.
 */
type Listener = (threadId: string) => void;
const listeners = new Set<Listener>();

export function onThreadRefused(listener: Listener): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

/** Reports a refusal (`status`, and the body's `detail`) of one of the
 *  thread's streams. */
export function noteThreadRefused(threadId: string, status: number | undefined, detail: unknown): void {
  if (status !== 403 && status !== 404) return;
  if (status === 403 && noteIfSuspended(detail)) return;
  listeners.forEach((listener) => listener(threadId));
}

/** The same for a stream's raw response, whose body it reads. */
export async function noteThreadResponseRefused(threadId: string, res: Response): Promise<void> {
  if (res.status !== 403 && res.status !== 404) return;
  let detail: unknown = null;
  try {
    detail = ((await res.clone().json()) as { detail?: unknown } | null)?.detail ?? null;
  } catch {
    // A body that is not JSON names no suspension.
  }
  noteThreadRefused(threadId, res.status, detail);
}
