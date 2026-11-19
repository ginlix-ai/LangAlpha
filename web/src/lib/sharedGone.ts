/**
 * Why a page left a shared workspace, or a thread of it, that stopped being
 * the viewer's: carried to where it lands as `location.state.sharedGone`,
 * where one quiet line says so (`SharedGoneNotice`).
 */
import type { NavigateFunction, NavigateOptions } from 'react-router';
import { getSharedStanding } from '@/api/sharedWorkspaces';

/** The viewer no longer reaches the workspace (removed, or out of the
 *  organization), or it was deleted. */
export type WorkspaceGone = 'removed' | 'deleted';
/** Or a thread of it the viewer had open was deleted by someone else, or made
 *  private by the person who started it. */
export type SharedGone = WorkspaceGone | 'thread' | 'private';

/** How to leave for the page that says `reason`. */
export function sharedGoneNav(reason: SharedGone): NavigateOptions {
  return { replace: true, state: { sharedGone: reason } };
}

/** The reason a navigation's state carries, or null for any other arrival. */
export function readSharedGone(state: unknown): SharedGone | null {
  const reason = (state as { sharedGone?: unknown } | null)?.sharedGone;
  return reason === 'removed' || reason === 'deleted' || reason === 'thread' || reason === 'private' ? reason : null;
}

/**
 * Leaves a page of a workspace the viewer lost for the list, saying why:
 * deleted when the server says so, and otherwise only that it is no longer
 * theirs, which is all the server tells anyone it would otherwise tell
 * whether it still exists. With `threadLost`, a thread was refused, and a
 * viewer still in its workspace goes back to the workspace instead. The
 * answer takes a request, so the page leaves only if `stillShown` says it is
 * still the one in view.
 */
export async function leaveGoneWorkspace(
  navigate: NavigateFunction, workspaceId: string | null | undefined, stillShown: () => boolean,
  { threadLost = false }: { threadLost?: boolean } = {},
): Promise<void> {
  const state = workspaceId ? await getSharedStanding(workspaceId).catch(() => 'gone' as const) : 'gone';
  if (!stillShown()) return;
  // Still in the workspace: what went is the thread, which its starter made
  // private (a delete is told by its own event first).
  if (threadLost && state === 'active' && workspaceId) {
    void navigate(`/chat/${workspaceId}`, sharedGoneNav('private'));
    return;
  }
  void navigate('/chat', sharedGoneNav(state === 'deleted' ? 'deleted' : 'removed'));
}
