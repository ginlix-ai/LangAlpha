/**
 * The workspace a thread's route opens it in. A navigation may name one
 * (state from a list row, a saved app, a page's link) to skip the lookup, and
 * the route takes it on trust; a thread belongs to one workspace, though, and
 * the server refuses a send that runs an existing thread in another when
 * either side is shared. So once the thread's own is known and the two differ
 * across that line, its own wins. A personal pair is left as named: a Home
 * thread on a former Home row is still sent with Home's id.
 */
export function threadWorkspaceFor(
  named: string | null,
  own: { workspaceId: string; shared: boolean } | null,
  isShared: (workspaceId: string) => boolean,
): string | null {
  if (!own || !named || named === own.workspaceId) return named ?? own?.workspaceId ?? null;
  return own.shared || isShared(named) ? own.workspaceId : named;
}
