// Shared expansion state for the workspace nav panel (which workspace folders and
// which threads' agent groups are open). One NavigationPanel mounts per cached
// ChatView instance, so this state must live OUTSIDE any component to stay
// consistent as the user switches threads/workspaces.
//
// It lives in its own module (not in NavigationPanel.tsx) for two reasons:
//   1. NavigationPanel.tsx can then export only its component, so Vite Fast
//      Refresh hot-swaps it cleanly during dev instead of forcing a full reload.
//   2. The state is anchored on `globalThis`, so even if this module is itself
//      re-evaluated by HMR the panels keep reading the SAME sets. A second set
//      instance would desync cached panels: a folder opened in one panel would
//      render collapsed in another.
//
// State resets only on a real page reload (globalThis is fresh then). That
// matches the product rule: a manually expanded folder stays open all session.

interface NavExpansionState {
  workspaces: Set<string>;
  threads: Set<string>;
  version: number;
  listeners: Set<() => void>;
}

const KEY = '__langalpha_nav_expansion__';
const root = globalThis as unknown as Record<string, unknown>;

const state: NavExpansionState =
  (root[KEY] as NavExpansionState | undefined) ??
  ((root[KEY] = { workspaces: new Set<string>(), threads: new Set<string>(), version: 0, listeners: new Set<() => void>() }) as NavExpansionState);

// Live sets — read directly in render; mutate then call notifyNavExpansion().
export const expandedWorkspaces = state.workspaces;
export const expandedThreads = state.threads;

// Live external store: bump a version and ping every subscribed panel so cached
// ChatViews re-render on any toggle and never show stale folder state.
export function notifyNavExpansion(): void {
  state.version += 1;
  state.listeners.forEach((fn) => fn());
}

export function subscribeNavExpansion(fn: () => void): () => void {
  state.listeners.add(fn);
  return () => {
    state.listeners.delete(fn);
  };
}

export function getNavExpansionVersion(): number {
  return state.version;
}

// Toggle a workspace folder open/closed.
export function toggleWorkspaceExpansion(workspaceId: string): void {
  if (state.workspaces.has(workspaceId)) {
    state.workspaces.delete(workspaceId);
  } else {
    state.workspaces.add(workspaceId);
  }
  notifyNavExpansion();
}

// Toggle a thread's agent-group open/closed. Mirrors toggleWorkspaceExpansion so
// both expansion mutations live in the store rather than half here, half inline.
export function toggleThreadExpansion(threadId: string): void {
  if (state.threads.has(threadId)) {
    state.threads.delete(threadId);
  } else {
    state.threads.add(threadId);
  }
  notifyNavExpansion();
}

// Reset on logout/teardown. Bumps the version via notifyNavExpansion() like
// every other mutator, so the store API stays uniform and no future caller has
// to know this path is special. On logout the panel is already unmounted, so
// the notify is a harmless no-op there (zero subscribers) — the consistency is
// the point. forgetNavPanelExpansion (below) stays quiet by design: it runs
// only on workspace delete, where the workspace-list invalidation already
// repaints the affected row.
export function resetNavPanelExpansion(): void {
  state.workspaces.clear();
  state.threads.clear();
  notifyNavExpansion();
}

// Forget a deleted workspace so the mount-effect doesn't re-expand it (and fire
// a spurious threads 404) on the next panel remount. Only deletion is a safe
// prune signal — the workspace list is a paged/limited slice, so absence there
// can mean "scrolled out", not "gone".
export function forgetNavPanelExpansion(workspaceId: string): void {
  state.workspaces.delete(workspaceId);
}
