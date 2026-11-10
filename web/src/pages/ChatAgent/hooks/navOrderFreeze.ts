/**
 * The nav tree's session-stable order. The server sorts threads (and, within
 * the 'custom' workspace sort, unpinned workspaces) by updated_at DESC, so the
 * item being chatted in would hoist to the top on every refetch. The tree
 * snapshots the order seen first in this page session and reorders later
 * responses to match. The stores are module-level because ChatAgent caches one
 * ChatView instance per thread (hook-local refs would re-snapshot on every
 * thread switch, defeating the freeze), and in a module of their own so the
 * row mutations that release a freeze need not import the tree's hook.
 * Reload starts a fresh session (fresh recency order).
 */
import { createEmitter } from '@/lib/emitter';

const frozenThreadOrders: Record<string, string[]> = {};
let frozenWorkspaceOrder: string[] | null = null;

/**
 * Last-seen manual arrangement (sort_order + pin state) per workspace. The
 * frozen order neutralizes recency hoisting, but a reorder/pin made in the
 * workspace gallery also lands as a refetch, which must NOT be frozen out:
 * when an existing workspace's sort_order or pin state changes between
 * refetches, the tree re-snapshots so it tracks the gallery's 'custom' sort.
 * Recency (updated_at) bumps and newly paged-in workspaces don't change these
 * values, so the active workspace still never hoists itself.
 */
export const lastWorkspaceArrangement = new Map<string, { sortOrder: number | null; pinned: boolean }>();

export const frozenThreadOrder = (wsId: string): string[] | undefined => frozenThreadOrders[wsId];
export const getFrozenWorkspaceOrder = (): string[] | null => frozenWorkspaceOrder;
export function setFrozenWorkspaceOrder(order: string[] | null): void {
  frozenWorkspaceOrder = order;
}

export function resetStableNavOrder() {
  for (const key of Object.keys(frozenThreadOrders)) delete frozenThreadOrders[key];
  frozenWorkspaceOrder = null;
  lastWorkspaceArrangement.clear();
}

// Drop a deleted workspace from the frozen-order stores so they don't retain
// ghost ids for the rest of the session. Deleted ids are already filtered out
// of the rendered list (applyStableOrderBy drops map-misses), so this is
// housekeeping, not a correctness fix — call it from the workspace delete path.
export function forgetStableNavOrder(workspaceId: string) {
  delete frozenThreadOrders[workspaceId];
  lastWorkspaceArrangement.delete(workspaceId);
  if (frozenWorkspaceOrder) {
    const idx = frozenWorkspaceOrder.indexOf(workspaceId);
    if (idx !== -1) frozenWorkspaceOrder.splice(idx, 1);
  }
}

// Bump notifications: chatting in a thread moves it to the top of its
// workspace's list (like normal chat apps), while clicking around never
// reorders. Subscribed hooks re-apply the frozen orders when the version ticks.
let navOrderVersion = 0;
const navOrderEmitter = createEmitter();
export const subscribeNavOrder = navOrderEmitter.subscribe;
export const getNavOrderVersion = () => navOrderVersion;

/** Announce a deliberate reordering; every mounted nav panel re-applies the frozen orders. */
export function bumpNavOrder(): void {
  navOrderVersion++;
  navOrderEmitter.emit();
}

/**
 * Release the WORKSPACE tier's session freeze and re-render every nav panel.
 * The pin path needs exactly this after its refetch: `resetStableNavOrder`
 * also clears the thread tier, `forgetStableNavOrder` is workspace-scoped, and
 * neither notifies — nothing re-renders on a bare module-store write.
 */
export function resetWorkspaceOrderFreeze(): void {
  frozenWorkspaceOrder = null;
  bumpNavOrder();
}

/**
 * Release one workspace's THREAD order freeze and re-render every nav panel,
 * so a thread unpinned there falls back to its recency slot instead of
 * squatting at the top for the session. Call it only once the workspace's
 * threads have been read again: a render while the cache is merely stale
 * would re-freeze the old order.
 */
export function releaseThreadOrderFreeze(wsId: string): void {
  delete frozenThreadOrders[wsId];
  bumpNavOrder();
}

/**
 * Move a thread to the top of its workspace's frozen order. Called when the
 * user sends a message (new turn, steering, edit/regenerate/retry). No-op if
 * the workspace's order hasn't been snapshotted yet — the initial snapshot is
 * recency-sorted, so the thread lands on top anyway.
 */
export function bumpThreadNavOrder(wsId: string, threadId: string | null | undefined) {
  if (!wsId || !threadId || threadId === '__default__') return;
  const frozen = frozenThreadOrders[wsId];
  if (!frozen) return;
  const idx = frozen.indexOf(threadId);
  if (idx === 0) return;
  if (idx > 0) frozen.splice(idx, 1);
  frozen.unshift(threadId);
  bumpNavOrder();
}

/**
 * Commit-phase writer for the thread tier's frozen order: store the assembled
 * order whenever it carries ids the snapshot doesn't have (or there is no
 * snapshot yet). Silent — no version bump — because this is absorption, not a
 * reordering. It covers three roles the old render-phase write served: the
 * first-load snapshot (`bumpThreadNavOrder` no-ops without one), the re-freeze
 * after a pin deletes the snapshot, and continuous absorption of new ids —
 * without the last, `applyStableOrderBy` re-classifies an unfrozen id
 * positionally every render and sinks a fresh thread to the bottom of the tree
 * as soon as an older frozen thread sorts above it.
 */
export function absorbThreadOrder(wsId: string, order: string[]): void {
  const frozen = frozenThreadOrders[wsId];
  if (frozen) {
    const known = new Set(frozen);
    if (order.every((id) => known.has(id))) return;
  }
  frozenThreadOrders[wsId] = order;
}
