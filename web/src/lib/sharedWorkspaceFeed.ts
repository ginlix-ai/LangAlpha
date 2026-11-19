/**
 * What the user feed says of shared workspaces, beside the thread events it
 * shares with every workspace. The server passes an account only the events
 * of the shared workspaces it reaches at that moment, and says once when it
 * stops reaching one (`shared_access_ended`) or when one it reached was
 * deleted. Each event moves the queries that read what changed; a missed one
 * is mended by the same reads on focus and on the feed's reconnect.
 */
import type { QueryClient } from '@tanstack/react-query';
import { getSharedStanding } from '@/api/sharedWorkspaces';
import { createEmitter } from '@/lib/emitter';
import { isThreadDeleted } from '@/lib/deletedThreads';
import { queryKeys } from '@/lib/queryKeys';
import { refreshArtifactThreads } from '@/lib/artifactCache';
import { threadStanding } from '@/lib/sharedThreads';
import { dropThreadFromViews, threadRowsOf } from '@/lib/threadListCache';
import type { Thread, User } from '@/types/api';
import type { SharedWorkspaceList } from '@/types/sharedWorkspaces';
import { pruneThread } from './threadLifecycle/store';
import type { WorkspaceGone } from './sharedGone';

export interface SharedWorkspaceGone {
  workspaceId: string;
  reason: WorkspaceGone;
}

const gone = createEmitter<SharedWorkspaceGone>();

/** Hear of a shared workspace the user no longer reaches, or that was
 *  deleted, so a page showing it can leave. */
export const subscribeSharedWorkspaceGone = gone.subscribe;

/** Why a shared workspace's thread left the viewer: deleted by someone else,
 *  or made private by the person who started it. */
export type SharedThreadGone = 'thread' | 'private';

const threadGone = createEmitter<{ threadId: string; workspaceId: string; reason: SharedThreadGone }>();

/** Hear of a shared workspace's thread deleted by someone else, or made
 *  private by its starter, so a page showing it can leave for its workspace. */
export const subscribeSharedThreadGone = threadGone.subscribe;

function tellThreadGone(threadId: string, workspaceId: string, reason: SharedThreadGone): void {
  try {
    threadGone.emit({ threadId, workspaceId, reason });
  } catch {
    // A page failing to leave must not drop the feed.
  }
}

/** A thread was deleted: if it was a shared workspace's and not by this tab,
 *  whoever has it open is told. */
export function tellSharedThreadDeleted(qc: QueryClient, threadId: string, workspaceId: string | null | undefined): void {
  if (!workspaceId || isThreadDeleted(threadId) || !isCachedSharedWorkspace(qc, workspaceId)) return;
  tellThreadGone(threadId, workspaceId, 'thread');
}

/** A thread of a shared workspace was shared with it or made private, by
 *  the viewer or by someone else. Shared, it joins the members' lists on
 *  their next read. Made private, it leaves every view of whoever did not
 *  start it, as a deleted one does; its starter reads it again. Who may send
 *  a comment to it changed either way. */
export function applyThreadVisibility(qc: QueryClient, { threadId, workspaceId, shared, viewerIsStarter }: {
  threadId: string;
  workspaceId: string;
  shared: boolean;
  viewerIsStarter: boolean;
}): void {
  void qc.invalidateQueries({ queryKey: queryKeys.threads.recentAll() });
  if (shared || viewerIsStarter) {
    void qc.invalidateQueries({ queryKey: queryKeys.threads.detail(threadId) });
    void qc.invalidateQueries({ queryKey: queryKeys.threads.byWorkspace(workspaceId) });
    refreshArtifactThreads(qc);
    return;
  }
  dropThreadFromViews(qc, threadId, workspaceId);
  void qc.invalidateQueries({ queryKey: queryKeys.sharedWorkspaces.list() });
  tellThreadGone(threadId, workspaceId, 'private');
}

/** Whether the viewer started a thread, as any cached read of it says; false
 *  when none names its starter. */
function viewerStarted(qc: QueryClient, threadId: string, workspaceId: string): boolean {
  const info = qc.getQueryData<Thread>(queryKeys.threads.detail(threadId))?.shared
    ?? threadRowsOf(qc.getQueryData(queryKeys.threads.byWorkspace(workspaceId))).find((r) => r.thread_id === threadId)?.shared;
  const me = qc.getQueryData<User>(queryKeys.user.me())?.user_id;
  return !!info && threadStanding(info, me, false).mine;
}

const SHARED_EVENTS = new Set(['shared_members', 'shared_workspace', 'shared_access_ended', 'thread_visibility', 'shared_reach']);

export function isSharedWorkspaceEvent(type: string | undefined): boolean {
  return !!type && SHARED_EVENTS.has(type);
}

/** Whether the user's shared list names the workspace. */
export function listsSharedWorkspace(list: SharedWorkspaceList | undefined, workspaceId: string | null | undefined): boolean {
  return !!workspaceId && !!list?.workspaces.some((e) => e.workspace.workspace_id === workspaceId);
}

/** Whether the user's cached shared list names the workspace: the only
 *  workspaces whose events change anything here beyond their threads. */
export function isCachedSharedWorkspace(qc: QueryClient, workspaceId: string | null | undefined): boolean {
  return listsSharedWorkspace(qc.getQueryData<SharedWorkspaceList>(queryKeys.sharedWorkspaces.list()), workspaceId);
}

/** A shared workspace's files may have changed with any member's turn. The
 *  one whose turn it was refreshes from its stream; the others from here. */
export function refreshSharedFiles(qc: QueryClient, workspaceId: string | null | undefined): void {
  if (!workspaceId || !isCachedSharedWorkspace(qc, workspaceId)) return;
  void qc.invalidateQueries({ queryKey: queryKeys.workspaceFiles.workspace(workspaceId) });
}

/** Which workspace a cached thread is in, from any thread list or detail. */
function cachedThreadWorkspace(qc: QueryClient, threadId: string): string | null {
  for (const [, data] of qc.getQueriesData({ queryKey: queryKeys.threads.all })) {
    const detail = data as { thread_id?: unknown; workspace_id?: unknown } | null | undefined;
    if (detail?.thread_id === threadId && typeof detail.workspace_id === 'string') return detail.workspace_id;
    const row = threadRowsOf(data).find((r) => r.thread_id === threadId);
    if (row?.workspace_id) return row.workspace_id;
  }
  return null;
}

/** A thread was read: when its shared workspace was listed as holding one
 *  unread, read the list again, which says whether any other still is. */
export function refreshSharedUnreadAfterSeen(qc: QueryClient, threadId: string): void {
  const list = qc.getQueryData<SharedWorkspaceList>(queryKeys.sharedWorkspaces.list());
  if (!list?.workspaces.some((e) => e.has_unread)) return;
  const workspaceId = cachedThreadWorkspace(qc, threadId);
  if (list.workspaces.some((e) => e.has_unread && e.workspace.workspace_id === workspaceId)) {
    void qc.invalidateQueries({ queryKey: queryKeys.sharedWorkspaces.list() });
  }
}

function leave(qc: QueryClient, workspaceId: string, reason: WorkspaceGone): void {
  // Its threads leave every cached list and the lifecycle store, so nothing
  // keeps drawing or asking for them; the lists are read again if shown.
  for (const [, data] of qc.getQueriesData({ queryKey: queryKeys.threads.byWorkspace(workspaceId) })) {
    for (const row of threadRowsOf(data)) pruneThread(row.thread_id);
  }
  qc.removeQueries({ queryKey: queryKeys.threads.byWorkspace(workspaceId) });
  qc.removeQueries({ queryKey: queryKeys.sharedWorkspaces.members(workspaceId) });
  void qc.invalidateQueries({ queryKey: queryKeys.sharedWorkspaces.list() });
  void qc.invalidateQueries({ queryKey: queryKeys.threads.recentAll() });
  try {
    gone.emit({ workspaceId, reason });
  } catch {
    // A page failing to leave must not drop the feed.
  }
}

function cachedSharedIds(qc: QueryClient): Set<string> {
  const list = qc.getQueryData<SharedWorkspaceList>(queryKeys.sharedWorkspaces.list());
  return new Set(list?.workspaces.map((e) => e.workspace.workspace_id) ?? []);
}

/** A feed connection just attached, and the feed has no replay: any shared
 *  workspace event sent while it was away is lost. Read the shared list and
 *  everything shown of those workspaces again, and leave each workspace the
 *  list no longer names, as its own event would have, asking the server
 *  whether it was deleted (anyone it would tell nothing is told only that it
 *  is no longer theirs). Nothing for an account whose list was never read. */
export async function resyncSharedWorkspaces(qc: QueryClient): Promise<void> {
  if (qc.getQueryData(queryKeys.sharedWorkspaces.list()) === undefined) return;
  const before = cachedSharedIds(qc);
  for (const id of before) void qc.invalidateQueries({ queryKey: queryKeys.workspaces.detail(id) });
  try {
    await qc.invalidateQueries({ queryKey: queryKeys.sharedWorkspaces.all }, { throwOnError: true });
  } catch {
    return; // Unread: the next attach or a focus reads it again.
  }
  const after = cachedSharedIds(qc);
  await Promise.all([...before].filter((id) => !after.has(id)).map(async (id) => {
    const state = await getSharedStanding(id).catch(() => 'gone' as const);
    if (state !== 'active') leave(qc, id, state === 'deleted' ? 'deleted' : 'removed');
  }));
}

/** A shared workspace's event on the user feed. */
export interface SharedWorkspaceEvent {
  type?: string;
  workspace_id?: string | null;
  /** On `shared_workspace`: what changed, `deleted` among them. */
  change?: string | null;
  /** On `thread_visibility`: the thread, and whether it is now shared. */
  thread_id?: string | null;
  shared?: boolean;
}

export function applySharedWorkspaceEvent(qc: QueryClient, evt: SharedWorkspaceEvent): void {
  if (evt.type === 'shared_reach') {
    // The account's organization or teams changed: its teams are read again
    // with the account, and the shared list with what each names, leaving
    // the workspaces it lost and showing the ones it gained.
    void qc.invalidateQueries({ queryKey: queryKeys.user.me() });
    void resyncSharedWorkspaces(qc);
    return;
  }
  const workspaceId = evt.workspace_id;
  if (!workspaceId) return;
  switch (evt.type) {
    case 'shared_members':
      // Someone joined, left or changed role, the workspace opened to
      // everyone or closed, or it names another team. The caller's own entry
      // (their role, a workspace just shared with them), the people list and
      // the team's roster are read again: the roster is keyed by workspace
      // alone, so a cached one of the old team would stay and be refused.
      void qc.invalidateQueries({ queryKey: queryKeys.sharedWorkspaces.members(workspaceId) });
      void qc.invalidateQueries({ queryKey: queryKeys.sharedWorkspaces.team(workspaceId) });
      void qc.invalidateQueries({ queryKey: queryKeys.sharedWorkspaces.list() });
      return;
    case 'shared_workspace':
      if (evt.change === 'deleted') {
        leave(qc, workspaceId, 'deleted');
        return;
      }
      // A rename, or who may share its work outside it: the entry, the
      // people view that shows the choice, and what may be published.
      void qc.invalidateQueries({ queryKey: queryKeys.sharedWorkspaces.list() });
      void qc.invalidateQueries({ queryKey: queryKeys.sharedWorkspaces.members(workspaceId) });
      void qc.invalidateQueries({ queryKey: queryKeys.workspaces.detail(workspaceId) });
      void qc.invalidateQueries({ queryKey: queryKeys.workspaces.lists() });
      void qc.invalidateQueries({ queryKey: queryKeys.orgLibrary.all });
      return;
    case 'shared_access_ended':
      leave(qc, workspaceId, 'removed');
      return;
    case 'thread_visibility':
      if (evt.thread_id && typeof evt.shared === 'boolean') {
        const threadId = evt.thread_id;
        applyThreadVisibility(qc, { threadId, workspaceId, shared: evt.shared, viewerIsStarter: viewerStarted(qc, threadId, workspaceId) });
      }
      return;
    default:
      return;
  }
}
