/**
 * What the feed's shared workspace events move: the queries that read what
 * changed, and, for a workspace the user lost or that was deleted, every view
 * of it. A connection that reattaches reads the shared list again and leaves
 * the workspaces it no longer names, since the feed has no replay.
 */
import { QueryClient, QueryObserver } from '@tanstack/react-query';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { queryKeys } from '@/lib/queryKeys';
import {
  applySharedWorkspaceEvent,
  applyThreadVisibility,
  refreshSharedFiles,
  resyncSharedWorkspaces,
  subscribeSharedThreadGone,
  subscribeSharedWorkspaceGone,
  tellSharedThreadDeleted,
  type SharedWorkspaceGone,
} from '../sharedWorkspaceFeed';
import { markThreadDeleted } from '../deletedThreads';
import { getSharedStanding, type SharedStanding } from '@/api/sharedWorkspaces';

vi.mock('@/api/sharedWorkspaces', () => ({ getSharedStanding: vi.fn() }));
const standing = vi.mocked(getSharedStanding);
import { applyFeedEvent, getEffectiveObservation, resetThreadLifecycle } from '../threadLifecycle/store';
import type { SharedWorkspaceList } from '@/types/sharedWorkspaces';

const W = 'ws-desk';
const W2 = 'ws-other';

function list(...ids: string[]): SharedWorkspaceList {
  return {
    can_create: true,
    workspaces: ids.map((id) => ({
      workspace: { workspace_id: id, name: id } as SharedWorkspaceList['workspaces'][number]['workspace'],
      shared: { role: 'member', can_manage: false, everyone: false, created_by: null, sharing: 'managers', can_share_out: false, can_change_links: false },
    })),
  };
}

let qc: QueryClient;
let gone: SharedWorkspaceGone[];
let unsubscribe: () => void;

beforeEach(() => {
  qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  resetThreadLifecycle();
  gone = [];
  unsubscribe = subscribeSharedWorkspaceGone((g) => gone.push(g));
});

afterEach(() => {
  unsubscribe();
  qc.clear();
});

const invalidated = (key: readonly unknown[]) => qc.getQueryState(key)?.isInvalidated;

describe('applySharedWorkspaceEvent', () => {
  it('a members change reads the people, the team and the shared list again', () => {
    qc.setQueryData(queryKeys.sharedWorkspaces.list(), list(W));
    qc.setQueryData(queryKeys.sharedWorkspaces.members(W), []);
    qc.setQueryData(queryKeys.sharedWorkspaces.team(W), {});
    qc.setQueryData(queryKeys.sharedWorkspaces.team(W2), {});

    applySharedWorkspaceEvent(qc, { type: 'shared_members', workspace_id: W });

    expect(invalidated(queryKeys.sharedWorkspaces.members(W))).toBe(true);
    expect(invalidated(queryKeys.sharedWorkspaces.list())).toBe(true);
    // Another tab named another team: the old roster is keyed by workspace.
    expect(invalidated(queryKeys.sharedWorkspaces.team(W))).toBe(true);
    expect(invalidated(queryKeys.sharedWorkspaces.team(W2))).toBe(false);
    expect(gone).toEqual([]);
  });

  it('a rename reads the workspace again', () => {
    qc.setQueryData(queryKeys.workspaces.detail(W), {});
    applySharedWorkspaceEvent(qc, { type: 'shared_workspace', workspace_id: W, change: 'updated' });
    expect(invalidated(queryKeys.workspaces.detail(W))).toBe(true);
    expect(gone).toEqual([]);
  });

  it.each([
    ['deleted', { type: 'shared_workspace', change: 'deleted' }],
    ['removed', { type: 'shared_access_ended' }],
  ])('a workspace %s leaves every view of it', (reason, evt) => {
    qc.setQueryData(queryKeys.threads.byWorkspace(W), { threads: [{ thread_id: 't-1' }] });
    qc.setQueryData(queryKeys.threads.byWorkspace(W2), { threads: [{ thread_id: 't-2' }] });
    qc.setQueryData(queryKeys.sharedWorkspaces.members(W), []);
    for (const t of ['t-1', 't-2']) {
      applyFeedEvent({ type: 'run_settled', thread_id: t, run_id: `r-${t}`, run_seq: 1, status: 'completed' });
    }

    applySharedWorkspaceEvent(qc, { workspace_id: W, ...evt });

    expect(gone).toEqual([{ workspaceId: W, reason }]);
    expect(qc.getQueryData(queryKeys.threads.byWorkspace(W))).toBeUndefined();
    expect(qc.getQueryData(queryKeys.sharedWorkspaces.members(W))).toBeUndefined();
    expect(getEffectiveObservation('t-1')).toBeUndefined();
    // Another workspace's threads stay.
    expect(getEffectiveObservation('t-2')?.status).toBe('completed');
  });

  it('an event naming no workspace does nothing', () => {
    applySharedWorkspaceEvent(qc, { type: 'shared_access_ended' });
    expect(gone).toEqual([]);
  });
});

describe('refreshSharedFiles', () => {
  it("reads a shared workspace's files again and leaves any other's alone", () => {
    qc.setQueryData(queryKeys.sharedWorkspaces.list(), list(W));
    qc.setQueryData(queryKeys.workspaceFiles.workspace(W), []);
    qc.setQueryData(queryKeys.workspaceFiles.workspace('mine'), []);

    refreshSharedFiles(qc, W);
    refreshSharedFiles(qc, 'mine');

    expect(invalidated(queryKeys.workspaceFiles.workspace(W))).toBe(true);
    expect(invalidated(queryKeys.workspaceFiles.workspace('mine'))).toBe(false);
  });
});

describe("a change to the account's teams", () => {
  it('reads the account and the shared list again, leaving what it lost', async () => {
    standing.mockReset();
    standing.mockResolvedValue('gone');
    qc.setQueryData(queryKeys.user.me(), { user: { user_id: 'u1' } });
    qc.setQueryData(queryKeys.sharedWorkspaces.list(), list(W, W2));
    const observer = new QueryObserver(qc, {
      queryKey: queryKeys.sharedWorkspaces.list(),
      queryFn: async () => list(W2),
      staleTime: Infinity,
    });
    const stop = observer.subscribe(() => {});
    applySharedWorkspaceEvent(qc, { type: 'shared_reach' });
    await vi.waitFor(() => expect(gone).toEqual([{ workspaceId: W, reason: 'removed' }]));
    stop();
    expect(invalidated(queryKeys.user.me())).toBe(true);
  });
});

describe('resyncSharedWorkspaces', () => {
  function serve(answer: () => SharedWorkspaceList): () => void {
    qc.setQueryData(queryKeys.sharedWorkspaces.list(), list(W, W2));
    const observer = new QueryObserver(qc, {
      queryKey: queryKeys.sharedWorkspaces.list(),
      queryFn: async () => answer(),
      staleTime: Infinity,
    });
    return observer.subscribe(() => {});
  }

  it.each<[SharedStanding | Error, SharedWorkspaceGone['reason'] | null]>([
    ['gone', 'removed'],
    ['deleted', 'deleted'],
    [new Error('offline'), 'removed'],
    ['active', null],
  ])('leaves a workspace the list no longer names as its standing says (%s)', async (answer, reason) => {
    standing.mockReset();
    if (answer instanceof Error) standing.mockRejectedValue(answer);
    else standing.mockResolvedValue(answer);
    const stop = serve(() => list(W2));
    await resyncSharedWorkspaces(qc);
    stop();

    expect(standing).toHaveBeenCalledWith(W);
    expect(standing).toHaveBeenCalledTimes(1);
    expect(gone).toEqual(reason ? [{ workspaceId: W, reason }] : []);
  });

  it('leaves nothing when the read fails', async () => {
    const stop = serve(() => {
      throw new Error('offline');
    });
    await resyncSharedWorkspaces(qc);
    stop();

    expect(gone).toEqual([]);
  });

  it('reads nothing for an account whose shared list was never read', async () => {
    await resyncSharedWorkspaces(qc);
    expect(qc.getQueryCache().getAll()).toEqual([]);
    expect(gone).toEqual([]);
  });
});

describe('tellSharedThreadDeleted', () => {
  it("tells of a shared workspace's thread deleted elsewhere, and nothing else", () => {
    qc.setQueryData(queryKeys.sharedWorkspaces.list(), list(W));
    const heard: { threadId: string; workspaceId: string; reason: string }[] = [];
    const stop = subscribeSharedThreadGone((e) => heard.push(e));

    tellSharedThreadDeleted(qc, 't-1', W);
    tellSharedThreadDeleted(qc, 't-2', 'mine');
    tellSharedThreadDeleted(qc, 't-3', null);
    markThreadDeleted('t-4');
    tellSharedThreadDeleted(qc, 't-4', W);
    stop();

    expect(heard).toEqual([{ threadId: 't-1', workspaceId: W, reason: 'thread' }]);
  });
});

describe('thread_visibility', () => {
  const T = 't-orchid';
  const row = (starter: string) => ({ thread_id: T, title: 'Orchid', shared: { created_by: { user_id: starter } } });
  let heard: { threadId: string; workspaceId: string; reason: string }[];
  let stop: () => void;

  beforeEach(() => {
    heard = [];
    stop = subscribeSharedThreadGone((e) => heard.push(e));
    qc.setQueryData(queryKeys.sharedWorkspaces.list(), list(W));
    qc.setQueryData(queryKeys.threads.byWorkspace(W), { threads: [row('maya')], total: 1 });
    qc.setQueryData(queryKeys.threads.recentAll(), { threads: [] });
  });
  afterEach(() => stop());

  const me = (userId: string) => qc.setQueryData(queryKeys.user.me(), { user_id: userId });
  const rows = () => (qc.getQueryData(queryKeys.threads.byWorkspace(W)) as { threads: { thread_id: string }[] }).threads;

  it('made private, it leaves the lists of whoever did not start it and sends them out of it', () => {
    me('lee');
    applySharedWorkspaceEvent(qc, { type: 'thread_visibility', workspace_id: W, thread_id: T, shared: false });
    expect(rows()).toEqual([]);
    expect(heard).toEqual([{ threadId: T, workspaceId: W, reason: 'private' }]);
    expect(invalidated(queryKeys.sharedWorkspaces.list())).toBe(true);
  });

  // A comment queued for the thread must leave the colleague's Send all: the
  // destinations and each comment's thread_hidden are read again, both ways.
  it.each([false, true])('made %s, the comment destinations and their flags are read again', (shared) => {
    me('lee');
    const destinations = queryKeys.artifacts.destinationsOf('a-1');
    const comments = queryKeys.artifacts.comments('a-1');
    qc.setQueryData(destinations, { threads: [{ thread_id: T }] });
    qc.setQueryData(comments, { comments: [{ thread_id: T, thread_hidden: false }] });
    applySharedWorkspaceEvent(qc, { type: 'thread_visibility', workspace_id: W, thread_id: T, shared });
    expect(invalidated(destinations)).toBe(true);
    expect(invalidated(comments)).toBe(true);
  });

  it('its starter keeps it and reads it again', () => {
    me('maya');
    applySharedWorkspaceEvent(qc, { type: 'thread_visibility', workspace_id: W, thread_id: T, shared: false });
    expect(rows().map((r) => r.thread_id)).toEqual([T]);
    expect(invalidated(queryKeys.threads.byWorkspace(W))).toBe(true);
    expect(heard).toEqual([]);
  });

  it('shared, every member reads the list again and no one is sent anywhere', () => {
    me('lee');
    applySharedWorkspaceEvent(qc, { type: 'thread_visibility', workspace_id: W, thread_id: T, shared: true });
    expect(invalidated(queryKeys.threads.byWorkspace(W))).toBe(true);
    expect(invalidated(queryKeys.threads.recentAll())).toBe(true);
    expect(heard).toEqual([]);
  });

  it('a change the viewer made keeps the row for them, whatever the cache says of its starter', () => {
    me('lee');
    qc.setQueryData(queryKeys.threads.detail(T), { thread_id: T });
    applyThreadVisibility(qc, { threadId: T, workspaceId: W, shared: false, viewerIsStarter: true });
    expect(rows().map((r) => r.thread_id)).toEqual([T]);
    expect(invalidated(queryKeys.threads.detail(T))).toBe(true);
    expect(heard).toEqual([]);
  });

  it('an event missing what changed does nothing', () => {
    me('lee');
    applySharedWorkspaceEvent(qc, { type: 'thread_visibility', workspace_id: W, thread_id: T });
    expect(rows().map((r) => r.thread_id)).toEqual([T]);
    expect(heard).toEqual([]);
  });
});
