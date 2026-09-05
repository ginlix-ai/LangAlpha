/**
 * Generation-guard lock for the feed's reconnect loop: auth churn
 * (stop → start) must leave EXACTLY one loop connected, and a dying loop must
 * never abort or release the live loop's connection.
 */
import { QueryClient, QueryObserver } from '@tanstack/react-query';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const transport = vi.hoisted(() => ({
  getAuthHeaders: vi.fn(),
  streamFetch: vi.fn(),
}));

vi.mock('@/pages/ChatAgent/utils/api/transport', () => transport);

import {
  startThreadLifecycleFeed,
  stopThreadLifecycleFeed,
} from '../feedClient';
import {
  applyFeedEvent,
  getEffectiveObservation,
  resetThreadLifecycle,
} from '../store';
import { CACHE_ONLY_META, queryKeys } from '@/lib/queryKeys';
import {
  getNavThreadsSnapshot,
  resetSharedWorkspaceThreads,
  setSharedWorkspaceThreads,
} from '@/lib/navThreadsStore';

/** One captured feed connection; stays open until settled or aborted. */
interface Conn {
  signal: AbortSignal;
  emit: (event: Record<string, unknown>) => void;
  /** Close it the way the server does at the duration cap. */
  close: () => void;
}

const closed = new WeakSet<Conn>();

let conns: Conn[] = [];
let pendingAuth: Array<() => void> = [];
let queryClient: QueryClient;

/** Connections that were neither closed by the server nor aborted. */
function liveConns(): Conn[] {
  return conns.filter((c) => !c.signal.aborted && !closed.has(c));
}

function installStreamFetch(): void {
  transport.streamFetch.mockImplementation(
    (
      _url: string,
      opts: RequestInit,
      onEvent: (event: Record<string, unknown>) => void,
    ) =>
      new Promise((resolve) => {
        const signal = opts.signal as AbortSignal;
        const done = (): void =>
          resolve({ disconnected: false, aborted: signal.aborted, contentLocation: null });
        const conn: Conn = {
          signal,
          emit: onEvent,
          close: () => {
            closed.add(conn);
            done();
          },
        };
        conns.push(conn);
        signal.addEventListener('abort', done);
      }),
  );
}

/** Let every queued microtask + due timer run. */
async function flush(ms = 0): Promise<void> {
  await vi.advanceTimersByTimeAsync(ms);
}

beforeEach(() => {
  vi.useFakeTimers();
  conns = [];
  pendingAuth = [];
  queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  resetThreadLifecycle();
  resetSharedWorkspaceThreads();
  transport.getAuthHeaders.mockReset();
  transport.getAuthHeaders.mockResolvedValue({});
  transport.streamFetch.mockReset();
  installStreamFetch();
});

afterEach(async () => {
  stopThreadLifecycleFeed();
  pendingAuth.forEach((r) => r());
  await flush();
  vi.useRealTimers();
  queryClient.clear();
});

describe('threadLifecycleFeed — generation guard', () => {
  it('stop during backoff then start leaves exactly one live loop', async () => {
    startThreadLifecycleFeed(queryClient);
    await flush();
    expect(conns).toHaveLength(1);

    // Short-lived connection → the loop drops into its backoff sleep.
    conns[0].close();
    await flush();
    expect(liveConns()).toHaveLength(0);
    expect(vi.getTimerCount()).toBe(1); // the backoff nap

    // stop() must cut the nap short, not merely mark it stale.
    stopThreadLifecycleFeed();
    expect(vi.getTimerCount()).toBe(0);

    startThreadLifecycleFeed(queryClient);
    await flush();

    // The stale loop's sleep was interrupted and it bailed; only the restart
    // connected. Long idle proves it isn't reconnecting behind the live one.
    await flush(120_000);
    expect(conns).toHaveLength(2);
    expect(liveConns()).toHaveLength(1);
    expect(liveConns()[0]).toBe(conns[1]);
  });

  it('stop while suspended lets the next start connect', async () => {
    startThreadLifecycleFeed(queryClient);
    await flush();
    expect(conns).toHaveLength(1);

    window.dispatchEvent(new Event('pagehide'));
    await flush();
    expect(conns[0].signal.aborted).toBe(true);
    expect(liveConns()).toHaveLength(0);

    stopThreadLifecycleFeed();
    startThreadLifecycleFeed(queryClient);
    await flush();

    expect(conns).toHaveLength(2);
    expect(liveConns()).toHaveLength(1);
    await flush(120_000);
    expect(conns).toHaveLength(2);
  });

  it('stop during pending auth never opens the connection', async () => {
    transport.getAuthHeaders.mockImplementation(
      () =>
        new Promise<Record<string, string>>((resolve) => {
          pendingAuth.push(() => resolve({}));
        }),
    );

    startThreadLifecycleFeed(queryClient);
    await flush();
    expect(transport.getAuthHeaders).toHaveBeenCalledTimes(1);
    expect(conns).toHaveLength(0);

    stopThreadLifecycleFeed();
    pendingAuth.shift()!();
    await flush(120_000);
    expect(conns).toHaveLength(0);

    // A later start still works.
    transport.getAuthHeaders.mockResolvedValue({});
    startThreadLifecycleFeed(queryClient);
    await flush();
    expect(liveConns()).toHaveLength(1);
  });

  it('a stale loop cannot release the live loop\'s abort controller', async () => {
    startThreadLifecycleFeed(queryClient);
    await flush();
    const stale = conns[0];

    // Restart with the stale loop still inside its (now aborted) streamFetch:
    // start() publishes the new controller before the stale loop's `finally`
    // gets to run.
    stopThreadLifecycleFeed();
    startThreadLifecycleFeed(queryClient);
    await flush();

    expect(stale.signal.aborted).toBe(true);
    expect(conns).toHaveLength(2);
    const live = conns[1];
    expect(live.signal.aborted).toBe(false);

    // If the stale loop's finally had nulled client.abort, this stop would be
    // unable to reach the live connection.
    stopThreadLifecycleFeed();
    expect(live.signal.aborted).toBe(true);
  });
});

describe('threadLifecycleFeed — thread_archived', () => {
  it('prunes the archived thread the same way a delete does', async () => {
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries');
    startThreadLifecycleFeed(queryClient);
    await flush();

    applyFeedEvent({
      type: 'run_settled',
      thread_id: 't-arch',
      run_id: 'r1',
      run_seq: 5,
      status: 'completed',
    });
    expect(getEffectiveObservation('t-arch')?.status).toBe('completed');

    invalidate.mockClear();
    conns[0].emit({
      event: 'thread_lifecycle',
      type: 'thread_archived',
      thread_id: 't-arch',
      workspace_id: 'w1',
    });
    await flush(400);

    // Entry gone: the archived row leaves the snapshot and takes its unseen
    // state with it (the store suite pins that prune clears every derivation).
    expect(getEffectiveObservation('t-arch')).toBeUndefined();
    expect(invalidate).toHaveBeenCalledWith({
      queryKey: queryKeys.threads.byWorkspace('w1'),
    });
  });

  it('drops the row from cached lists and the Show-more store, sparing the archived view', async () => {
    // Cross-tab archive: the invalidate only reaches enabled observers, so the
    // event must remove the row from the caches + the nav store directly —
    // otherwise the sidebar (cache-only page-0 observers, store extras) keeps
    // rendering it until reload.
    startThreadLifecycleFeed(queryClient);
    await flush();

    const finiteKey = [...queryKeys.threads.byWorkspace('w1'), 10, 0];
    const archivedKey = queryKeys.threads.gallery('w1', true);
    const rows = () => [{ thread_id: 't-arch' }, { thread_id: 't-keep' }];
    queryClient.setQueryData(finiteKey, { threads: rows(), total: 2 });
    queryClient.setQueryData(archivedKey, {
      pages: [{ threads: [{ thread_id: 't-arch' }], total: 1 }],
      pageParams: [0],
    });
    setSharedWorkspaceThreads(() => ({ w1: { threads: rows(), loading: false, total: 2 } }));

    conns[0].emit({
      event: 'thread_lifecycle',
      type: 'thread_archived',
      thread_id: 't-arch',
      workspace_id: 'w1',
    });
    await flush(400);

    const finite = queryClient.getQueryData(finiteKey) as { threads: Array<{ thread_id: string }> };
    expect(finite.threads.map((t) => t.thread_id)).toEqual(['t-keep']);
    expect(getNavThreadsSnapshot()['w1'].threads.map((t) => t.thread_id)).toEqual(['t-keep']);
    // The archived view legitimately holds the row — it must be spared.
    const archived = queryClient.getQueryData(archivedKey) as { pages: Array<{ threads: Array<{ thread_id: string }> }> };
    expect(archived.pages[0].threads.map((t) => t.thread_id)).toEqual(['t-arch']);
  });

  it('thread_unarchived drops the row from the archived view only and refetches the lists', async () => {
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries');
    startThreadLifecycleFeed(queryClient);
    await flush();

    const finiteKey = [...queryKeys.threads.byWorkspace('w1'), 10, 0];
    const archivedKey = queryKeys.threads.gallery('w1', true);
    queryClient.setQueryData(finiteKey, { threads: [{ thread_id: 't-keep' }], total: 1 });
    queryClient.setQueryData(archivedKey, {
      pages: [{ threads: [{ thread_id: 't-back' }, { thread_id: 't-stay' }], total: 2 }],
      pageParams: [0],
    });

    invalidate.mockClear();
    conns[0].emit({
      event: 'thread_lifecycle',
      type: 'thread_unarchived',
      thread_id: 't-back',
      workspace_id: 'w1',
    });
    await flush(400);

    const archived = queryClient.getQueryData(archivedKey) as { pages: Array<{ threads: Array<{ thread_id: string }> }> };
    expect(archived.pages[0].threads.map((t) => t.thread_id)).toEqual(['t-stay']);
    // Live lists are untouched here — the invalidate refetch re-adds the row.
    const finite = queryClient.getQueryData(finiteKey) as { threads: Array<{ thread_id: string }> };
    expect(finite.threads.map((t) => t.thread_id)).toEqual(['t-keep']);
    expect(invalidate).toHaveBeenCalledWith({
      queryKey: queryKeys.threads.byWorkspace('w1'),
    });
  });
});

describe('threadLifecycleFeed — thread_title', () => {
  it('propagates a title clear (empty string) instead of dropping it', async () => {
    startThreadLifecycleFeed(queryClient);
    await flush();

    const finiteKey = [...queryKeys.threads.byWorkspace('w1'), 10, 0];
    queryClient.setQueryData(finiteKey, {
      threads: [{ thread_id: 't1', title: 'Old', updated_at: '2026-01-01T00:00:00Z' }],
      total: 1,
    });

    conns[0].emit({
      event: 'thread_lifecycle',
      type: 'thread_title',
      thread_id: 't1',
      workspace_id: 'w1',
      title: '',
      updated_at: '2026-01-02T00:00:00Z',
    });
    await flush(400);

    const finite = queryClient.getQueryData(finiteKey) as { threads: Array<{ title?: string }> };
    expect(finite.threads[0].title).toBe('');
  });
});

describe('threadLifecycleFeed — thread_pinned', () => {
  it('patches the flag in cached lists and refetches for ordering', async () => {
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries');
    startThreadLifecycleFeed(queryClient);
    await flush();

    const finiteKey = [...queryKeys.threads.byWorkspace('w1'), 10, 0];
    queryClient.setQueryData(finiteKey, {
      threads: [{ thread_id: 't1', is_pinned: false }, { thread_id: 't2', is_pinned: false }],
      total: 2,
    });

    invalidate.mockClear();
    conns[0].emit({
      event: 'thread_lifecycle',
      type: 'thread_pinned',
      thread_id: 't1',
      workspace_id: 'w1',
      pinned: true,
    });
    await flush(400);

    const finite = queryClient.getQueryData(finiteKey) as { threads: Array<{ thread_id: string; is_pinned?: boolean }> };
    expect(finite.threads.find((t) => t.thread_id === 't1')?.is_pinned).toBe(true);
    expect(finite.threads.find((t) => t.thread_id === 't2')?.is_pinned).toBe(false);
    expect(invalidate).toHaveBeenCalledWith({
      queryKey: queryKeys.threads.byWorkspace('w1'),
    });
  });
});

describe('threadLifecycleFeed — cache-only list refetch', () => {
  it('explicitly fetches an invalidated list whose only observers opt in', async () => {
    // The nav tree's page-0 queries observe with `enabled: false`, which
    // invalidateQueries (refetchType 'active') marks stale but never refetches
    // — a thread started in the background would stay missing until the user
    // navigates into the workspace.
    startThreadLifecycleFeed(queryClient);
    await flush();

    const finiteKey = [...queryKeys.threads.byWorkspace('w1'), 10, 0];
    queryClient.setQueryData(finiteKey, { threads: [], total: 0 });
    const queryFn = vi.fn().mockResolvedValue({ threads: [{ thread_id: 't-new' }], total: 1 });
    const observer = new QueryObserver(queryClient, {
      queryKey: finiteKey,
      queryFn,
      enabled: false,
      meta: CACHE_ONLY_META,
    });
    const unsubscribe = observer.subscribe(() => {});

    conns[0].emit({
      event: 'thread_lifecycle',
      type: 'run_started',
      thread_id: 't-new',
      workspace_id: 'w1',
      run_id: 'r1',
      run_seq: 1,
      status: 'running',
    });
    await flush(400);

    expect(queryFn).toHaveBeenCalledTimes(1);
    const finite = queryClient.getQueryData(finiteKey) as { threads: Array<{ thread_id: string }> };
    expect(finite.threads.map((t) => t.thread_id)).toEqual(['t-new']);
    unsubscribe();
  });

  it('leaves observer-less queries lazy', async () => {
    // An unmounted gallery's cache entry has zero observers — invalidation
    // alone is right there (it refetches on next mount); eager-fetching every
    // stale entry would refetch views nobody is rendering. The queryFn arrives
    // via defaults rather than an observer so the entry stays observer-less
    // while still being fetchable — otherwise nothing here can fail.
    startThreadLifecycleFeed(queryClient);
    await flush();

    const finiteKey = [...queryKeys.threads.byWorkspace('w1'), 10, 0];
    const queryFn = vi.fn().mockResolvedValue({ threads: [{ thread_id: 't-new' }], total: 1 });
    queryClient.setQueryDefaults(finiteKey, { queryFn });
    queryClient.setQueryData(finiteKey, { threads: [], total: 0 });

    conns[0].emit({
      event: 'thread_lifecycle',
      type: 'run_started',
      thread_id: 't-new',
      workspace_id: 'w1',
      run_id: 'r1',
      run_seq: 1,
      status: 'running',
    });
    await flush(400);

    expect(queryFn).not.toHaveBeenCalled();
    const finite = queryClient.getQueryData(finiteKey) as { threads: Array<{ thread_id: string }> };
    expect(finite.threads).toEqual([]);
  });

  it('still fetches when only one of several observers opts in', async () => {
    // Observers on one key share its arguments, so one voucher covers the key.
    // This is the production shape, not an edge case: a page-0 list is observed
    // by the sidebar tree (opted in) AND by every parked ChatView, which mounts
    // useNavigationData disabled and registers a plain no-meta observer on its
    // own workspace's list. Requiring every observer to opt in froze exactly
    // the lists this refetch exists to thaw.
    startThreadLifecycleFeed(queryClient);
    await flush();

    const finiteKey = [...queryKeys.threads.byWorkspace('w1'), 10, 0];
    queryClient.setQueryData(finiteKey, { threads: [], total: 0 });
    const queryFn = vi.fn().mockResolvedValue({ threads: [{ thread_id: 't-new' }], total: 1 });
    const navObserver = new QueryObserver(queryClient, {
      queryKey: finiteKey,
      queryFn,
      enabled: false,
      meta: CACHE_ONLY_META,
    });
    const parkedChatView = new QueryObserver(queryClient, {
      queryKey: finiteKey,
      queryFn,
      enabled: false,
    });
    const unsubscribes = [navObserver.subscribe(() => {}), parkedChatView.subscribe(() => {})];

    conns[0].emit({
      event: 'thread_lifecycle',
      type: 'run_started',
      thread_id: 't-new',
      workspace_id: 'w1',
      run_id: 'r1',
      run_seq: 1,
      status: 'running',
    });
    await flush(400);

    expect(queryFn).toHaveBeenCalledTimes(1);
    const finite = queryClient.getQueryData(finiteKey) as { threads: Array<{ thread_id: string }> };
    expect(finite.threads.map((t) => t.thread_id)).toEqual(['t-new']);
    unsubscribes.forEach((u) => u());
  });

  it('leaves a disabled query that never opted in alone on a full resync', async () => {
    // `enabled: false` also means "the argument isn't here yet" — ChatAgent's
    // thread lookup with no :threadId, the nav tree's page-0 query with no
    // current workspace. Those queryFns throw on the missing id, so fetching
    // them parks a permanent error that ChatAgent's redirect effect then reads
    // on every navigation, bouncing the user out of /chat/* forever. The
    // connection's first snapshot invalidates the whole threads prefix, which
    // is what used to sweep these argument-less entries in.
    startThreadLifecycleFeed(queryClient);
    await flush();

    const detailKey = queryKeys.threads.detail(undefined as unknown as string);
    queryClient.setQueryData(detailKey, { thread_id: null });
    const queryFn = vi.fn().mockRejectedValue(new Error('Thread ID is required'));
    const observer = new QueryObserver(queryClient, { queryKey: detailKey, queryFn, enabled: false });
    const unsubscribe = observer.subscribe(() => {});

    conns[0].emit({ event: 'snapshot', as_of_seq: 1, oldest_included_unseen_seq: 0, live: [], unseen: [] });
    await flush(400);

    expect(queryFn).not.toHaveBeenCalled();
    expect(queryClient.getQueryState(detailKey)?.status).not.toBe('error');
    unsubscribe();
  });
});

describe('threadLifecycleFeed — snapshot invalidation', () => {
  it('a snapshot scopes its invalidate to the workspaces it touches', async () => {
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries');
    startThreadLifecycleFeed(queryClient);
    await flush();
    // Consume the connection's first-snapshot resync so the scoped path runs.
    conns[0].emit({ event: 'snapshot', as_of_seq: 1, oldest_included_unseen_seq: 0, live: [], unseen: [] });
    await flush(400);

    invalidate.mockClear();
    conns[0].emit({
      event: 'snapshot',
      as_of_seq: 2,
      oldest_included_unseen_seq: 0,
      live: [
        { thread_id: 't1', workspace_id: 'w1', run_id: 'r1', run_seq: 2, status: 'running', last_seen_run_seq: 0 },
      ],
      unseen: [
        { thread_id: 't2', workspace_id: 'w2', run_id: 'r2', run_seq: 1, status: 'completed', last_seen_run_seq: 0 },
      ],
    });
    await flush(400);

    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.threads.byWorkspace('w1') });
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.threads.byWorkspace('w2') });
    expect(invalidate).not.toHaveBeenCalledWith({ queryKey: queryKeys.threads.all });
  });

  it("a connection's first snapshot invalidates the whole threads prefix", async () => {
    // Events may have been missed while detached (frozen tab, backend restart,
    // 600s cap) and the feed has no replay — the first frame of every
    // connection must re-read server truth for all enabled observers.
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries');
    startThreadLifecycleFeed(queryClient);
    await flush();

    conns[0].emit({ event: 'snapshot', as_of_seq: 1, oldest_included_unseen_seq: 0, live: [], unseen: [] });
    await flush(400);
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.threads.all });

    // Healthy close (server duration cap) → instant re-attach → resync again.
    await flush(31_000);
    conns[0].close();
    await flush();
    expect(liveConns()).toHaveLength(1);

    invalidate.mockClear();
    liveConns()[0].emit({ event: 'snapshot', as_of_seq: 3, oldest_included_unseen_seq: 0, live: [], unseen: [] });
    await flush(400);
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.threads.all });
  });
});
