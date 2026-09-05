/**
 * Stable nav ordering in useNavigationData.
 *
 * The backend returns threads (and unpinned workspaces within the 'custom'
 * sort) `updated_at DESC`, so the item being chatted in would hoist to the
 * top whenever React Query refetches mid-conversation. The hook freezes the
 * order seen first in the page session (module-level, surviving the per-thread
 * ChatView remounts); refetches reorder to the frozen sequence, genuinely new
 * ids surface at the top, paginated-in ids append below, deleted ids drop out.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { renderHook, waitFor, act } from '@testing-library/react';
import { QueryClientProvider } from '@tanstack/react-query';
import { createTestQueryClient } from '@/test/utils';
import {
  useNavigationData,
  applyStableOrder,
  applyStableOrderBy,
  resetStableNavOrder,
  bumpThreadNavOrder,
} from '../useNavigationData';
import { resetSharedWorkspaceThreads } from '@/lib/navThreadsStore';
import { isArchivedThreadsKey, patchThreadRows } from '@/lib/threadRowActions';
import { isCacheOnlyMeta, queryKeys } from '@/lib/queryKeys';
import { resetNavPrefs, setNavPrefs } from '../../utils/navPrefs';

vi.mock('../../utils/api', () => ({
  getWorkspaces: vi.fn(),
  getWorkspaceThreads: vi.fn(),
  reorderWorkspaces: vi.fn(),
  updateWorkspace: vi.fn(),
  updateThread: vi.fn(),
}));

import { getWorkspaces, getWorkspaceThreads, reorderWorkspaces, updateWorkspace, updateThread } from '../../utils/api';

const mockGetWorkspaces = getWorkspaces as Mock;
const mockGetWorkspaceThreads = getWorkspaceThreads as Mock;
const mockReorderWorkspaces = reorderWorkspaces as Mock;
const mockUpdateWorkspace = updateWorkspace as Mock;
const mockUpdateThread = updateThread as Mock;

interface TestThread {
  thread_id: string;
  title: string;
  [key: string]: unknown;
}

const thread = (id: string): TestThread => ({ thread_id: id, title: `Thread ${id}` });
const threads = (...ids: string[]) => ids.map(thread);

describe('applyStableOrder (pure)', () => {
  it('snapshots server order on first sight (no frozen order yet)', () => {
    const server = threads('t-3', 't-1', 't-2');
    const { order, threads: result } = applyStableOrder(undefined, server);

    expect(order).toEqual(['t-3', 't-1', 't-2']);
    expect(result).toEqual(server);
  });

  it('keeps the frozen order when the server reshuffles by recency', () => {
    const frozen = ['t-3', 't-1', 't-2'];
    // t-1 was active, so the server now lists it first.
    const server = threads('t-1', 't-3', 't-2');
    const { order, threads: result } = applyStableOrder(frozen, server);

    expect(order).toEqual(frozen);
    expect(result.map((t) => t.thread_id)).toEqual(frozen);
  });

  it('surfaces a genuinely new thread id at the top and adds it to the order', () => {
    const frozen = ['t-3', 't-1', 't-2'];
    const server = threads('t-new', 't-1', 't-3', 't-2');
    const { order, threads: result } = applyStableOrder(frozen, server);

    expect(order).toEqual(['t-new', 't-3', 't-1', 't-2']);
    expect(result.map((t) => t.thread_id)).toEqual(['t-new', 't-3', 't-1', 't-2']);
  });

  it('drops ids missing from the server response (deleted threads) without error', () => {
    const frozen = ['t-3', 't-1', 't-2'];
    const server = threads('t-3', 't-2'); // t-1 deleted server-side
    const { threads: result } = applyStableOrder(frozen, server);

    expect(result.map((t) => t.thread_id)).toEqual(['t-3', 't-2']);
  });

  it('handles an empty server response', () => {
    const { order, threads: result } = applyStableOrder(['t-1'], []);

    expect(order).toEqual(['t-1']);
    expect(result).toEqual([]);
  });

  it('appends unseen ids that arrive after known ids (pagination) below the stable block', () => {
    const frozen = ['t-3', 't-1'];
    // t-old arrives after known ids — a paginated-in older entry, not a new thread.
    const server = threads('t-1', 't-3', 't-old');
    const { order } = applyStableOrderBy(frozen, server, (t) => t.thread_id);

    expect(order).toEqual(['t-3', 't-1', 't-old']);
  });
});

describe('useNavigationData — stable thread ordering', () => {
  let threadsByWs: Record<string, TestThread[]>;

  beforeEach(() => {
    vi.clearAllMocks();
    resetStableNavOrder();
    resetSharedWorkspaceThreads();
    resetNavPrefs();
    threadsByWs = {
      'ws-1': threads('t-3', 't-1', 't-2'),
      'ws-2': threads('u-2', 'u-1'),
    };
    mockGetWorkspaces.mockResolvedValue({
      workspaces: [{ workspace_id: 'ws-1' }, { workspace_id: 'ws-2' }],
      total: 2,
    });
    mockGetWorkspaceThreads.mockImplementation((wsId: string) =>
      Promise.resolve({ threads: threadsByWs[wsId] ?? [] }),
    );
  });

  function setup(initialWsId = 'ws-1') {
    const queryClient = createTestQueryClient();
    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
    const rendered = renderHook(({ wsId }) => useNavigationData(wsId), {
      wrapper,
      initialProps: { wsId: initialWsId },
    });
    const idsFor = (wsId: string) =>
      (rendered.result.current.workspaceThreads[wsId]?.threads ?? []).map((t) => t.thread_id);
    return { ...rendered, queryClient, idsFor };
  }

  it('first load preserves server (recency) order', async () => {
    const { idsFor } = setup();

    await waitFor(() => expect(idsFor('ws-1')).toEqual(['t-3', 't-1', 't-2']));
  });

  it('refetches the current workspace when the thread page size pref changes', async () => {
    // Regression: the threads query key must carry threadPageSize. The queryFn
    // fetches `threadPageSize` rows, so a key that ignored it would replay the
    // cached page instead of fetching the newly-requested size.
    const { idsFor } = setup();
    await waitFor(() => expect(idsFor('ws-1')).toEqual(['t-3', 't-1', 't-2']));
    expect(mockGetWorkspaceThreads).toHaveBeenCalledWith('ws-1', 10, 0);

    await act(async () => {
      setNavPrefs({ threadPageSize: 20 });
    });

    await waitFor(() => expect(mockGetWorkspaceThreads).toHaveBeenCalledWith('ws-1', 20, 0));
  });

  it('page-0 observers opt into the lifecycle-feed thaw', async () => {
    // The other half of this contract lives in refetchCacheOnlyLists
    // (lib/threadLifecycle/feedClient.ts), which fetches a stale page-0 list
    // only when an observer vouches for it via meta. Nothing else fails if this
    // hook stops sending the flag — the tree would just silently stop picking
    // up background runs — so assert the exact predicate that consumer uses.
    const { queryClient, idsFor } = setup();
    await waitFor(() => expect(idsFor('ws-1')).toEqual(['t-3', 't-1', 't-2']));

    const pageZero = queryClient
      .getQueryCache()
      .findAll({ queryKey: [...queryKeys.threads.byWorkspace('ws-1'), 10, 0] });

    expect(pageZero).toHaveLength(1);
    expect(pageZero[0].observers.some((o) => isCacheOnlyMeta(o.options.meta))).toBe(true);
  });

  it('refetch with reshuffled updated_at keeps the frozen order', async () => {
    const { idsFor, queryClient } = setup();
    await waitFor(() => expect(idsFor('ws-1')).toEqual(['t-3', 't-1', 't-2']));

    // User chats in t-1 → server now returns it first.
    threadsByWs['ws-1'] = threads('t-1', 't-3', 't-2');
    await act(async () => {
      await queryClient.invalidateQueries();
    });

    await waitFor(() => expect(mockGetWorkspaceThreads.mock.calls.length).toBeGreaterThan(1));
    expect(idsFor('ws-1')).toEqual(['t-3', 't-1', 't-2']);
  });

  it('a new thread id surfaces at the top', async () => {
    const { idsFor, queryClient } = setup();
    await waitFor(() => expect(idsFor('ws-1')).toEqual(['t-3', 't-1', 't-2']));

    threadsByWs['ws-1'] = threads('t-new', 't-1', 't-3', 't-2');
    await act(async () => {
      await queryClient.invalidateQueries();
    });

    await waitFor(() => expect(idsFor('ws-1')).toEqual(['t-new', 't-3', 't-1', 't-2']));
  });

  it('absorbs a new thread id into the frozen order so a later bump cannot sink it', async () => {
    // Regression: the frozen order is written from an effect, not during
    // render. If a newly-seen id were never absorbed, `applyStableOrderBy`
    // would re-classify it positionally on every render — and the moment an
    // older frozen thread sorts above it, it drops to the paginated-in tail.
    const { idsFor, queryClient } = setup();
    await waitFor(() => expect(idsFor('ws-1')).toEqual(['t-3', 't-1', 't-2']));

    threadsByWs['ws-1'] = threads('t-new', 't-3', 't-1', 't-2');
    await act(async () => {
      await queryClient.invalidateQueries();
    });
    await waitFor(() => expect(idsFor('ws-1')).toEqual(['t-new', 't-3', 't-1', 't-2']));

    // Chatting in an older thread hoists it; the server reshuffles it above
    // t-new. Without absorption t-new is unknown-and-late → tail.
    act(() => {
      bumpThreadNavOrder('ws-1', 't-2');
    });
    threadsByWs['ws-1'] = threads('t-2', 't-3', 't-1', 't-new');
    await act(async () => {
      await queryClient.invalidateQueries();
    });

    await waitFor(() => expect(mockGetWorkspaceThreads.mock.calls.length).toBeGreaterThan(2));
    expect(idsFor('ws-1')).toEqual(['t-2', 't-new', 't-3', 't-1']);
  });

  it('a deleted thread id drops out without error', async () => {
    const { idsFor, queryClient } = setup();
    await waitFor(() => expect(idsFor('ws-1')).toEqual(['t-3', 't-1', 't-2']));

    threadsByWs['ws-1'] = threads('t-3', 't-2');
    await act(async () => {
      await queryClient.invalidateQueries();
    });

    await waitFor(() => expect(idsFor('ws-1')).toEqual(['t-3', 't-2']));
  });

  it('keeps the frozen order when leaving and returning to a workspace', async () => {
    const { idsFor, rerender } = setup();
    await waitFor(() => expect(idsFor('ws-1')).toEqual(['t-3', 't-1', 't-2']));

    // Recency changed while the user was away, but within a page session the
    // order stays frozen — it refreshes on reload, like other chat apps.
    threadsByWs['ws-1'] = threads('t-1', 't-3', 't-2');

    rerender({ wsId: 'ws-2' });
    await waitFor(() => expect(idsFor('ws-2')).toEqual(['u-2', 'u-1']));

    rerender({ wsId: 'ws-1' });
    await waitFor(() => expect((idsFor('ws-1')).length).toBeGreaterThan(0));
    expect(idsFor('ws-1')).toEqual(['t-3', 't-1', 't-2']);
  });

  it('chatting in a thread bumps it to the top (bumpThreadNavOrder)', async () => {
    const { idsFor } = setup();
    await waitFor(() => expect(idsFor('ws-1')).toEqual(['t-3', 't-1', 't-2']));

    act(() => {
      bumpThreadNavOrder('ws-1', 't-2');
    });

    expect(idsFor('ws-1')).toEqual(['t-2', 't-3', 't-1']);
  });

  it('a bumped position survives later refetch reshuffles', async () => {
    const { idsFor, queryClient } = setup();
    await waitFor(() => expect(idsFor('ws-1')).toEqual(['t-3', 't-1', 't-2']));

    act(() => {
      bumpThreadNavOrder('ws-1', 't-1');
    });
    expect(idsFor('ws-1')).toEqual(['t-1', 't-3', 't-2']);

    threadsByWs['ws-1'] = threads('t-2', 't-1', 't-3');
    await act(async () => {
      await queryClient.invalidateQueries();
    });

    await waitFor(() => expect(mockGetWorkspaceThreads.mock.calls.length).toBeGreaterThan(1));
    expect(idsFor('ws-1')).toEqual(['t-1', 't-3', 't-2']);
  });

  it('bump is a no-op before the workspace order is snapshotted', () => {
    expect(() => bumpThreadNavOrder('ws-never-loaded', 't-1')).not.toThrow();
  });

  it('keeps the frozen order across hook remounts (per-thread ChatView instances)', async () => {
    const first = setup();
    await waitFor(() => expect(first.idsFor('ws-1')).toEqual(['t-3', 't-1', 't-2']));
    first.unmount();

    // A thread switch mounts a fresh ChatView (fresh hook instance) and the
    // active thread is now first in the server response — order must not move.
    threadsByWs['ws-1'] = threads('t-1', 't-3', 't-2');
    const second = setup();
    await waitFor(() => expect(second.idsFor('ws-1').length).toBeGreaterThan(0));
    expect(second.idsFor('ws-1')).toEqual(['t-3', 't-1', 't-2']);
  });

  it('shares loaded thread lists across concurrent hook instances (cached panels)', async () => {
    // Two cached ChatView panels are alive at once, each its own hook instance.
    // Thread lists live in a session-global shared store, so a folder loaded by
    // one panel is visible to the other immediately. Before the shared store,
    // the second panel rendered the folder open-but-empty (the flash that read
    // as an auto-collapse) until its own fetch landed.
    const a = setup('ws-1');
    const b = setup('ws-1');
    await waitFor(() => expect(a.idsFor('ws-1')).toEqual(['t-3', 't-1', 't-2']));

    // Panel A opens ws-2 (neither panel's current workspace).
    expect(b.idsFor('ws-2')).toEqual([]);
    await act(async () => {
      a.result.current.expandWorkspace('ws-2');
    });

    // Panel B sees A's load through the shared store — no empty flash.
    await waitFor(() => expect(b.idsFor('ws-2')).toEqual(['u-2', 'u-1']));
    expect(a.idsFor('ws-2')).toEqual(['u-2', 'u-1']);
  });
});

describe('useNavigationData — stable workspace ordering', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    resetStableNavOrder();
    resetSharedWorkspaceThreads();
    resetNavPrefs();
    mockGetWorkspaceThreads.mockResolvedValue({ threads: [] });
  });

  function setup(initialWsId = 'ws-1') {
    const queryClient = createTestQueryClient();
    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
    const rendered = renderHook(({ wsId }) => useNavigationData(wsId), {
      wrapper,
      initialProps: { wsId: initialWsId },
    });
    const wsIds = () => rendered.result.current.workspaces.map((ws) => ws.workspace_id);
    return { ...rendered, queryClient, wsIds };
  }

  it('does not hoist the active workspace when refetches reshuffle recency', async () => {
    mockGetWorkspaces.mockResolvedValue({
      workspaces: [{ workspace_id: 'ws-1' }, { workspace_id: 'ws-2' }, { workspace_id: 'ws-3' }],
      total: 3,
    });
    const { wsIds, queryClient } = setup('ws-3');
    await waitFor(() => expect(wsIds()).toEqual(['ws-1', 'ws-2', 'ws-3']));

    // Chatting in ws-3 bumps its updated_at → server now returns it first.
    mockGetWorkspaces.mockResolvedValue({
      workspaces: [{ workspace_id: 'ws-3' }, { workspace_id: 'ws-1' }, { workspace_id: 'ws-2' }],
      total: 3,
    });
    await act(async () => {
      await queryClient.invalidateQueries();
    });

    await waitFor(() => expect(mockGetWorkspaces.mock.calls.length).toBeGreaterThan(1));
    expect(wsIds()).toEqual(['ws-1', 'ws-2', 'ws-3']);
  });

  it('re-snapshots when a refetch changes sort_order (reorder made in the gallery)', async () => {
    mockGetWorkspaces.mockResolvedValue({
      workspaces: [
        { workspace_id: 'ws-1', sort_order: 0 },
        { workspace_id: 'ws-2', sort_order: 1 },
        { workspace_id: 'ws-3', sort_order: 2 },
      ],
      total: 3,
    });
    const { wsIds, queryClient } = setup('ws-1');
    await waitFor(() => expect(wsIds()).toEqual(['ws-1', 'ws-2', 'ws-3']));

    // The user reorders in the workspace gallery: ws-3 moves to the top. That
    // persists new sort_order values and invalidates the shared list query.
    mockGetWorkspaces.mockResolvedValue({
      workspaces: [
        { workspace_id: 'ws-3', sort_order: 0 },
        { workspace_id: 'ws-1', sort_order: 1 },
        { workspace_id: 'ws-2', sort_order: 2 },
      ],
      total: 3,
    });
    await act(async () => {
      await queryClient.invalidateQueries();
    });

    await waitFor(() => expect(mockGetWorkspaces.mock.calls.length).toBeGreaterThan(1));
    // The nav must adopt the gallery's new manual order, not freeze it out.
    expect(wsIds()).toEqual(['ws-3', 'ws-1', 'ws-2']);
  });

  it('keeps the frozen order when only updated_at recency shifts, even with sort_order present', async () => {
    // sort_order is stable across refetches; only the recency tiebreak moves.
    // The nav must hold its frozen order (no hoist) — sort_order didn't change.
    mockGetWorkspaces.mockResolvedValue({
      workspaces: [
        { workspace_id: 'ws-1', sort_order: 0 },
        { workspace_id: 'ws-2', sort_order: 0 },
        { workspace_id: 'ws-3', sort_order: 0 },
      ],
      total: 3,
    });
    const { wsIds, queryClient } = setup('ws-3');
    await waitFor(() => expect(wsIds()).toEqual(['ws-1', 'ws-2', 'ws-3']));

    // Same sort_order values, server reshuffles by recency (ws-3 chatted in).
    mockGetWorkspaces.mockResolvedValue({
      workspaces: [
        { workspace_id: 'ws-3', sort_order: 0 },
        { workspace_id: 'ws-1', sort_order: 0 },
        { workspace_id: 'ws-2', sort_order: 0 },
      ],
      total: 3,
    });
    await act(async () => {
      await queryClient.invalidateQueries();
    });

    await waitFor(() => expect(mockGetWorkspaces.mock.calls.length).toBeGreaterThan(1));
    expect(wsIds()).toEqual(['ws-1', 'ws-2', 'ws-3']);
  });

  it("under 'activity' order, follows server recency instead of freezing", async () => {
    // 'activity' mirrors the gallery's recency sort — the server's order is
    // authoritative, so a recency reshuffle DOES reorder the nav (unlike
    // 'custom', which freezes to avoid hoisting the active workspace).
    setNavPrefs({ orderBy: 'activity' });
    mockGetWorkspaces.mockResolvedValue({
      workspaces: [{ workspace_id: 'ws-1' }, { workspace_id: 'ws-2' }, { workspace_id: 'ws-3' }],
      total: 3,
    });
    const { wsIds, queryClient } = setup('ws-3');
    await waitFor(() => expect(wsIds()).toEqual(['ws-1', 'ws-2', 'ws-3']));

    // ws-3 chatted in → server returns it first; activity adopts the new order.
    mockGetWorkspaces.mockResolvedValue({
      workspaces: [{ workspace_id: 'ws-3' }, { workspace_id: 'ws-1' }, { workspace_id: 'ws-2' }],
      total: 3,
    });
    await act(async () => {
      await queryClient.invalidateQueries();
    });

    await waitFor(() => expect(mockGetWorkspaces.mock.calls.length).toBeGreaterThan(1));
    expect(wsIds()).toEqual(['ws-3', 'ws-1', 'ws-2']);
  });

  it('exposes drag-reorder only under the custom order', async () => {
    mockGetWorkspaces.mockResolvedValue({ workspaces: [{ workspace_id: 'ws-1' }], total: 1 });
    const { result } = setup('ws-1');
    await waitFor(() => expect(result.current.workspaces.length).toBe(1));
    // Default order is 'custom' → reorder available.
    expect(result.current.canReorderWorkspaces).toBe(true);

    act(() => { setNavPrefs({ orderBy: 'name' }); });
    await waitFor(() => expect(result.current.canReorderWorkspaces).toBe(false));
  });

  it('shows every workspace by default (workspaceLimit "all")', async () => {
    const all = Array.from({ length: 12 }, (_, i) => ({ workspace_id: `ws-${i + 1}` }));
    mockGetWorkspaces.mockResolvedValue({ workspaces: all, total: 12 });
    const { wsIds, result } = setup('ws-1');

    await waitFor(() => expect(wsIds().length).toBe(12));
    expect(result.current.hasMore).toBe(false);
  });

  it('pages in the remainder automatically when the first fetch is partial', async () => {
    const firstPage = Array.from({ length: 20 }, (_, i) => ({ workspace_id: `ws-${i + 1}` }));
    const rest = Array.from({ length: 5 }, (_, i) => ({ workspace_id: `ws-${i + 21}` }));
    mockGetWorkspaces.mockImplementation((_limit: number, offset: number) =>
      Promise.resolve(offset === 0
        ? { workspaces: firstPage, total: 25 }
        : { workspaces: rest, total: 25 }),
    );
    const { wsIds } = setup('ws-1');

    await waitFor(() => expect(wsIds().length).toBe(25));
    expect(wsIds()[24]).toBe('ws-25');
  });

  it('keeps the current workspace in view at the bottom with a numeric limit', async () => {
    // 10 workspaces, limit 9 — the current one (ws-10) is outside the visible
    // slice and must join at the bottom.
    setNavPrefs({ workspaceLimit: 9 });
    const all = Array.from({ length: 10 }, (_, i) => ({ workspace_id: `ws-${i + 1}` }));
    mockGetWorkspaces.mockResolvedValue({ workspaces: all, total: 10 });
    const { wsIds } = setup('ws-10');

    await waitFor(() => expect(wsIds().length).toBe(10));
    expect(wsIds()[0]).toBe('ws-1');
    expect(wsIds()[9]).toBe('ws-10');
  });
});

describe('useNavigationData — drag-reorder workspaces', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    resetStableNavOrder();
    resetSharedWorkspaceThreads();
    resetNavPrefs();
    mockGetWorkspaceThreads.mockResolvedValue({ threads: [] });
    mockReorderWorkspaces.mockResolvedValue(undefined);
    mockGetWorkspaces.mockResolvedValue({
      workspaces: [
        { workspace_id: 'ws-pin', is_pinned: true },
        { workspace_id: 'ws-flash', status: 'flash' },
        { workspace_id: 'ws-1' },
        { workspace_id: 'ws-2' },
        { workspace_id: 'ws-3' },
      ],
      total: 5,
    });
  });

  function setup() {
    const queryClient = createTestQueryClient();
    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
    const rendered = renderHook(() => useNavigationData('ws-1'), { wrapper });
    const wsIds = () => rendered.result.current.workspaces.map((ws) => ws.workspace_id);
    return { ...rendered, wsIds };
  }

  it('moves the dragged workspace and persists sequential sort_order including flash', async () => {
    const { wsIds, result } = setup();
    await waitFor(() => expect(wsIds().length).toBe(5));

    await act(async () => {
      await result.current.reorderWorkspace('ws-3', 'ws-1');
    });

    expect(wsIds()).toEqual(['ws-pin', 'ws-flash', 'ws-3', 'ws-1', 'ws-2']);
    // Flash's slot is written too — leaving it out ties its sort_order with
    // a neighbor's and the pinned block reshuffles on every updated_at bump.
    expect(mockReorderWorkspaces).toHaveBeenCalledWith([
      { workspace_id: 'ws-pin', sort_order: 0 },
      { workspace_id: 'ws-flash', sort_order: 1 },
      { workspace_id: 'ws-3', sort_order: 2 },
      { workspace_id: 'ws-1', sort_order: 3 },
      { workspace_id: 'ws-2', sort_order: 4 },
    ]);
  });

  it('refuses a drop across the pinned/unpinned boundary', async () => {
    const { wsIds, result } = setup();
    await waitFor(() => expect(wsIds().length).toBe(5));

    await act(async () => {
      await result.current.reorderWorkspace('ws-1', 'ws-pin');
    });

    expect(wsIds()).toEqual(['ws-pin', 'ws-flash', 'ws-1', 'ws-2', 'ws-3']);
    expect(mockReorderWorkspaces).not.toHaveBeenCalled();
  });

  it('refuses flash drags across the pin boundary, in both directions', async () => {
    const { wsIds, result } = setup();
    await waitFor(() => expect(wsIds().length).toBe(5));

    await act(async () => {
      await result.current.reorderWorkspace('ws-flash', 'ws-2');
      await result.current.reorderWorkspace('ws-2', 'ws-flash');
    });

    expect(wsIds()).toEqual(['ws-pin', 'ws-flash', 'ws-1', 'ws-2', 'ws-3']);
    expect(mockReorderWorkspaces).not.toHaveBeenCalled();
  });

  it('lets flash reorder within the pinned block and persists it', async () => {
    const { wsIds, result } = setup();
    await waitFor(() => expect(wsIds().length).toBe(5));

    await act(async () => {
      await result.current.reorderWorkspace('ws-flash', 'ws-pin');
    });

    expect(wsIds()).toEqual(['ws-flash', 'ws-pin', 'ws-1', 'ws-2', 'ws-3']);
    expect(mockReorderWorkspaces).toHaveBeenCalledWith([
      { workspace_id: 'ws-flash', sort_order: 0 },
      { workspace_id: 'ws-pin', sort_order: 1 },
      { workspace_id: 'ws-1', sort_order: 2 },
      { workspace_id: 'ws-2', sort_order: 3 },
      { workspace_id: 'ws-3', sort_order: 4 },
    ]);
  });

  it('lets a pinned workspace take the flash slot and persists it', async () => {
    const { wsIds, result } = setup();
    await waitFor(() => expect(wsIds().length).toBe(5));

    await act(async () => {
      await result.current.reorderWorkspace('ws-pin', 'ws-flash');
    });

    expect(wsIds()).toEqual(['ws-flash', 'ws-pin', 'ws-1', 'ws-2', 'ws-3']);
    expect(mockReorderWorkspaces).toHaveBeenCalledWith([
      { workspace_id: 'ws-flash', sort_order: 0 },
      { workspace_id: 'ws-pin', sort_order: 1 },
      { workspace_id: 'ws-1', sort_order: 2 },
      { workspace_id: 'ws-2', sort_order: 3 },
      { workspace_id: 'ws-3', sort_order: 4 },
    ]);
  });

  it('rolls the optimistic order back when persisting fails', async () => {
    const { wsIds, result } = setup();
    await waitFor(() => expect(wsIds().length).toBe(5));
    mockReorderWorkspaces.mockRejectedValueOnce(new Error('boom'));

    await act(async () => {
      await result.current.reorderWorkspace('ws-3', 'ws-1');
    });

    expect(wsIds()).toEqual(['ws-pin', 'ws-flash', 'ws-1', 'ws-2', 'ws-3']);
  });
});

describe('useNavigationData — pinned-block partition & pin freeze reset', () => {
  // The server's custom sort: is_pinned DESC, sort_order ASC, updated_at DESC.
  // It has NO flash ranking — flash competes on those fields like any row.
  const serverSort = (rows: Record<string, unknown>[]) =>
    [...rows].sort((a, b) => {
      if (Boolean(a.is_pinned) !== Boolean(b.is_pinned)) return a.is_pinned ? -1 : 1;
      if (a.sort_order !== b.sort_order) return (a.sort_order as number) - (b.sort_order as number);
      return String(b.updated_at).localeCompare(String(a.updated_at));
    });

  let server: Record<string, unknown>[];

  beforeEach(() => {
    vi.clearAllMocks();
    resetStableNavOrder();
    resetSharedWorkspaceThreads();
    resetNavPrefs();
    mockGetWorkspaceThreads.mockResolvedValue({ threads: [] });
    server = [
      { workspace_id: 'ws-flash', status: 'flash', is_pinned: true, sort_order: 0, updated_at: '2026-01-01' },
      { workspace_id: 'ws-a', is_pinned: false, sort_order: 0, updated_at: '2026-01-03' },
      { workspace_id: 'ws-b', is_pinned: false, sort_order: 1, updated_at: '2026-01-02' },
    ];
    mockGetWorkspaces.mockImplementation(async () => ({
      workspaces: serverSort(server).map((w) => ({ ...w })),
      total: server.length,
    }));
    mockUpdateWorkspace.mockImplementation(async (id: string, patch: Record<string, unknown>) => {
      const w = server.find((s) => s.workspace_id === id);
      if (w) Object.assign(w, patch);
    });
  });

  function setup() {
    const queryClient = createTestQueryClient();
    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
    const rendered = renderHook(() => useNavigationData('ws-a'), { wrapper });
    const wsIds = () => rendered.result.current.workspaces.map((ws) => ws.workspace_id);
    return { ...rendered, wsIds };
  }

  it('never renders an unpinned workspace above flash, even when the server ranks it higher', async () => {
    // A flash row without is_pinned falls into the server's unpinned block,
    // where newer recency puts ws-a above it — the partition must still treat
    // flash as pinned and keep every unpinned row below it.
    server[0] = { ...server[0], is_pinned: false };
    const { wsIds } = setup();
    await waitFor(() => expect(wsIds().length).toBe(3));
    expect(wsIds()).toEqual(['ws-flash', 'ws-a', 'ws-b']);
  });

  it('lets a pinned workspace outrank flash inside the pinned block', async () => {
    // Flash is always-pinned, not always-first: a pinned row that beats it on
    // the server sort (sort_order tie, newer updated_at) legitimately renders
    // above it, while the unpinned rows stay below both.
    server.push({ workspace_id: 'ws-pin', is_pinned: true, sort_order: 0, updated_at: '2026-01-05' });
    const { wsIds } = setup();
    await waitFor(() => expect(wsIds().length).toBe(4));
    expect(wsIds()).toEqual(['ws-pin', 'ws-flash', 'ws-a', 'ws-b']);
  });

  it('returns an unpinned workspace to its custom slot instead of freezing the pinned-era order', async () => {
    // The regression needs the real-world sequence: the OPTIMISTIC render must
    // commit (pre-consuming the arrangement change and re-freezing the
    // pre-sort order) BEFORE the server responds and the refetch re-sorts.
    // A resolved mock lets act() batch both into one render, hiding the bug —
    // so the update is held on a deferred promise across each toggle.
    let release: (() => void) | undefined;
    mockUpdateWorkspace.mockImplementation(async (id: string, patch: Record<string, unknown>) => {
      await new Promise<void>((r) => { release = r; });
      const w = server.find((s) => s.workspace_id === id);
      if (w) Object.assign(w, patch);
    });
    const togglePin = async (pinned: boolean) => {
      let done: Promise<void> = Promise.resolve();
      await act(async () => {
        done = result.current.pinWorkspace('ws-b', pinned);
        // A macrotask, not a microtask: React Query batches cache
        // notifications past a bare Promise.resolve(), and the optimistic
        // render MUST commit (recording the arrangement change) before the
        // server responds for the sequence to match the browser.
        await new Promise((r) => setTimeout(r, 0));
      });
      await act(async () => {
        release!();
        await done;
      });
    };

    const { wsIds, result } = setup();
    await waitFor(() => expect(wsIds()).toEqual(['ws-flash', 'ws-a', 'ws-b']));

    // Pin ws-b: the server re-sorts it into the pinned block; the frozen
    // order must follow the refetch, not keep the optimistic-era snapshot.
    await togglePin(true);
    await waitFor(() => expect(wsIds()).toEqual(['ws-flash', 'ws-b', 'ws-a']));

    // Unpin: ws-b must fall back to its sort_order slot BELOW ws-a. The
    // regression froze the pinned-era order (ws-b first) because the
    // optimistic patch pre-consumed the arrangement change, so the server's
    // re-sorted refetch never re-snapshotted.
    await togglePin(false);
    await waitFor(() => expect(wsIds()).toEqual(['ws-flash', 'ws-a', 'ws-b']));
  });
});

describe('useNavigationData — pin & rename workspace', () => {
  // A stateful "server": updateWorkspace mutates it and the invalidate-driven
  // refetch reads it back, so the post-commit view reflects the persisted change
  // (a static mock would clobber the optimistic patch on refetch).
  let server: { workspace_id: string; name: string; is_pinned: boolean }[];

  beforeEach(() => {
    vi.clearAllMocks();
    resetStableNavOrder();
    resetSharedWorkspaceThreads();
    resetNavPrefs();
    mockGetWorkspaceThreads.mockResolvedValue({ threads: [] });
    server = [
      { workspace_id: 'ws-1', name: 'Alpha', is_pinned: false },
      { workspace_id: 'ws-2', name: 'Beta', is_pinned: false },
    ];
    mockGetWorkspaces.mockImplementation(async () => ({
      workspaces: server.map((w) => ({ ...w })),
      total: server.length,
    }));
    mockUpdateWorkspace.mockImplementation(async (id: string, patch: Record<string, unknown>) => {
      const w = server.find((s) => s.workspace_id === id);
      if (w) Object.assign(w, patch);
    });
  });

  function setup() {
    const queryClient = createTestQueryClient();
    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
    const rendered = renderHook(() => useNavigationData('ws-1'), { wrapper });
    const byId = (id: string) => rendered.result.current.workspaces.find((ws) => ws.workspace_id === id);
    return { ...rendered, byId };
  }

  it('optimistically pins a workspace and persists is_pinned', async () => {
    const { result, byId } = setup();
    await waitFor(() => expect(result.current.workspaces.length).toBe(2));

    await act(async () => {
      await result.current.pinWorkspace('ws-2', true);
    });

    await waitFor(() => expect(byId('ws-2')?.is_pinned).toBe(true));
    expect(mockUpdateWorkspace).toHaveBeenCalledWith('ws-2', { is_pinned: true });
  });

  it('optimistically renames a workspace and persists the trimmed name', async () => {
    const { result, byId } = setup();
    await waitFor(() => expect(result.current.workspaces.length).toBe(2));

    await act(async () => {
      await result.current.renameWorkspace('ws-1', '  Gamma  ');
    });

    await waitFor(() => expect(byId('ws-1')?.name).toBe('Gamma'));
    expect(mockUpdateWorkspace).toHaveBeenCalledWith('ws-1', { name: 'Gamma' });
  });

  it('skips the rename request for a blank name', async () => {
    const { result } = setup();
    await waitFor(() => expect(result.current.workspaces.length).toBe(2));

    await act(async () => {
      await result.current.renameWorkspace('ws-1', '   ');
    });

    expect(mockUpdateWorkspace).not.toHaveBeenCalled();
  });

  it('rolls back the optimistic rename when persisting fails', async () => {
    const { result, byId } = setup();
    await waitFor(() => expect(result.current.workspaces.length).toBe(2));
    mockUpdateWorkspace.mockRejectedValueOnce(new Error('boom'));

    await act(async () => {
      await result.current.renameWorkspace('ws-1', 'Gamma');
    });

    expect(byId('ws-1')?.name).toBe('Alpha');
  });
});

describe('useNavigationData — pin thread', () => {
  // Stateful thread "server": the pin path refetches page 0 explicitly, so the
  // post-commit view has to reflect what the test set as the new server order.
  let serverThreads: TestThread[];

  beforeEach(() => {
    vi.clearAllMocks();
    resetStableNavOrder();
    resetSharedWorkspaceThreads();
    resetNavPrefs();
    mockGetWorkspaces.mockResolvedValue({ workspaces: [{ workspace_id: 'ws-1' }], total: 1 });
    serverThreads = threads('t-3', 't-1', 't-2');
    mockGetWorkspaceThreads.mockImplementation(async () => ({
      threads: serverThreads.map((t) => ({ ...t })),
      total: serverThreads.length,
    }));
    mockUpdateThread.mockResolvedValue({});
  });

  function setup() {
    const queryClient = createTestQueryClient();
    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
    const rendered = renderHook(() => useNavigationData('ws-1'), { wrapper });
    const ids = () =>
      (rendered.result.current.workspaceThreads['ws-1']?.threads ?? []).map((t) => t.thread_id);
    return { ...rendered, ids };
  }

  it('pin hoists the row and unpin re-freezes from the refetched order', async () => {
    const { result, ids } = setup();
    await waitFor(() => expect(ids()).toEqual(['t-3', 't-1', 't-2']));

    // Pin t-2 — the server re-sorts pinned-first.
    serverThreads = [{ ...thread('t-2'), is_pinned: true }, thread('t-3'), thread('t-1')];
    await act(async () => {
      await result.current.pinThread('ws-1', 't-2', true);
    });
    await waitFor(() => expect(ids()).toEqual(['t-2', 't-3', 't-1']));
    expect(mockUpdateThread).toHaveBeenCalledWith('t-2', { is_pinned: true });

    // Unpin — back to recency order. The pinned-era freeze must be released
    // AFTER the refetch lands, or t-2 keeps squatting at the top all session.
    serverThreads = threads('t-3', 't-1', 't-2');
    await act(async () => {
      await result.current.pinThread('ws-1', 't-2', false);
    });

    await waitFor(() => expect(ids()).toEqual(['t-3', 't-1', 't-2']));
    expect(mockUpdateThread).toHaveBeenLastCalledWith('t-2', { is_pinned: false });
  });
});

describe('useNavigationData — thread paging', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    resetStableNavOrder();
    resetSharedWorkspaceThreads();
    resetNavPrefs();
    mockGetWorkspaces.mockResolvedValue({
      workspaces: [{ workspace_id: 'ws-1' }],
      total: 1,
    });
  });

  function setup() {
    const queryClient = createTestQueryClient();
    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
    const rendered = renderHook(() => useNavigationData('ws-1'), { wrapper });
    const entry = () => rendered.result.current.workspaceThreads['ws-1'];
    return { ...rendered, entry, queryClient };
  }

  it('fetches the first page with the configured page size and exposes the total', async () => {
    setNavPrefs({ threadPageSize: 5 });
    mockGetWorkspaceThreads.mockResolvedValue({ threads: threads('t-1', 't-2'), total: 2 });
    const { entry } = setup();

    await waitFor(() => expect(entry()?.threads.length).toBe(2));
    expect(mockGetWorkspaceThreads).toHaveBeenCalledWith('ws-1', 5, 0);
    expect(entry()?.total).toBe(2);
  });

  it('loadMoreThreads re-requests the grown prefix and appends older threads below', async () => {
    // Show more asks for the CURRENT top shown+page rows (limit 3+10, offset 0),
    // never an offset page — offset paging assumed the local list was a prefix
    // of the live server order and died on drift.
    mockGetWorkspaceThreads.mockImplementation((_wsId: string, limit: number) =>
      Promise.resolve(limit === 10
        ? { threads: threads('t-3', 't-1', 't-2'), total: 5 }
        : { threads: threads('t-3', 't-1', 't-2', 't-old-1', 't-old-2'), total: 5 }),
    );
    const { entry, result } = setup();
    await waitFor(() => expect(entry()?.threads.length).toBe(3));

    await act(async () => {
      await result.current.loadMoreThreads('ws-1');
    });

    await waitFor(() => expect(entry()?.threads.length).toBe(5));
    expect(entry()?.threads.map((t) => t.thread_id)).toEqual(['t-3', 't-1', 't-2', 't-old-1', 't-old-2']);
    expect(mockGetWorkspaceThreads).toHaveBeenLastCalledWith('ws-1', 13, 0);
  });

  it('loadMoreThreads surfaces threads created above a stale head and honors the fresh total', async () => {
    // The bug this pins: the shown head goes stale (cache-only observers never
    // refetch) while new threads land above it on the server. Offset paging
    // returned an all-duplicate page — spinner flash, zero new rows, button
    // alive forever. The grown prefix carries the new rows (frozen order
    // surfaces unseen-before-known ids on top) and the authoritative total.
    mockGetWorkspaceThreads.mockImplementation((_wsId: string, limit: number) =>
      Promise.resolve(limit === 10
        ? { threads: threads('t-1', 't-2', 't-3'), total: 6 }
        : { threads: threads('t-n1', 't-n2', 't-n3', 't-1', 't-2', 't-3'), total: 6 }),
    );
    const { entry, result } = setup();
    await waitFor(() => expect(entry()?.threads.length).toBe(3));

    await act(async () => {
      await result.current.loadMoreThreads('ws-1');
    });

    await waitFor(() => expect(entry()?.threads.length).toBe(6));
    expect(entry()?.threads.map((t) => t.thread_id)).toEqual(['t-n1', 't-n2', 't-n3', 't-1', 't-2', 't-3']);
    expect(entry()?.total).toBe(6);
  });

  it('loadMoreThreads retires the button via the fresh total when the server list shrank', async () => {
    // Rows archived from another surface shrink the server list under the
    // local one. The re-requested prefix returns what actually exists plus the
    // true total, so `threads.length < total` goes false and the button leaves
    // instead of no-op flashing forever.
    mockGetWorkspaceThreads.mockImplementation((_wsId: string, limit: number) =>
      Promise.resolve(limit === 10
        ? { threads: threads('t-1', 't-2', 't-3'), total: 6 }
        : { threads: threads('t-1', 't-2', 't-3'), total: 3 }),
    );
    const { entry, result } = setup();
    await waitFor(() => expect(entry()?.threads.length).toBe(3));

    await act(async () => {
      await result.current.loadMoreThreads('ws-1');
    });

    await waitFor(() => expect(entry()?.total).toBe(3));
    expect(entry()?.threads.map((t) => t.thread_id)).toEqual(['t-1', 't-2', 't-3']);
  });

  it('loadMoreThreads drops local rows missing from a short (complete) response', async () => {
    // A row archived in another tab while this one was frozen misses its feed
    // event (best-effort, no replay) and survives as a store extra. The next
    // Show more re-requests the grown prefix; a response shorter than the
    // request IS the complete server list, so a local row missing from it is
    // genuinely gone and must not be re-appended as a ghost.
    mockGetWorkspaceThreads.mockImplementation((_wsId: string, limit: number) =>
      Promise.resolve(
        limit === 10
          ? { threads: threads('t-1', 't-2', 't-3'), total: 5 }
          : limit === 13
            ? { threads: threads('t-1', 't-2', 't-3', 't-old-1', 't-old-2'), total: 5 }
            : { threads: threads('t-1', 't-2', 't-3', 't-old-2'), total: 4 },
      ),
    );
    const { entry, result } = setup();
    await waitFor(() => expect(entry()?.threads.length).toBe(3));
    await act(async () => {
      await result.current.loadMoreThreads('ws-1');
    });
    await waitFor(() => expect(entry()?.threads.length).toBe(5));

    // t-old-1 was archived elsewhere; the 15-row request comes back with 4.
    await act(async () => {
      await result.current.loadMoreThreads('ws-1');
    });

    await waitFor(() =>
      expect(entry()?.threads.map((t) => t.thread_id)).toEqual(['t-1', 't-2', 't-3', 't-old-2']));
    expect(entry()?.total).toBe(4);
  });

  it('a Show-more row removed via the shared patcher (gallery archive path) leaves the tree', async () => {
    // The ghost-row regression: ThreadGallery's archive calls bare
    // patchThreadRows — it has no access to this hook's patchThread. Before
    // the patcher composed the shared store, the removed row survived as a
    // store extra and kept rendering in the sidebar until reload.
    mockGetWorkspaceThreads.mockImplementation((_wsId: string, limit: number) =>
      Promise.resolve(limit === 10
        ? { threads: threads('t-1', 't-2', 't-3'), total: 5 }
        : { threads: threads('t-1', 't-2', 't-3', 't-old-1', 't-old-2'), total: 5 }),
    );
    const { entry, result, queryClient } = setup();
    await waitFor(() => expect(entry()?.threads.length).toBe(3));
    await act(async () => {
      await result.current.loadMoreThreads('ws-1');
    });
    await waitFor(() => expect(entry()?.threads.length).toBe(5));

    act(() => {
      patchThreadRows<TestThread>(
        queryClient,
        queryKeys.threads.byWorkspace('ws-1'),
        (rows) => (rows.some((t) => t.thread_id === 't-old-1') ? rows.filter((t) => t.thread_id !== 't-old-1') : rows),
        { skipKey: isArchivedThreadsKey },
      );
    });

    await waitFor(() =>
      expect(entry()?.threads.map((t) => t.thread_id)).toEqual(['t-1', 't-2', 't-3', 't-old-2']));
  });

  it('loadMoreThreads is single-flight per workspace under a rapid double-tap', async () => {
    mockGetWorkspaceThreads.mockImplementation((_wsId: string, limit: number) =>
      Promise.resolve(limit === 10
        ? { threads: threads('t-3', 't-1', 't-2'), total: 5 }
        : { threads: threads('t-3', 't-1', 't-2', 't-old-1', 't-old-2'), total: 5 }),
    );
    const { entry, result } = setup();
    await waitFor(() => expect(entry()?.threads.length).toBe(3));

    // Both taps fire before the first resolves. The limit is snapshotted before
    // the await, so without the guard both would fetch the same grown prefix.
    await act(async () => {
      await Promise.all([
        result.current.loadMoreThreads('ws-1'),
        result.current.loadMoreThreads('ws-1'),
      ]);
    });

    const grownPrefixCalls = mockGetWorkspaceThreads.mock.calls.filter(([, limit]) => limit === 13);
    expect(grownPrefixCalls.length).toBe(1);
    await waitFor(() => expect(entry()?.threads.length).toBe(5));
  });
});
