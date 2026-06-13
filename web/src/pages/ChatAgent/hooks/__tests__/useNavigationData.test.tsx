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
import { resetNavPrefs, setNavPrefs } from '../../utils/navPrefs';

vi.mock('../../utils/api', () => ({
  getWorkspaces: vi.fn(),
  getWorkspaceThreads: vi.fn(),
  reorderWorkspaces: vi.fn(),
  updateWorkspace: vi.fn(),
}));

import { getWorkspaces, getWorkspaceThreads, reorderWorkspaces, updateWorkspace } from '../../utils/api';

const mockGetWorkspaces = getWorkspaces as Mock;
const mockGetWorkspaceThreads = getWorkspaceThreads as Mock;
const mockReorderWorkspaces = reorderWorkspaces as Mock;
const mockUpdateWorkspace = updateWorkspace as Mock;

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
});

describe('useNavigationData — stable workspace ordering', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    resetStableNavOrder();
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

  it('moves the dragged workspace and persists sequential sort_order without flash', async () => {
    const { wsIds, result } = setup();
    await waitFor(() => expect(wsIds().length).toBe(5));

    await act(async () => {
      await result.current.reorderWorkspace('ws-3', 'ws-1');
    });

    expect(wsIds()).toEqual(['ws-pin', 'ws-flash', 'ws-3', 'ws-1', 'ws-2']);
    expect(mockReorderWorkspaces).toHaveBeenCalledWith([
      { workspace_id: 'ws-pin', sort_order: 0 },
      { workspace_id: 'ws-3', sort_order: 1 },
      { workspace_id: 'ws-1', sort_order: 2 },
      { workspace_id: 'ws-2', sort_order: 3 },
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

  it('refuses to move the flash workspace or drop onto it', async () => {
    const { wsIds, result } = setup();
    await waitFor(() => expect(wsIds().length).toBe(5));

    await act(async () => {
      await result.current.reorderWorkspace('ws-flash', 'ws-2');
      await result.current.reorderWorkspace('ws-2', 'ws-flash');
    });

    expect(wsIds()).toEqual(['ws-pin', 'ws-flash', 'ws-1', 'ws-2', 'ws-3']);
    expect(mockReorderWorkspaces).not.toHaveBeenCalled();
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

describe('useNavigationData — pin & rename workspace', () => {
  // A stateful "server": updateWorkspace mutates it and the invalidate-driven
  // refetch reads it back, so the post-commit view reflects the persisted change
  // (a static mock would clobber the optimistic patch on refetch).
  let server: { workspace_id: string; name: string; is_pinned: boolean }[];

  beforeEach(() => {
    vi.clearAllMocks();
    resetStableNavOrder();
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

describe('useNavigationData — thread paging', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    resetStableNavOrder();
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
    return { ...rendered, entry };
  }

  it('fetches the first page with the configured page size and exposes the total', async () => {
    setNavPrefs({ threadPageSize: 5 });
    mockGetWorkspaceThreads.mockResolvedValue({ threads: threads('t-1', 't-2'), total: 2 });
    const { entry } = setup();

    await waitFor(() => expect(entry()?.threads.length).toBe(2));
    expect(mockGetWorkspaceThreads).toHaveBeenCalledWith('ws-1', 5, 0);
    expect(entry()?.total).toBe(2);
  });

  it('loadMoreThreads appends the next page below the already-shown threads', async () => {
    mockGetWorkspaceThreads.mockImplementation((_wsId: string, _limit: number, offset: number) =>
      Promise.resolve(offset === 0
        ? { threads: threads('t-3', 't-1', 't-2'), total: 5 }
        : { threads: threads('t-old-1', 't-old-2'), total: 5 }),
    );
    const { entry, result } = setup();
    await waitFor(() => expect(entry()?.threads.length).toBe(3));

    await act(async () => {
      await result.current.loadMoreThreads('ws-1');
    });

    await waitFor(() => expect(entry()?.threads.length).toBe(5));
    expect(entry()?.threads.map((t) => t.thread_id)).toEqual(['t-3', 't-1', 't-2', 't-old-1', 't-old-2']);
    expect(mockGetWorkspaceThreads).toHaveBeenLastCalledWith('ws-1', 10, 3);
  });

  it('loadMoreThreads is single-flight per workspace under a rapid double-tap', async () => {
    mockGetWorkspaceThreads.mockImplementation((_wsId: string, _limit: number, offset: number) =>
      Promise.resolve(offset === 0
        ? { threads: threads('t-3', 't-1', 't-2'), total: 5 }
        : { threads: threads('t-old-1', 't-old-2'), total: 5 }),
    );
    const { entry, result } = setup();
    await waitFor(() => expect(entry()?.threads.length).toBe(3));

    // Both taps fire before the first resolves. The offset is snapshotted before
    // the await, so without the guard both would fetch the same offset-3 page.
    await act(async () => {
      await Promise.all([
        result.current.loadMoreThreads('ws-1'),
        result.current.loadMoreThreads('ws-1'),
      ]);
    });

    const offset3Calls = mockGetWorkspaceThreads.mock.calls.filter(([, , offset]) => offset === 3);
    expect(offset3Calls.length).toBe(1);
    await waitFor(() => expect(entry()?.threads.length).toBe(5));
  });
});
