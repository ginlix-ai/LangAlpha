/**
 * Saving a drop in the gallery grid: the order it writes, the lists it keeps
 * in step (the sidebar's among them), the rollback, and the one-save-at-a-time
 * guard that used to live in the reorder list.
 */
import { act } from '@testing-library/react';
import { QueryClient } from '@tanstack/react-query';
import type { Mock } from 'vitest';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { queryKeys } from '@/lib/queryKeys';
import { renderHookWithProviders } from '@/test/utils';
import type { WorkspacesResponse } from '@/types/api';

vi.mock('../../utils/api', () => ({ reorderWorkspaces: vi.fn() }));
vi.mock('@/components/ui/use-toast', () => ({ toast: vi.fn() }));

import { toast } from '@/components/ui/use-toast';
import { reorderWorkspaces } from '../../utils/api';
import { CUSTOM_LIST_KEY, reorderedList, useGalleryReorder } from '../workspaceGallery/useGalleryReorder';
import type { WorkspaceRecord } from '../workspaceGallery/types';

const mockReorder = reorderWorkspaces as Mock;

const ws = (id: string, extra: Partial<WorkspaceRecord> = {}): WorkspaceRecord => ({
  workspace_id: id,
  is_home: false,
  name: id,
  status: 'running',
  updated_at: '2026-09-20T00:00:00Z',
  ...extra,
} as WorkspaceRecord);

const HOME = ws('home', { is_home: true, is_pinned: true });
const PIN_A = ws('pin-a', { is_pinned: true });
const PIN_B = ws('pin-b', { is_pinned: true });
const A = ws('a');
const B = ws('b');
const C = ws('c');
const ids = (rows: { workspace_id: string }[] | null | undefined) => rows?.map((r) => r.workspace_id);

// The sidebar's list: the first page of the custom order.
const NAV_KEY = queryKeys.workspaces.list({ limit: 20, offset: 0, sortBy: 'custom', includeHome: true });
const RECENT_KEY = queryKeys.workspaces.list({ view: 'all', sortBy: 'activity', includeHome: true });

function setup(rows: WorkspaceRecord[]) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false, gcTime: Infinity, staleTime: Infinity } } });
  queryClient.setQueryData<WorkspacesResponse>(CUSTOM_LIST_KEY, { workspaces: rows, total: rows.length });
  queryClient.setQueryData<WorkspacesResponse>(NAV_KEY, { workspaces: rows, total: rows.length });
  queryClient.setQueryData<WorkspacesResponse>(RECENT_KEY, { workspaces: [...rows].reverse(), total: rows.length });
  const hook = renderHookWithProviders(() => useGalleryReorder(), { queryClient });
  const list = (key = CUSTOM_LIST_KEY) => queryClient.getQueryData<WorkspacesResponse>(key)?.workspaces;
  return { ...hook, queryClient, list };
}

beforeEach(() => {
  vi.clearAllMocks();
  mockReorder.mockResolvedValue(undefined);
});

describe('reorderedList', () => {
  it('moves a card onto another\'s slot, Home holding its own', () => {
    expect(ids(reorderedList([HOME, PIN_A, A, B, C], 'c', 'a'))).toEqual(['home', 'pin-a', 'c', 'a', 'b']);
    // Home below a pinned workspace, as the sidebar can leave it, stays there.
    expect(ids(reorderedList([PIN_A, HOME, PIN_B, A], 'pin-b', 'pin-a'))).toEqual(['pin-b', 'home', 'pin-a', 'a']);
  });

  it('refuses a move across the pin boundary, or one that takes Home', () => {
    expect(reorderedList([HOME, PIN_A, A, B], 'a', 'pin-a')).toBeNull();
    expect(reorderedList([HOME, PIN_A, A, B], 'pin-a', 'b')).toBeNull();
    expect(reorderedList([HOME, PIN_A, A], 'pin-a', 'home')).toBeNull();
    expect(reorderedList([HOME, A], 'a', 'a')).toBeNull();
  });

  it('reads the pinned block ahead of the rest even when the list does not', () => {
    expect(ids(reorderedList([A, HOME, B, PIN_A], 'b', 'a'))).toEqual(['home', 'pin-a', 'b', 'a']);
  });
});

describe('useGalleryReorder', () => {
  it('saves every row with its slot, Home first and the pinned block kept', async () => {
    const { result } = setup([HOME, PIN_A, A, B, C]);

    await act(async () => { expect(await result.current.move('c', 'a')).toBe(true); });

    expect(mockReorder).toHaveBeenCalledTimes(1);
    expect(mockReorder).toHaveBeenCalledWith([
      { workspace_id: 'home', sort_order: 0 },
      { workspace_id: 'pin-a', sort_order: 1 },
      { workspace_id: 'c', sort_order: 2 },
      { workspace_id: 'a', sort_order: 3 },
      { workspace_id: 'b', sort_order: 4 },
    ]);
  });

  it('shows the new order at once, in the gallery and the sidebar, before the save lands', async () => {
    let finish!: () => void;
    mockReorder.mockReturnValue(new Promise<void>((resolve) => { finish = resolve; }));
    const { result, list } = setup([HOME, PIN_A, A, B, C]);

    let saved: Promise<boolean> | undefined;
    act(() => { saved = result.current.move('c', 'a'); });

    expect(ids(list())).toEqual(['home', 'pin-a', 'c', 'a', 'b']);
    expect(list()?.map((r) => r.sort_order)).toEqual([0, 1, 2, 3, 4]);
    expect(ids(list(NAV_KEY))).toEqual(['home', 'pin-a', 'c', 'a', 'b']);
    // An order the drop does not decide is left as it was.
    expect(ids(list(RECENT_KEY))).toEqual(['c', 'b', 'a', 'pin-a', 'home']);
    expect(result.current.isSaving).toBe(true);

    await act(async () => { finish(); await saved; });
    expect(result.current.isSaving).toBe(false);
  });

  it('puts every list back when the save is refused', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
    mockReorder.mockRejectedValue(new Error('offline'));
    const { result, list } = setup([HOME, PIN_A, A, B, C]);

    await act(async () => { expect(await result.current.move('c', 'a')).toBe(false); });

    expect(ids(list())).toEqual(['home', 'pin-a', 'a', 'b', 'c']);
    expect(ids(list(NAV_KEY))).toEqual(['home', 'pin-a', 'a', 'b', 'c']);
    expect(result.current.isSaving).toBe(false);
    expect(toast).toHaveBeenCalledWith({ variant: 'destructive', title: "Couldn't save the new order" });
  });

  it('keeps a delete, a create and a rename made while the refused save was in flight', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
    let refuse!: () => void;
    mockReorder.mockReturnValueOnce(new Promise<void>((_, reject) => { refuse = () => reject(new Error('offline')); }));
    const { result, queryClient, list } = setup([HOME, PIN_A, A, B, C]);

    let saved!: Promise<boolean>;
    act(() => { saved = result.current.move('c', 'a'); });
    // Each lands, refetch included, before the save is refused.
    for (const key of [CUSTOM_LIST_KEY, NAV_KEY]) {
      queryClient.setQueryData<WorkspacesResponse>(key, (prev) => ({
        ...prev!,
        workspaces: [...prev!.workspaces.filter((r) => r.workspace_id !== 'b').map((r) => (r.workspace_id === 'a' ? { ...r, name: 'Renamed' } : r)), ws('d')],
      }));
    }
    await act(async () => { refuse(); expect(await saved).toBe(false); });

    for (const key of [CUSTOM_LIST_KEY, NAV_KEY]) {
      expect(ids(list(key))).toEqual(['home', 'pin-a', 'a', 'c', 'd']);
      expect(list(key)?.find((r) => r.workspace_id === 'a')?.name).toBe('Renamed');
    }
    // The order fields come back as they were, not as the refused save set them.
    expect(list()?.map((r) => r.sort_order)).toEqual([undefined, undefined, undefined, undefined, undefined]);
  });

  it('turns a drop away at once while a save is in flight', async () => {
    mockReorder.mockReturnValueOnce(new Promise<void>(() => {}));
    const { result } = setup([HOME, A, B, C]);

    let first: boolean | undefined;
    let second: boolean | undefined;
    act(() => {
      first = result.current.drop('a', 'c');
      second = result.current.drop('b', 'a');
    });

    expect(first).toBe(true);
    expect(second).toBe(false);
    expect(mockReorder).toHaveBeenCalledTimes(1);
  });

  it('waits for a read of the list in flight and moves from what it brings', async () => {
    const { result, queryClient, list } = setup([HOME, A, B, C]);
    // The sidebar has just put c first; the read that carries it is still out.
    let land!: (data: WorkspacesResponse) => void;
    void queryClient.fetchQuery({
      queryKey: CUSTOM_LIST_KEY,
      queryFn: () => new Promise<WorkspacesResponse>((resolve) => { land = resolve; }),
      staleTime: 0,
    });

    let saved: Promise<boolean> | undefined;
    act(() => { saved = result.current.move('a', 'b'); });
    // Nothing written, and the read not cancelled back to the older list.
    expect(mockReorder).not.toHaveBeenCalled();
    expect(result.current.isSaving).toBe(true);

    await act(async () => { land({ workspaces: [HOME, C, A, B], total: 4 }); expect(await saved).toBe(true); });

    expect(mockReorder).toHaveBeenCalledWith([
      { workspace_id: 'home', sort_order: 0 },
      { workspace_id: 'c', sort_order: 1 },
      { workspace_id: 'b', sort_order: 2 },
      { workspace_id: 'a', sort_order: 3 },
    ]);
    expect(ids(list())).toEqual(['home', 'c', 'b', 'a']);
  });

  it('refuses the drop when the read it waits for fails, rather than save the older order', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const { result, queryClient, list } = setup([HOME, A, B, C]);
    let fail!: () => void;
    queryClient.fetchQuery({
      queryKey: CUSTOM_LIST_KEY,
      queryFn: () => new Promise<WorkspacesResponse>((_, reject) => { fail = () => reject(new Error('offline')); }),
      staleTime: 0,
    }).catch(() => {});

    let saved: Promise<boolean> | undefined;
    act(() => { saved = result.current.move('a', 'b'); });
    await act(async () => { fail(); expect(await saved).toBe(false); });

    expect(mockReorder).not.toHaveBeenCalled();
    expect(ids(list())).toEqual(['home', 'a', 'b', 'c']);
    expect(result.current.isSaving).toBe(false);
    expect(toast).toHaveBeenCalledWith({ variant: 'destructive', title: "Couldn't save the new order" });
  });

  it('ignores another drop until the current order is saved', async () => {
    let finish!: () => void;
    mockReorder.mockReturnValueOnce(new Promise<void>((resolve) => { finish = resolve; }));
    const { result, list } = setup([HOME, A, B, C]);

    let first: Promise<boolean> | undefined;
    act(() => { first = result.current.move('a', 'c'); });
    // Before the render that would disable the cards, too.
    let second: boolean | undefined;
    await act(async () => { second = await result.current.move('b', 'a'); });

    expect(second).toBe(false);
    expect(mockReorder).toHaveBeenCalledTimes(1);
    expect(ids(list())).toEqual(['home', 'b', 'c', 'a']);

    await act(async () => { finish(); await first; });
    await act(async () => { expect(await result.current.move('b', 'a')).toBe(true); });
    expect(mockReorder).toHaveBeenCalledTimes(2);
  });

  it('refetches every list once saved, so other orders catch up', async () => {
    const { result, queryClient } = setup([HOME, A, B]);
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries');

    await act(async () => { await result.current.move('b', 'a'); });

    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.workspaces.lists() });
  });

  it('writes nothing for a refused move', async () => {
    const { result, list } = setup([HOME, PIN_A, A, B]);

    await act(async () => { expect(await result.current.move('a', 'pin-a')).toBe(false); });

    expect(mockReorder).not.toHaveBeenCalled();
    expect(ids(list())).toEqual(['home', 'pin-a', 'a', 'b']);
  });
});
