/**
 * What a machine action refreshes, and what it leaves alone.
 *
 * The storage breakdown keys under the `computers` prefix but is a `du` over
 * the whole machine, up to half a minute on a small one. A rename, an
 * always-on toggle or a workspace joining the machine changes none of those
 * bytes, so none of them may reach that key; only a settled spec change does.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { QueryClient } from '@tanstack/react-query';
import { act } from '@testing-library/react';

import { renderHookWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';

vi.mock('@/components/ui/use-toast', () => ({ toast: vi.fn() }));

vi.mock('../../utils/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../utils/api')>()),
  renameComputer: vi.fn(async () => ({})),
  setComputerAlwaysOn: vi.fn(async () => ({})),
}));

import { useComputerActions } from '../useComputerActions';
import {
  invalidateMachine,
  invalidateMachineStorage,
  refreshComputersAfterTurn,
} from '../useComputers';

const ID = '5319ad0e-7835-4ca6-9541-b7c2961a7bf6';
const LISTS = queryKeys.computers.lists();
const STORAGE = queryKeys.computers.storage(ID);

/** A client holding both projections of the machine, neither stale. */
function seeded() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false, gcTime: Infinity } } });
  client.setQueryData(LISTS, {
    computers: [{ computer_id: ID, name: 'Alpha Research', status: 'running', is_always_on: false }],
    total: 1,
  });
  client.setQueryData(STORAGE, { live: true, workspaces: [], other_bytes: 0 });
  client.setQueryData(queryKeys.workspaces.lists(), { workspaces: [], total: 0 });
  return client;
}

const invalidated = (client: QueryClient, key: readonly unknown[]) =>
  client.getQueryState(key)?.isInvalidated === true;

describe('invalidateMachine', () => {
  it('re-reads the rows and the workspaces, not the storage breakdown', () => {
    const client = seeded();
    invalidateMachine(client);
    expect(invalidated(client, LISTS)).toBe(true);
    expect(invalidated(client, queryKeys.workspaces.lists())).toBe(true);
    expect(invalidated(client, STORAGE)).toBe(false);
  });

  it('invalidateMachineStorage is the one path to the breakdown', () => {
    const client = seeded();
    invalidateMachineStorage(client, ID);
    expect(invalidated(client, STORAGE)).toBe(true);
    expect(invalidated(client, LISTS)).toBe(false);
  });
});

describe('useComputerActions', () => {
  beforeEach(() => vi.clearAllMocks());

  it.each([
    ['rename', (a: ReturnType<typeof useComputerActions>) => a.rename.mutateAsync({ computerId: ID, name: 'Beta' })],
    ['always-on', (a: ReturnType<typeof useComputerActions>) => a.turnOffAlwaysOn.mutateAsync({ computerId: ID })],
  ])('%s refreshes the rows and leaves the storage breakdown alone', async (_name, run) => {
    const client = seeded();
    const { result } = renderHookWithProviders(() => useComputerActions(), { queryClient: client });
    await act(async () => { await run(result.current); });
    expect(invalidated(client, LISTS)).toBe(true);
    expect(invalidated(client, STORAGE)).toBe(false);
  });
});

describe('refreshComputersAfterTurn', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  /** Stand in for the list read: the server's row gains `measuredAt` or not. */
  function answering(client: QueryClient, measuredAt?: string) {
    return vi.spyOn(client, 'invalidateQueries').mockImplementation(async () => {
      if (measuredAt) {
        client.setQueryData(LISTS, {
          computers: [{ computer_id: ID, name: 'Alpha Research', status: 'running', disk: { measured_at: measuredAt } }],
          total: 1,
        });
      }
    });
  }

  it('re-reads the rows once for several turn ends, after the delay, and never the breakdown', () => {
    const client = seeded();
    const spy = vi.spyOn(client, 'invalidateQueries');

    refreshComputersAfterTurn(client, [1_000]);
    refreshComputersAfterTurn(client, [1_000]);
    vi.advanceTimersByTime(999);
    expect(spy).not.toHaveBeenCalled();
    expect(invalidated(client, LISTS)).toBe(false);

    vi.advanceTimersByTime(1);
    expect(spy).toHaveBeenCalledTimes(1);
    expect(spy).toHaveBeenCalledWith({ queryKey: LISTS });
    expect(invalidated(client, LISTS)).toBe(true);
    expect(invalidated(client, STORAGE)).toBe(false);
  });

  it('keeps reading on the schedule while no new reading has landed, then stops', async () => {
    const client = seeded();
    const spy = answering(client);

    refreshComputersAfterTurn(client, [1_000, 5_000, 20_000]);
    await vi.advanceTimersByTimeAsync(1_000);
    expect(spy).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(3_999);
    expect(spy).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(spy).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(15_000);
    expect(spy).toHaveBeenCalledTimes(3);
    await vi.advanceTimersByTimeAsync(60_000);
    expect(spy).toHaveBeenCalledTimes(3);
  });

  it('stops at the first read that brings a new reading', async () => {
    const client = seeded();
    const spy = answering(client, '2026-09-25T12:00:00Z');

    refreshComputersAfterTurn(client, [1_000, 5_000, 20_000]);
    await vi.advanceTimersByTimeAsync(60_000);
    expect(spy).toHaveBeenCalledTimes(1);
  });

  it('a sign-out cancels the series, a read in flight included', async () => {
    const { runAuthResets } = await import('@/lib/authResets');
    const client = seeded();
    const spy = answering(client);

    refreshComputersAfterTurn(client, [1_000, 5_000]);
    runAuthResets();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(spy).not.toHaveBeenCalled();

    refreshComputersAfterTurn(client, [1_000, 5_000]);
    vi.advanceTimersByTime(1_000);
    runAuthResets();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(spy).toHaveBeenCalledTimes(1);
  });
});
