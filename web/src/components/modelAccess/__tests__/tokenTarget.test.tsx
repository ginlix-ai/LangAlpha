/**
 * A token saved for a computer the Claude Code list was read without lands
 * in that list, so what it unlocks (setup's Continue) reads it at once.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { QueryClient, useQuery } from '@tanstack/react-query';
import { runAuthResets } from '@/lib/authResets';
import { renderHookWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';
import type { ClaudeCodeComputer, ClaudeCodeComputers, ClaudeCodeStatus } from '@/api/model';

const MAC: ClaudeCodeComputer = {
  computer_id: 'mac', name: 'MacBook Pro', kind: 'helper', state: 'signed_in', source: 'machine', email: null, expires_at: null,
};
vi.mock('@/api/model', () => ({ saveClaudeCodeComputerToken: vi.fn(), saveClaudeCodeToken: vi.fn() }));

import { saveClaudeCodeComputerToken, saveClaudeCodeToken } from '@/api/model';
import { SAVED_TOKEN, useTokenRoutes } from '../tokenTarget';

const ROUTE = { kind: 'computer', computerId: 'mac', name: 'MacBook Pro' } as const;

describe('useTokenRoutes on a computer', () => {
  beforeEach(() => {
    vi.mocked(saveClaudeCodeComputerToken).mockReset();
    vi.mocked(saveClaudeCodeComputerToken).mockResolvedValue(MAC);
  });

  it('keeps nothing from a save answered after the account changed', async () => {
    let answer!: (entry: ClaudeCodeComputer) => void;
    vi.mocked(saveClaudeCodeComputerToken).mockReturnValue(new Promise((resolve) => { answer = resolve; }));
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } });
    const key = queryKeys.features.claudeCodeComputers();
    queryClient.setQueryData<ClaudeCodeComputers>(key, { computers: [] });
    const { result } = renderHookWithProviders(() => useTokenRoutes(ROUTE), { queryClient });
    let saved!: Promise<void>;
    act(() => { saved = result.current.save('sk-ant-oat01-x'); });
    runAuthResets();
    await act(async () => { answer(MAC); await saved; });
    expect(queryClient.getQueryData<ClaudeCodeComputers>(key)?.computers).toEqual([]);
  });

  it('drops a list read that left before the save, which would put the old row back', async () => {
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } });
    const key = queryKeys.features.claudeCodeComputers();
    const reads: Array<(list: ClaudeCodeComputers) => void> = [];
    const read = () => new Promise<ClaudeCodeComputers>((resolve) => { reads.push(resolve); });
    const { result } = renderHookWithProviders(
      () => ({ routes: useTokenRoutes(ROUTE), list: useQuery({ queryKey: key, queryFn: read }) }),
      { queryClient },
    );
    await waitFor(() => expect(reads).toHaveLength(1));
    act(() => reads[0]({ computers: [] }));
    await waitFor(() => expect(result.current.list.data).toEqual({ computers: [] }));
    void queryClient.refetchQueries({ queryKey: key });
    await waitFor(() => expect(reads).toHaveLength(2));
    await act(async () => { await result.current.routes.save('sk-ant-oat01-x'); });
    expect(queryClient.getQueryData<ClaudeCodeComputers>(key)?.computers).toEqual([MAC]);
    await act(async () => { reads[1]({ computers: [] }); });
    expect(queryClient.getQueryData<ClaudeCodeComputers>(key)?.computers).toEqual([MAC]);
  });

  it('reads the list again where a save found none to add its computer to', async () => {
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } });
    const key = queryKeys.features.claudeCodeComputers();
    const reads: Array<(list: ClaudeCodeComputers) => void> = [];
    const read = () => new Promise<ClaudeCodeComputers>((resolve) => { reads.push(resolve); });
    const { result } = renderHookWithProviders(
      () => ({ routes: useTokenRoutes(ROUTE), list: useQuery({ queryKey: key, queryFn: read }) }),
      { queryClient },
    );
    await waitFor(() => expect(reads).toHaveLength(1));
    await act(async () => { await result.current.routes.save('sk-ant-oat01-x'); });
    await waitFor(() => expect(reads).toHaveLength(2));
    act(() => reads[1]({ computers: [MAC] }));
    await waitFor(() => expect(result.current.list.data?.computers).toEqual([MAC]));
  });

  it('adds the computer its token was saved on where the list was read without it', async () => {
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } });
    const key = queryKeys.features.claudeCodeComputers();
    queryClient.setQueryData<ClaudeCodeComputers>(key, { computers: [] });
    const { result } = renderHookWithProviders(() => useTokenRoutes(ROUTE), { queryClient });
    await act(async () => { await result.current.save('sk-ant-oat01-x'); });
    expect(queryClient.getQueryData<ClaudeCodeComputers>(key)?.computers).toEqual([MAC]);
  });
});

describe('useTokenRoutes on the saved token', () => {
  it('drops a status read that left before the save, which would put the old status back', async () => {
    const before: ClaudeCodeStatus = { enabled: true, state: 'off', email: null, expires_at: null, saved: false };
    const after: ClaudeCodeStatus = { ...before, saved: true };
    vi.mocked(saveClaudeCodeToken).mockResolvedValue(after);
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } });
    const key = queryKeys.features.claudeCodeStatus();
    const reads: Array<(status: ClaudeCodeStatus) => void> = [];
    const { result } = renderHookWithProviders(
      () => ({ routes: useTokenRoutes(SAVED_TOKEN), status: useQuery({ queryKey: key, queryFn: () => new Promise<ClaudeCodeStatus>((resolve) => { reads.push(resolve); }) }) }),
      { queryClient },
    );
    await waitFor(() => expect(reads).toHaveLength(1));
    await act(async () => { await result.current.routes.save('sk-ant-oat01-x'); });
    expect(queryClient.getQueryData(key)).toEqual(after);
    await act(async () => { reads[0](before); });
    expect(queryClient.getQueryData(key)).toEqual(after);
  });
});

