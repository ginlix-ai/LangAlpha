/**
 * The thread's background commands as a query: read while the chat is in
 * front, read again on a timer only while one runs, re-read on an
 * invalidation, and settled by a stop's answer.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { QueryClient } from '@tanstack/react-query';
import { renderHookWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';
import type { CommandRun } from '@/types/commands';

vi.mock('../../utils/api', () => ({ fetchCommands: vi.fn(), stopCommand: vi.fn() }));

import { fetchCommands, stopCommand } from '../../utils/api';
import { COMMAND_TICK_MS, invalidateThreadCommands, useStopCommand, useThreadCommands } from '../useCommands';

const mockFetch = fetchCommands as Mock;
const mockStop = stopCommand as Mock;

const THREAD = '7d1c2a4e-0b3f-4c55-9a61-2f8e5d9b0c17';

function command(over: Partial<CommandRun> = {}): CommandRun {
  return {
    command_id: 'bg-3f9a1c2e7d10', tool: 'Bash', description: 'Run 10-year backtest', status: 'running',
    exit_code: null, failure_reason: null, tool_call_id: 'call_1', log_path: '.agents/threads/7d1c2a4e/commands/bg-3f9a1c2e7d10.log',
    started_at: '2030-01-01T10:00:00Z', handed_off_at: '2030-01-01T10:05:00Z', ended_at: null,
    ...over,
  } as CommandRun;
}

const RUNNING = command();
const ENDED = command({ status: 'completed', exit_code: 0, ended_at: '2030-01-01T10:41:00Z' });
const answer = (commands: CommandRun[]) => ({ thread_id: THREAD, commands });

/** A client that keeps an entry no hook observes, as the app's does. */
function keepingClient(): QueryClient {
  return new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
}

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  vi.useRealTimers();
});

describe('useThreadCommands', () => {
  it('reads the listing', async () => {
    mockFetch.mockResolvedValue(answer([RUNNING, ENDED]));
    const { result } = renderHookWithProviders(() => useThreadCommands(THREAD));
    await waitFor(() => expect(result.current).toEqual([RUNNING, ENDED]));
    expect(mockFetch).toHaveBeenCalledWith(THREAD);
  });

  it('reads nothing for a chat with no thread yet, or one kept behind another', async () => {
    mockFetch.mockResolvedValue(answer([RUNNING]));
    const none = renderHookWithProviders(() => useThreadCommands('__default__'));
    const hidden = renderHookWithProviders(() => useThreadCommands(THREAD, false));
    await act(async () => {});
    expect(mockFetch).not.toHaveBeenCalled();
    expect(none.result.current).toEqual([]);
    expect(hidden.result.current).toEqual([]);
  });

  it('reads again while a command runs, and stops once none does', async () => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] });
    mockFetch.mockResolvedValueOnce(answer([RUNNING])).mockResolvedValue(answer([ENDED]));
    const { result } = renderHookWithProviders(() => useThreadCommands(THREAD));
    // vi.waitFor, which steps the fake clock as it polls; RTL's would stall.
    await vi.waitFor(() => expect(result.current).toEqual([RUNNING]));

    await act(async () => { await vi.advanceTimersByTimeAsync(COMMAND_TICK_MS); });
    await vi.waitFor(() => expect(result.current).toEqual([ENDED]));
    expect(mockFetch).toHaveBeenCalledTimes(2);

    await act(async () => { await vi.advanceTimersByTimeAsync(COMMAND_TICK_MS * 4); });
    expect(mockFetch).toHaveBeenCalledTimes(2);
  });

  it('reads again when the stream says a command moved', async () => {
    mockFetch.mockResolvedValueOnce(answer([])).mockResolvedValue(answer([RUNNING]));
    const { result, queryClient } = renderHookWithProviders(() => useThreadCommands(THREAD));
    await waitFor(() => expect(mockFetch).toHaveBeenCalledTimes(1));
    act(() => invalidateThreadCommands(queryClient, THREAD));
    await waitFor(() => expect(result.current).toEqual([RUNNING]));
  });
});

describe('useStopCommand', () => {
  it("shows the stopped row the answer carries, without another read", async () => {
    const client = keepingClient();
    client.setQueryData(queryKeys.threads.commands(THREAD), answer([RUNNING]));
    const stopped = command({ status: 'stopped', ended_at: '2030-01-01T10:20:00Z' });
    mockStop.mockResolvedValue({ stopped: true, command: stopped });
    const { result } = renderHookWithProviders(() => useStopCommand(THREAD), { queryClient: client });
    await act(async () => { await result.current.mutateAsync('bg-3f9a1c2e7d10'); });
    expect(mockStop).toHaveBeenCalledWith(THREAD, 'bg-3f9a1c2e7d10');
    expect(client.getQueryData(queryKeys.threads.commands(THREAD))).toEqual(answer([stopped]));
  });

  it('reads the listing again when the answer carries no row', async () => {
    const client = keepingClient();
    client.setQueryData(queryKeys.threads.commands(THREAD), answer([RUNNING]));
    mockStop.mockResolvedValue({ stopped: true });
    const { result } = renderHookWithProviders(() => useStopCommand(THREAD), { queryClient: client });
    await act(async () => { await result.current.mutateAsync('bg-3f9a1c2e7d10'); });
    expect(client.getQueryState(queryKeys.threads.commands(THREAD))?.isInvalidated).toBe(true);
  });

  it('refuses without a thread to stop it in', async () => {
    const { result } = renderHookWithProviders(() => useStopCommand(null));
    await expect(result.current.mutateAsync('bg-3f9a1c2e7d10')).rejects.toThrow();
    expect(mockStop).not.toHaveBeenCalled();
  });
});
