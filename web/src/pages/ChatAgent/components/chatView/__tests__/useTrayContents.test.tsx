/**
 * The tasks chip stands for a running task no row names (a workflow's own
 * steps, or a row not loaded yet) even while it keeps settled rows, so the
 * tray still reads as working.
 */
import { describe, it, expect, vi } from 'vitest';
import type { Mock } from 'vitest';
import type { ReactNode } from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { renderHook, waitFor } from '@testing-library/react';
import type { Wakeup } from '@/types/wakeups';
import type { CommandRun } from '@/types/commands';

vi.mock('@/hooks/useFeatures', () => ({ useFeatureEnabled: () => true }));
vi.mock('../../../utils/api', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  fetchWakeups: vi.fn(),
}));

import { fetchWakeups, type OwedReportKind } from '../../../utils/api';
import { trayWorking, useTrayContents } from '../useTrayContents';

const THREAD = '7d1c2a4e-0b3f-4c55-9a61-2f8e5d9b0c17';

function readTray(hasActiveSubagents: boolean) {
  const client = new QueryClient();
  const wrapper = ({ children }: { children: ReactNode }) => <QueryClientProvider client={client}>{children}</QueryClientProvider>;
  const { result } = renderHook(() => useTrayContents({
    marketWatch: null,
    commands: [],
    threadId: null,
    isActive: true,
    cards: {},
    taskHistory: { 'task:Qm3xLp': { description: 'Pull filings', status: 'completed', messages: [], lastActivityMs: 1_700_000_000_000 } },
    messages: [],
    isLoading: false,
    hasActiveSubagents,
    reportBackOwed: false,
    owedThreads: null,
    owedKinds: null,
    isHome: false,
  }), { wrapper });
  return result.current;
}

describe('useTrayContents: a running task no row names', () => {
  it('keeps the settled rows and still reads as working', () => {
    const contents = readTray(true);
    expect(contents?.tasks).toEqual({ rows: [expect.objectContaining({ id: 'task:Qm3xLp', running: false })], unnamedRunning: true });
    expect(trayWorking(contents!)).toBe(true);
  });

  it('is quiet once nothing runs', () => {
    const contents = readTray(false);
    expect(contents?.tasks?.unnamedRunning).toBe(false);
    expect(trayWorking(contents!)).toBe(false);
  });
});

// Neutral placeholder ticker and a made-up level only.
function wakeup(over: Partial<Wakeup>): Wakeup {
  return {
    wakeup_id: 'w-1', short_id: 'w_3f2a', label: null, kind: 'price', state: 'armed',
    timezone: null, cron: null, at: null, until: null, next_fire_at: null,
    fires_done: 0, fires_total: 1, ends_at: null, expires_at: '2030-01-02T00:00:00Z', fired_at: null,
    price: { symbol: 'NVDA', kind: 'above', value: 205, pct_from: null, armed_price: null, fired_price: null },
    order: null, cancellable: true, legacy: false, created_at: '2030-01-01T00:00:00Z',
    ...over,
  };
}

async function readWakeups(items: Wakeup[]) {
  (fetchWakeups as Mock).mockResolvedValue({ thread_id: THREAD, user_timezone: null, wakeups: items });
  const client = new QueryClient();
  const wrapper = ({ children }: { children: ReactNode }) => <QueryClientProvider client={client}>{children}</QueryClientProvider>;
  const { result } = renderHook(() => useTrayContents({
    marketWatch: null,
    commands: [],
    threadId: THREAD,
    isActive: true,
    cards: {},
    taskHistory: {},
    messages: [],
    isLoading: false,
    hasActiveSubagents: false,
    reportBackOwed: false,
    owedThreads: null,
    owedKinds: null,
    isHome: false,
  }), { wrapper });
  await waitFor(() => expect(fetchWakeups).toHaveBeenCalledWith(THREAD));
  // Settled once the answer has rendered: the read is the only thing in flight.
  await waitFor(() => expect(client.isFetching()).toBe(0));
  return result.current;
}

describe('useTrayContents: wakeups', () => {
  it('keeps the wakeups chip while one is pending, with only the pending ones', async () => {
    const pending = wakeup({});
    const contents = await readWakeups([pending, wakeup({ wakeup_id: 'b', state: 'fired', cancellable: false })]);
    expect(contents?.wakeups?.items).toEqual([pending]);
    expect(contents?.watch).toBeNull();
  });

  it('counts a fired wakeup the agent has not read yet as pending', async () => {
    expect((await readWakeups([wakeup({ state: 'held' })]))?.wakeups).not.toBeNull();
  });

  it('drops it once every wakeup has ended', async () => {
    expect(await readWakeups([wakeup({ state: 'fired' }), wakeup({ wakeup_id: 'b', state: 'cancelled' })])).toBeNull();
  });
});

describe('useTrayContents: background commands', () => {
  function readCommands(commands: CommandRun[]) {
    const client = new QueryClient();
    const wrapper = ({ children }: { children: ReactNode }) => <QueryClientProvider client={client}>{children}</QueryClientProvider>;
    const { result } = renderHook(() => useTrayContents({
      marketWatch: null,
      commands,
      threadId: null,
      isActive: true,
      cards: {},
      taskHistory: {},
      messages: [],
      isLoading: false,
      hasActiveSubagents: false,
      reportBackOwed: false,
      owedThreads: null,
    owedKinds: null,
      isHome: false,
    }), { wrapper });
    return result.current;
  }

  const row = (over: Partial<CommandRun>): CommandRun => ({
    command_id: 'bg-3f9a1c2e7d10', tool: 'Bash', description: 'Run 10-year backtest', status: 'running',
    exit_code: null, failure_reason: null, tool_call_id: 'call_1', log_path: '.agents/threads/x/commands/bg-3f9a1c2e7d10.log',
    started_at: '2030-01-01T10:00:00Z', handed_off_at: '2030-01-01T10:05:00Z', ended_at: null,
    ...over,
  }) as CommandRun;

  it('keeps a chip for the running ones only, and works while one runs', () => {
    const running = row({});
    const contents = readCommands([running, row({ command_id: 'bg-b', status: 'completed', ended_at: '2030-01-01T10:41:00Z' })]);
    expect(contents?.commands).toEqual([running]);
    expect(trayWorking(contents!)).toBe(true);
  });

  it('draws no chip once every command has ended', () => {
    expect(readCommands([row({ status: 'stopped', ended_at: '2030-01-01T10:20:00Z' })])).toBeNull();
  });
});

function readOwed(owedKinds: OwedReportKind[] | null, isHome: boolean) {
  const client = new QueryClient();
  const wrapper = ({ children }: { children: ReactNode }) => <QueryClientProvider client={client}>{children}</QueryClientProvider>;
  const { result } = renderHook(() => useTrayContents({
    marketWatch: null,
    commands: [],
    threadId: null,
    isActive: true,
    cards: {},
    taskHistory: {},
    messages: [],
    isLoading: false,
    hasActiveSubagents: false,
    reportBackOwed: true,
    owedThreads: null,
    owedKinds,
    isHome,
  }), { wrapper });
  return result.current?.reportBackLabel ?? null;
}

describe('useTrayContents: an owed report is named by whose it is', () => {
  it('names nothing for a background command, whose notice needs no wait', () => {
    expect(readOwed(['command'], true)).toBeNull();
    expect(readOwed(['command'], false)).toBeNull();
  });

  it("names a subagent's on Home and an Analyst's anywhere", () => {
    expect(readOwed(['command', 'task'], true)).toBe('Waiting for subagent results to report back');
    expect(readOwed(['analyst', 'task'], true)).toBe('Waiting for the Analyst to report back');
  });

  it('falls back to who usually reports back when the read named no kinds', () => {
    expect(readOwed(null, true)).toBe('Waiting for the Analyst to report back');
    expect(readOwed([], false)).toBe('Waiting for subagent results to report back');
  });
});
