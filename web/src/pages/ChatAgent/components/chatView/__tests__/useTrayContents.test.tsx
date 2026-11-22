/**
 * The tasks chip stands for a running task no row names (a workflow's own
 * steps, or a row not loaded yet) even while it keeps settled rows, so the
 * tray still reads as working.
 */
import { describe, it, expect, vi } from 'vitest';
import type { ReactNode } from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { renderHook } from '@testing-library/react';
import type { Wakeup } from '@/types/wakeups';
import type { CommandRun } from '@/types/commands';

vi.mock('@/hooks/useFeatures', () => ({ useFeatureEnabled: () => true }));

import { trayContentsOf, useTrayWork, type WorkChips } from '../useTrayContents';
import { threadWork, trayWorking } from '../threadStatus';

function working(chips: WorkChips): boolean {
  const contents = trayContentsOf({ ...chips, reportBack: null }, null);
  return !!contents && trayWorking(contents, threadWork(chips, false));
}

function readTray(hasActiveSubagents: boolean) {
  const client = new QueryClient();
  const wrapper = ({ children }: { children: ReactNode }) => <QueryClientProvider client={client}>{children}</QueryClientProvider>;
  const { result } = renderHook(() => useTrayWork({
    marketWatch: null,
    commands: [],
    wakeups: null,
    cards: {},
    taskHistory: { 'task:Qm3xLp': { description: 'Pull filings', status: 'completed', messages: [], lastActivityMs: 1_700_000_000_000 } },
    messages: [],
    isLoading: false,
    hasActiveSubagents,
    owedThreads: null,
    isHome: false,
  }), { wrapper });
  return result.current;
}

describe('useTrayWork: a running task no row names', () => {
  it('keeps the settled rows and still reads as working', () => {
    const contents = readTray(true);
    expect(contents?.tasks).toEqual({ rows: [expect.objectContaining({ id: 'task:Qm3xLp', running: false })], unnamedRunning: true });
    expect(working(contents)).toBe(true);
  });

  it('is quiet once nothing runs', () => {
    const contents = readTray(false);
    expect(contents?.tasks?.unnamedRunning).toBe(false);
    expect(working(contents)).toBe(false);
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

function readWakeups(items: Wakeup[]) {
  const client = new QueryClient();
  const wrapper = ({ children }: { children: ReactNode }) => <QueryClientProvider client={client}>{children}</QueryClientProvider>;
  const { result } = renderHook(() => useTrayWork({
    marketWatch: null,
    commands: [],
    wakeups: { items, userTimezone: null },
    cards: {},
    taskHistory: {},
    messages: [],
    isLoading: false,
    hasActiveSubagents: false,
    owedThreads: null,
    isHome: false,
  }), { wrapper });
  return result.current;
}

describe('useTrayWork: wakeups', () => {
  it('keeps the wakeups chip while one is pending, with only the pending ones', () => {
    const pending = wakeup({});
    const contents = readWakeups([pending, wakeup({ wakeup_id: 'b', state: 'fired', cancellable: false })]);
    expect(contents?.wakeups?.items).toEqual([pending]);
    expect(contents?.watch).toBeNull();
  });

  it('counts a fired wakeup the agent has not read yet as pending', () => {
    expect((readWakeups([wakeup({ state: 'held' })]))?.wakeups).not.toBeNull();
  });

  it('drops it once every wakeup has ended', () => {
    expect(readWakeups([wakeup({ state: 'fired' }), wakeup({ wakeup_id: 'b', state: 'cancelled' })]).wakeups).toBeNull();
  });
});

describe('useTrayWork: background commands', () => {
  function readCommands(commands: CommandRun[]) {
    const client = new QueryClient();
    const wrapper = ({ children }: { children: ReactNode }) => <QueryClientProvider client={client}>{children}</QueryClientProvider>;
    const { result } = renderHook(() => useTrayWork({
      marketWatch: null,
      commands,
      wakeups: null,
      cards: {},
      taskHistory: {},
      messages: [],
      isLoading: false,
      hasActiveSubagents: false,
      owedThreads: null,
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
    expect(working(contents)).toBe(true);
  });

  it('draws no chip once every command has ended', () => {
    expect(readCommands([row({ status: 'stopped', ended_at: '2030-01-01T10:20:00Z' })]).commands).toBeNull();
  });
});
