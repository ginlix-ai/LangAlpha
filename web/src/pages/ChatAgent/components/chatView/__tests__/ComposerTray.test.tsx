/**
 * The tray's chips: the market watch as a control with a live label, a list
 * whose rows close it as they open their target, the wakeups with a Cancel on each, and an Analyst's thread
 * opened behind the host's leave guard.
 */
import { describe, it, expect, vi } from 'vitest';
import type { Mock } from 'vitest';
import { act, cleanup, render, screen, fireEvent, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter, Route, Routes, useLocation } from 'react-router';
import { RouteLeaveGuardContext, type RouteLeaveGuard } from '../../../contexts/RouteLeaveGuardContext';
import type { Wakeup } from '@/types/wakeups';

vi.mock('../../../utils/api', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  cancelWakeups: vi.fn().mockResolvedValue({ results: [], wakeups: [] }),
  stopCommand: vi.fn().mockResolvedValue({ stopped: true, command: null }),
}));

import { cancelWakeups, stopCommand } from '../../../utils/api';
import type { CommandRun } from '@/types/commands';
import type { RosterTask } from '../delegatedWork';
import { ComposerTray } from '../ComposerTray';
import { ThreadCommandsContext } from '../../commands/threadCommandsContext';
import { trayWorking, type TrayContents } from '../useTrayContents';

const EMPTY: TrayContents = { watch: null, wakeups: null, commands: null, tasks: null, analysts: null, reportBackLabel: null };
const THREAD = '7d1c2a4e-0b3f-4c55-9a61-2f8e5d9b0c17';

function Where() {
  const location = useLocation();
  return <output data-testid="where">{location.pathname}</output>;
}

function renderTray(
  contents: Partial<TrayContents>,
  { onOpenTask = vi.fn(), onOpenWatch = vi.fn(), onOpenCommand = vi.fn(), guard = (go) => go() }: {
    onOpenTask?: (agentId: string) => void;
    onOpenWatch?: () => void;
    onOpenCommand?: (toolCallId: string) => void;
    guard?: RouteLeaveGuard;
  } = {},
) {
  return render(
    <QueryClientProvider client={new QueryClient()}>
      <MemoryRouter initialEntries={['/chat/t/here']}>
        <RouteLeaveGuardContext value={guard}>
          <ThreadCommandsContext value={THREAD}>
          <Routes>
            <Route
              path="*"
              element={(
                <>
                  <ComposerTray
                    contents={{ ...EMPTY, ...contents }}
                    phase="open"
                    trayRef={{ current: null }}
                    dockRef={{ current: null }}
                    from={{ threadId: 'here', workspaceId: 'home' }}
                    onOpenTask={onOpenTask}
                    onOpenWatch={onOpenWatch}
                    onOpenCommand={onOpenCommand}
                    threadId={THREAD}
                  />
                  <Where />
                </>
              )}
            />
          </Routes>
          </ThreadCommandsContext>
        </RouteLeaveGuardContext>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

// Neutral placeholder tickers and made-up levels only.
function wakeup(over: Partial<Wakeup>): Wakeup {
  return {
    wakeup_id: 'w-1', short_id: 'w_3f2a', label: null, kind: 'price', state: 'armed',
    timezone: null, cron: null, at: null, until: null, next_fire_at: null,
    fires_done: 0, fires_total: 1, ends_at: null, expires_at: '2099-01-02T00:00:00Z', fired_at: null,
    price: { symbol: 'NVDA', kind: 'above', value: 205, pct_from: null, armed_price: null, fired_price: null },
    order: null, cancellable: true, legacy: false, created_at: '2030-01-01T00:00:00Z',
    ...over,
  };
}

function task(id: string, description: string, running = true): RosterTask {
  return { id, name: `Task-${id}`, description, isMainAgent: false, status: running ? 'active' : 'completed', running };
}

function listed(...rows: RosterTask[]): TrayContents['tasks'] {
  return { rows, unnamedRunning: false };
}

describe('ComposerTray', () => {
  it('shows the watched tickers on a button that opens the watch', () => {
    const onOpenWatch = vi.fn();
    renderTray({ watch: { symbols: ['NVDA', 'TSLA'] } }, { onOpenWatch });
    const chip = screen.getByRole('button', { name: 'Open live market watch' });
    expect(chip).toHaveAttribute('title', 'Live prices for the tickers the agent is watching');
    fireEvent.click(chip);
    expect(onOpenWatch).toHaveBeenCalledTimes(1);
  });

  it('announces the watch from its label, not the button', () => {
    renderTray({ watch: { symbols: ['AAPL'] } });
    const label = screen.getByText('Watching AAPL');
    expect(label).toHaveAttribute('role', 'status');
    expect(label).toHaveAttribute('aria-live', 'polite');
    expect(label.closest('button')).not.toHaveAttribute('role');
  });

  it('counts pending wakeups with no next time when none is timed', () => {
    renderTray({ wakeups: { items: [wakeup({}), wakeup({ wakeup_id: 'w-2' })], userTimezone: null } });
    expect(screen.getByRole('button', { name: /2 wakeups/ })).toHaveTextContent('2 wakeups');
  });

  it('names the next time when a pending wakeup is timed', () => {
    renderTray({
      wakeups: {
        items: [wakeup({ kind: 'at', price: null, at: '2099-01-02T14:30:00Z', next_fire_at: '2099-01-02T14:30:00Z' })],
        userTimezone: 'UTC',
      },
    });
    expect(screen.getByRole('button', { name: /1 wakeup · / })).toBeInTheDocument();
  });

  it('cancels one wakeup from the chip, or all of them', async () => {
    renderTray({ wakeups: { items: [wakeup({}), wakeup({ wakeup_id: 'w-2', label: 'Entry' })], userTimezone: null } });
    fireEvent.click(screen.getByRole('button', { name: /2 wakeups/ }));
    fireEvent.click(screen.getByRole('button', { name: 'Cancel the wakeup Entry' }));
    await waitFor(() => expect(cancelWakeups as Mock).toHaveBeenCalledWith(THREAD, ['w-2']));
    fireEvent.click(screen.getByRole('button', { name: 'Cancel all' }));
    await waitFor(() => expect(cancelWakeups as Mock).toHaveBeenLastCalledWith(THREAD, 'all'));
  });

  it('names when the watch last updated', () => {
    renderTray({ watch: { symbols: ['NVDA'], timestamp: 1_700_000_000 } });
    expect(screen.getByRole('button', { name: 'Open live market watch' }).getAttribute('title')).toMatch(/updated/);
  });

  it('closes the task list as a row opens its task', () => {
    const onOpenTask = vi.fn();
    renderTray({ tasks: listed(task('a', 'Pull filings'), task('b', '', false)) }, { onOpenTask });
    fireEvent.click(screen.getByRole('button', { name: /2 tasks · 1 running/ }));
    // A row is named by its task's words, and by its name without them.
    fireEvent.click(screen.getByRole('button', { name: /Pull filings/ }));
    expect(onOpenTask).toHaveBeenCalledWith('a');
    expect(screen.queryByRole('button', { name: /Task-b/ })).toBeNull();
  });

  it('spins for a task only while it runs, whatever its status reads', () => {
    const spinner = (name: string) => screen.getByRole('button', { name: new RegExp(name) }).querySelector('[role="status"]');
    renderTray({ tasks: listed(task('a', 'Pull filings')) });
    expect(spinner('Pull filings')).not.toBeNull();
    cleanup();
    // Its run ended unseen: the status still reads live, but nothing runs.
    renderTray({ tasks: listed({ ...task('b', 'Size the market', false), status: 'active' }) });
    expect(spinner('Size the market')).toBeNull();
  });

  it('spins for a task no row names beside the rows it keeps', () => {
    const contents = { ...EMPTY, tasks: { rows: [task('b', 'Size the market', false)], unnamedRunning: true } };
    expect(trayWorking(contents)).toBe(true);
    renderTray(contents);
    const chip = screen.getByRole('button', { name: /1 task/ });
    expect(chip.querySelector('[role="status"]')).not.toBeNull();
    fireEvent.click(chip);
    expect(screen.getByRole('button', { name: /Size the market/ })).toBeInTheDocument();
  });

  it("names each task's status for a screen reader", () => {
    renderTray({ tasks: listed(task('a', 'Pull filings'), { ...task('b', 'Size the market', false), status: 'error' }) });
    fireEvent.click(screen.getByRole('button', { name: /2 tasks · 1 running/ }));
    expect(screen.getByRole('button', { name: /Working.*Pull filings/ })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Failed.*Size the market/ })).toBeInTheDocument();
  });

  it('draws no status for an Analyst nothing has reported on yet', () => {
    const analyst = { threadId: 'a1', workspaceId: 'w1', workspaceName: 'Semis', title: 'Chip demand', running: false };
    renderTray({ analysts: [{ ...analyst, status: null }, { ...analyst, threadId: 'a2', workspaceName: 'Banks', status: 'completed' }] });
    fireEvent.click(screen.getByRole('button', { name: /2 Analysts/ }));
    const row = screen.getAllByRole('button', { name: /Chip demand/ })[0];
    expect(row.querySelector('[role="status"]')).toBeNull();
    expect(row).not.toHaveTextContent(/Starting|·/);
  });

  it("asks the leave guard before opening an Analyst's thread", () => {
    const guard = vi.fn<RouteLeaveGuard>();
    renderTray(
      { analysts: [{ threadId: 'analyst-1', workspaceId: 'ws-1', workspaceName: 'Semis', title: 'Chip demand', status: 'running', running: true }] },
      { guard },
    );
    fireEvent.click(screen.getByRole('button', { name: /Semis/ }));
    expect(guard).toHaveBeenCalledTimes(1);
    expect(screen.getByTestId('where')).toHaveTextContent('/chat/t/here');
    act(() => guard.mock.calls[0][0]());
    expect(screen.getByTestId('where')).toHaveTextContent('/chat/t/analyst-1');
  });
});

function command(over: Partial<CommandRun>): CommandRun {
  return {
    command_id: 'bg-3f9a1c2e7d10', tool: 'Bash', description: 'Run 10-year backtest', status: 'running',
    exit_code: null, failure_reason: null, tool_call_id: 'call_backtest', log_path: '.agents/threads/7d1c2a4e/commands/bg-3f9a1c2e7d10.log',
    // Started 17 minutes before the test reads it.
    started_at: new Date(Date.now() - 17 * 60_000 - 5_000).toISOString(), handed_off_at: new Date(Date.now() - 12 * 60_000).toISOString(), ended_at: null,
    ...over,
  } as CommandRun;
}

describe('ComposerTray: background commands', () => {
  it('counts the running commands and spins, beside the tasks', () => {
    renderTray({ commands: [command({})], tasks: listed(task('a', 'Pull filings')) });
    const chip = screen.getByRole('button', { name: '1 command running' });
    expect(chip.querySelector('[role="status"]')).not.toBeNull();
    expect(screen.getByRole('button', { name: /Pull filings/ })).toBeInTheDocument();
    expect(trayWorking({ ...EMPTY, commands: [command({})] })).toBe(true);
  });

  it('lists each by its description and how long it has run, never its id, and says the Agent hears of it', () => {
    renderTray({ commands: [command({}), command({ command_id: 'bg-a41c0e9b2f37', description: null, tool: 'ExecuteCode', tool_call_id: 'call_pull' })] });
    fireEvent.click(screen.getByRole('button', { name: '2 commands running' }));
    const list = screen.getByRole('dialog');
    expect(list).toHaveTextContent('Run 10-year backtest');
    expect(list).toHaveTextContent('17m');
    // A command the agent gave no description is named by its tool.
    expect(list).toHaveTextContent('Execute Code');
    expect(list).not.toHaveTextContent('bg-');
    expect(list).toHaveTextContent('The Agent picks each one up when it finishes.');
  });

  it('says the Agent picks a lone command up, in the singular', () => {
    renderTray({ commands: [command({})] });
    fireEvent.click(screen.getByRole('button', { name: '1 command running' }));
    expect(screen.getByRole('dialog')).toHaveTextContent('The Agent picks this up when it finishes.');
  });

  it('stops a command from its row', async () => {
    renderTray({ commands: [command({})] });
    fireEvent.click(screen.getByRole('button', { name: '1 command running' }));
    fireEvent.click(screen.getByRole('button', { name: 'Stop Run 10-year backtest' }));
    await waitFor(() => expect(stopCommand as Mock).toHaveBeenCalledWith(THREAD, 'bg-3f9a1c2e7d10'));
  });

  it('says a stop that failed, and keeps the Stop for another try', async () => {
    (stopCommand as Mock).mockRejectedValueOnce(new Error('Request failed with status code 503'));
    renderTray({ commands: [command({})] });
    fireEvent.click(screen.getByRole('button', { name: '1 command running' }));
    fireEvent.click(screen.getByRole('button', { name: 'Stop Run 10-year backtest' }));
    expect(await screen.findByRole('alert')).toHaveTextContent("Couldn't stop it. Try again.");
    expect(screen.getByRole('button', { name: 'Stop Run 10-year backtest' })).toBeEnabled();
  });

  it('opens the tool card a row started from, closing the list', () => {
    const onOpenCommand = vi.fn();
    renderTray({ commands: [command({})] }, { onOpenCommand });
    fireEvent.click(screen.getByRole('button', { name: '1 command running' }));
    fireEvent.click(screen.getByRole('button', { name: /^Run 10-year backtest/ }));
    expect(onOpenCommand).toHaveBeenCalledWith('call_backtest');
    expect(screen.queryByRole('dialog')).toBeNull();
  });
});
