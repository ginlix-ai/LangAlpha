/**
 * The tray's row at a density, with the layout forced (jsdom has no layout to
 * measure): each chip mounted as the control its face asks for and named in
 * full, chips moved into the Status button counted there, and a probe that
 * mounts no control. What each face says is `trayFaces.test`'s.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter } from 'react-router';
import type { Wakeup } from '@/types/wakeups';
import type { PlanStep } from '@/types/chat';
import type { CommandRun } from '@/types/commands';
import type { ThreadQuote } from '../threadQuotesContext';
import type { TrayLayout } from '../trayDensity';
import type { TrayFoldPhase } from '../useTrayFold';
import type { TrayContents } from '../useTrayContents';
import type { RosterTask } from '../delegatedWork';

const forced = vi.hoisted(() => ({
  layout: { density: 'full', moved: [] } as TrayLayout,
  /** What each render asked of the density hook. */
  asked: [] as { shown: boolean; hold: boolean }[],
}));

vi.mock('../useTrayDensity', () => ({
  useTrayDensity: (_row: unknown, _probe: unknown, opts: { shown: boolean; hold: boolean }) => {
    forced.asked.push(opts);
    return forced.layout;
  },
}));

import { ComposerTray, ComposerTrayStrip } from '../ComposerTray';
import { FixedThreadQuotes } from '../threadQuotes';
import { ThreadStatusContext, threadWork, type ThreadStatus } from '../threadStatus';

const EMPTY: TrayContents = { plan: null, watch: null, wakeups: null, commands: null, tasks: null, analysts: null, reportBack: null };

function threadStatus(contents: Partial<TrayContents> | null): ThreadStatus {
  const full = contents && { ...EMPTY, ...contents };
  return {
    contents: full,
    work: threadWork(full, false),
    latestPlan: null,
    allWakeups: null,
    focus: null,
    from: { threadId: 'here', workspaceId: 'home' },
    threadId: 'thread-1',
    onOpenTask: vi.fn(),
    onOpenCommand: vi.fn(),
    onJumpToLatest: vi.fn(),
    onOpenStatus: vi.fn(),
  };
}

function tray(status: ThreadStatus, { strip = false, phase = 'open' }: { strip?: boolean; phase?: TrayFoldPhase } = {}) {
  return (
    <QueryClientProvider client={new QueryClient()}>
      <MemoryRouter>
        <FixedThreadQuotes quotes={{ bySymbol: QUOTES, phase: 'open' }}>
          <ThreadStatusContext value={status}>
            {strip
              ? <ComposerTrayStrip />
              : <ComposerTray phase={phase} trayRef={{ current: null }} dockRef={{ current: null }} onSettled={vi.fn()} />}
          </ThreadStatusContext>
        </FixedThreadQuotes>
      </MemoryRouter>
    </QueryClientProvider>
  );
}

function renderTray(contents: Partial<TrayContents>, layout: TrayLayout, opts: { strip?: boolean; phase?: TrayFoldPhase } = {}) {
  forced.layout = layout;
  return render(tray(threadStatus(contents), opts));
}

// Neutral placeholder tickers and made-up prices only.
function quote(price: number, pct: number): ThreadQuote {
  return { price, pct, ext: null, places: 2, currency: 'USD', line: [], live: true };
}

const QUOTES = { NVDA: quote(125.53, -14.01), AMD: quote(400, -13.25) };

function priceWakeup(): Wakeup {
  return {
    wakeup_id: 'w-1', short_id: 'w_3f2a', label: null, kind: 'price', state: 'armed',
    timezone: null, cron: null, at: null, until: null, next_fire_at: null,
    fires_done: 0, fires_total: 1, ends_at: null, expires_at: '2099-01-02T00:00:00Z', fired_at: null,
    price: { symbol: 'AMD', kind: 'above', value: 471.6, pct_from: null, armed_price: null, fired_price: null, last: 400, level: 471.6 },
    order: null, cancellable: true, legacy: false, created_at: '2030-01-01T00:00:00Z',
  };
}

const PLAN: PlanStep[] = [
  { title: 'Pull segment data', status: 'completed' },
  { title: 'Model margins by segment', status: 'in_progress' },
  { title: 'Chart the comparison', status: 'pending' },
];

function task(id: string, running: boolean): RosterTask {
  return { id, name: `Task-${id}`, description: `Work ${id}`, isMainAgent: false, status: running ? 'active' : 'completed', running };
}

const COMMAND = {
  command_id: 'bg-3f9a1c2e7d10', tool: 'Bash', description: 'Run backtest', status: 'running', exit_code: null,
  failure_reason: null, tool_call_id: 'call_1', log_path: 'x.log', started_at: '2030-01-01T00:00:00Z',
  handed_off_at: '2030-01-01T00:05:00Z', ended_at: null,
} as CommandRun;

const TIGHT: TrayLayout = { density: 'tight', moved: [] };
const REPORT = 'Waiting for subagent results to report back';

const watchChip = () => screen.getByRole('button', { name: 'Open live market watch' });

beforeEach(() => {
  forced.asked = [];
});

describe('ComposerTray densities: the controls', () => {
  it('names each control in full when tight', () => {
    renderTray({
      plan: PLAN,
      watch: { symbols: ['NVDA', 'AMD'] },
      wakeups: { items: [priceWakeup()], userTimezone: null },
      tasks: { rows: [task('a', true), task('b', false)], unnamedRunning: false },
      commands: [COMMAND],
      analysts: [
        { threadId: 'a1', workspaceId: 'w1', workspaceName: 'Semis', title: 'Chip demand', status: 'running', running: true },
        { threadId: 'a2', workspaceId: 'w2', workspaceName: 'Banks', title: 'Loan books', status: 'completed', running: false },
      ],
    }, TIGHT);
    expect(screen.getByRole('button', { name: /Plan · step 2 of 3/ })).toBeInTheDocument();
    expect(watchChip()).toBeInTheDocument();
    expect(screen.getByRole('button', { name: '1 wakeup · AMD 17.9% away' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: '2 subagents · 1 running' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: '1 command running' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: '2 Analysts · 1 running' })).toBeInTheDocument();
  });

  it('mounts a chip whose face only says what runs as a live region, no control', () => {
    renderTray({ reportBack: REPORT, tasks: { rows: [], unnamedRunning: true } }, TIGHT);
    const lines = screen.getAllByRole('status').filter((el) => el.getAttribute('aria-live') === 'polite' && el.hasAttribute('title'));
    expect(lines.map((el) => el.getAttribute('title'))).toEqual([expect.stringMatching(/running/i), REPORT]);
    // The Status button is the row's only control.
    expect(screen.getAllByRole('button')).toHaveLength(1);
  });
});

describe('ComposerTray densities: chips moved into Status', () => {
  const contents = {
    plan: PLAN,
    watch: { symbols: ['NVDA'] },
    wakeups: { items: [priceWakeup()], userTimezone: null },
    tasks: { rows: [task('a', true), task('b', false)], unnamedRunning: false },
    reportBack: REPORT,
  };

  for (const strip of [false, true]) {
    it(`leaves the row and is counted on the Status button${strip ? ', in the strip' : ''}`, () => {
      renderTray(contents, { density: 'tight', moved: ['reportBack', 'wakeups'] }, { strip });
      expect(screen.queryByRole('button', { name: /wakeup/ })).toBeNull();
      expect(screen.queryByText(REPORT, { ignore: '[aria-hidden] *' })).toBeNull();
      const open = screen.getByRole('button', { name: 'Open Status · 2 more' });
      expect(open).toHaveTextContent('+2');
      // What stays in the row.
      expect(screen.getByRole('button', { name: /Model margins by segment/ })).toBeInTheDocument();
      expect(watchChip()).toBeInTheDocument();
      expect(screen.getByRole('button', { name: /2 subagents/ })).toBeInTheDocument();
    });
  }

  it('names the Status button plainly with nothing moved', () => {
    renderTray(contents, TIGHT);
    expect(screen.getByRole('button', { name: 'Open Status' })).not.toHaveTextContent('+');
  });
});

describe('ComposerTray densities: the probe', () => {
  it('measures each chip at every density as its face alone, with no control mounted', () => {
    const { container } = renderTray({
      plan: PLAN,
      watch: { symbols: ['NVDA'] },
      wakeups: { items: [priceWakeup()], userTimezone: null },
      tasks: { rows: [task('a', true), task('b', false)], unnamedRunning: false },
    }, { density: 'full', moved: [] });
    const levels = [...container.querySelectorAll('[data-level]')];
    expect(levels.map((level) => level.getAttribute('data-level'))).toEqual(['full', 'compact', 'tight', 'more']);
    for (const level of levels.slice(0, 3)) {
      expect([...level.querySelectorAll('[data-chip]')].map((chip) => chip.getAttribute('data-chip')))
        .toEqual(['plan', 'watch', 'wakeups', 'tasks', 'status']);
    }
    expect(container.querySelectorAll('[data-level] button, [data-level] [aria-haspopup]')).toHaveLength(0);
    // The faces carry the row's classes, so they measure as it draws them.
    const plan = screen.getByRole('button', { name: /Model margins by segment/ });
    expect(container.querySelector('[data-level="full"] [data-chip="plan"] > span')).toHaveClass(...plan.classList);
  });
});

describe('ComposerTray densities: what the layout follows', () => {
  it('holds the layout while the liquid runs either way, and only then', () => {
    const contents = { plan: PLAN };
    const held = (phase: TrayFoldPhase) => {
      forced.asked = [];
      const { unmount } = renderTray(contents, TIGHT, { phase });
      unmount();
      return forced.asked.at(-1)?.hold;
    };
    expect(held('open')).toBe(false);
    expect(held('folding')).toBe(true);
    expect(held('unfolding')).toBe(true);
  });

  it('shows the strip, and measures it, only once the thread has something to show', () => {
    forced.layout = TIGHT;
    const { rerender, container } = render(tray(threadStatus(null), { strip: true }));
    expect(container.querySelector('[data-strip]')).toBeNull();
    expect(forced.asked.at(-1)?.shown).toBe(false);
    rerender(tray(threadStatus({ plan: PLAN }), { strip: true }));
    expect(container.querySelector('[data-strip]')).not.toBeNull();
    expect(forced.asked.at(-1)?.shown).toBe(true);
  });
});
