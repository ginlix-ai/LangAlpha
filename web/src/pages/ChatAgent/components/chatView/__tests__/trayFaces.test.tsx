/**
 * The chips' faces as functions of their entry and a density: the words a
 * chip shows, and the full meaning it keeps in its name and title.
 */
import { describe, it, expect } from 'vitest';
import { render, within } from '@testing-library/react';
import i18n from '@/i18n';
import type { PlanStep } from '@/types/chat';
import type { CommandRun } from '@/types/commands';
import type { Wakeup } from '@/types/wakeups';
import type { RosterTask } from '../delegatedWork';
import type { RosterAnalyst } from '../useAnalystRoster';
import { chipFace, countListFace, openStatusFace, statusFace, type ChipFace, type FaceContext } from '../trayFaces';
import type { ThreadQuote } from '../threadQuotesContext';

// Neutral placeholder tickers and made-up prices only.
function quote(price: number, pct: number): ThreadQuote {
  return { price, pct, ext: null, places: 2, currency: 'USD', line: [], live: true };
}

const ctx = (quotes: Record<string, ThreadQuote> = {}): FaceContext => ({
  t: i18n.t.bind(i18n),
  locale: 'en-US',
  now: Date.parse('2030-01-01T12:00:00Z'),
  quotes,
});

function drawn(face: ChipFace): HTMLElement {
  return render(<span className={face.className} title={face.title}>{face.content}</span>).container.firstElementChild as HTMLElement;
}

function shown(face: ChipFace): string {
  return drawn(face).textContent ?? '';
}

function priceWakeup(over: Partial<Wakeup> = {}): Wakeup {
  return {
    wakeup_id: 'w-1', short_id: 'w_3f2a', label: null, kind: 'price', state: 'armed',
    timezone: null, cron: null, at: null, until: null, next_fire_at: null,
    fires_done: 0, fires_total: 1, ends_at: null, expires_at: '2099-01-02T00:00:00Z', fired_at: null,
    price: { symbol: 'AMD', kind: 'above', value: 471.6, pct_from: null, armed_price: null, fired_price: null, last: 400, level: 471.6 },
    order: null, cancellable: true, legacy: false, created_at: '2030-01-01T00:00:00Z',
    ...over,
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

function analyst(id: string, running: boolean): RosterAnalyst {
  return { threadId: id, workspaceId: `w-${id}`, workspaceName: `Desk ${id}`, title: `Thread ${id}`, status: running ? 'running' : 'completed', running };
}

const COMMAND = {
  command_id: 'bg-3f9a1c2e7d10', tool: 'Bash', description: 'Run backtest', status: 'running', exit_code: null,
  failure_reason: null, tool_call_id: 'call_1', log_path: 'x.log', started_at: '2030-01-01T00:00:00Z',
  handed_off_at: '2030-01-01T00:05:00Z', ended_at: null,
} as CommandRun;

describe('countListFace', () => {
  const counted = { count: 3, label: '3 commands', name: '3 commands running', icon: <svg data-icon="" />, working: true };

  it('says its count in words while there is room, and bare with its icon when tight', () => {
    const full = countListFace(counted, 'full', 'en-US');
    expect(shown(full)).toMatch(/3 commands$/);
    expect(render(<span>{full.content}</span>).container.querySelector('[data-icon]')).toBeNull();
    const tight = countListFace(counted, 'tight', 'en-US');
    expect(shown(tight)).toMatch(/3$/);
    expect(render(<span>{tight.content}</span>).container.querySelector('[data-icon]')).not.toBeNull();
    // Named in full at every density.
    expect(tight.name).toBe('3 commands running');
    expect(tight.title).toBe('3 commands running');
  });
});

describe('statusFace', () => {
  it('needs no title while its words show in full', () => {
    expect(statusFace('Waiting for results', 'full').title).toBeUndefined();
    expect(statusFace('Waiting for results', 'tight').title).toBe('Waiting for results');
  });

  it('truncates its words hard when tight, as the owed report draws them', () => {
    const label = 'Waiting for subagent results to report back';
    const face = chipFace('reportBack', label, 'tight', ctx());
    expect(face.kind).toBe('status');
    expect(face.title).toBe(label);
    expect(within(drawn(face)).getByText(label)).toHaveClass('max-w-24');
  });
});

describe('openStatusFace', () => {
  it('counts the chips moved into it, in its name and on its face', () => {
    expect(openStatusFace(0, ctx()).name).toBe('Open Status');
    const more = openStatusFace(2, ctx());
    expect(more.name).toBe('Open Status · 2 more');
    expect(shown(more)).toBe('+2');
  });
});

describe('the wakeups face', () => {
  const list = { items: [priceWakeup()], userTimezone: null };
  const quotes = { AMD: quote(400, -13.25) };

  it('keeps the distance as it gives up the count and the time', () => {
    expect(shown(chipFace('wakeups', list, 'full', ctx(quotes)))).toMatch(/1 wakeup · AMD 17\.9% away$/);
    expect(shown(chipFace('wakeups', list, 'compact', ctx(quotes)))).toMatch(/^AMD 17\.9% away$/);
    expect(shown(chipFace('wakeups', list, 'tight', ctx(quotes)))).toMatch(/^17\.9%$/);
    const compact = chipFace('wakeups', list, 'compact', ctx(quotes));
    expect(compact.name).toBe('1 wakeup · AMD 17.9% away');
    expect(compact.title).toContain('1 wakeup · AMD 17.9% away');
  });

  it('reads the distance at the after-hours print, not the close in the headline', () => {
    const extended: ThreadQuote = { ...quote(100, 1), ext: { label: 'AH', price: 108, change: 8, pct: 8 } };
    const target = { items: [priceWakeup({ price: { ...priceWakeup().price!, level: 110, value: 110 } })], userTimezone: null };
    expect(shown(chipFace('wakeups', target, 'compact', ctx({ AMD: extended })))).toBe('AMD 1.9% away');
  });

  it('counts them when tight with no distance to give', () => {
    const untimed = { items: [priceWakeup({ price: null, kind: 'cron' }), priceWakeup({ wakeup_id: 'w-2', price: null, kind: 'cron' })], userTimezone: null };
    const tight = chipFace('wakeups', untimed, 'tight', ctx());
    expect(shown(tight)).toMatch(/^2$/);
    expect(tight.name).toMatch(/^2 wakeups/);
  });
});

describe('the watch face', () => {
  it('prices each ticker in full and draws only directions when tight', () => {
    const watch = { symbols: ['NVDA', 'AMD'] };
    const quotes = { NVDA: quote(125.53, -14.01), AMD: quote(400, 2.5) };
    expect(shown(chipFace('watch', watch, 'full', ctx(quotes)))).toContain('NVDA$125.53-14.01%·AMD$400.00+2.50%');
    const tight = chipFace('watch', watch, 'tight', ctx(quotes));
    expect(shown(tight)).not.toContain('%');
    expect(tight.title).toContain('NVDA $125.53 -14.01% · AMD $400.00 +2.50%');
    expect(tight.name).toBe('Open live market watch');
  });

  it('keeps the symbol and a place of the move when compact, the full line in its title', () => {
    const quotes = { NVDA: quote(125.53, -14.01), AMD: quote(400, -13.25) };
    const compact = chipFace('watch', { symbols: ['NVDA', 'AMD'] }, 'compact', ctx(quotes));
    const chip = drawn(compact);
    expect(chip).toHaveTextContent('NVDA-14.0%AMD-13.3%');
    expect(chip).not.toHaveTextContent('$');
    expect(compact.title).toContain('NVDA $125.53 -14.01% · AMD $400.00 -13.25%');
    expect(within(chip).getByText('Watching NVDA, AMD')).toHaveClass('sr-only');
  });

  it('draws each move as an arrow when tight, a flat one as a dot, read the same as in full', () => {
    const quotes = { NVDA: quote(125.53, -14.01), AMD: quote(400, 2.5), TSM: quote(180, 0.01) };
    const chip = drawn(chipFace('watch', { symbols: ['NVDA', 'AMD', 'TSM'] }, 'tight', ctx(quotes)));
    expect(chip).toHaveTextContent(/^Watching NVDA, AMD, TSMNVDAAMDTSM·$/);
    expect(chip.querySelectorAll('svg path')).toHaveLength(2);
    expect(within(chip).getByText('Watching NVDA, AMD, TSM')).toHaveClass('sr-only');
  });

  it('marks the first three tickers when tight and counts the rest', () => {
    const watch = { symbols: ['NVDA', 'AMD', 'TSM', 'AVGO', 'QCOM'] };
    expect(shown(chipFace('watch', watch, 'tight', ctx()))).toMatch(/NVDAAMDTSM\+2$/);
  });

  it('names more tickers than it prices, as a live region truncated when short of room', () => {
    const quotes = { NVDA: quote(125.53, -14.01), AMD: quote(400, -13.25) };
    const chip = drawn(chipFace('watch', { symbols: ['NVDA', 'AMD', 'TSLA'] }, 'compact', ctx(quotes)));
    const label = within(chip).getByText('Watching NVDA, AMD, TSLA');
    expect(label).toHaveClass('truncate');
    expect(label).toHaveAttribute('role', 'status');
  });
});

describe('the plan face', () => {
  it('truncates its title shorter when compact', () => {
    expect(within(drawn(chipFace('plan', PLAN, 'compact', ctx()))).getByRole('status')).toHaveClass('max-w-32');
  });

  it('counts the step when tight, the title read but not shown, from the same live region', () => {
    const chip = drawn(chipFace('plan', PLAN, 'tight', ctx()));
    expect(chip).toHaveTextContent('2/3');
    const title = within(chip).getByRole('status');
    expect(title).toHaveTextContent('Model margins by segment');
    expect(title).toHaveClass('sr-only');
    expect(chip).toHaveTextContent(/Plan · step 2 of 3/);
  });
});

describe('the delegated work faces', () => {
  /** What a tight face shows past its spinner (the loader's own text). */
  const tightCount = (face: ChipFace) => {
    const chip = drawn(face);
    expect(chip.querySelector('svg')).not.toBeNull();
    return chip.textContent?.replace(/^.*Loading/, '');
  };

  it('count by icon and number when tight, named in full', () => {
    const tasks = chipFace('tasks', { rows: [task('a', true), task('b', false)], unnamedRunning: false }, 'tight', ctx());
    expect(tightCount(tasks)).toBe('2');
    expect(tasks.name).toBe('2 subagents · 1 running');
    const commands = chipFace('commands', [COMMAND], 'tight', ctx());
    expect(tightCount(commands)).toBe('1');
    expect(commands.name).toBe('1 command running');
    const analysts = chipFace('analysts', [analyst('a', true), analyst('b', false)], 'tight', ctx());
    expect(tightCount(analysts)).toBe('2');
    expect(analysts.name).toBe('2 Analysts · 1 running');
  });

  it('say which control they need: a line, the one thing they name, or a list', () => {
    const tasks = (rows: RosterTask[], unnamedRunning = false) => chipFace('tasks', { rows, unnamedRunning }, 'full', ctx()).kind;
    expect(tasks([], true)).toBe('status');
    expect(tasks([task('a', true)])).toBe('button');
    // A lone row beside a task it cannot name is a list, which spins for both.
    expect(tasks([task('a', true)], true)).toBe('list');
    expect(tasks([task('a', true), task('b', false)])).toBe('list');
    expect(chipFace('analysts', [analyst('a', true)], 'full', ctx()).kind).toBe('button');
    expect(chipFace('analysts', [analyst('a', true), analyst('b', false)], 'full', ctx()).kind).toBe('list');
    expect(chipFace('commands', [COMMAND], 'full', ctx()).kind).toBe('list');
  });
});
