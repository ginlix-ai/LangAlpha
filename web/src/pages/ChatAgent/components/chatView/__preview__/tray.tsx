/* eslint-disable react-refresh/only-export-components -- entry file, not a module */
import React, { StrictMode, useEffect, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { useTranslation } from 'react-i18next';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter } from 'react-router';
import i18n, { initI18n } from '@/i18n';
import { switchLocale } from '@/lib/locale';
import { ThemeProvider, useTheme } from '@/contexts/ThemeContext';
import '@/index.css';
import type { CommandRun } from '@/types/commands';
import type { PlanStep } from '@/types/chat';
import type { Wakeup } from '@/types/wakeups';
import { ComposerTray, ComposerTrayStrip } from '../ComposerTray';
import { ThreadStatusContext, threadWork, type ThreadStatus } from '../threadStatus';
import { FixedThreadQuotes } from '../threadQuotes';
import type { ThreadQuote } from '../threadQuotesContext';
import type { TrayContents } from '../useTrayContents';
import type { RosterTask } from '../delegatedWork';
import type { RosterAnalyst } from '../useAnalystRoster';
import { ThreadCommandsContext } from '../../commands/threadCommandsContext';

/**
 * Dev-only stress harness for the composer tray's one-row layout: the REAL
 * ComposerTray and ComposerTrayStrip over fixture contents and fixed quotes,
 * in a column of a given width. Open /tray-preview.html on the dev server.
 * Not bundled in production (vite builds index.html only).
 *
 * Knobs are query parameters, so a screenshot is one URL:
 *   ?width=560&scenario=all-long&locale=zh-CN&tick=1&strip=1&zoom=125&theme=dark
 * `width` is the composer column (768 in the app, the tray's row 32px less);
 * `auto` fills the page. With no `scenario` every one is stacked, labeled.
 * `strip=1` draws the inside variant, `strip=both` both.
 *
 * `window.__trayPreview` drives a sweep without reloads: `setWidth(n)`,
 * `setTick(on)`, `measure()` (rows, overflow and layout per scenario) and
 * `changes()` (layout switches per scenario since `resetChanges()`).
 */

const params = new URLSearchParams(window.location.search);
const widthParam = params.get('width');
const knobs = {
  theme: params.get('theme') === 'dark' ? 'dark' : 'light',
  locale: params.get('locale') === 'zh-CN' ? 'zh-CN' : 'en-US',
  scenario: params.get('scenario'),
  tick: params.get('tick') === '1',
  strip: params.get('strip') === 'both' ? 'both' : params.get('strip') === '1' ? 'strip' : 'tray',
  width: widthParam === 'auto' ? null : Number(widthParam) || 768,
  zoom: Number(params.get('zoom')) || 100,
} as const;

if (knobs.zoom !== 100) document.documentElement.style.fontSize = `${knobs.zoom}%`;

// No request leaves the page: every fixture is here.
window.fetch = async (input) => {
  const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
  console.warn('[tray-preview] blocked fetch', url);
  return new Response(JSON.stringify({ detail: 'preview' }), { status: 503, headers: { 'content-type': 'application/json' } });
};

const THREAD = '3f9a1c2e-7d10-4b2a-9c55-0e1f2a3b4c5d';
const T = Date.now();
const M = 60_000;
const iso = (ms: number) => new Date(ms).toISOString();

// ---- Fixtures. Placeholder tickers and made-up prices only. ----

function quote(price: number | null, pct: number | null, places = 2, currency: string | null = 'USD'): ThreadQuote {
  return { price, pct, ext: null, places, currency, line: [], live: true };
}

const QUOTES: Record<string, ThreadQuote> = {
  NVDA: quote(125.53, -14.01),
  AMD: quote(400, -13.25),
  'BRK.A': quote(712345.67, 1.23),
  '^GSPC': quote(6123.45, -0.42, 2, null),
  'DOGE-USD': quote(0.123456, 12.345, 6),
  AAPL: quote(231.2, 0.4),
  MSFT: quote(428.9, -1.1),
  TSLA: quote(251.05, 3.2),
  META: quote(590.12, -2.05),
  GOOGL: quote(171.33, 0.9),
  AMZN: quote(198.7, -0.3),
};

/** Each tick straddles a digit boundary, so the chips gain and lose a digit
 *  (and a group separator) every time: the worst case for a row at its edge. */
function tickedQuotes(i: number): Record<string, ThreadQuote> {
  return Object.fromEntries(Object.entries(QUOTES).map(([symbol, q]) => {
    if (q.price == null) return [symbol, q];
    const boundary = 10 ** Math.ceil(Math.log10(q.price));
    const step = 10 ** -q.places;
    const up = i % 2 === 1;
    const price = up ? boundary + step : boundary - step;
    const pct = q.pct == null ? null : Math.sign(q.pct || 1) * (up ? 10.01 : 9.99);
    return [symbol, { ...q, price, pct }];
  }));
}

function plan(titles: string[], at: number): PlanStep[] {
  return titles.map((title, i) => ({
    title,
    status: i < at ? 'completed' : i === at ? 'in_progress' : 'pending',
  }));
}

const LONG_EN = 'Reconcile every segment’s reported revenue against the 10-K footnotes and the earnings-call transcript before building the margin bridge';
const LONG_CJK = '对照十年期年报附注与业绩电话会纪要，逐项核对各业务分部收入并重建利润率桥接模型';
const PLAN_LONG_EN = plan(['Pull segment data', LONG_EN, 'Chart the comparison', 'Write the memo'], 1);
const PLAN_LONG_CJK = plan(['拉取分部数据', LONG_CJK, '绘制对比图', '撰写备忘录'], 1);
const PLAN_12 = plan(Array.from({ length: 12 }, (_, i) => `Step ${i + 1}: model the next segment`), 6);

function wakeup(over: Partial<Wakeup> & Pick<Wakeup, 'wakeup_id' | 'kind'>): Wakeup {
  return {
    short_id: `w_${over.wakeup_id.slice(-4)}`, label: null, state: 'armed',
    timezone: null, cron: null, at: null, until: null, next_fire_at: null,
    fires_done: 0, fires_total: 1, ends_at: null, expires_at: iso(T + 30 * 24 * 60 * M), fired_at: null,
    price: null, order: null, cancellable: true, legacy: false, created_at: iso(T - 60 * M),
    ...over,
  };
}

const W_AT = wakeup({ wakeup_id: 'wk-at-0001', kind: 'at', at: iso(T + 3 * 60 * M), next_fire_at: iso(T + 3 * 60 * M) });
const W_CRON = wakeup({ wakeup_id: 'wk-cron-0002', kind: 'cron', cron: '30 9 * * 1-5', timezone: 'America/New_York', next_fire_at: iso(T + 20 * 60 * M), fires_total: 0 });
const W_PRICE = wakeup({
  wakeup_id: 'wk-price-0003', kind: 'price',
  price: { symbol: 'BRK.A', kind: 'above', value: 750000, pct_from: null, armed_price: 700000, fired_price: null, last: 712345.67, level: 750000 },
});
const W_PRICE_NVDA = wakeup({
  wakeup_id: 'wk-price-0004', kind: 'price',
  price: { symbol: 'NVDA', kind: 'above', value: 110, pct_from: null, armed_price: 95, fired_price: null, last: 100, level: 110 },
});
const W_REACHED = wakeup({
  wakeup_id: 'wk-price-0005', kind: 'price',
  price: { symbol: 'AMD', kind: 'below', value: 410, pct_from: null, armed_price: 450, fired_price: null, last: 400, level: 410 },
});
const W_ORDER = wakeup({
  wakeup_id: 'wk-order-0006', kind: 'order',
  order: { vendor_order_id: 'ord-1', summary: 'Buy 10 NVDA limit 120.00' },
});
const wakeups = (items: Wakeup[]) => ({ items, userTimezone: 'America/New_York' });

function command(id: string, description: string, startedMinAgo = 17): CommandRun {
  return {
    command_id: `bg-${id}`, tool: 'Bash', description, status: 'running', exit_code: null, failure_reason: null,
    tool_call_id: `call_${id}`, log_path: `.agents/threads/3f9a1c2e/commands/bg-${id}.log`,
    started_at: iso(T - startedMinAgo * M), handed_off_at: iso(T - (startedMinAgo - 5) * M), ended_at: null,
  };
}
const CMD_LONG = command('3f9a1c2e7d10', 'Rebuild the factor cache for every ticker in the universe since 2015 and rerun the backtest');
const CMDS = [CMD_LONG, command('a41c0e9b2f37', 'Run 10-year backtest', 9), command('77c0e41a9b2e', 'Pull filings for 500 tickers', 6), command('0b9d3c6e1a58', 'Train the regime model', 4), command('e2f4a6b8c0d1', 'Serve the dashboard', 2)];

function task(id: string, description: string, running: boolean, status: RosterTask['status'] = running ? 'active' : 'completed'): RosterTask {
  return { id: `task:${id}`, name: `Task-${id}`, description, isMainAgent: false, status, running };
}
const TASK_LONG = task('k7Xm2p', 'Compare the momentum literature since 1993 across every published replication and summarize the effect sizes by decade', true);
const TASKS = [TASK_LONG, task('Qm3xLp', 'Summarize the Fed minutes', true), task('r7Kp2q', 'Extract NVDA segment data', true), task('z1Yt9w', 'Draft the chart captions', false)];
const TASKS_ERROR = [task('Qm3xLp', 'Summarize the Fed minutes', false, 'error'), task('r7Kp2q', 'Extract NVDA segment data', true)];

function analyst(id: string, workspaceName: string | null, title: string | null, running: boolean, status: RosterAnalyst['status'] = running ? 'running' : 'completed'): RosterAnalyst {
  return { threadId: `th-${id}`, workspaceId: `ws-${id}`, workspaceName, title, status, running, startedAt: T - 12 * M };
}
const AN_LONG = analyst('a1', 'Semiconductor supply chain deep dive (Asia, 2024 to 2030)', 'Size the AI capex cycle', true);
const ANALYSTS = [AN_LONG, analyst('a2', 'Rates', 'Fed path', true), analyst('a3', 'Energy', 'OPEC+ cuts', false)];
const AN_NEEDS = analyst('a4', 'Semiconductor supply chain deep dive (Asia, 2024 to 2030)', 'Size the AI capex cycle', false, 'needs_input');

const EMPTY: TrayContents = { plan: null, watch: null, wakeups: null, commands: null, tasks: null, analysts: null, reportBack: null };

interface Scenario {
  id: string;
  group: string;
  label: string;
  contents: Partial<TrayContents>;
  /** A locale key for the report-back chip, translated at render. */
  reportBackKey?: string;
  awaitingAnswer?: boolean;
}

const RB_KEY = 'chat.taskReportBackPending';

const SCENARIOS: Scenario[] = [
  { id: 'plan-long-en', group: 'plan', label: 'Plan, very long English title', contents: { plan: PLAN_LONG_EN } },
  { id: 'plan-long-cjk', group: 'plan', label: 'Plan, long CJK title', contents: { plan: PLAN_LONG_CJK } },
  { id: 'plan-12', group: 'plan', label: 'Plan, 12 steps', contents: { plan: PLAN_12 } },
  { id: 'watch-1', group: 'watch', label: 'Watch, 1 ticker', contents: { watch: { symbols: ['NVDA'] } } },
  { id: 'watch-2-big', group: 'watch', label: 'Watch, 2 tickers: BRK.A and an index', contents: { watch: { symbols: ['BRK.A', '^GSPC'] } } },
  { id: 'watch-2-crypto', group: 'watch', label: 'Watch, 2 tickers: BRK.A and a 6-place crypto', contents: { watch: { symbols: ['BRK.A', 'DOGE-USD'] } } },
  { id: 'watch-3', group: 'watch', label: 'Watch, 3 tickers (named, not priced)', contents: { watch: { symbols: ['BRK.A', '^GSPC', 'DOGE-USD'] } } },
  { id: 'watch-8', group: 'watch', label: 'Watch, 8 tickers', contents: { watch: { symbols: ['NVDA', 'AMD', 'AAPL', 'MSFT', 'TSLA', 'META', 'GOOGL', 'AMZN'] } } },
  { id: 'watch-noquote', group: 'watch', label: 'Watch, a ticker with no quote yet', contents: { watch: { symbols: ['NVDA', 'NEWCO'] } } },
  { id: 'wk-time', group: 'wakeups', label: 'Wakeups, one time-based', contents: { wakeups: wakeups([W_AT]) } },
  { id: 'wk-mixed', group: 'wakeups', label: 'Wakeups, many mixed', contents: { wakeups: wakeups([W_AT, W_CRON, W_PRICE, W_PRICE_NVDA, W_REACHED, W_ORDER]) } },
  { id: 'wk-price', group: 'wakeups', label: 'Wakeups, a price wakeup with a distance', contents: { wakeups: wakeups([W_PRICE]) } },
  { id: 'wk-reached', group: 'wakeups', label: 'Wakeups, a reached price wakeup', contents: { wakeups: wakeups([W_REACHED]) } },
  { id: 'cmd-1', group: 'commands', label: 'Commands, one long', contents: { commands: [CMD_LONG] } },
  { id: 'cmd-many', group: 'commands', label: 'Commands, five', contents: { commands: CMDS } },
  { id: 'task-lone', group: 'subagents', label: 'Subagents, a lone one with a long title', contents: { tasks: { rows: [TASK_LONG], unnamedRunning: false } } },
  { id: 'tasks-many', group: 'subagents', label: 'Subagents, several running', contents: { tasks: { rows: TASKS, unnamedRunning: false } } },
  { id: 'tasks-error', group: 'subagents', label: 'Subagents, one failed while the turn waits on the reader', contents: { tasks: { rows: TASKS_ERROR, unnamedRunning: false } }, awaitingAnswer: true },
  { id: 'tasks-unnamed', group: 'subagents', label: 'Subagents, running but unnamed', contents: { tasks: { rows: [], unnamedRunning: true } } },
  { id: 'an-lone', group: 'analysts', label: 'Analysts, a lone one with a long workspace name', contents: { analysts: [AN_LONG] } },
  { id: 'an-many', group: 'analysts', label: 'Analysts, several', contents: { analysts: ANALYSTS } },
  { id: 'an-needs', group: 'analysts', label: 'Analysts, a lone one needing input', contents: { analysts: [AN_NEEDS] } },
  { id: 'an-many-needs', group: 'analysts', label: 'Analysts, several with one needing input', contents: { analysts: [...ANALYSTS, AN_NEEDS] } },
  { id: 'report-back', group: 'reportBack', label: 'Report back, the longest label', contents: {}, reportBackKey: RB_KEY },
  {
    id: 'all', group: 'combo', label: 'Everything at once', reportBackKey: RB_KEY,
    contents: {
      plan: PLAN_12, watch: { symbols: ['NVDA', 'AMD'] }, wakeups: wakeups([W_AT, W_CRON, W_PRICE_NVDA]),
      commands: CMDS.slice(0, 2), tasks: { rows: TASKS, unnamedRunning: false }, analysts: ANALYSTS,
    },
  },
  {
    id: 'all-long', group: 'combo', label: 'Everything, long text', reportBackKey: RB_KEY,
    contents: {
      plan: PLAN_LONG_EN, watch: { symbols: ['BRK.A', 'DOGE-USD'] }, wakeups: wakeups([W_PRICE, W_AT, W_CRON, W_ORDER]),
      commands: [CMD_LONG], tasks: { rows: [TASK_LONG], unnamedRunning: false }, analysts: [AN_LONG],
    },
  },
  {
    id: 'unmovable-long', group: 'unmovable', label: 'Unmovable set at its longest: plan, lone subagent, lone Analyst, Status',
    contents: { plan: PLAN_LONG_EN, tasks: { rows: [TASK_LONG], unnamedRunning: false }, analysts: [AN_LONG] },
  },
  {
    id: 'unmovable-cjk', group: 'unmovable', label: 'Unmovable set, CJK plan and an unnamed subagent',
    contents: { plan: PLAN_LONG_CJK, tasks: { rows: [], unnamedRunning: true }, analysts: [AN_NEEDS] },
  },
];

const shown = knobs.scenario ? SCENARIOS.filter((s) => s.id === knobs.scenario || s.group === knobs.scenario) : SCENARIOS;

// ---- Measurement, read by the sweep through `window.__trayPreview`. ----

interface Measured {
  scenario: string;
  variant: string;
  layout: string | null;
  /** Distinct tops among the row's chips: 1 is one row. */
  rows: number;
  /** How far the chips reach past the row's right edge, in px. */
  overflow: number;
  /** The row's own scrollWidth past its clientWidth. */
  scrollOverflow: number;
  /** A chip whose content spills out of it (scrollWidth past clientWidth). */
  spilled: string[];
  rowWidth: number;
}

function measure(): { pageScroll: number; results: Measured[] } {
  const results: Measured[] = [];
  for (const row of document.querySelectorAll<HTMLElement>('[data-tray-layout]')) {
    const host = row.closest<HTMLElement>('[data-scenario]');
    if (!host) continue;
    const box = row.getBoundingClientRect();
    const chips = [...row.children].filter((el): el is HTMLElement => el instanceof HTMLElement && el.getClientRects().length > 0);
    const tops = new Set(chips.map((el) => Math.round(el.getBoundingClientRect().top)));
    const right = Math.max(box.right, ...chips.map((el) => el.getBoundingClientRect().right));
    results.push({
      scenario: host.dataset.scenario!,
      variant: host.dataset.variant!,
      layout: row.dataset.trayLayout ?? null,
      rows: tops.size,
      overflow: Math.round((right - box.right) * 10) / 10,
      scrollOverflow: row.scrollWidth - row.clientWidth,
      spilled: chips.filter((el) => el.scrollWidth > el.clientWidth + 1).map((el) => el.textContent ?? ''),
      rowWidth: Math.round(box.width),
    });
  }
  const doc = document.documentElement;
  return { pageScroll: doc.scrollWidth - doc.clientWidth, results };
}

const changeCounts: Record<string, number> = {};
const history: Record<string, string[]> = {};
const layoutWatch = new MutationObserver((records) => {
  for (const record of records) {
    const row = record.target as HTMLElement;
    const host = row.closest<HTMLElement>('[data-scenario]');
    if (!host || record.oldValue === row.dataset.trayLayout) continue;
    const key = `${host.dataset.scenario}/${host.dataset.variant}`;
    changeCounts[key] = (changeCounts[key] ?? 0) + 1;
    (history[key] ??= []).push(row.dataset.trayLayout ?? '');
  }
});
layoutWatch.observe(document.body, { subtree: true, attributes: true, attributeFilter: ['data-tray-layout'], attributeOldValue: true });

type Control = { setWidth: (w: number | null) => void; setTick: (on: boolean) => void };
const control: { current: Control | null } = { current: null };

declare global {
  interface Window {
    __trayPreview: {
      measure: typeof measure;
      changes: () => { counts: Record<string, number>; history: Record<string, string[]> };
      resetChanges: () => void;
      setWidth: (w: number | null) => void;
      setTick: (on: boolean) => void;
    };
  }
}

window.__trayPreview = {
  measure,
  changes: () => ({ counts: { ...changeCounts }, history: { ...history } }),
  resetChanges: () => {
    for (const key of Object.keys(changeCounts)) delete changeCounts[key];
    for (const key of Object.keys(history)) delete history[key];
  },
  setWidth: (w) => control.current?.setWidth(w),
  setTick: (on) => control.current?.setTick(on),
};

// ---- Rendering. ----

function ScenarioTray({ scenario, variant, quotes }: {
  scenario: Scenario;
  variant: 'tray' | 'strip';
  quotes: Record<string, ThreadQuote>;
}): React.ReactElement {
  const { t } = useTranslation();
  const trayRef = useRef<HTMLDivElement>(null);
  const dockRef = useRef<HTMLElement>(null);
  const contents: TrayContents = { ...EMPTY, ...scenario.contents, reportBack: scenario.reportBackKey ? t(scenario.reportBackKey) : null };
  const status: ThreadStatus = {
    contents,
    work: threadWork(contents, !!scenario.awaitingAnswer),
    latestPlan: scenario.contents.plan ? { steps: scenario.contents.plan, state: 'live' } : null,
    allWakeups: scenario.contents.wakeups ?? null,
    focus: null,
    from: { threadId: THREAD, workspaceId: 'home' },
    threadId: THREAD,
    onOpenTask: (id) => console.info('[tray-preview] open task', id),
    onOpenCommand: (id) => console.info('[tray-preview] open command', id),
    onJumpToLatest: () => undefined,
    onOpenStatus: (section) => console.info('[tray-preview] open status', section),
  };
  return (
    <FixedThreadQuotes quotes={{ bySymbol: quotes, phase: 'open' }}>
      <ThreadStatusContext value={status}>
        {variant === 'tray' ? (
          <ComposerTray phase="open" trayRef={trayRef} dockRef={dockRef} onSettled={() => undefined} />
        ) : (
          // The composer's box as chat-input draws it: a 1px edge around the strip.
          <div className="flex flex-col rounded-2xl border border-(--color-border-input) bg-(--color-bg-card)">
            <ComposerTrayStrip />
            <div className="px-3 pt-3 pb-2 text-sm text-(--color-text-quaternary)">Ask anything</div>
          </div>
        )}
      </ThreadStatusContext>
    </FixedThreadQuotes>
  );
}

function Surfaces(): React.ReactElement {
  const [width, setWidth] = useState<number | null>(knobs.width);
  const [ticking, setTicking] = useState(knobs.tick);
  const [tick, setTick] = useState(0);
  useEffect(() => {
    control.current = { setWidth, setTick: setTicking };
    return () => { control.current = null; };
  }, []);
  useEffect(() => {
    if (!ticking) return;
    const id = window.setInterval(() => setTick((n) => n + 1), 400);
    return () => window.clearInterval(id);
  }, [ticking]);
  const quotes = ticking ? tickedQuotes(tick) : QUOTES;
  const variants: ('tray' | 'strip')[] = knobs.strip === 'both' ? ['tray', 'strip'] : [knobs.strip];
  return (
    <div className="space-y-5">
      <div className="font-mono text-[0.6875rem] text-(--color-text-quaternary)">
        width {width ?? 'auto'} · {knobs.locale} · {variants.join('+')}{ticking ? ' · ticking' : ''}{knobs.zoom !== 100 ? ` · ${knobs.zoom}%` : ''}
      </div>
      {shown.map((scenario) => (
        <section key={scenario.id} className="space-y-1">
          <div className="font-mono text-[0.625rem] text-(--color-text-quaternary)">{scenario.id}: {scenario.label}</div>
          {variants.map((variant) => (
            <div
              key={variant}
              data-scenario={scenario.id}
              data-variant={variant}
              // A dashed edge marks the column, so a chip past it shows.
              className="relative outline outline-1 outline-dashed outline-(--color-border-muted)"
              style={{ width: width ?? '100%' }}
            >
              <ScenarioTray scenario={scenario} variant={variant} quotes={quotes} />
            </div>
          ))}
        </section>
      ))}
    </div>
  );
}

const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false, enabled: false } } });

function Preview(): React.ReactElement {
  const { setTheme } = useTheme();
  useEffect(() => setTheme(knobs.theme), [setTheme]);
  return (
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <ThreadCommandsContext value={THREAD}>
          <main className="font-content min-h-screen bg-(--color-bg-page) px-2 py-6 text-(--color-text-primary)">
            <Surfaces />
          </main>
        </ThreadCommandsContext>
      </MemoryRouter>
    </QueryClientProvider>
  );
}

void initI18n()
  .then(() => switchLocale(i18n, knobs.locale))
  .then(() =>
    createRoot(document.getElementById('root')!).render(
      <StrictMode>
        <ThemeProvider>
          <Preview />
        </ThemeProvider>
      </StrictMode>,
    ),
  );
