/* eslint-disable react-refresh/only-export-components -- entry file, not a module */
import React, { StrictMode, useEffect, useMemo, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import i18n, { initI18n, loadCopy } from '@/i18n';
import { queryKeys } from '@/lib/queryKeys';
import { switchLocale } from '@/lib/locale';
import { ThemeProvider, useTheme } from '@/contexts/ThemeContext';
import '@/index.css';
import type { MessageProvenance, SubagentTaskRecord } from '@/types/chat';
import type { SubagentTelemetry } from '@/pages/ChatAgent/session/subagents/resolveSubagentTelemetry';
import { SubagentTelemetryContext } from '../../SubagentTelemetryContext';
import { MessageActionsProvider } from '../MessageActionsContext';
import { ReportRow } from '../ReportRow';
import { SubagentRun, type SubagentRunMember } from '../SubagentRun';
import type { SubagentInfo, ToolCallProcessRecord } from '../types';

/**
 * Dev-only harness rendering the REAL subagent rows and groups from fixture
 * tasks and telemetry, for design iteration and screenshots. Open
 * /subagents-preview.html on the dev server. Not bundled in production (vite
 * builds index.html only).
 *
 * Knobs are query parameters, so a screenshot is one URL:
 *   ?theme=dark&locale=zh-CN&only=group6-live
 * `only` names one section's `data-preview` id. Live fixtures are timed from
 * page load, so their clocks run.
 */

const T = Date.now();
const S = 1000;
const M = 60 * S;

const params = new URLSearchParams(window.location.search);
const knobs = {
  theme: params.get('theme') === 'dark' ? 'dark' : 'light',
  locale: params.get('locale') === 'zh-CN' ? 'zh-CN' : 'en-US',
  only: params.get('only'),
} as const;

/** What each run went out on, named from the catalog below. A user's own
 *  endpoint has no mark; a model without levels no effort. */
const ON_CLAUDE = { model: { id: 'claude-sonnet-4-6', provider: 'anthropic', effort: 'high' } };
const ON_OPUS = { model: { id: 'claude-opus-5-5', provider: 'anthropic', effort: 'max' } };
const ON_SOL = { model: { id: 'gpt-6.2-sol', provider: 'openai', effort: 'xhigh' } };
const ON_OWN = { model: { id: 'qwen-local', provider: 'my-endpoint', effort: 'medium' } };
const ON_FLASH = { model: { id: 'deepseek-v5-flash', provider: 'deepseek' } };

/** A run under way, on the model most fixtures share. */
const BEGUN = { tokenUsage: { input: 9_000, output: 1_200, total: 10_200 }, ...ON_CLAUDE };

interface Fixture {
  task: Partial<SubagentTaskRecord>;
  telemetry?: Partial<SubagentTelemetry>;
  createdAt?: number;
}

const TELEMETRY = new Map<string, SubagentTelemetry>();
const TELEMETRY_SOURCE = { resolve: (id: string) => TELEMETRY.get(id), followsRuns: true };

function members(prefix: string, fixtures: Fixture[]): SubagentRunMember[] {
  return fixtures.map((f, i) => {
    const id = `${prefix}-${i}`;
    if (f.telemetry) {
      TELEMETRY.set(id, { toolCalls: 0, tokenUsage: { input: 0, output: 0, total: 0 }, ...f.telemetry });
    }
    return {
      subagentId: id,
      task: {
        subagentId: id,
        description: 'Research task',
        prompt: '',
        type: 'research',
        action: 'init',
        status: 'running',
        ...f.task,
      },
      toolCallProcess: f.createdAt ? ({ _createdAt: f.createdAt } as ToolCallProcessRecord) : undefined,
    };
  });
}

const SINGLES: { id: string; label: string; fixture: Fixture; pausing?: boolean; readOnly?: boolean }[] = [
  {
    id: 'starting',
    label: 'Starting (launch time only)',
    fixture: { task: { description: 'Pull the last four 10-Qs for NVDA' }, createdAt: T - 3 * S },
  },
  {
    id: 'working',
    label: 'Working',
    fixture: {
      task: { description: 'Hyperscaler capex guidance for 2027' },
      telemetry: {
        ...BEGUN,
        startedAtMs: T - M - 12 * S,
        currentToolCall: { toolName: 'WebSearch', args: { query: 'MSFT FY26 Q1 call transcript capex' } },
      },
    },
  },
  {
    id: 'working-model',
    label: 'Working, on Opus at Max',
    fixture: {
      task: { description: 'Compare services gross margin across the last eight quarters', type: 'equity-analyst' },
      telemetry: {
        ...BEGUN,
        ...ON_OPUS,
        startedAtMs: T - 2 * M - 14 * S,
        currentToolCall: { toolName: 'get_financial_statements', args: { symbol: 'AAPL' } },
      },
    },
  },
  {
    id: 'pausing',
    label: 'Credit pause pending',
    pausing: true,
    fixture: {
      task: { description: 'Screen mid-cap semis by FCF yield', type: 'data-prep' },
      telemetry: { ...BEGUN, startedAtMs: T - 48 * S },
    },
  },
  {
    id: 'done',
    label: 'Done',
    fixture: {
      task: { description: 'Hyperscaler capex guidance for 2027', status: 'completed' },
      telemetry: {
        ...BEGUN,
        ...ON_OWN,
        startedAtMs: T - 9 * M,
        finishedAtMs: T - 9 * M + 3 * M + 40 * S,
        lastText: '## Summary\n\nReturned 2027 guidance for all four, with sources.',
      },
    },
  },
  {
    id: 'done-untimed',
    label: 'Done, from before times and models were kept',
    fixture: {
      task: { description: 'Draft the sector one-pager', type: 'report-builder', status: 'completed' },
      telemetry: { ...BEGUN, model: undefined },
    },
  },
  {
    id: 'failed',
    label: 'Failed',
    fixture: {
      task: { description: 'Marvell custom-silicon backlog', type: 'general-purpose', status: 'error' },
      telemetry: { ...BEGUN, ...ON_FLASH, startedAtMs: T - 5 * M, finishedAtMs: T - 5 * M + 43 * S, stopReason: 'The source refused the request (403)' },
    },
  },
  {
    id: 'stopped',
    label: 'Stopped',
    fixture: {
      task: { description: 'Scan 13F filings for new positions', status: 'cancelled', type: 'general-purpose' },
      telemetry: { ...BEGUN, startedAtMs: T - 6 * M, finishedAtMs: T - 5 * M - 20 * S, stopReason: 'Stopped by the user' },
    },
  },
  {
    id: 'refused',
    label: "Couldn't start (inert)",
    fixture: {
      task: {
        description: 'Hyperscaler capex guidance for 2027',
        status: 'error',
        launchFailed: true,
        launchRefusal: 'subagents_off',
        result: 'Refused: the user has turned subagents off for this thread. Do this work yourself.',
      },
    },
  },
  {
    id: 'refused-followup',
    label: "Couldn't start, a follow-up (opens its target)",
    fixture: {
      task: {
        description: 'Also cover the China segment',
        action: 'update',
        resumeTargetId: 'task:A1b2C3',
        status: 'error',
        launchFailed: true,
        result: 'Refused: subagents are off for this thread.',
      },
    },
  },
  {
    id: 'updated',
    label: 'Update sent',
    fixture: { task: { description: 'Also cover the China segment', action: 'update', resumeTargetId: 'task:A1b2C3' } },
  },
  {
    id: 'resumed',
    label: 'Resumed',
    fixture: { task: { description: 'Pick up the margin comparison', action: 'resume', resumeTargetId: 'task:A1b2C3' } },
  },
  {
    id: 'readonly',
    label: 'Read-only (shared link)',
    readOnly: true,
    fixture: {
      task: { description: 'Summarize the Q3 earnings call for MSFT', status: 'completed' },
      telemetry: { ...BEGUN, startedAtMs: T - 9 * M, finishedAtMs: T - 5 * M, lastText: 'Azure grew 33%.' },
    },
  },
];

/** The mockup's six, timed from page load so the live clocks run. */
const SIX: Fixture[] = [
  {
    task: { description: 'Pull ten years of AAPL, MSFT and GOOGL statements', type: 'data-prep', status: 'completed' },
    telemetry: { ...BEGUN, startedAtMs: T - 221 * S, finishedAtMs: T - 131 * S, lastText: 'Saved 3 statements x 10 years to data/financials.' },
  },
  {
    task: { description: 'Survey analyst notes on cloud margins', type: 'research' },
    telemetry: {
      ...BEGUN,
      ...ON_SOL,
      startedAtMs: T - 219 * S,
      currentToolCall: { toolName: 'WebSearch', args: { query: 'cloud gross margin 2026' } },
    },
  },
  {
    task: { description: 'Value each on EV/FCF against its five-year range', type: 'equity-analyst' },
    telemetry: { ...BEGUN, startedAtMs: T - 211 * S, currentToolCall: { toolName: 'ExecuteCode', args: {} } },
  },
  {
    task: { description: 'Fetch the daily closes since 2016', type: 'data-prep', status: 'error' },
    telemetry: { ...BEGUN, startedAtMs: T - 216 * S, finishedAtMs: T - 171 * S, stopReason: 'Rate limited by the price provider after 3 retries' },
  },
  {
    task: { description: 'Collect the 10-K risk factor changes', type: 'research', status: 'completed' },
    telemetry: { ...BEGUN, startedAtMs: T - 217 * S, finishedAtMs: T - 68 * S, lastText: 'Two new risk factors at GOOGL: AI regulation and data residency.' },
  },
  {
    task: { description: 'Draft the comparison memo', type: 'report-builder' },
    createdAt: T - 7 * S,
  },
];

const GROUP6_LIVE = members('g6', SIX);

/** Three models side by side. */
const GROUP3_MODELS = members('g3', [SIX[0], SIX[1], { ...SIX[3], telemetry: { ...SIX[3].telemetry, ...ON_OWN } }]);

/** One maker's model on each route the composer offers it, and the
 *  deployment's default: each wears the mark the model menu gives its row. */
const ROUTES = members('routes', ([
  ['Claude Opus 5.5 on its API', 'claude-opus-5-5', 'anthropic', 'high'],
  ['Claude Opus 5.5 on a Claude subscription', 'claude-opus-5-5-oauth', 'claude-oauth', 'high'],
  ['Claude Opus 5.5 on Claude Code', 'claude-opus-5-5-code', 'claude-code', 'max'],
  ['DeepSeek, the built-in default', 'deepseek-flash', 'deepseek', 'high'],
] as const).map(([description, id, provider, effort]) => ({
  task: { description, type: 'research', status: 'completed' },
  telemetry: { ...BEGUN, model: { id, provider, effort }, startedAtMs: T - 90 * S, finishedAtMs: T - 30 * S, lastText: 'Margins held inside the guided range.' },
})));

const GROUP2_LIVE = members('g2', [
  {
    task: { description: 'Pull NVDA segment revenue for FY22 to FY25', type: 'data-prep', status: 'completed' },
    telemetry: { ...BEGUN, startedAtMs: T - 2 * M, finishedAtMs: T - 40 * S, lastText: 'Data center went from 39% to 88% of revenue.' },
  },
  {
    task: { description: 'Read the last two earnings call transcripts', type: 'research' },
    telemetry: {
      ...BEGUN,
      startedAtMs: T - 2 * M + 4 * S,
      currentToolCall: { toolName: 'WebSearch', args: { query: 'NVDA Q2 FY26 earnings call transcript' } },
    },
  },
]);

const SETTLED_START = T - 40 * M;
const GROUP6_SETTLED = members('g6s', [
  ['Pull ten years of AAPL, MSFT and GOOGL statements', 'data-prep', 0, 90, 'completed'],
  ['Survey analyst notes on cloud margins', 'research', 2, 258, 'completed'],
  ['Value each on EV/FCF against its five-year range', 'equity-analyst', 10, 241, 'completed'],
  ['Fetch the daily closes since 2016', 'data-prep', 5, 50, 'error'],
  ['Collect the 10-K risk factor changes', 'research', 4, 153, 'completed'],
  ['Draft the comparison memo', 'report-builder', 214, 258, 'completed'],
].map(([description, type, from, to, status]) => ({
  task: { description: description as string, type: type as string, status: status as string },
  telemetry: {
    ...BEGUN,
    startedAtMs: SETTLED_START + (from as number) * S,
    finishedAtMs: SETTLED_START + (to as number) * S,
    lastText: status === 'completed' ? 'Wrote the findings to results/cloud-margins.md.' : undefined,
    stopReason: status === 'error' ? 'Rate limited by the price provider after 3 retries' : undefined,
  },
})));

const OLD = { ...BEGUN, model: undefined };
const GROUP_UNTIMED = members('gu', [
  { task: { description: 'Summarize the Fed minutes', status: 'completed' }, telemetry: { ...OLD, lastText: 'Two members wanted a pause in December.' } },
  { task: { description: 'Pull treasury yields', type: 'data-prep', status: 'completed' }, telemetry: { ...OLD } },
  { task: { description: 'Chart the curve', type: 'report-builder' }, telemetry: { ...OLD } },
]);

const SINGLE_MEMBERS = SINGLES.map((s) => ({ ...s, members: members(s.id, [s.fixture]) }));

/** The catalog the composer would have loaded, so a row names its model. */
const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
queryClient.setQueryData(queryKeys.models.all, {
  model_metadata: {
    'claude-sonnet-4-6': { display_name: 'Claude Sonnet 4.6', provider: 'anthropic' },
    'claude-opus-5-5': { display_name: 'Claude Opus 5.5', provider: 'anthropic' },
    'gpt-6.2-sol': { display_name: 'GPT-6.2 Sol', provider: 'openai' },
    'qwen-local': { display_name: 'Qwen 4 Local', provider: 'my-endpoint' },
    'deepseek-v5-flash': { display_name: 'DeepSeek V5 Flash', provider: 'deepseek' },
    'claude-opus-5-5-oauth': { display_name: 'Claude Opus 5.5', provider: 'claude-oauth' },
    'claude-opus-5-5-code': { display_name: 'Claude Opus 5.5', provider: 'claude-code' },
    'deepseek-flash': { display_name: 'DeepSeek-V4.1-Flash', provider: 'deepseek' },
  },
});

function opened(info: SubagentInfo): void {
  console.info('[subagents-preview] open', info);
}

type TaskReport = Extract<MessageProvenance, { kind: 'task_report' }>;
const taskReport = (over: Partial<TaskReport>): TaskReport => ({
  kind: 'task_report',
  taskId: 'r7Kp2q',
  displayId: 'Task-r7Kp2q',
  description: 'Extract NVDA 4-quarter segment data from the 10-Qs',
  failed: false,
  subagentType: 'data-prep',
  ...over,
});

const TASK_REPORTS: Array<{ label: string; report: TaskReport }> = [
  { label: 'finished', report: taskReport({}) },
  { label: 'failed', report: taskReport({ description: 'Extract AMD 4-quarter segment data', failed: true }) },
  { label: 'research', report: taskReport({ description: 'Summarize this week’s semis news', subagentType: 'research' }) },
  { label: 'a user-defined role', report: taskReport({ description: 'Check the comps against the house style', subagentType: 'house-style' }) },
  { label: 'before roles (legacy)', report: taskReport({ description: null, subagentType: undefined }) },
  {
    label: 'a title too long for the line',
    report: taskReport({ description: 'Rebuild the segment margin bridge for every quarter since FY2024 and reconcile it against the reported totals' }),
  },
];

const REPORT_ACTIONS = { onOpenSubagentTask: opened };


/** Calls as a run makes them, looped: long ones, gaps while the model thinks
 *  or writes, bursts that return in a fifth of a second, and a batch run in
 *  parallel, as recorded on a real run. Each step is what the telemetry says
 *  for that long. */
type ScriptStep = [Pick<SubagentTelemetry, 'currentToolCall' | 'runningCalls' | 'between'>, number];
const CALL = (toolName: string, args: Record<string, unknown>, runningCalls?: number) => ({
  currentToolCall: { toolName, args }, ...(runningCalls ? { runningCalls } : {}),
});
const THINKING = { between: 'thinking' } as const;
const WRITING = { between: 'writing' } as const;
const SCRIPTS: ScriptStep[][] = [
  [
    [CALL('WebSearch', { query: 'Kessler BIS testimony H200 China shipments' }), 4200],
    [THINKING, 900],
    [CALL('Bash', { command: 'ls data/' }), 200],
    [THINKING, 400],
    [CALL('WebFetch', { url: 'https://mp.weixin.qq.com/s/abc' }), 150],
    [CALL('WebFetch', { url: 'https://arxiv.org/abs/2511.01234' }), 300],
    [THINKING, 2600],
    [WRITING, 2200],
  ],
  [
    [CALL('WebFetch', { url: 'https://mp.weixin.qq.com/s/xyz' }, 3), 2500],
    [CALL('WebFetch', { url: 'https://mp.weixin.qq.com/s/xyz' }, 2), 1800],
    [CALL('WebFetch', { url: 'https://mp.weixin.qq.com/s/xyz' }), 1700],
    [THINKING, 3000],
    [CALL('ExecuteCode', {}), 250],
    [CALL('Read', { file_path: 'notes/h200.md' }), 180],
    [CALL('Bash', { command: 'python parse.py' }), 2400],
  ],
];

function cycling(prefix: string): { members: SubagentRunMember[]; ids: string[] } {
  const m = members(prefix, SCRIPTS.map((_, i) => ({
    task: { description: i === 0 ? 'Export-control timeline gaps + China-side actions' : "Chinese labs' training hardware (Blackwell vs Hopper)", status: 'running' },
    telemetry: { ...BEGUN, startedAtMs: T },
  })));
  return { members: m, ids: m.map((x) => x.subagentId) };
}
const CYCLING = cycling('cycling');

/** Steps each script on its own clock and hands the rows a new source per
 *  step, as the live stream does. */
function CyclingRun(): React.ReactElement {
  const [at, setAt] = useState(() => SCRIPTS.map(() => 0));
  useEffect(() => {
    const timers = SCRIPTS.map((script, i) => {
      let step = 0;
      let timer: ReturnType<typeof setTimeout>;
      const next = () => {
        timer = setTimeout(() => {
          step = (step + 1) % script.length;
          setAt((prev) => prev.map((v, j) => (j === i ? step : v)));
          next();
        }, script[step][1]);
      };
      next();
      return () => clearTimeout(timer);
    });
    return () => timers.forEach((stop) => stop());
  }, []);
  const source = useMemo(() => ({
    followsRuns: true,
    resolve: (id: string) => {
      const i = CYCLING.ids.indexOf(id);
      const base = TELEMETRY.get(id);
      if (i < 0 || !base) return base;
      return { ...base, ...SCRIPTS[i][at[i]][0] };
    },
  }), [at]);
  return (
    <SubagentTelemetryContext.Provider value={source}>
      <SubagentRun members={CYCLING.members} onOpen={opened} />
    </SubagentTelemetryContext.Provider>
  );
}

function Section({ id, label, children }: { id: string; label: string; children: React.ReactNode }): React.ReactElement | null {
  if (knobs.only && knobs.only !== id) return null;
  return (
    <section data-preview={id} className="space-y-1.5">
      <div className="font-mono text-[0.6875rem] text-(--color-text-quaternary)">{label}</div>
      {children}
    </section>
  );
}

function Preview(): React.ReactElement {
  const { setTheme } = useTheme();
  useEffect(() => setTheme(knobs.theme), [setTheme]);
  useEffect(() => {
    void switchLocale(i18n, knobs.locale);
  }, []);

  return (
    <QueryClientProvider client={queryClient}>
      <SubagentTelemetryContext.Provider value={TELEMETRY_SOURCE}>
        <main className="font-content min-h-screen bg-(--color-bg-page) px-4 py-8 text-(--color-text-primary)">
          <div className="mx-auto w-full max-w-3xl space-y-6">
            <Section id="cycling" label="2 subagents, calls as they come">
              <CyclingRun />
            </Section>
            <Section id="routes" label="One model on each route, and the built-in default">
              <SubagentRun members={ROUTES} onOpen={opened} />
            </Section>
            <Section id="group6-live" label="6 subagents, live">
              <SubagentRun members={GROUP6_LIVE} onOpen={opened} />
            </Section>
            <Section id="group6-settled" label="6 subagents, settled (mounts folded)">
              <SubagentRun members={GROUP6_SETTLED} onOpen={opened} />
            </Section>
            <Section id="group3-badge" label="3 subagents, three models">
              <SubagentRun members={GROUP3_MODELS} onOpen={opened} />
            </Section>
            <Section id="group2-live" label="2 subagents, live">
              <SubagentRun members={GROUP2_LIVE} onOpen={opened} />
            </Section>
            <Section id="group-untimed" label="3 subagents, no times (degraded)">
              <SubagentRun members={GROUP_UNTIMED} onOpen={opened} />
            </Section>
            <Section id="group-readonly" label="Group, read-only">
              <SubagentRun members={GROUP2_LIVE} />
            </Section>
            <Section id="reports" label="Report rows">
              <MessageActionsProvider actions={REPORT_ACTIONS}>
                {TASK_REPORTS.map((r) => (
                  <div key={r.label}>
                    <div className="font-mono text-[0.625rem] text-(--color-text-quaternary)">{r.label}</div>
                    <ReportRow id={`task-report-${r.label}`} provenance={r.report} />
                  </div>
                ))}
              </MessageActionsProvider>
              <div className="font-mono text-[0.625rem] text-(--color-text-quaternary)">read-only</div>
              <ReportRow id="task-report-read-only" provenance={TASK_REPORTS[0].report} readOnly />
            </Section>
            {SINGLE_MEMBERS.map((s) => (
              <Section key={s.id} id={s.id} label={s.label}>
                <SubagentRun
                  members={s.members}
                  pausing={s.pausing}
                  onOpen={s.readOnly ? undefined : opened}
                />
              </Section>
            ))}
          </div>
        </main>
      </SubagentTelemetryContext.Provider>
    </QueryClientProvider>
  );
}

// The tool labels a working row names live in the chat page's lazy copy.
void initI18n().then(() => loadCopy(['toolArtifact'])).then(() =>
  createRoot(document.getElementById('root')!).render(
    <StrictMode>
      <ThemeProvider>
        <Preview />
      </ThemeProvider>
    </StrictMode>,
  ),
);
