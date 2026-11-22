/* eslint-disable react-refresh/only-export-components -- entry file, not a module */
import React, { StrictMode, useEffect, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter } from 'react-router';
import { queryKeys } from '@/lib/queryKeys';
import i18n, { initI18n } from '@/i18n';
import { switchLocale } from '@/lib/locale';
import { ThemeProvider, useTheme } from '@/contexts/ThemeContext';
import '@/index.css';
import type { MessageProvenance } from '@/types/chat';
import type { CommandRun, CommandsResponse } from '@/types/commands';
import ActivityBlock from '../../ActivityBlock';
import { WorkspaceCreatedSlip } from '../../messageList/WorkspaceCreatedSlip';
import { preapprovedCardOf } from '../../../utils/preapprovedCards';
import ToolCallDetailView from '../../ToolCallDetailView';
import { ComposerTray } from '../../chatView/ComposerTray';
import { ThreadStatusContext, threadWork } from '../../chatView/threadStatus';
import type { TrayContents } from '../../chatView/useTrayContents';
import type { RosterTask } from '../../chatView/delegatedWork';
import { MessageActionsProvider } from '../../messageList/MessageActionsContext';
import { ReportRow, type ReportProvenance } from '../../messageList/ReportRow';
import type { ToolActivityItem } from '../../messageList/activityTypes';
import { useThreadCommands } from '../../../hooks/useCommands';
import { ThreadCommandsContext } from '../threadCommandsContext';
import { runningCommands } from '../commandWords';

/**
 * Dev-only harness rendering the REAL command surfaces (the tool rows, the
 * tray chip, the report captions and the tool card's detail) over the real
 * listing hook, whose requests a fetch stub answers from fixtures. Open
 * /commands-preview.html on the dev server. Not bundled in production (vite
 * builds index.html only).
 *
 * Knobs are query parameters, so a screenshot is one URL:
 *   ?theme=dark&locale=zh-CN&only=tool-rows&stop=fail
 * `stop=fail` makes every Stop fail, to see its error.
 */

const params = new URLSearchParams(window.location.search);
const knobs = {
  theme: params.get('theme') === 'dark' ? 'dark' : 'light',
  locale: params.get('locale') === 'zh-CN' ? 'zh-CN' : 'en-US',
  only: params.get('only'),
  stopFails: params.get('stop') === 'fail',
} as const;

const THREAD = '3f9a1c2e-7d10-4b2a-9c55-0e1f2a3b4c5d';
const T = Date.now();
const M = 60_000;
const iso = (ms: number) => new Date(ms).toISOString();

function command(over: Partial<CommandRun> & Pick<CommandRun, 'command_id' | 'description'>): CommandRun {
  return {
    tool: 'Bash', status: 'running', exit_code: null, failure_reason: null,
    tool_call_id: `call_${over.command_id}`, log_path: `.agents/threads/3f9a1c2e/commands/${over.command_id}.log`,
    started_at: iso(T - 17 * M), handed_off_at: iso(T - 12 * M), ended_at: null,
    ...over,
  } as CommandRun;
}

let rows: CommandRun[] = [
  command({ command_id: 'bg-3f9a1c2e7d10', description: 'Run 10-year backtest' }),
  command({ command_id: 'bg-a41c0e9b2f37', description: 'Rebuild the factor cache for every ticker in the universe since 2015', started_at: iso(T - 6 * M), handed_off_at: iso(T - M) }),
  command({
    command_id: 'bg-5d2e8a1f0c44', description: 'Rebuild the factor cache', status: 'completed', exit_code: 0,
    started_at: iso(T - 80 * M), handed_off_at: iso(T - 75 * M), ended_at: iso(T - 39 * M),
  }),
  command({
    command_id: 'bg-77c0e41a9b2e', tool: 'ExecuteCode', description: 'Pull filings for 500 tickers', status: 'failed', exit_code: 1,
    failure_reason: 'exit', started_at: iso(T - 30 * M), handed_off_at: iso(T - 25 * M), ended_at: iso(T - 23 * M),
  }),
  command({
    command_id: 'bg-0b9d3c6e1a58', description: 'Train the regime model', status: 'failed', failure_reason: 'after_turn_limit',
    started_at: iso(T - 180 * M), handed_off_at: iso(T - 175 * M), ended_at: iso(T - 68 * M),
  }),
  command({
    command_id: 'bg-e2f4a6b8c0d1', description: 'Serve the dashboard', status: 'stopped',
    started_at: iso(T - 50 * M), handed_off_at: iso(T - 45 * M), ended_at: iso(T - 20 * M),
  }),
];

// The listing and the stop, answered here as the server would.
const realFetch = window.fetch.bind(window);
window.fetch = async (input, init) => {
  const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
  const path = new URL(url, window.location.origin).pathname;
  const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } });
  if (path === `/api/v1/threads/${THREAD}/commands`) {
    return json({ thread_id: THREAD, commands: rows } satisfies CommandsResponse);
  }
  const stop = path.match(new RegExp(`^/api/v1/threads/${THREAD}/commands/([^/]+)/stop$`));
  if (stop && init?.method === 'POST') {
    await new Promise((r) => setTimeout(r, 700));
    if (knobs.stopFails) return json({ detail: 'Computer unreachable' }, 503);
    const id = decodeURIComponent(stop[1]);
    rows = rows.map((row) => (row.command_id === id && row.status === 'running'
      ? { ...row, status: 'stopped', ended_at: new Date().toISOString() }
      : row));
    return json({ stopped: true, command: rows.find((row) => row.command_id === id) ?? null });
  }
  return realFetch(input, init);
};

const HANDOFF = (id: string) => `Still running after 5m, so it moved to the background as command_id ${id}. Its output goes to .agents/threads/3f9a1c2e/commands/${id}.log as it runs, and you will be notified when it finishes.`;

function toolItem(id: string, toolName: 'Bash' | 'ExecuteCode', description: string, result: string): ToolActivityItem {
  return {
    type: 'tool_call', id, toolCallId: id, toolName,
    toolCall: { args: toolName === 'Bash' ? { command: 'python backtest.py --years 10', description } : { code: 'pull_filings(TICKERS)', description } },
    toolCallResult: { content: result },
    isComplete: true, _liveState: 'completed',
  };
}

const TOOL_ITEMS: ToolActivityItem[] = [
  toolItem('call_plain', 'Bash', 'List the data folder', 'prices.parquet\nfilings/\n'),
  ...rows.map((row) => toolItem(row.tool_call_id!, row.tool, row.description ?? '', HANDOFF(row.command_id))),
];

// Bash calls that run `la`, each named for what it did. The thread and the
// workspace below are in the cache, as the sidebar's reads would leave them.
const LA_THREAD = '346bee3b-8f31-43d3-b8a3-f6489e03d7aa';
const LA_WS = '0f1e2d3c-4b5a-4968-8776-655443322110';
function laItem(id: string, command: string, description: string, result: Partial<ToolActivityItem> = {}): ToolActivityItem {
  return {
    type: 'tool_call', id, toolCallId: id, toolName: 'Bash',
    toolCall: { args: { command, description } },
    toolCallResult: { content: 'ok' },
    isComplete: true, _liveState: 'completed',
    ...result,
  };
}
const LA_ITEMS: ToolActivityItem[] = [
  laItem('la_list', 'la workspaces list --include-home', 'List workspaces'),
  laItem('la_output', `la threads output ${LA_THREAD}`, 'Read the analyst thread'),
  laItem('la_create', "la workspaces create 'Model test' --description 'Which model answers best'", 'Create a workspace'),
  laItem('la_new', `la threads new -w ${LA_WS} --detach 'Size the AI capex cycle'`, 'Start a thread'),
  laItem('la_delete_unknown', 'la threads delete 11111111-2222-4333-8444-555555555555 -y', 'Delete an old thread'),
  laItem('la_cancel', `la threads cancel ${LA_THREAD}`, 'Stop the analyst'),
  laItem('la_failed', `la threads delete ${LA_THREAD} -y`, 'Delete the thread', {
    toolCallResult: { content: 'ERROR: Command failed (exit code 4)\nla: Not found' }, isFailed: true,
  }),
  laItem('la_piped', 'la threads list --json | jq length', 'Count threads'),
  laItem('la_running', `la threads delete ${LA_THREAD} -y`, 'Delete the thread', {
    toolCallResult: undefined, isComplete: false, _liveState: 'active',
  }),
];

const TASKS: RosterTask[] = [
  { id: 'task:k7Xm2p', name: 'Task-k7Xm2p', description: 'Compare momentum papers', isMainAgent: false, status: 'active', running: true },
  { id: 'task:Qm3xLp', name: 'Task-Qm3xLp', description: 'Summarize the Fed minutes', isMainAgent: false, status: 'completed', running: false },
];

type CommandReport = Extract<MessageProvenance, { kind: 'command_report' }>;
const report = (over: Partial<CommandReport>): CommandReport => ({
  kind: 'command_report', tool: 'Bash', description: 'Run 10-year backtest',
  status: 'completed', failureReason: null, toolCallId: 'call_bg-5d2e8a1f0c44', ...over,
});
const REPORTS: Array<{ label: string; provenance: ReportProvenance }> = [
  { label: 'finished', provenance: report({}) },
  { label: 'failed', provenance: report({ tool: 'ExecuteCode', description: 'Pull filings for 500 tickers', status: 'failed', failureReason: 'exit', toolCallId: 'call_bg-77c0e41a9b2e' }) },
  { label: 'stopped early (the hour after the turn)', provenance: report({ description: 'Train the regime model', status: 'failed', failureReason: 'after_turn_limit' }) },
  { label: 'no description (falls back to the tool)', provenance: report({ description: null, tool: 'ExecuteCode' }) },
  { label: 'a task report beside it, for comparison', provenance: { kind: 'task_report', taskId: 'r7Kp2q', displayId: 'Task-r7Kp2q', description: 'Extract NVDA 4-quarter segment data', failed: false, subagentType: 'data-prep' } },
];

const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
queryClient.setQueryData(queryKeys.threads.page(LA_WS, 20), {
  threads: [{ thread_id: LA_THREAD, workspace_id: LA_WS, title: 'NVDA fair value' }],
});
queryClient.setQueryData(queryKeys.workspaces.list({ limit: 100 }), {
  workspaces: [{ workspace_id: LA_WS, name: 'Semis' }, { workspace_id: 'ws-model-test', name: 'Model test' }],
});

function Section({ id, label, children }: { id: string; label: string; children: React.ReactNode }): React.ReactElement | null {
  if (knobs.only && knobs.only !== id) return null;
  return (
    <section data-preview={id} className="space-y-2">
      <div className="font-mono text-[0.6875rem] text-(--color-text-quaternary)">{label}</div>
      {children}
    </section>
  );
}

function Surfaces(): React.ReactElement {
  const listing = useThreadCommands(THREAD);
  const [opened, setOpened] = useState<string | null>(null);
  const running = runningCommands(listing);
  const contents: TrayContents = {
    plan: null, watch: null, wakeups: null, analysts: null, reportBack: null,
    commands: running.length ? running : null,
    tasks: { rows: TASKS, unnamedRunning: false },
  };
  const detail = [...TOOL_ITEMS, ...LA_ITEMS].find((item) => item.toolCallId === (opened ?? 'call_bg-3f9a1c2e7d10'));
  const actions = { onToolCallDetailClick: setOpened };
  return (
    <ThreadCommandsContext value={THREAD}>
      <MessageActionsProvider actions={actions}>
        <Section id="tool-rows" label="Tool rows (expanded activity): a plain call, then one per command state">
          <ActivityBlock items={TOOL_ITEMS} isStreaming={false} presentation="expanded" onToolCallClick={(item) => item.type === 'tool_call' && setOpened(item.toolCallId)} />
        </Section>
        <Section id="la-steps" label="la steps: named for what they did; a failure, a pipe and an unnamed id keep what they can say">
          {/* A workspace `la` created leaves the group for its card, as the
              message draws it (buildRenderBlocks): the steps around it split. */}
          {(() => {
            const at = LA_ITEMS.findIndex((item) => item.toolCallId === 'la_create');
            const card = preapprovedCardOf(LA_ITEMS[at]);
            const open = (item: ToolActivityItem | { type: string }) => item.type === 'tool_call' && setOpened((item as ToolActivityItem).toolCallId);
            return (
              <>
                <ActivityBlock items={LA_ITEMS.slice(0, at)} isStreaming={false} presentation="expanded" onToolCallClick={open} />
                {card?.type === 'create_workspace' && (
                  <WorkspaceCreatedSlip
                    name={String(card.proposal.workspace_name)}
                    description={card.proposal.workspace_description as string | undefined}
                  />
                )}
                <ActivityBlock items={LA_ITEMS.slice(at + 1)} isStreaming={false} presentation="expanded" onToolCallClick={open} />
              </>
            );
          })()}
        </Section>
        <Section id="reports" label="Report captions">
          {REPORTS.map((r) => (
            <div key={r.label}>
              <div className="font-mono text-[0.625rem] text-(--color-text-quaternary)">{r.label}</div>
              <ReportRow id={`report-${r.label}`} provenance={r.provenance} />
            </div>
          ))}
          <div className="font-mono text-[0.625rem] text-(--color-text-quaternary)">read-only</div>
          <ReportRow id="report-read-only" provenance={REPORTS[0].provenance} readOnly />
        </Section>
        <Section id="detail" label={`Tool card detail (${opened ?? 'call_bg-3f9a1c2e7d10'}); a caption's Open lands here`}>
          <div className="flex h-[22rem] flex-col overflow-hidden rounded-lg border border-(--color-border-muted)">
            {detail && <ToolCallDetailView toolCallProcess={{ toolName: detail.toolName, toolCall: { id: detail.toolCallId, args: detail.toolCall?.args }, toolCallResult: detail.toolCallResult, isComplete: true }} />}
          </div>
        </Section>
        <Section id="tray" label="Composer tray (open the commands chip)">
          <div className="h-56" />
          <div className="rounded-2xl border border-(--color-border-muted) pt-3 pb-14">
            <ThreadStatusContext
              value={{
                contents,
                work: threadWork(contents, false),
                latestPlan: null,
                allWakeups: null,
                focus: null,
                from: null,
                threadId: THREAD,
                onOpenTask: (id) => console.info('[commands-preview] open task', id),
                onOpenCommand: setOpened,
                onJumpToLatest: () => undefined,
                onOpenStatus: () => undefined,
              }}
            >
              <ComposerTray phase="open" trayRef={{ current: null }} dockRef={{ current: null }} onSettled={() => undefined} />
            </ThreadStatusContext>
          </div>
        </Section>
      </MessageActionsProvider>
    </ThreadCommandsContext>
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
      <MemoryRouter>
        <main className="font-content min-h-screen bg-(--color-bg-page) px-4 py-8 text-(--color-text-primary)">
          <div className="mx-auto w-full max-w-3xl space-y-8">
            <Surfaces />
          </div>
        </main>
      </MemoryRouter>
    </QueryClientProvider>
  );
}

void initI18n().then(() =>
  createRoot(document.getElementById('root')!).render(
    <StrictMode>
      <ThemeProvider>
        <Preview />
      </ThemeProvider>
    </StrictMode>,
  ),
);
