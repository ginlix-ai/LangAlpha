/**
 * The thread's status is built once: the plan the panel shows, the plans the
 * transcript draws and the tray's chip all come from one read. A question
 * the reader has not answered pauses the turn without ending its plan, and a
 * render of the chat that changes nothing leaves the status as it was.
 */
import { describe, expect, it, vi } from 'vitest';
import type { ReactNode } from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { renderHook } from '@testing-library/react';
import type { ChatMessage, PlanStep } from '@/types/chat';
import type { OwedReportKind } from '../../../utils/api';

vi.mock('@/hooks/useFeatures', () => ({ useFeatureEnabled: () => true }));

import { createAssistantMessage, createUserMessage } from '../../../hooks/utils/messageHelpers';
import { useThreadStatusValue } from '../useThreadStatusValue';
import { awaitsReport, threadWork } from '../threadStatus';
import type { TrayWork } from '../useTrayContents';
import type { RosterAnalyst } from '../useAnalystRoster';
import type { Wakeup } from '@/types/wakeups';

function transcript(...statuses: PlanStep['status'][]): ChatMessage[] {
  return [
    createUserMessage('Compare margins', undefined, { id: 'u1' }) as ChatMessage,
    {
      ...createAssistantMessage('a1', undefined),
      planSnapshots: [{ order: 1, steps: statuses.map((status, i) => ({ title: `Step ${i + 1}`, status })) }],
    },
  ];
}

type Inputs = Parameters<typeof useThreadStatusValue>[0];

function inputs(over: Partial<Inputs> = {}): Inputs {
  return {
    marketWatch: null,
    commands: [],
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
    awaitingAnswer: false,
    from: null,
    onSelectAgent: vi.fn(),
    onOpenSubagentTask: vi.fn(),
    onOpenCommand: vi.fn(),
    onOpenStatusPanel: vi.fn(),
    onJumpToLatest: vi.fn(),
    ...over,
  };
}

function renderStatus(over: Partial<Inputs>) {
  const client = new QueryClient();
  const wrapper = ({ children }: { children: ReactNode }) => <QueryClientProvider client={client}>{children}</QueryClientProvider>;
  return renderHook((props: Partial<Inputs>) => useThreadStatusValue(inputs(props)), { wrapper, initialProps: over });
}

function readStatus(messages: ChatMessage[], { isLoading = false, awaitingAnswer = false } = {}) {
  return renderStatus({ messages, isLoading, awaitingAnswer }).result.current;
}

describe('useThreadStatusValue', () => {
  it('puts a running plan in the tray and leaves it out of the transcript', () => {
    const { status, plansUnderMessages } = readStatus(transcript('completed', 'in_progress'), { isLoading: true });
    expect(status.contents?.plan).toBe(status.latestPlan?.steps);
    expect(status.latestPlan?.state).toBe('live');
    expect(plansUnderMessages.size).toBe(0);
  });

  it('keeps an unfinished plan waiting while the turn waits on the reader', () => {
    const { status, plansUnderMessages } = readStatus(transcript('completed', 'in_progress'), { awaitingAnswer: true });
    expect(status.latestPlan?.state).toBe('waiting');
    expect(status.work.awaitingAnswer).toBe(true);
    expect(plansUnderMessages.get('u1')?.state).toBe('waiting');
    expect(status.contents).toBeNull();
  });

  it('ends it once nothing will bring the agent back', () => {
    const { status } = readStatus(transcript('completed', 'in_progress'));
    expect(status.latestPlan?.state).toBe('ended');
  });

  it('keeps its identity across a render that changes nothing, and moves when the work does', () => {
    const messages = transcript('completed', 'in_progress');
    const { result, rerender } = renderStatus({ messages, isLoading: true });
    const first = result.current.status;
    rerender({ messages, isLoading: true });
    expect(result.current.status).toBe(first);
    // A new transcript whose plan is the same keeps it too.
    rerender({ messages: [...messages], isLoading: true });
    expect(result.current.status.latestPlan).toBe(first.latestPlan);
    rerender({ messages, isLoading: true, awaitingAnswer: true });
    expect(result.current.status).not.toBe(first);
    expect(result.current.status.work.counts.needs).toBe(1);
  });
});

function readOwed(owedKinds: OwedReportKind[] | null, isHome: boolean): string | null {
  return renderStatus({ reportBackOwed: true, owedKinds, isHome }).result.current.status.contents?.reportBack ?? null;
}

describe('useThreadStatusValue: an owed report is named by whose it is', () => {
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

  it('names none while a task no row names still runs', () => {
    expect(renderStatus({ reportBackOwed: true, hasActiveSubagents: true }).result.current.status.contents?.reportBack).toBeNull();
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

function analyst(status: RosterAnalyst['status'], running: boolean): RosterAnalyst {
  return { threadId: `t-${status}`, workspaceId: 'w-1', workspaceName: null, title: null, status, running };
}

const NO_WORK: TrayWork = { watch: null, wakeups: null, commands: null, tasks: null, analysts: null, reportBack: null };

function waits(work: Partial<TrayWork>, awaitingAnswer = false): boolean {
  const tray = { ...NO_WORK, ...work };
  return awaitsReport(threadWork(tray, awaitingAnswer), tray);
}

describe('awaitsReport: what keeps an unfinished plan waiting', () => {
  it('waits on a question the reader has not answered, with nothing else going', () => {
    expect(waits({}, true)).toBe(true);
    expect(waits({}, false)).toBe(false);
  });

  it('waits on an Analyst that needs the reader, which neither runs nor names a report', () => {
    expect(waits({ analysts: [analyst('needs_input', false)] })).toBe(true);
  });

  it('waits on running handed-off work, a pending wakeup or an owed report', () => {
    expect(waits({ tasks: { rows: [], unnamedRunning: true } })).toBe(true);
    expect(waits({ analysts: [analyst('running', true)] })).toBe(true);
    expect(waits({ reportBack: 'Waiting' })).toBe(true);
    expect(waits({ wakeups: { items: [wakeup({})], userTimezone: null } })).toBe(true);
  });

  it('ends once handed-off work has settled', () => {
    expect(waits({ analysts: [analyst('completed', false)] })).toBe(false);
  });

  it('leaves out a wakeup an earlier request set, as the close-out does', () => {
    const tray = { ...NO_WORK, wakeups: { items: [wakeup({ created_at: '2030-01-01T00:00:00Z' })], userTimezone: null } };
    const work = threadWork(tray, false);
    expect(awaitsReport(work, tray, Date.parse('2030-01-03T00:00:00Z'))).toBe(false);
    expect(awaitsReport(work, tray, Date.parse('2030-01-01T00:05:00Z'))).toBe(true);
  });
});
