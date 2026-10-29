/**
 * Subagents in a reply: a lone one as a row, two or more as a group whose
 * header folds a row each. Pins what each state says, what opens what, the
 * fold rule, the model badge, and how the run reads while the server's times
 * are absent.
 */
import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen, within } from '@testing-library/react';
import '@testing-library/jest-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import type { SubagentTaskRecord } from '@/types/chat';
import { SubagentTelemetryContext } from '../../SubagentTelemetryContext';
import type { SubagentTelemetry } from '../../../session/subagents/resolveSubagentTelemetry';
import { SubagentRun, type SubagentRunMember } from '../SubagentRun';
import { MessageContentSegments } from '../MessageContentSegments';
import { MessageActionsProvider } from '../MessageActionsContext';
import type { ToolCallProcessRecord } from '../types';

const NOW = Date.UTC(2026, 9, 21, 14, 0, 0);
const ZERO = { input: 0, output: 0, total: 0 };
const BEGUN = { tokenUsage: { input: 10, output: 5, total: 15 } };

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['Date'] });
  vi.setSystemTime(NOW);
});
afterEach(() => {
  vi.useRealTimers();
});

function task(id: string, over: Partial<SubagentTaskRecord> = {}): SubagentTaskRecord {
  return {
    subagentId: id,
    description: `Research ${id}`,
    prompt: '',
    type: 'research',
    action: 'init',
    status: 'running',
    ...over,
  };
}

type Telemetry = Partial<SubagentTelemetry> | undefined;

function member(id: string, over: Partial<SubagentTaskRecord> = {}, createdAt?: number): SubagentRunMember {
  return {
    subagentId: id,
    task: task(id, over),
    toolCallProcess: createdAt == null ? undefined : ({ _createdAt: createdAt } as ToolCallProcessRecord),
  };
}

function ui(
  members: SubagentRunMember[],
  telemetry: Record<string, Telemetry> = {},
  { onOpen, pausing, follows = true }: {
    onOpen?: (info: unknown) => void;
    pausing?: boolean;
    follows?: boolean;
  } = {},
): React.ReactElement {
  const resolve = (id: string): SubagentTelemetry | undefined => {
    const t = telemetry[id];
    return t ? { toolCalls: 0, tokenUsage: ZERO, ...t } : undefined;
  };
  return (
    <SubagentTelemetryContext.Provider value={{ resolve, followsRuns: follows }}>
      <SubagentRun members={members} pausing={pausing} onOpen={onOpen} />
    </SubagentTelemetryContext.Provider>
  );
}

const detail = (): string => (screen.queryByTestId('subagent-row-detail')?.textContent ?? '').trim();
const rowStatus = (): string => screen.getByTestId('subagent-row-status').textContent ?? '';
const elapsed = (): string | null => screen.queryByTestId('subagent-row-elapsed')?.textContent ?? null;

describe('a lone subagent', () => {
  it('reads as starting up until the run shows a sign of life', () => {
    render(ui([member('a')]));
    expect(screen.getAllByText('Starting up').length).toBeGreaterThan(0);
    expect(screen.queryByRole('status')).toBeNull();
    // No time is known, so none is shown.
    expect(elapsed()).toBeNull();
  });

  it("times a live row from its launch when the server's start has not arrived", () => {
    render(ui([member('a', {}, NOW - 5_000)]));
    expect(elapsed()).toBe('0:05');
  });

  it('ignores the reconnect sentinel, which is not a time', () => {
    render(ui([member('a', {}, 1)]));
    expect(elapsed()).toBeNull();
  });

  it('says what a working run is doing, with the live glyph and its clock', () => {
    render(ui([member('a')], {
      a: { ...BEGUN, startedAtMs: NOW - 65_000, currentToolCall: { toolName: 'WebSearch', args: { query: 'AAPL margins' } } },
    }));
    expect(screen.getByText('Running')).toBeInTheDocument();
    expect(detail()).not.toBe('');
    expect(detail()).not.toBe('Running');
    expect(elapsed()).toBe('1:05');
  });

  it('leaves the second line to the status when the current call is not known', () => {
    render(ui([member('a')], { a: BEGUN }));
    expect(screen.getByText('Running')).toBeInTheDocument();
    expect(detail()).toBe('');
  });

  it('reads as finishing its step while the turn holds an unanswered credit pause', () => {
    render(ui([member('a')], { a: BEGUN }, { pausing: true }));
    expect(screen.getByText('Finishing current step')).toBeInTheDocument();
  });

  it('shows the first line of what a finished run said, and how long it took', () => {
    render(ui([member('a', { status: 'completed' })], {
      a: {
        ...BEGUN,
        startedAtMs: NOW - 300_000,
        finishedAtMs: NOW - 42_000,
        lastText: '## Summary\n\n**Apple** grew services revenue 14% year on year.\n\nMore below.',
      },
    }));
    expect(detail()).toBe('Apple grew services revenue 14% year on year.');
    expect(elapsed()).toBe('4:18');
    expect(screen.queryByRole('status')).toBeNull();
  });

  it('shows no time for a settled run whose times were never kept', () => {
    render(ui([member('a', { status: 'completed' }, NOW - 10_000)], { a: BEGUN }));
    expect(rowStatus()).toBe('Finished');
    expect(detail()).toBe('');
    expect(elapsed()).toBeNull();
  });

  it('states why a stopped run stopped, clamped', () => {
    render(ui([member('a', { status: 'cancelled' })], {
      a: { ...BEGUN, stopReason: 'x'.repeat(400) },
    }));
    expect(detail().length).toBeLessThan(400);
    expect(detail().endsWith('…')).toBe(true);
    expect(screen.getByText('Stopped')).toBeInTheDocument();
  });

  it('states why a failed run failed', () => {
    render(ui([member('a', { status: 'error' })], {
      a: { ...BEGUN, stopReason: 'transport_lost: the stream tore mid-run' },
    }));
    expect(detail()).toContain('transport_lost');
  });

  it('leaves a credit stop to the notice at the foot of the message', () => {
    render(ui([member('a', { status: 'cancelled' })], {
      a: { ...BEGUN, stopReason: 'Monthly credit limit reached', stopReasonType: 'credit_stop' },
    }));
    expect(rowStatus()).toBe('Stopped');
    expect(detail()).toBe('');
  });

  it('reads a follow-up by its verb, and opens the task it was sent to', () => {
    const onOpen = vi.fn();
    render(ui([member('a', { action: 'update', resumeTargetId: 'task:T1' })], {}, { onOpen }));
    expect(rowStatus()).toBe('Updated');
    fireEvent.click(screen.getByRole('button'));
    expect(onOpen).toHaveBeenCalledWith(expect.objectContaining({ subagentId: 'task:T1' }));
  });

  it('reads a resume as resumed', () => {
    render(ui([member('a', { action: 'resume', resumeTargetId: 'task:T1' })]));
    expect(rowStatus()).toBe('Resumed');
  });

  it('says how a resumed run went wrong, and why', () => {
    const failed = render(ui([member('a', { action: 'resume', status: 'error' })], {
      a: { ...BEGUN, stopReason: 'Sandbox unreachable' },
    }));
    expect(rowStatus()).toBe('Failed');
    expect(detail()).toBe('Sandbox unreachable');
    failed.unmount();

    render(ui([member('a', { action: 'resume', status: 'cancelled' })]));
    expect(rowStatus()).toBe('Stopped');
  });

  it("keeps a running resume live where runs are not followed, as a launch's record keeps it", () => {
    render(ui([member('a', { action: 'resume' }, NOW - 5_000)], {}, { follows: false }));
    expect(rowStatus()).toBe('Resumed');
    expect(elapsed()).toBe('0:05');
  });

  it('opens its subagent from a button named for it, described by its state', () => {
    const onOpen = vi.fn();
    render(ui([member('a', { status: 'completed' })], {
      a: { ...BEGUN, lastText: 'Margins held at 46%.' },
    }, { onOpen }));
    const row = screen.getByRole('button', { name: 'Open subagent: Research a' });
    expect(row).toHaveAccessibleDescription('Finished Margins held at 46%.');
    fireEvent.keyDown(row, { key: 'Enter' });
    fireEvent.click(row);
    expect(onOpen).toHaveBeenCalledWith({
      subagentId: 'a', description: 'Research a', type: 'research', status: 'completed',
    });
  });

  it('says how long a settled run took, but not a clock still ticking', () => {
    const settled = render(ui([member('a', { status: 'completed' })], {
      a: { ...BEGUN, lastText: 'Margins held.', startedAtMs: NOW - 65_000, finishedAtMs: NOW - 5_000 },
    }, { onOpen: vi.fn() }));
    expect(screen.getByRole('button')).toHaveAccessibleDescription('Finished Margins held. 1:00');
    settled.unmount();

    render(ui([member('a')], { a: { ...BEGUN, startedAtMs: NOW - 65_000 } }, { onOpen: vi.fn() }));
    expect(elapsed()).toBe('1:05');
    expect(screen.getByRole('button')).toHaveAccessibleDescription('Running');
  });

  it('renders inert on a read-only surface: no button, no focus stop', () => {
    const { container } = render(ui([member('a', { status: 'completed' })]));
    expect(screen.queryByRole('button')).toBeNull();
    expect(container.querySelector('[tabindex]')).toBeNull();
  });

  it('draws no chevron, whether the row opens or not', () => {
    for (const onOpen of [vi.fn(), undefined]) {
      const { container, unmount } = render(ui([member('a', { status: 'completed' })], {}, { onOpen }));
      expect(screen.queryAllByRole('button')).toHaveLength(onOpen ? 1 : 0);
      expect(container.querySelector('svg.lucide-chevron-right')).toBeNull();
      unmount();
    }
  });
});

describe('a subagent on another model', () => {
  const withCatalog = (node: React.ReactElement): React.ReactElement => {
    const client = new QueryClient();
    client.setQueryData(queryKeys.models.all, {
      model_metadata: { 'claude-sonnet-4-6': { display_name: 'Claude Sonnet 4.6' } },
    });
    return <QueryClientProvider client={client}>{node}</QueryClientProvider>;
  };

  it('badges its tile with the provider, and says the model in words', () => {
    render(withCatalog(ui([member('a', { status: 'completed' })], {
      a: { ...BEGUN, lastText: 'Margins held.', model: { id: 'claude-sonnet-4-6', provider: 'anthropic' } },
    }, { onOpen: vi.fn() })));
    expect(screen.getByTestId('subagent-model-badge')).toHaveAttribute('title', 'Claude Sonnet 4.6');
    expect(screen.getByRole('button', { name: 'Open subagent: Research a' }))
      .toHaveAccessibleDescription('Finished Margins held. Model: Claude Sonnet 4.6');
  });

  it('draws no badge, and says nothing of it, for a provider whose mark we do not ship', () => {
    render(withCatalog(ui([member('a', { status: 'completed' })], {
      a: { ...BEGUN, lastText: 'Margins held.', model: { id: 'x-1', provider: 'nobody' } },
    }, { onOpen: vi.fn() })));
    expect(screen.queryByTestId('subagent-model-badge')).toBeNull();
    expect(screen.getByRole('button')).toHaveAccessibleDescription('Finished Margins held.');
  });
});

describe('a group of subagents', () => {
  const header = (): HTMLElement => screen.getByRole('button', { name: /\d+ subagents/ });
  const status = (): string => screen.getByTestId('run-group-tally').textContent ?? '';

  function liveTelemetry(): Record<string, Telemetry> {
    return {
      a: { ...BEGUN, startedAtMs: NOW - 120_000 },
      b: { ...BEGUN, startedAtMs: NOW - 115_000, finishedAtMs: NOW - 30_000 },
    };
  }
  const liveMembers = () => [member('a'), member('b', { type: 'data-prep', status: 'completed' })];

  it('says how many and how they are doing, open while one works', () => {
    render(ui(liveMembers(), liveTelemetry(), { onOpen: vi.fn() }));
    const head = screen.getByRole('button', { name: /2 subagents/ });
    expect(head).toHaveAttribute('aria-expanded', 'true');
    expect(status()).toMatch(/1 working.*1 done/);
    expect(screen.getAllByTestId('subagent-row')).toHaveLength(2);
  });

  it('lists every member when none has a time, with no clock to show', () => {
    render(ui([member('a'), member('b')], { a: BEGUN, b: BEGUN }));
    expect(screen.getAllByTestId('subagent-row')).toHaveLength(2);
    expect(screen.queryByTestId('subagent-row-elapsed')).toBeNull();
  });

  it('opens a member from its row, and leaves a refused launch inert', () => {
    const onOpen = vi.fn();
    render(ui(
      [member('a'), member('b', { status: 'error', launchFailed: true, result: 'Refused: subagents are off.' })],
      liveTelemetry(),
      { onOpen },
    ));
    const rows = screen.getAllByTestId('subagent-row');
    expect(rows[0].tagName).toBe('BUTTON');
    expect(rows[1].tagName).toBe('DIV');
    expect(rows[1]).toHaveTextContent("Couldn't start");
    expect(rows[1]).toHaveTextContent('Subagents are off.');
    fireEvent.click(rows[0]);
    expect(onOpen).toHaveBeenCalledWith(expect.objectContaining({ subagentId: 'a' }));
  });

  it('starts folded when it mounts already settled, and says how long it took', () => {
    render(ui(
      [member('a', { status: 'completed' }), member('b', { status: 'error' })],
      {
        a: { ...BEGUN, startedAtMs: NOW - 600_000, finishedAtMs: NOW - 400_000 },
        b: { ...BEGUN, startedAtMs: NOW - 590_000, finishedAtMs: NOW - 342_000 },
      },
    ));
    expect(header()).toHaveAttribute('aria-expanded', 'false');
    expect(screen.queryByTestId('subagent-row')).toBeNull();
    expect(status()).toMatch(/Finished in 4m 18s.*1 failed/);
    expect(status()).not.toMatch(/done/);
  });

  it('says only that it finished when a member has no finish time', () => {
    render(ui([member('a', { status: 'completed' }), member('b', { status: 'completed' })], {
      a: { ...BEGUN, startedAtMs: NOW - 60_000 },
      b: BEGUN,
    }));
    expect(status()).toBe('Finished');
  });

  it('folds itself once every member finishes', () => {
    const { rerender } = render(ui(liveMembers(), liveTelemetry()));
    expect(header()).toHaveAttribute('aria-expanded', 'true');
    rerender(ui(
      [member('a', { status: 'completed' }), member('b', { status: 'completed' })],
      { ...liveTelemetry(), a: { ...BEGUN, startedAtMs: NOW - 120_000, finishedAtMs: NOW } },
    ));
    expect(header()).toHaveAttribute('aria-expanded', 'false');
  });

  it('keeps a fold set by hand when the run settles', () => {
    const { rerender } = render(ui(liveMembers(), liveTelemetry()));
    fireEvent.click(header());
    expect(header()).toHaveAttribute('aria-expanded', 'false');
    rerender(ui(liveMembers(), liveTelemetry()));
    expect(header()).toHaveAttribute('aria-expanded', 'false');

    fireEvent.click(header());
    rerender(ui(
      [member('a', { status: 'completed' }), member('b', { status: 'completed' })],
      { ...liveTelemetry(), a: { ...BEGUN, startedAtMs: NOW - 120_000, finishedAtMs: NOW } },
    ));
    // Opened by hand, it stays open after the last member finishes.
    expect(header()).toHaveAttribute('aria-expanded', 'true');
  });

  it('stays open while a resume\'s run goes on, and never says a group of updates finished', () => {
    const { rerender } = render(ui(
      [member('a', { action: 'resume' }), member('b', { action: 'update' })],
      { a: { ...BEGUN, startedAtMs: NOW - 20_000, running: true }, b: { ...BEGUN, startedAtMs: NOW - 3_600_000, finishedAtMs: NOW - 3_000_000 } },
    ));
    expect(header()).toHaveAttribute('aria-expanded', 'true');
    // Timed from the resume's run alone: the update started none.
    expect(status()).toMatch(/1 updated.*1 resumed.*0:20/);

    rerender(ui([member('a', { action: 'update' }), member('b', { action: 'update' })], {}));
    expect(status()).toBe('2 updated');
  });

  it('gives focus on a row to the header when it folds itself', () => {
    const { rerender } = render(ui(liveMembers(), liveTelemetry(), { onOpen: vi.fn() }));
    screen.getAllByTestId('subagent-row')[0].focus();
    rerender(ui(
      [member('a', { status: 'completed' }), member('b', { status: 'completed' })],
      { ...liveTelemetry(), a: { ...BEGUN, startedAtMs: NOW - 120_000, finishedAtMs: NOW } },
      { onOpen: vi.fn() },
    ));
    expect(header()).toHaveFocus();
  });

  it('has the header control the rows it folds', () => {
    render(ui(liveMembers(), liveTelemetry()));
    const controls = header().getAttribute('aria-controls');
    expect(controls).toBeTruthy();
    expect(within(document.getElementById(controls!)!).getAllByTestId('subagent-row')).toHaveLength(2);
  });
});

describe('in the transcript', () => {
  type SegmentsProps = React.ComponentProps<typeof MessageContentSegments>;
  const props = (ids: string[]): SegmentsProps => ({
    segments: ids.map((id, i) => ({ type: 'subagent_task', order: i, subagentId: id })) as SegmentsProps['segments'],
    reasoningProcesses: {},
    toolCallProcesses: {},
    todoListProcesses: {},
    subagentTasks: Object.fromEntries(ids.map((id) => [id, task(id)])),
    isStreaming: true,
  });
  const actions = { onOpenSubagentTask: vi.fn() };
  const segments = (p: SegmentsProps): React.ReactElement => (
    <MessageActionsProvider actions={actions}>
      <MessageContentSegments {...p} />
    </MessageActionsProvider>
  );

  it('grows a lone row into a group without remounting it', () => {
    const { rerender } = render(segments(props(['a'])));
    const first = screen.getByTestId('subagent-run');
    expect(screen.getByTestId('subagent-row').tagName).toBe('BUTTON');

    rerender(segments(props(['a', 'b'])));
    expect(screen.getByTestId('subagent-run')).toBe(first);
    expect(screen.getByTestId('subagent-group')).toBeInTheDocument();
  });

  it('renders every row inert when the surface is read-only', () => {
    render(segments({ ...props(['a']), readOnly: true }));
    expect(screen.queryByRole('button')).toBeNull();
  });
});
