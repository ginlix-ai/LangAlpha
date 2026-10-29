// @vitest-environment node
/**
 * The thread's background commands change on the server, and the stream is
 * how a view hears it: a tool result whose artifact names the command it
 * moved to the background, a status read, which can stop one, and a report
 * that a command ended, whether it opened a turn or a live turn took it in.
 * Each re-reads the listing; nothing else does.
 */
import { describe, expect, it, vi } from 'vitest';
import type { ReportSource } from '@/types/api';
import type { SSEEvent } from '../../types';
import { createStreamEventProcessor } from '../processStreamEvent';

type Processor = Parameters<typeof createStreamEventProcessor>;

const COMMAND: ReportSource = {
  kind: 'command_report', command_id: 'bg-3f9a1c2e7d10', tool: 'Bash', description: 'Run 10-year backtest',
  status: 'completed', tool_call_id: 'call_1',
};
const TASK: ReportSource = { kind: 'task', task_id: 'k3', display_id: 'Task-k3', description: 'Pull filings', failed: false };

function setup() {
  const rt = {
    setMessages: vi.fn(),
    queueMessages: vi.fn(),
    flushMessages: vi.fn(),
    updateSubagentCard: vi.fn(),
    lastEventIdRef: { current: null },
    currentRunIdRef: { current: null },
    latestRunIdRef: { current: null },
    latestRunEpochRef: { current: 0 },
    currentMessageRef: { current: 'a1' },
    threadIdRef: { current: 'thread-1' },
    pendingPTCBackfillRef: { current: new Map() },
  };
  const deps = {
    clearModelStatus: () => {}, armReportBack: vi.fn(), refreshWorkspaces: vi.fn(), refreshWakeups: vi.fn(),
    refreshWorkspaceFolder: null, refreshCommands: vi.fn(),
  };
  const refs = {
    contentOrderCounterRef: { current: 0 },
    currentReasoningIdRef: { current: null },
    currentToolCallIdRef: { current: null },
    subagentStateRefs: {},
    steeringAtOrderRef: { current: null },
    isReconnect: false,
  };
  const process = createStreamEventProcessor(
    rt as unknown as Processor[0],
    deps as unknown as Processor[1],
    'a1',
    refs as unknown as Processor[3],
    () => null,
  );
  const toolCallResult = (content: string, artifact?: Record<string, unknown>) =>
    process({ event: 'tool_call_result', tool_call_id: 'tc1', content, artifact } as SSEEvent);
  return { deps, process, toolCallResult };
}

describe('the stream re-reads the thread\'s commands', () => {
  it('when a call moved its command to the background', () => {
    const { deps, toolCallResult } = setup();
    toolCallResult('Still running after 5m.', { command_id: 'bg-3f9a1c2e7d10' });
    expect(deps.refreshCommands).toHaveBeenCalledOnce();
    // Its end can start a turn the watch attaches; arming it owes nothing.
    expect(deps.armReportBack).toHaveBeenCalledExactlyOnceWith(null);
  });

  it('not for a call that returned as any call does, whatever its output names', () => {
    const { deps, toolCallResult } = setup();
    toolCallResult('prices.parquet\nfilings/\n');
    toolCallResult('bg-3f9a1c2e7d10.log\n', {});
    expect(deps.refreshCommands).not.toHaveBeenCalled();
    expect(deps.armReportBack).not.toHaveBeenCalled();
  });

  it('when a status read returns, which may have stopped one', () => {
    const { deps, process, toolCallResult } = setup();
    process({ event: 'tool_calls', tool_calls: [{ id: 'tc1', name: 'ThreadStatus', args: {} }] } as unknown as SSEEvent);
    toolCallResult('No commands, tasks, wakeups or analysts.');
    expect(deps.refreshCommands).toHaveBeenCalledOnce();
  });

  it('when a command\'s report opens a turn', () => {
    const { deps, process } = setup();
    process({ event: 'metadata', run_id: 'run-2', turn_index: 3, report_source: COMMAND } as unknown as SSEEvent);
    expect(deps.refreshCommands).toHaveBeenCalledOnce();
  });

  it('when a live turn takes a command\'s report in, and not a task\'s', () => {
    const { deps, process } = setup();
    process({ event: 'steering_delivered', messages: [{ content: '', report_source: TASK }] } as unknown as SSEEvent);
    expect(deps.refreshCommands).not.toHaveBeenCalled();
    process({ event: 'steering_delivered', messages: [{ content: '', report_source: COMMAND }] } as unknown as SSEEvent);
    expect(deps.refreshCommands).toHaveBeenCalledOnce();
  });
});
