/**
 * A hand-off the user approved in advance arms the report-back watch straight
 * from its tool_call_result (there was no approval click to arm it from). A
 * reconnect's replayed backlog arms it too: that backlog belongs to a run still
 * live, so its report-back is still owed, and an in-page reconnect does not
 * re-read /status to arm it any other way.
 */
import { describe, expect, it, vi } from 'vitest';
import type { SSEEvent } from '../../types';
import { createStreamEventProcessor } from '../processStreamEvent';

type Processor = Parameters<typeof createStreamEventProcessor>;

const preapprovedDispatchedContent = JSON.stringify({
  success: true,
  preapproved: true,
  status: 'dispatched',
  report_back: true,
  thread_id: 'thread-1',
  workspace_id: 'ws-1',
});

function setup(isReconnect: boolean) {
  const rt = {
    setMessages: vi.fn(),
    queueMessages: vi.fn(),
    flushMessages: vi.fn(),
    updateSubagentCard: vi.fn(),
    lastEventIdRef: { current: null },
    threadIdRef: { current: null },
    pendingPTCBackfillRef: { current: new Map() },
  };
  const deps = {
    clearModelStatus: () => {},
    armReportBack: vi.fn(),
    refreshWorkspaces: vi.fn(),
  };
  const refs = {
    contentOrderCounterRef: { current: 0 },
    currentReasoningIdRef: { current: null },
    currentToolCallIdRef: { current: null },
    subagentStateRefs: {},
    isReconnect,
  };
  const process = createStreamEventProcessor(
    rt as unknown as Processor[0],
    deps as unknown as Processor[1],
    'a1',
    refs as Processor[3],
    (event) => (event.agent as string) ?? null,
  );
  const toolCallResult = (content: string) =>
    process({ event: 'tool_call_result', tool_call_id: 'tc1', content } as SSEEvent);
  return { deps, toolCallResult };
}

describe('tool_call_result arms the report-back watch', () => {
  it('arms it for a live preapproved dispatched result', () => {
    const { deps, toolCallResult } = setup(false);
    toolCallResult(preapprovedDispatchedContent);
    expect(deps.armReportBack).toHaveBeenCalledOnce();
  });

  it('arms it on a reconnect replay too', () => {
    const { deps, toolCallResult } = setup(true);
    toolCallResult(preapprovedDispatchedContent);
    expect(deps.armReportBack).toHaveBeenCalledOnce();
  });

  it('does not arm it for a result that was not preapproved', () => {
    const { deps, toolCallResult } = setup(false);
    toolCallResult(JSON.stringify({ success: true, status: 'dispatched', report_back: true }));
    expect(deps.armReportBack).not.toHaveBeenCalled();
  });
});
