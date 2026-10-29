/**
 * An agent that creates a workspace, or hands work to one, changes rows the
 * sidebar and gallery cache. Nothing else refreshed them, so a workspace the
 * Chief of Staff created appeared only after a reload.
 */
import { describe, expect, it, vi } from 'vitest';
import type { SSEEvent } from '../../types';
import { createStreamEventProcessor } from '../processStreamEvent';

type Processor = Parameters<typeof createStreamEventProcessor>;

function setup() {
  const rt = {
    setMessages: vi.fn(),
    queueMessages: vi.fn(),
    flushMessages: vi.fn(),
    updateSubagentCard: vi.fn(),
    lastEventIdRef: { current: null },
    threadIdRef: { current: null },
    pendingPTCBackfillRef: { current: new Map() },
  };
  const deps = { clearModelStatus: () => {}, armReportBack: vi.fn(), refreshWorkspaces: vi.fn() };
  const refs = {
    contentOrderCounterRef: { current: 0 },
    currentReasoningIdRef: { current: null },
    currentToolCallIdRef: { current: null },
    subagentStateRefs: {},
    isReconnect: false,
  };
  const process = createStreamEventProcessor(
    rt as unknown as Processor[0],
    deps as unknown as Processor[1],
    'a1',
    refs as Processor[3],
    () => null,
  );
  const toolCallResult = (content: string) =>
    process({ event: 'tool_call_result', tool_call_id: 'tc1', content } as SSEEvent);
  return { deps, toolCallResult };
}

describe('tool_call_result refreshes the workspace lists', () => {
  it('refreshes them when an agent created a workspace', () => {
    const { deps, toolCallResult } = setup();
    toolCallResult(JSON.stringify({ success: true, workspace_id: 'ws-1', workspace_name: 'NVDA' }));
    expect(deps.refreshWorkspaces).toHaveBeenCalledOnce();
  });

  it('leaves them alone for a result that names no workspace or was declined', () => {
    const { deps, toolCallResult } = setup();
    toolCallResult(JSON.stringify({ success: true, thread_id: 'th-1' }));
    toolCallResult('User declined workspace creation. Unless they said to do the work here, do not do it yourself instead: ask them where they want it.');
    toolCallResult(JSON.stringify({ success: false, workspace_id: 'ws-1' }));
    expect(deps.refreshWorkspaces).not.toHaveBeenCalled();
  });
});
