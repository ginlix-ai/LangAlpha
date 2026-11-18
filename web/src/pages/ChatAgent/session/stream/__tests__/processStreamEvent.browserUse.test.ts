/**
 * A call that opens the browser is seen before its command runs, so a view that
 * looked then found nothing. Its result is what says the browser is up, and it
 * tells the view to look again, for the agent's calls and a subagent's alike.
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
    onBrowserUse: vi.fn(),
    subagentHistory: { toolCalls: new Map() },
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
  return { rt, process };
}

const call = (id: string, command: string, extra: Record<string, unknown> = {}, agent?: string) => ({
  event: 'tool_calls',
  ...(agent && { agent }),
  tool_calls: [{ id, name: 'Bash', args: { command, ...extra } }],
}) as unknown as SSEEvent;

const result = (id: string, agent?: string) => ({
  event: 'tool_call_result',
  ...(agent && { agent }),
  tool_call_id: id,
  content: 'ok',
}) as unknown as SSEEvent;

describe('the browser is seen when its call is made and when it returns', () => {
  it('tells the view on the call and again on its result', () => {
    const { rt, process } = setup();
    process(call('b1', 'agent-browser open https://example.com'));
    expect(rt.onBrowserUse).toHaveBeenCalledTimes(1);
    process(result('b1'));
    expect(rt.onBrowserUse).toHaveBeenCalledTimes(2);
  });

  it('does the same for a subagent call', () => {
    const { rt, process } = setup();
    process(call('b1', 'agent-browser snapshot', {}, 'task:t1'));
    process(result('b1', 'task:t1'));
    expect(rt.onBrowserUse).toHaveBeenCalledTimes(2);
  });

  it('says nothing for another call, or one run on another computer', () => {
    const { rt, process } = setup();
    process(call('l1', 'ls'));
    process(result('l1'));
    process(call('b1', 'agent-browser open x', { computer: 'Mac mini' }));
    process(result('b1'));
    expect(rt.onBrowserUse).not.toHaveBeenCalled();
  });
});
