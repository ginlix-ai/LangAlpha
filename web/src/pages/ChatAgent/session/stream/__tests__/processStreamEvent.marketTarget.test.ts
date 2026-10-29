// @vitest-environment node
/**
 * A price alert a running turn took in arrives as a `steering_delivered`
 * entry carrying its notice and no words. The transcript breaks where a
 * delivered steer breaks it: the bubble it interrupted settles, the alert's
 * row follows (never a user bubble), and the rest of the turn streams into a
 * new bubble on the same turn. The thread's targets are re-read, since the
 * one that fired changed state on the server. The rows are the fold's
 * (`steeringDelivery.test.ts`); this pins the stream around it.
 */
import { describe, expect, it, vi } from 'vitest';
import type { ChatMessage } from '@/types/chat';
import type { SteeringDeliveredEntry } from '@/types/sse';
import type { SSEEvent } from '../../types';
import { createStreamEventProcessor } from '../processStreamEvent';

type Processor = Parameters<typeof createStreamEventProcessor>;

// Neutral placeholder ticker and made-up prices only.
const NOTICE = {
  target_id: 'tg-1',
  notice: 'hit',
  symbol: 'ACME',
  kind: 'above',
  value: 205,
  pct_from: null,
  fired_price: 205.12,
  fired_at: '2030-01-01T15:05:00Z',
  expires_at: null,
} as const;
const ALERT: SteeringDeliveredEntry = { content: '', timestamp: 1_700_000_000, market_alert: NOTICE };
// The notice as a price wakeup's, which its row is drawn from.
const AS_WAKEUP = {
  wakeup_id: 'tg-1', kind: 'price', notice: 'fire', seq: 1, total: 1, fired_at: '2030-01-01T15:05:00Z',
  price: { symbol: 'ACME', kind: 'above', value: 205, pct_from: null, fired_price: 205.12 },
};

function setup(extra: Array<Record<string, unknown>> = []) {
  let messages = [
    { id: 'u0', role: 'user', content: 'watch ACME', turnIndex: 0 },
    {
      id: 'a1',
      role: 'assistant',
      content: '',
      isStreaming: true,
      turnIndex: 0,
      contentSegments: [],
      reasoningProcesses: {},
      toolCallProcesses: { tc1: { isInProgress: true, isComplete: false } },
    },
    ...extra,
  ] as unknown as ChatMessage[];
  const setMessages = (updater: (prev: ChatMessage[]) => ChatMessage[]) => {
    messages = updater(messages);
  };
  const rt = {
    setMessages,
    queueMessages: setMessages,
    flushMessages: vi.fn(),
    updateSubagentCard: vi.fn(),
    currentMessageRef: { current: 'a1' as string | null },
    lastEventIdRef: { current: null },
    threadIdRef: { current: 'thread-1' },
    pendingPTCBackfillRef: { current: new Map() },
  };
  const deps = { clearModelStatus: () => {}, refreshMarketWatch: vi.fn(), refreshWakeups: vi.fn() };
  const refs = {
    contentOrderCounterRef: { current: 4 },
    currentReasoningIdRef: { current: null },
    currentToolCallIdRef: { current: 'tc1' as string | null },
    subagentStateRefs: {},
    steeringAtOrderRef: { current: 7 as number | null },
    isReconnect: false,
  };
  const process = createStreamEventProcessor(
    rt as unknown as Processor[0],
    deps as unknown as Processor[1],
    'a1',
    refs as unknown as Processor[3],
    (event) => (event.agent as string) ?? null,
  );
  const deliver = (entries: SteeringDeliveredEntry[]) =>
    process({ event: 'steering_delivered', messages: entries } as unknown as SSEEvent);
  return { rt, deps, refs, process, deliver, read: () => messages as unknown as Array<Record<string, unknown>> };
}

describe('live: a price alert the turn took in', () => {
  it('settles the bubble, adds the alert row, and continues the turn in a new bubble', () => {
    const { rt, deps, refs, deliver, read } = setup();
    deliver([ALERT]);

    expect(deps.refreshWakeups).toHaveBeenCalledOnce();
    const [user, settled, alert, next] = read();
    expect(read()).toHaveLength(4);
    expect(user.id).toBe('u0');
    expect(settled).toMatchObject({ id: 'a1', isStreaming: false });
    expect((settled.toolCallProcesses as Record<string, unknown>).tc1).toMatchObject({ isInProgress: false, isComplete: true });
    expect(alert).toMatchObject({
      id: 'market-alert-tg-1:hit',
      role: 'notification',
      timestamp: new Date(1_700_000_000_000),
      wakeup: { source: 'delivered', legacy: true, notices: [AS_WAKEUP] },
    });
    expect(next).toMatchObject({ role: 'assistant', isSteering: true, isStreaming: true, turnIndex: 0 });
    expect(rt.currentMessageRef.current).toBe(next.id);
    expect(refs.contentOrderCounterRef.current).toBe(0);
    expect(refs.currentToolCallIdRef.current).toBeNull();
    // The user's own steer, still to come, keeps the boundary its acceptance set.
    expect(refs.steeringAtOrderRef.current).toBe(7);
  });

  it('streams the reply after the alert into the continuation', () => {
    const { process, deliver, read } = setup();
    deliver([ALERT]);
    process({ event: 'message_chunk', content_type: 'text', content: 'It crossed.', agent: 'main' } as SSEEvent);

    const rows = read();
    expect(rows.find((m) => m.id === 'a1')?.content).toBe('');
    expect(rows[3]).toMatchObject({ role: 'assistant', isSteering: true, content: 'It crossed.' });
  });

  it("draws the alert ahead of the user's words it was delivered with", () => {
    const { refs, deliver, read } = setup([
      { id: 's1', role: 'user', content: 'also check margins', steering: true, turnIndex: 0 },
    ]);
    deliver([ALERT, { content: 'also check margins' }]);

    expect(read().map((m) => m.id)).toEqual([
      'u0', 'a1', 'market-alert-tg-1:hit', 's1', expect.stringMatching(/^assistant-/),
    ]);
    expect(read()[3]).toMatchObject({ steering: false, steeringDelivered: true });
    expect(refs.steeringAtOrderRef.current).toBeNull();
  });

  it('draws one row for an alert that arrives twice', () => {
    const { deliver, read } = setup();
    deliver([ALERT]);
    deliver([ALERT]);
    expect(read().filter((m) => m.role === 'notification')).toHaveLength(1);
  });

  it('re-reads the wakeups but draws nothing for an alert naming no target', () => {
    const { deps, deliver, read } = setup();
    deliver([{ content: '', market_alert: { symbol: 'ACME' } } as unknown as SteeringDeliveredEntry]);
    expect(deps.refreshWakeups).toHaveBeenCalledOnce();
    expect(read().some((m) => m.role === 'notification')).toBe(false);
  });

  it('leaves the wakeups alone for a delivery with no alert in it', () => {
    const { deps, deliver } = setup();
    deliver([{ content: 'and margins' }]);
    expect(deps.refreshWakeups).not.toHaveBeenCalled();
  });
});

// A live stamp carries no targets, so one the agent arms mid-turn would show
// only at turn end without this re-read.
describe("live: the agent's watch_market call", () => {
  const call = (id: string, name: string) => ({
    event: 'tool_calls',
    agent: 'main',
    tool_calls: [{ id, name, args: { targets: [{ symbol: 'ACME', kind: 'above', value: 205 }] } }],
  } as unknown as SSEEvent);
  const result = (id: string) =>
    ({ event: 'tool_call_result', agent: 'main', tool_call_id: id, content: 'Targets set:\nACME above 205' } as unknown as SSEEvent);

  it('re-reads the targets once its result arrives', () => {
    const { deps, process } = setup();
    process(call('tc-w', 'watch_market'));
    expect(deps.refreshMarketWatch).not.toHaveBeenCalled();
    process(result('tc-w'));
    expect(deps.refreshMarketWatch).toHaveBeenCalledOnce();
    process(result('tc-w'));
    expect(deps.refreshMarketWatch).toHaveBeenCalledOnce();
  });

  it("leaves them alone for another tool's result", () => {
    const { deps, process } = setup();
    process(call('tc-q', 'get_quote'));
    process(result('tc-q'));
    expect(deps.refreshMarketWatch).not.toHaveBeenCalled();
  });
});

describe("live: the agent's schedule_wakeup call", () => {
  const call = (id: string) => ({
    event: 'tool_calls',
    agent: 'main',
    tool_calls: [{ id, name: 'schedule_wakeup', args: { triggers: [{ at: '2030-01-02T15:00' }] } }],
  } as unknown as SSEEvent);
  const result = (id: string) =>
    ({ event: 'tool_call_result', agent: 'main', tool_call_id: id, content: 'Set 1 wakeup.' } as unknown as SSEEvent);

  it('re-reads the wakeups once its result arrives', () => {
    const { deps, process } = setup();
    process(call('tc-s'));
    expect(deps.refreshWakeups).not.toHaveBeenCalled();
    process(result('tc-s'));
    expect(deps.refreshWakeups).toHaveBeenCalledOnce();
    process(result('tc-s'));
    expect(deps.refreshWakeups).toHaveBeenCalledOnce();
  });
});
