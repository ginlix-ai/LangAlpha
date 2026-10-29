// @vitest-environment node
/**
 * A run can fail where no `error` frame can be written for it (its stream's
 * quota is spent), so its stream ends with a bare `run_end {outcome: "error"}`.
 * The turn settles as an internal failure on it: its thought stops ticking,
 * it is marked failed so Retry shows, and the banner says it failed. A failure
 * the turn already shows is not reported twice, and after a drop the turn
 * spans two bubbles: the one the dropped stream left, and the reconnect's own,
 * which names it. A hidden opener's reply that streamed nothing stays failed,
 * since it is the only place its turn's Retry can live.
 */
import { describe, expect, it, vi } from 'vitest';
import type { ChatMessage } from '@/types/chat';
import type { SSEEvent } from '../../types';
import { createStreamEventProcessor } from '../processStreamEvent';

type Processor = Parameters<typeof createStreamEventProcessor>;

const BUBBLE = 'a1';
const RECONNECT = 'assistant-reconnect-1';

const emptyBubble = (id: string, extra: Record<string, unknown> = {}) => ({
  id, role: 'assistant', content: '', isStreaming: true, contentSegments: [],
  reasoningProcesses: {}, toolCallProcesses: {}, ...extra,
}) as unknown as ChatMessage;

function setup({ banner: initialBanner = null, reply = {} }: { banner?: unknown; reply?: Record<string, unknown> } = {}) {
  let messages: ChatMessage[] = [emptyBubble(BUBBLE, reply)];
  const setMessages = (updater: (prev: ChatMessage[]) => ChatMessage[]) => {
    messages = updater(messages);
  };
  let banner: unknown = initialBanner;
  const setMessageError = vi.fn((next: unknown) => {
    banner = typeof next === 'function' ? (next as (prev: unknown) => unknown)(banner) : next;
  });
  const rt = {
    setMessages,
    queueMessages: setMessages,
    flushMessages: vi.fn(),
    updateSubagentCard: vi.fn(),
    lastEventIdRef: { current: null },
    threadIdRef: { current: 'thread-1' },
    threadId: 'thread-1',
    setThreadId: vi.fn(),
    pendingPTCBackfillRef: { current: new Map() },
    subagentHistory: { toolCalls: new Map() },
    setTokenUsage: vi.fn(),
    setMessageError,
    setFallbackSuggestion: vi.fn(),
    t: (k: string) => k,
  };
  const deps = {
    clearModelStatus: vi.fn(), armReportBack: vi.fn(), refreshWorkspaces: vi.fn(),
    readMessage: (id: string) => messages.find((m) => m.id === id),
  };
  const processor = (bubbleId: string, isReconnect: boolean) => createStreamEventProcessor(
    rt as unknown as Processor[0],
    deps as unknown as Processor[1],
    bubbleId,
    {
      contentOrderCounterRef: { current: 0 },
      currentReasoningIdRef: { current: null },
      currentToolCallIdRef: { current: null },
      subagentStateRefs: {},
      isReconnect,
    } as unknown as Processor[3],
    (event) => (event.agent as string) ?? null,
  );
  // What reconnectToStream does after a drop: the dropped bubble stops
  // streaming, and the run continues, from its backlog, in a processor of the
  // reconnect's own on a bubble that names the dropped one.
  const reconnect = () => {
    messages = [
      ...messages.map((m) => (m.id === BUBBLE ? { ...m, isStreaming: false } as ChatMessage : m)),
      emptyBubble(RECONNECT, { continues: BUBBLE }),
    ];
    return processor(RECONNECT, true);
  };
  const bubble = (id = BUBBLE) => messages.find((m) => m.id === id) as unknown as Record<string, unknown>;
  return { process: processor(BUBBLE, false), reconnect, bubble, banner: () => banner, setMessageError, rt, deps };
}

const runEnd = { event: 'run_end', thread_id: 'thread-1', run_id: 'run-1', outcome: 'error' } as SSEEvent;
const upstream = { event: 'error', error: 'Error code: 503', error_kind: 'upstream', status_code: 503 } as SSEEvent;
const PROSE = 'Bond yields rose';
const midThought = (process: (event: SSEEvent) => void) => {
  process({ event: 'message_chunk', content_type: 'text', content: PROSE } as SSEEvent);
  process({ event: 'message_chunk', content_type: 'reasoning_signal', content: 'start' } as SSEEvent);
  process({ event: 'message_chunk', content_type: 'reasoning', content: 'Checking the curve' } as SSEEvent);
};
const thoughtOf = (msg: Record<string, unknown>) =>
  Object.values(msg.reasoningProcesses as Record<string, Record<string, unknown>>)[0];

describe('live: the stream of a run that failed without an error frame', () => {
  it('settles the bubble as failed, keeps its text and closes its open thought', () => {
    const { process, bubble, banner } = setup();
    midThought(process);

    process(runEnd);

    expect(bubble()).toMatchObject({ isStreaming: false, error: true, content: PROSE });
    expect(bubble().structuredError).toBeUndefined();
    expect(thoughtOf(bubble()).isReasoning).toBe(false);
    expect(banner()).toEqual({ message: '', kind: 'internal' });
  });

  it('settles a bubble that streamed nothing without `error`, leaving the banner as the report', () => {
    const { process, bubble, banner } = setup();

    process(runEnd);

    expect(bubble().isStreaming).toBe(false);
    expect(bubble().error).toBeUndefined();
    expect(banner()).toEqual({ message: '', kind: 'internal' });
  });

  it('reports a failure the stream framed only once', () => {
    const { process, bubble, setMessageError } = setup();
    process({ event: 'message_chunk', content_type: 'text', content: PROSE } as SSEEvent);
    process(upstream);

    process(runEnd);

    expect(setMessageError.mock.calls).toEqual([[null]]);
    expect(bubble().structuredError).toMatchObject({ kind: 'upstream', statusCode: 503 });
  });

  it('puts the failure up over a notice and drops the switch suggestion', () => {
    const { process, reconnect, banner, rt, deps } = setup({ banner: { message: 'chat.busyReply', kind: 'notice' } });
    process({ event: 'message_chunk', content_type: 'text', content: PROSE } as SSEEvent);

    reconnect()(runEnd);

    expect(banner()).toEqual({ message: '', kind: 'internal' });
    expect(rt.setFallbackSuggestion).toHaveBeenCalledWith(null);
    expect(deps.clearModelStatus).toHaveBeenCalled();
  });
});

describe('reconnect: a drop between the stream and the run\'s end', () => {
  it('leaves a failure the dropped stream framed to its frame', () => {
    const { process, reconnect, bubble, setMessageError } = setup();
    process({ event: 'message_chunk', content_type: 'text', content: PROSE } as SSEEvent);
    process(upstream);

    reconnect()(runEnd);

    expect(setMessageError.mock.calls).toEqual([[null]]);
    expect(bubble()).toMatchObject({ error: true, structuredError: { kind: 'upstream', statusCode: 503 } });
    expect(bubble(RECONNECT).error).toBeUndefined();
  });

  it('adds no banner for an internal frame after a card the dropped stream showed', () => {
    const { process, reconnect, bubble, setMessageError } = setup();
    process({ event: 'message_chunk', content_type: 'text', content: PROSE } as SSEEvent);
    process({ event: 'error', error: 'Turn timed out after 21600 seconds', type: 'timeout_error' } as SSEEvent);

    const resumed = reconnect();
    resumed({ event: 'error', error: 'Stream ended unexpectedly', error_kind: 'internal' } as SSEEvent);
    resumed(runEnd);

    expect(setMessageError.mock.calls).toEqual([[null]]);
    expect(bubble()).toMatchObject({ error: true, structuredError: { message: 'Turn timed out after 21600 seconds' } });
  });

  it('settles the bubble the drop left mid-thought, not the reconnect\'s empty one', () => {
    const { process, reconnect, bubble, banner } = setup();
    midThought(process);

    reconnect()(runEnd);

    expect(bubble()).toMatchObject({ isStreaming: false, error: true, content: PROSE });
    expect(thoughtOf(bubble())).toMatchObject({ isReasoning: false, reasoningComplete: true });
    expect(bubble(RECONNECT)).toMatchObject({ isStreaming: false });
    expect(bubble(RECONNECT).error).toBeUndefined();
    expect(banner()).toEqual({ message: '', kind: 'internal' });
  });
});

describe('a hidden opener\'s reply that streamed nothing', () => {
  // The only bubble its turn has: hidden, the banner would be left with no
  // Retry and no message on screen to send again.
  const HIDDEN = { reply: { hiddenOpener: true } };
  const internalCard = { message: '', kind: 'internal' };

  it('stays failed when the run ends failed, with the card a reload draws', () => {
    const { process, bubble, banner } = setup(HIDDEN);

    process(runEnd);

    expect(bubble()).toMatchObject({ isStreaming: false, error: true, structuredError: internalCard });
    expect(banner()).toEqual({ message: '', kind: 'internal' });
  });

  it('stays failed on an internal error frame', () => {
    const { process, bubble, banner } = setup(HIDDEN);

    process({ event: 'error', error: 'worker lost', error_kind: 'internal' } as SSEEvent);

    expect(bubble()).toMatchObject({ isStreaming: false, error: true, structuredError: internalCard });
    expect(banner()).toEqual({ message: '', kind: 'internal' });
  });

  it('after a drop, fails the opener\'s reply rather than the reconnect\'s empty one', () => {
    const { reconnect, bubble } = setup(HIDDEN);

    reconnect()(runEnd);

    expect(bubble()).toMatchObject({ error: true, structuredError: internalCard });
    expect(bubble(RECONNECT)).toMatchObject({ isStreaming: false });
    expect(bubble(RECONNECT).error).toBeUndefined();
  });

  it('keeps what it streamed as its reply, the card left to the banner as for any turn', () => {
    const { process, bubble } = setup(HIDDEN);
    process({ event: 'message_chunk', content_type: 'text', content: PROSE } as SSEEvent);

    process(runEnd);

    expect(bubble()).toMatchObject({ error: true, content: PROSE });
    expect(bubble().structuredError).toBeUndefined();
  });
});
