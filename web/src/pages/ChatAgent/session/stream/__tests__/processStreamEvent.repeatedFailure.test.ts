// @vitest-environment node
/**
 * A server older than this view ends a turn at its time limit with two frames:
 * `timeout_error`, which the view draws as the turn's card, and then the same
 * stop again as an internal error. A turn has one failure, so the second frame
 * adds no banner over the card.
 */
import { describe, expect, it, vi } from 'vitest';
import type { ChatMessage } from '@/types/chat';
import type { SSEEvent } from '../../types';
import { createStreamEventProcessor } from '../processStreamEvent';

type Processor = Parameters<typeof createStreamEventProcessor>;

const BUBBLE = 'a1';

function setup() {
  let messages: ChatMessage[] = [{
    id: BUBBLE, role: 'assistant', content: '', isStreaming: true, contentSegments: [],
    reasoningProcesses: {}, toolCallProcesses: {},
  } as unknown as ChatMessage];
  const setMessages = (updater: (prev: ChatMessage[]) => ChatMessage[]) => {
    messages = updater(messages);
  };
  let banner: unknown = null;
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
  const deps = { clearModelStatus: () => {}, armReportBack: vi.fn(), refreshWorkspaces: vi.fn() };
  const process = createStreamEventProcessor(
    rt as unknown as Processor[0],
    deps as unknown as Processor[1],
    BUBBLE,
    {
      contentOrderCounterRef: { current: 0 },
      currentReasoningIdRef: { current: null },
      currentToolCallIdRef: { current: null },
      subagentStateRefs: {},
      isReconnect: false,
    } as unknown as Processor[3],
    (event) => (event.agent as string) ?? null,
  );
  const bubble = () => messages.find((m) => m.id === BUBBLE) as unknown as Record<string, unknown>;
  return { process, bubble, banner: () => banner, setMessageError };
}

const PROSE = 'Bond yields rose';
const timeLimit = { event: 'error', error: 'Turn timed out after 21600 seconds', type: 'timeout_error' } as SSEEvent;
const internal = { event: 'error', error: 'Stream ended unexpectedly', error_kind: 'internal' } as SSEEvent;

describe('live: a turn framed as failed twice', () => {
  it('shows the time limit on the turn and no banner for the internal frame after it', () => {
    const { process, bubble, setMessageError } = setup();
    process({ event: 'message_chunk', content_type: 'text', content: PROSE } as SSEEvent);

    process(timeLimit);
    process(internal);

    expect(setMessageError.mock.calls).toEqual([[null]]);
    expect(bubble()).toMatchObject({ error: true, isStreaming: false, content: PROSE });
  });

  it('still banners an internal frame that is the turn\'s only failure', () => {
    const { process, banner } = setup();
    process({ event: 'message_chunk', content_type: 'text', content: PROSE } as SSEEvent);

    process(internal);

    expect(banner()).toMatchObject({ kind: 'internal' });
  });
});
