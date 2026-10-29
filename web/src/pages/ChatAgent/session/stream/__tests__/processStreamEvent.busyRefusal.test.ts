// @vitest-environment node
/**
 * A send refused because the chat was busy arrives as an `admission_conflict`
 * frame whose words are the server's, written for the CLI ("use /reconnect
 * ... or /cancel"). Its failed reply says why in the view's words instead,
 * keyed by the code, and reads as a notice, since its Retry sends it again.
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
  const rt = {
    setMessages,
    queueMessages: setMessages,
    flushMessages: vi.fn(),
    setMessageError: vi.fn(),
    setFallbackSuggestion: vi.fn(),
    latestRunIdRef: { current: null },
    latestRunEpochRef: { current: 0 },
    lastEventIdRef: { current: null },
    threadIdRef: { current: 'thread-1' },
    threadId: 'thread-1',
    setThreadId: vi.fn(),
    t: (k: string) => k,
  };
  const deps = { clearModelStatus: () => {} };
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
    BUBBLE,
    refs as unknown as Processor[3],
    () => null,
  );
  const bubble = () => messages.find((m) => m.id === BUBBLE) as unknown as Record<string, unknown>;
  return { process, bubble };
}

const refusal = (code: string, error: string) => ({
  event: 'error', thread_id: 'thread-1', type: 'workflow_error', error_type: 'admission_conflict', code, error,
} as SSEEvent);

const CLI_WORDS = 'The turn is still running. Wait a moment, then retry — or use /reconnect to continue streaming, or /cancel to stop it.';

describe('live: a send refused because the chat was busy', () => {
  it.each([
    ['running', 'chat.busyRunningSend'],
    ['stopping', 'chat.busyStopping'],
    ['compacting', 'chat.busyCompacting'],
  ])('says the chat was %s in its own words, as a notice', (code, key) => {
    const { process, bubble } = setup();

    process(refusal(code, CLI_WORDS));

    expect(bubble()).toMatchObject({ error: true, content: '', structuredError: { message: key, kind: 'notice' } });
  });

  it('keeps the server\'s words for a refusal that is not about a busy chat', () => {
    const { process, bubble } = setup();

    process(refusal('turn_conflict', 'A concurrent request already wrote a different query at this turn; refresh the thread and resend.'));

    expect(bubble().content).toBe('');
    expect(bubble().structuredError).toEqual({ message: 'A concurrent request already wrote a different query at this turn; refresh the thread and resend.' });
  });
});
