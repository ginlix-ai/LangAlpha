/**
 * Tests the steering "demotion" path in handleSendSteering.
 *
 * When the user sends a follow-up while the agent is mid-stream, the UI
 * POSTs to the same endpoint and tags the user message as `steering: true`.
 * The backend usually replies with a ``steering_accepted`` SSE event and
 * the message is delivered mid-turn.
 *
 * RACE: if the agent's previous turn flipped to a terminal state between
 * the UI's ``isLoading`` snapshot and the POST landing, the backend opens
 * a *new* turn instead. The very first event the UI sees is then the
 * authoritative ``metadata`` frame for that fresh run (not
 * ``steering_accepted``). The hook must DEMOTE the steering bubble back to
 * a normal user message, append a fresh assistant placeholder, and route
 * all subsequent chunks into that new assistant — otherwise the new turn
 * would render on top of the previous turn's assistant bubble.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';

// ---------------------------------------------------------------------------
// Mocks — declared before any imports that depend on them
// ---------------------------------------------------------------------------

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (k: string) => k }),
}));

vi.mock('@/lib/supabase', () => ({ supabase: null }));

vi.mock('../utils/threadStorage', () => ({
  getStoredThreadId: vi.fn().mockReturnValue(null),
  setStoredThreadId: vi.fn(),
  removeStoredThreadId: vi.fn(),
}));

// The real handleTextContent mutates the assistant message via the setter
// it receives. We capture the calls so we can assert chunks were routed
// to the *new* (demoted) assistant message id, not the old one.
const textContentCalls: Array<{ assistantMessageId: string; text: string }> = [];

vi.mock('../utils/streamEventHandlers', () => ({
  handleReasoningSignal: vi.fn(),
  handleReasoningContent: vi.fn(),
  handleTextContent: vi.fn(({ event, assistantMessageId }) => {
    textContentCalls.push({
      assistantMessageId,
      text: (event?.content as string) || '',
    });
    return true;
  }),
  handleToolCalls: vi.fn(),
  handleToolCallResult: vi.fn(),
  handleToolCallChunks: vi.fn(),
  handleTodoUpdate: vi.fn(),
  isSubagentEvent: vi.fn().mockReturnValue(false),
  handleSubagentMessageChunk: vi.fn(),
  handleSubagentToolCallChunks: vi.fn(),
  handleSubagentToolCalls: vi.fn(),
  handleSubagentToolCallResult: vi.fn(),
  handleTaskSteeringAccepted: vi.fn(),
  getOrCreateTaskRefs: vi.fn().mockReturnValue({
    contentOrderCounterRef: { current: 0 },
    currentReasoningIdRef: { current: null },
    currentToolCallIdRef: { current: null },
  }),
}));

vi.mock('../utils/historyEventHandlers', () => ({
  handleHistoryUserMessage: vi.fn(),
  handleHistoryReasoningSignal: vi.fn(),
  handleHistoryReasoningContent: vi.fn(),
  handleHistoryTextContent: vi.fn(),
  handleHistoryToolCalls: vi.fn(),
  handleHistoryToolCallResult: vi.fn(),
  handleHistoryTodoUpdate: vi.fn(),
  handleHistorySteeringDelivered: vi.fn(),
  handleHistoryInterrupt: vi.fn(),
  handleHistoryArtifact: vi.fn(),
}));

vi.mock('../../utils/api', () => ({
  fetchMarketWatch: vi.fn().mockResolvedValue({ thread_id: 't', symbols: [] }),
  sendChatMessageStream: vi.fn(),
  sendHitlResponse: vi.fn(),
  replayThreadHistory: vi.fn().mockResolvedValue(undefined),
  getWorkflowStatus: vi.fn().mockResolvedValue({ can_reconnect: false, status: 'completed' }),
  reconnectToWorkflowStream: vi.fn(),
  streamSubagentTaskEvents: vi.fn(),
  fetchThreadTurns: vi.fn().mockResolvedValue({ turns: [], retry_checkpoint_id: null }),
  submitFeedback: vi.fn(),
  removeFeedback: vi.fn(),
  getThreadFeedback: vi.fn().mockResolvedValue([]),
  watchThread: vi.fn(),
}));

import { sendChatMessageStream } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';
import type { UserMessage, AssistantMessage } from '@/types/chat';

const mockSendStream = sendChatMessageStream as Mock;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type OnEvent = (e: Record<string, unknown>) => void;

interface Deferred<T> {
  promise: Promise<T>;
  resolve: (v: T) => void;
}

function deferred<T>(): Deferred<T> {
  let resolve!: (v: T) => void;
  const promise = new Promise<T>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

describe('useChatMessages — handleSendSteering demotion path', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    textContentCalls.length = 0;
  });

  it('first non-steering event demotes the steering bubble to a new turn', async () => {
    // First call: hang forever so isLoading stays true while we trigger
    // the second (steering) POST. We capture the onEvent callback in case
    // we need to release it, but the test only needs the side effects of
    // the SECOND call's events.
    const firstHang = deferred<{ disconnected: boolean }>();
    let secondOnEvent: OnEvent | null = null;

    mockSendStream.mockImplementation(
      async (
        _msg: string,
        _ws: string,
        _tid: string | null,
        _hist: unknown[],
        _plan: boolean,
        onEvent: OnEvent,
      ) => {
        if (mockSendStream.mock.calls.length === 1) {
          // Emit thread_id so subsequent history-load short-circuits.
          onEvent({ event: 'thread_id', thread_id: 'thread-demote-1' });
          return firstHang.promise;
        }
        // Second call = the steering POST. Capture the callback for the
        // test to drive directly.
        secondOnEvent = onEvent;
        return { disconnected: false };
      },
    );

    const { result } = renderHookWithProviders(() =>
      useChatMessages('ws-demote'),
    );

    // Fire the first message and let `isLoading` flip to true.
    let firstSend: Promise<unknown> = Promise.resolve();
    await act(async () => {
      firstSend = result.current.handleSendMessage('first turn', false);
      // Yield once so the synchronous setIsLoading flush propagates.
      await Promise.resolve();
    });

    await waitFor(() => {
      expect(result.current.isLoading).toBe(true);
    });

    // Second send while still streaming → routes to handleSendSteering.
    let steeringSend: Promise<unknown> = Promise.resolve();
    await act(async () => {
      steeringSend = result.current.handleSendMessage('follow-up steering', false);
      // Let the handler kick off and the mock implementation run.
      await Promise.resolve();
      await Promise.resolve();
    });

    // Two POSTs total now (original + steering).
    expect(mockSendStream).toHaveBeenCalledTimes(2);
    expect(secondOnEvent).not.toBeNull();

    // The user message added by handleSendSteering should be tagged
    // ``steering: true`` initially.
    await waitFor(() => {
      const userMsgs = result.current.messages.filter(
        (m): m is UserMessage => m.role === 'user',
      );
      // Two user messages: the original send and the steering send.
      expect(userMsgs.length).toBeGreaterThanOrEqual(2);
      const steeringUser = userMsgs[userMsgs.length - 1];
      expect((steeringUser as UserMessage & { steering?: boolean }).steering).toBe(true);
    });

    // Snapshot the assistants BEFORE demotion fires, so we can detect the
    // newly-appended one.
    const assistantsBefore = result.current.messages.filter(
      (m) => m.role === 'assistant',
    );

    // First non-steering event from the steering stream = backend opened a
    // new turn instead of accepting steering mid-flight. Per the SSE
    // protocol, the very first frame is ``metadata`` carrying the new run_id.
    await act(async () => {
      secondOnEvent!({
        event: 'metadata',
        thread_id: 'thread-demote-1',
        run_id: 'run-demoted-1',
      });
      // A subsequent message_chunk should render into the *new* assistant.
      secondOnEvent!({
        event: 'message_chunk',
        role: 'assistant',
        content: 'demoted reply',
        content_type: 'text',
        agent: 'main',
        id: 'msg-demoted-1',
      });
      await Promise.resolve();
    });

    // ---- (a) the user message is no longer flagged as steering ---------
    await waitFor(() => {
      const userMsgs = result.current.messages.filter(
        (m): m is UserMessage => m.role === 'user',
      );
      const steeringUser = userMsgs[userMsgs.length - 1] as UserMessage & {
        steering?: boolean;
      };
      expect(steeringUser.steering).toBeFalsy();
    });

    // ---- (b) a new assistant placeholder was appended ------------------
    const assistantsAfter = result.current.messages.filter(
      (m): m is AssistantMessage => m.role === 'assistant',
    );
    expect(assistantsAfter.length).toBe(assistantsBefore.length + 1);
    const demotedAssistant = assistantsAfter[assistantsAfter.length - 1];
    // The demoted assistant must be a fresh id, not the original one.
    if (assistantsBefore.length > 0) {
      expect(demotedAssistant.id).not.toBe(assistantsBefore[0].id);
    }

    // ---- (c) message_chunk content was routed into the NEW assistant ---
    // We mocked handleTextContent so it records the target assistantMessageId.
    expect(textContentCalls.length).toBeGreaterThan(0);
    const routedTo = textContentCalls[textContentCalls.length - 1].assistantMessageId;
    expect(routedTo).toBe(demotedAssistant.id);

    // Cleanup: release the first hang so the original POST resolves and
    // the awaiting handlers finish without leaking timers.
    firstHang.resolve({ disconnected: false });
    await act(async () => {
      await firstSend.catch(() => undefined);
      await steeringSend.catch(() => undefined);
    });
  });
});
