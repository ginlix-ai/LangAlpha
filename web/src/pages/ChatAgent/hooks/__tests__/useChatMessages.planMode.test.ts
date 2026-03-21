/**
 * Tests that plan mode is preserved across AskUserQuestion interrupts.
 *
 * When the user enables plan mode and the agent asks a question (via AskUserQuestion
 * interrupt), answering or skipping the question must resume the workflow with
 * plan_mode: true so the backend rebuilds the graph with the SubmitPlan tool.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';

// ---------------------------------------------------------------------------
// Mocks – declared before any imports that depend on them
// ---------------------------------------------------------------------------

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (k: string) => k }),
}));

vi.mock('@/lib/supabase', () => ({ supabase: null }));

// Thread storage – no-op
vi.mock('../utils/threadStorage', () => ({
  getStoredThreadId: vi.fn().mockReturnValue(null),
  setStoredThreadId: vi.fn(),
  removeStoredThreadId: vi.fn(),
}));

// Stream event handlers – stubs that don't touch messages
vi.mock('../utils/streamEventHandlers', () => ({
  handleReasoningSignal: vi.fn(),
  handleReasoningContent: vi.fn(),
  handleTextContent: vi.fn(),
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

// The API module – the core of what we're testing
vi.mock('../../utils/api', () => ({
  sendChatMessageStream: vi.fn(),
  sendHitlResponse: vi.fn(),
  replayThreadHistory: vi.fn(),
  getWorkflowStatus: vi.fn(),
  reconnectToWorkflowStream: vi.fn(),
  streamSubagentTaskEvents: vi.fn(),
  fetchThreadTurns: vi.fn(),
  submitFeedback: vi.fn(),
  removeFeedback: vi.fn(),
  getThreadFeedback: vi.fn(),
}));

import { sendChatMessageStream, sendHitlResponse } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';

const mockSendStream = sendChatMessageStream as Mock;
const mockSendHitl = sendHitlResponse as Mock;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Mock `sendChatMessageStream` to capture the `onEvent` callback and call it
 * with the provided events before resolving. This simulates the SSE stream.
 */
function mockStreamWithEvents(events: Record<string, unknown>[]) {
  mockSendStream.mockImplementation(
    async (_msg: string, _ws: string, _tid: string | null, _hist: unknown[], _plan: boolean, onEvent: (e: Record<string, unknown>) => void) => {
      for (const e of events) onEvent(e);
      return { disconnected: false };
    },
  );
}

/** Build a workflow_interrupt event that looks like an AskUserQuestion. */
function makeAskUserQuestionInterrupt(interruptId: string, _questionId?: string) {
  return {
    event: 'interrupt',
    interrupt_id: interruptId,
    action_requests: [
      {
        type: 'ask_user_question',
        question: 'What stocks are you interested in?',
        options: [],
        allow_multiple: false,
      },
    ],
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('useChatMessages – plan mode across AskUserQuestion interrupts', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    // sendHitlResponse resolves immediately (empty stream)
    mockSendHitl.mockResolvedValue({ disconnected: false });
  });

  it('handleAnswerQuestion forwards plan mode when answering a question', async () => {
    const interruptId = 'int-ask-1';

    // 1. Stream delivers an AskUserQuestion interrupt
    mockStreamWithEvents([makeAskUserQuestionInterrupt(interruptId)]);

    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    // 2. Send a message with planMode=true → sets currentPlanModeRef.current = true
    //    and the mock stream fires the interrupt event
    await act(async () => {
      await result.current.handleSendMessage('Analyze AAPL', true);
    });

    // 3. Answer the question — should resume with plan_mode forwarded
    await act(async () => {
      result.current.handleAnswerQuestion('AAPL and MSFT', interruptId, interruptId);
    });

    // 4. Verify sendHitlResponse was called with planMode = true (5th arg)
    await waitFor(() => {
      expect(mockSendHitl).toHaveBeenCalledTimes(1);
    });
    const planModeArg = mockSendHitl.mock.calls[0][4];
    expect(planModeArg).toBe(true);
  });

  it('handleSkipQuestion forwards plan mode when skipping a question', async () => {
    const interruptId = 'int-ask-2';

    mockStreamWithEvents([makeAskUserQuestionInterrupt(interruptId)]);

    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    await act(async () => {
      await result.current.handleSendMessage('Analyze AAPL', true);
    });

    await act(async () => {
      result.current.handleSkipQuestion(interruptId, interruptId);
    });

    await waitFor(() => {
      expect(mockSendHitl).toHaveBeenCalledTimes(1);
    });
    const planModeArg = mockSendHitl.mock.calls[0][4];
    expect(planModeArg).toBe(true);
  });

  it('handleAnswerQuestion sends plan_mode=false when plan mode is not active', async () => {
    const interruptId = 'int-ask-3';

    mockStreamWithEvents([makeAskUserQuestionInterrupt(interruptId)]);

    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    // Send without plan mode
    await act(async () => {
      await result.current.handleSendMessage('Analyze AAPL', false);
    });

    await act(async () => {
      result.current.handleAnswerQuestion('AAPL', interruptId, interruptId);
    });

    await waitFor(() => {
      expect(mockSendHitl).toHaveBeenCalledTimes(1);
    });
    const planModeArg = mockSendHitl.mock.calls[0][4];
    expect(planModeArg).toBe(false);
  });
});
