/**
 * Tests that turn index calculations correctly exclude steering assistant messages.
 *
 * Steering messages (mid-turn follow-ups sent while the agent is running) create
 * extra assistant message bubbles in the frontend that don't correspond to backend
 * turns. The turn index must skip these when mapping to backend checkpoint data.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';

// ---------------------------------------------------------------------------
// Mocks
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

vi.mock('../../utils/api', () => ({
  sendChatMessageStream: vi.fn(),
  sendHitlResponse: vi.fn(),
  replayThreadHistory: vi.fn().mockResolvedValue(undefined),
  getWorkflowStatus: vi.fn().mockResolvedValue({ can_reconnect: false, status: 'completed' }),
  reconnectToWorkflowStream: vi.fn(),
  streamSubagentTaskEvents: vi.fn(),
  fetchThreadTurns: vi.fn(),
  submitFeedback: vi.fn(),
  removeFeedback: vi.fn(),
  getThreadFeedback: vi.fn().mockResolvedValue([]),
}));

import {
  sendChatMessageStream,
  fetchThreadTurns,
} from '../../utils/api';
import { useChatMessages } from '../useChatMessages';

const mockSendStream = sendChatMessageStream as Mock;
const mockFetchTurns = fetchThreadTurns as Mock;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type StreamCallback = (e: Record<string, unknown>) => void;

/**
 * Mock the stream to emit thread_id, then N steering_delivered events, then
 * on the NEXT call emit just thread_id (no steering). This simulates two turns
 * where the first has steering continuations and the second doesn't.
 */
function mockTwoTurnsWithSteering(steeringCount: number) {
  let callCount = 0;
  mockSendStream.mockImplementation(
    async (
      _msg: string,
      _ws: string,
      _tid: string | null,
      _hist: unknown[],
      _plan: boolean,
      onEvent: StreamCallback,
    ) => {
      callCount++;
      onEvent({ event: 'thread_id', thread_id: 'thread-1' });
      if (callCount === 1) {
        for (let i = 0; i < steeringCount; i++) {
          onEvent({
            event: 'steering_delivered',
            messages: [{ content: `follow-up ${i}`, timestamp: Date.now() / 1000 }],
          });
        }
      }
      return { disconnected: false };
    },
  );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('useChatMessages – turn index with steering messages', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockFetchTurns.mockResolvedValue({
      turns: [
        { turn_index: 0, edit_checkpoint_id: null, regenerate_checkpoint_id: 'cp-0' },
        { turn_index: 1, edit_checkpoint_id: 'cp-0', regenerate_checkpoint_id: 'cp-1' },
      ],
      retry_checkpoint_id: 'cp-1',
    });
  });

  it('steering_delivered creates assistant messages with isSteering flag', async () => {
    mockTwoTurnsWithSteering(3);
    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    await act(async () => {
      await result.current.handleSendMessage('hello', false);
    });

    await waitFor(() => {
      const assistants = result.current.messages.filter(
        (m: Record<string, unknown>) => m.role === 'assistant',
      );
      // 1 real assistant + 3 steering assistants
      expect(assistants.length).toBe(4);

      const steering = assistants.filter((m: Record<string, unknown>) => m.isSteering);
      expect(steering.length).toBe(3);

      // The first assistant (from the original turn) should NOT be steering
      expect(assistants[0].isSteering).toBeFalsy();
    });
  });

  it('turn index calculation excludes isSteering messages', () => {
    // Unit test of the filtering logic used in handleRegenerate/handleEditMessage/deriveTurnIndex
    const messages = [
      { id: 'u0', role: 'user' },
      { id: 'a0', role: 'assistant' },                           // turn 0
      { id: 'su1', role: 'user', steeringDelivered: true },
      { id: 'sa1', role: 'assistant', isSteering: true },        // steering (not a turn)
      { id: 'su2', role: 'user', steeringDelivered: true },
      { id: 'sa2', role: 'assistant', isSteering: true },        // steering (not a turn)
      { id: 'su3', role: 'user', steeringDelivered: true },
      { id: 'sa3', role: 'assistant', isSteering: true },        // steering (not a turn)
      { id: 'u1', role: 'user' },
      { id: 'a1', role: 'assistant' },                           // turn 1
      { id: 'u2', role: 'user' },
      { id: 'a2', role: 'assistant' },                           // turn 2
    ];

    // Regenerate turn index: count non-steering assistants up to and including target
    const regenTurnIndex = (msgId: string) => {
      const msgIndex = messages.findIndex(m => m.id === msgId);
      return messages.slice(0, msgIndex + 1).filter(m => m.role === 'assistant' && !m.isSteering).length - 1;
    };

    // Edit turn index: count non-steering assistants before target user message
    const editTurnIndex = (msgId: string) => {
      const msgIndex = messages.findIndex(m => m.id === msgId);
      return messages.slice(0, msgIndex).filter(m => m.role === 'assistant' && !m.isSteering).length;
    };

    // Regenerate: last assistant (a2) should be turn 2
    expect(regenTurnIndex('a2')).toBe(2);
    // Regenerate: middle real assistant (a1) should be turn 1
    expect(regenTurnIndex('a1')).toBe(1);
    // Regenerate: first assistant (a0) should be turn 0
    expect(regenTurnIndex('a0')).toBe(0);

    // Without the fix (counting all assistants), a2 would be turn 5 — WRONG
    const brokenRegenIndex = messages.slice(0, messages.findIndex(m => m.id === 'a2') + 1)
      .filter(m => m.role === 'assistant').length - 1;
    expect(brokenRegenIndex).toBe(5); // demonstrates the bug

    // Edit: editing u1 (after 3 steering pairs) should be turn 1
    expect(editTurnIndex('u1')).toBe(1);
    // Edit: editing u2 should be turn 2
    expect(editTurnIndex('u2')).toBe(2);
  });

  it('handleRegenerate uses correct turnIndex with steering messages present', async () => {
    mockTwoTurnsWithSteering(2);
    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    // Send two messages: first with steering, second without
    await act(async () => {
      await result.current.handleSendMessage('hello', false);
    });
    await act(async () => {
      await result.current.handleSendMessage('next question', false);
    });

    // Wait for all messages to settle
    let lastAssistantId: string;
    await waitFor(() => {
      const assistants = result.current.messages.filter(
        (m: Record<string, unknown>) => m.role === 'assistant',
      );
      expect(assistants.length).toBe(4); // 2 real + 2 steering
      lastAssistantId = assistants[assistants.length - 1].id as string;
    });

    // Regenerate the last assistant message
    await act(async () => {
      await result.current.handleRegenerate(lastAssistantId!);
    });

    await waitFor(() => {
      expect(mockFetchTurns).toHaveBeenCalledWith('thread-1');
    });

    // Without the fix, turnIndex=3 would exceed backend's 2 turns → "checkpoint data unavailable"
    // With the fix, turnIndex=1 correctly maps to turns[1]
    expect(result.current.messageError).toBeNull();
  });
});
