/**
 * Tests for useChatMessages SSE `error` event routing by `error_kind`.
 *
 * Backend emits two kinds of stream errors:
 *   - `upstream` — LLM provider failed. Render inline on the failed turn so
 *     the user sees it next to the turn they sent. Banner cleared.
 *   - `internal` — our pipeline failed. Drop the optimistic assistant bubble
 *     (matches the 429 pattern) and surface the error in a banner.
 *
 * Regression guard: the empty-bubble-under-a-banner anti-pattern that looks
 * broken in the UI must not re-emerge for internal errors.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';

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
  fetchThreadTurns: vi.fn().mockResolvedValue({ turns: [], retry_checkpoint_id: null }),
  submitFeedback: vi.fn(),
  removeFeedback: vi.fn(),
  getThreadFeedback: vi.fn().mockResolvedValue([]),
  watchThread: vi.fn(),
}));

import { sendChatMessageStream } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';

const mockSendStream = sendChatMessageStream as Mock;

function mockStreamWithEvents(events: Array<Record<string, unknown>>) {
  // Intentionally skip the `thread_id` event: it triggers a history-load
  // useEffect that resets messageError to null, which races the test
  // assertions. The error-routing logic under test runs identically
  // regardless of whether we received a thread_id first.
  mockSendStream.mockImplementation(
    async (
      _msg: string,
      _ws: string,
      _tid: string | null,
      _hist: unknown[],
      _plan: boolean,
      onEvent: (e: Record<string, unknown>) => void,
    ) => {
      for (const e of events) onEvent(e);
      return { disconnected: false };
    },
  );
}

describe('useChatMessages — SSE error routing by error_kind', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('internal error drops the optimistic assistant bubble and sets the banner', async () => {
    mockStreamWithEvents([
      {
        event: 'error',
        error: 'DB pool closed',
        error_kind: 'internal',
      },
    ]);

    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    await act(async () => {
      await result.current.handleSendMessage('hello', false);
    });

    await waitFor(() => {
      // Only the user's message remains; no assistant bubble.
      expect(result.current.messages.some((m) => m.role === 'assistant')).toBe(false);
      expect(result.current.messages.some((m) => m.role === 'user')).toBe(true);
    });

    const err = result.current.messageError;
    expect(err).toBeTruthy();
    if (typeof err === 'object' && err && 'message' in err) {
      expect(err.message).toContain('DB pool closed');
      expect(err.kind).toBe('internal');
    }
  });

  it('upstream error keeps the assistant bubble with structuredError + clears banner', async () => {
    mockStreamWithEvents([
      {
        event: 'error',
        error: '401 invalid api key',
        error_kind: 'upstream',
        status_code: 401,
        hints: ['api_key', 'model_access', 'not_a_real_hint'],
      },
    ]);

    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    await act(async () => {
      await result.current.handleSendMessage('hello', false);
    });

    await waitFor(() => {
      const assistant = result.current.messages.find((m) => m.role === 'assistant') as
        | { error?: boolean; structuredError?: { kind?: string; statusCode?: number; hints?: string[] } }
        | undefined;
      expect(assistant).toBeTruthy();
      expect(assistant?.error).toBe(true);
      expect(assistant?.structuredError?.kind).toBe('upstream');
      expect(assistant?.structuredError?.statusCode).toBe(401);
      // Unknown hint values are filtered out by isUpstreamHint.
      expect(assistant?.structuredError?.hints).toEqual(['api_key', 'model_access']);
    });

    expect(result.current.messageError).toBe(null);
  });

  it('legacy error (no error_kind) falls through to the unclassified inline path', async () => {
    mockStreamWithEvents([
      {
        event: 'error',
        error: 'Something broke',
      },
    ]);

    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    await act(async () => {
      await result.current.handleSendMessage('hello', false);
    });

    await waitFor(() => {
      const assistant = result.current.messages.find((m) => m.role === 'assistant') as
        | { error?: boolean; structuredError?: unknown }
        | undefined;
      expect(assistant?.error).toBe(true);
      // No structuredError since backend didn't classify.
      expect(assistant?.structuredError).toBeUndefined();
    });
  });
});
