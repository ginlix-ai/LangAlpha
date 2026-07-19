/**
 * Tests for useChatMessages workspace_status SSE event handling.
 *
 * The hook translates SSE `workspace_status` events into the
 * `workspaceStarting` state consumed by ChatView. We need to verify:
 * - `starting` with `sandbox_state: "archived"` -> 'archived' (slow restore copy)
 * - `starting` without `sandbox_state`          -> 'starting' (generic copy)
 * - `starting` with an unknown `sandbox_state`  -> 'starting' (graceful fallback)
 * - `ready`                                     -> false (spinner clears)
 *
 * Note: the hook's `cleanupAfterStreamEnd` resets `workspaceStarting` to
 * false when the stream completes, so we keep the stream open with a
 * controllable promise while asserting mid-flight state.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';

// ---------------------------------------------------------------------------
// Mocks (mirrors useChatMessages.turnIndex.test.ts scaffolding)
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
  fetchMarketWatch: vi.fn().mockResolvedValue({ thread_id: 't', symbols: [] }),
  sendChatMessageStream: vi.fn(),
  sendHitlResponse: vi.fn(),
  replayThreadHistory: vi.fn().mockResolvedValue(undefined),
  getWorkflowStatus: vi.fn().mockResolvedValue({ can_reconnect: false, status: 'completed' }),
  reconnectToWorkflowStream: vi.fn(),
  openThreadMuxStream: vi.fn(() => new Promise<void>(() => {})),
  fetchThreadTurns: vi.fn().mockResolvedValue({ turns: [], retry_checkpoint_id: null }),
  submitFeedback: vi.fn(),
  removeFeedback: vi.fn(),
  getThreadFeedback: vi.fn().mockResolvedValue([]),
}));

import { sendChatMessageStream } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';

const mockSendStream = sendChatMessageStream as Mock;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type StreamCallback = (e: Record<string, unknown>) => void;

/**
 * Drive the hook with a stream that delivers `events` synchronously via
 * onEvent, then HANGS without resolving so the hook stays mid-stream.
 * Call the returned `resolve` to end the stream.
 */
function mockHangingStream(events: Array<Record<string, unknown>>) {
  let resolve: () => void = () => {};
  const hang = new Promise<{ disconnected: false }>((r) => {
    resolve = () => r({ disconnected: false });
  });
  mockSendStream.mockImplementation(
    async (
      _msg: string,
      _ws: string,
      _tid: string | null,
      _hist: unknown[],
      _plan: boolean,
      onEvent: StreamCallback,
    ) => {
      onEvent({ event: 'thread_id', thread_id: 'thread-1' });
      for (const e of events) {
        onEvent(e);
      }
      return hang;
    },
  );
  return { resolve };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('useChatMessages – workspace_status SSE handling', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('starting + sandbox_state="archived" sets workspaceStarting to "archived"', async () => {
    const { resolve } = mockHangingStream([
      { event: 'workspace_status', status: 'starting' },
      { event: 'workspace_status', status: 'starting', sandbox_state: 'archived' },
    ]);

    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    // Fire without awaiting so stream stays mid-flight.
    act(() => {
      void result.current.handleSendMessage('hello', false);
    });

    await waitFor(() => {
      expect(result.current.workspaceStarting).toBe('archived');
    });

    resolve();
  });

  it('starting without sandbox_state sets workspaceStarting to "starting"', async () => {
    const { resolve } = mockHangingStream([
      { event: 'workspace_status', status: 'starting' },
    ]);

    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    act(() => {
      void result.current.handleSendMessage('hello', false);
    });

    await waitFor(() => {
      expect(result.current.workspaceStarting).toBe('starting');
    });

    resolve();
  });

  it('unknown sandbox_state falls back to "starting" (graceful degradation)', async () => {
    // If backend adds a new sandbox_state value the frontend doesn't know about,
    // we must render the generic spinner copy instead of crashing or showing nothing.
    const { resolve } = mockHangingStream([
      { event: 'workspace_status', status: 'starting', sandbox_state: 'destroyed' },
    ]);

    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    act(() => {
      void result.current.handleSendMessage('hello', false);
    });

    await waitFor(() => {
      expect(result.current.workspaceStarting).toBe('starting');
    });

    resolve();
  });

  it('ready after starting clears workspaceStarting to false mid-stream', async () => {
    // Explicit ready event must clear the spinner even before the stream ends.
    const { resolve } = mockHangingStream([
      { event: 'workspace_status', status: 'starting', sandbox_state: 'archived' },
      { event: 'workspace_status', status: 'ready' },
    ]);

    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    act(() => {
      void result.current.handleSendMessage('hello', false);
    });

    await waitFor(() => {
      expect(result.current.workspaceStarting).toBe(false);
    });

    resolve();
  });

  it('no workspace_status events leaves workspaceStarting at false (warm path)', async () => {
    mockSendStream.mockImplementation(
      async (
        _msg: string,
        _ws: string,
        _tid: string | null,
        _hist: unknown[],
        _plan: boolean,
        onEvent: StreamCallback,
      ) => {
        onEvent({ event: 'thread_id', thread_id: 'thread-1' });
        return { disconnected: false };
      },
    );

    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    await act(async () => {
      await result.current.handleSendMessage('hello', false);
    });

    expect(result.current.workspaceStarting).toBe(false);
  });

  it('stream cleanup clears workspaceStarting regardless of last seen state', async () => {
    // Safety net: if ready never comes (backend crashed, network died), the
    // hook's cleanupAfterStreamEnd must still clear the spinner when the stream
    // closes so the UI does not get stuck.
    mockSendStream.mockImplementation(
      async (
        _msg: string,
        _ws: string,
        _tid: string | null,
        _hist: unknown[],
        _plan: boolean,
        onEvent: StreamCallback,
      ) => {
        onEvent({ event: 'thread_id', thread_id: 'thread-1' });
        onEvent({ event: 'workspace_status', status: 'starting', sandbox_state: 'archived' });
        // Stream ends without a ready event.
        return { disconnected: false };
      },
    );

    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    await act(async () => {
      await result.current.handleSendMessage('hello', false);
    });

    // After cleanupAfterStreamEnd runs, spinner should be cleared.
    await waitFor(() => {
      expect(result.current.workspaceStarting).toBe(false);
    });
  });
});
