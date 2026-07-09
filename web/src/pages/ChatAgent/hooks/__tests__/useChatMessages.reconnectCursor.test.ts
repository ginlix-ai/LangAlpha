/**
 * Regression: reconnect after refresh must NOT carry the history-replay cursor
 * into the live workflow stream.
 *
 * `/messages/replay` numbers events with a cumulative per-thread counter (a
 * thread with lots of history reaches id 900+). The live workflow stream
 * (`workflow:stream:{tid}:{rid}`) resets its ids to 1 per run. If the client
 * reconnects with the replay-derived `last_event_id`, the backend's XREAD
 * cursor overshoots the fresh run's id space, blocks forever, and the stream
 * delivers zero events — the response sits frozen.
 *
 * Contract pinned here: after replaying history, the reconnect is issued with
 * `last_event_id == null` (replay the live stream from the start), regardless of
 * how large the replayed event ids were. `lastEventIdRef` is reserved for ids
 * received on the LIVE stream only.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (k: string) => k }),
}));

vi.mock('@/lib/supabase', () => ({ supabase: null }));

vi.mock('../utils/threadStorage', () => ({
  getStoredThreadId: vi.fn().mockReturnValue('thread-A'),
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
  // Replay a thread with substantial history: events carry a large cumulative
  // id-space (last id 930), mirroring the real `/messages/replay` counter.
  replayThreadHistory: vi.fn().mockImplementation(async (tid: string, onEvent: (e: Record<string, unknown>) => void) => {
    onEvent({ event: 'user_message', _eventId: 929, turn_index: 0, content: 'earlier turn' });
    onEvent({ event: 'replay_done', _eventId: 930, thread_id: tid });
  }),
  getWorkflowStatus: vi.fn().mockResolvedValue({
    can_reconnect: true,
    status: 'active',
    active_tasks: [],
    is_shared: false,
  }),
  reconnectToWorkflowStream: vi.fn().mockResolvedValue({ disconnected: false }),
  streamSubagentTaskEvents: vi.fn(),
  fetchThreadTurns: vi.fn().mockResolvedValue({ turns: [], retry_checkpoint_id: null }),
  submitFeedback: vi.fn(),
  removeFeedback: vi.fn(),
  getThreadFeedback: vi.fn().mockResolvedValue([]),
}));

import { reconnectToWorkflowStream } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';

const mockReconnect = reconnectToWorkflowStream as Mock;

describe('useChatMessages – reconnect cursor after refresh', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('reconnects with a null last_event_id even when history replay had large event ids', async () => {
    renderHookWithProviders(() => useChatMessages('ws-A', 'thread-A'));

    await waitFor(() => {
      expect(mockReconnect).toHaveBeenCalledTimes(1);
    });

    // Signature: reconnectToWorkflowStream(threadId, runId, lastEventId, onEvent)
    const [threadIdArg, runIdArg, lastEventIdArg] = mockReconnect.mock.calls[0];
    expect(threadIdArg).toBe('thread-A');
    expect(runIdArg).toBeNull(); // fresh attach — no run_id known yet
    // The bug: this was 930 (the replay cursor), which overshoots the live
    // per-run stream and freezes the reconnect. Must be null/0.
    expect(lastEventIdArg ?? null).toBeNull();
  });
});
