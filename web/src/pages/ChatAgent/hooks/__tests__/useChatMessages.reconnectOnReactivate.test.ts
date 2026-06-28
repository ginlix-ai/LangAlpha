/**
 * Regression: reconnect a cached, re-shown view to a run that started while it
 * was hidden.
 *
 * ChatView instances are kept MOUNTED in an LRU cache (useChatViewCache) with a
 * stable key, so revisiting a thread reactivates the SAME useChatMessages
 * instance — it does NOT remount, and the thread-load effect (deps
 * [workspaceId, threadId, reloadTrigger]) does NOT re-fire. So when a second
 * round dispatches a follow-up turn into an already-visited PTC thread, reopening
 * that thread showed the PRIOR, completed turn instead of streaming the live one;
 * only a full page refresh (fresh mount → /status → reconnect) recovered.
 *
 * `reconnectIfStaleRun` (called by ChatView's become-active effect on the
 * inactive→active transition) closes that gap: it re-checks /status and attaches
 * to the live run when it differs from what's on screen. The backend only
 * reports a run_id while a run is active, so this is purely the live-run path —
 * an idle thread (run_id=null) is a no-op, and a run already on screen is too.
 *
 * Harness mirrors the sibling report-back suite: REAL hook internals, mocked api
 * module (the path only touches getWorkflowStatus / reconnectToWorkflowStream /
 * replayThreadHistory / fetchThreadTurns).
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

vi.mock('../../utils/api', () => ({
  sendChatMessageStream: vi.fn(),
  sendHitlResponse: vi.fn(),
  cancelWorkflow: vi.fn().mockResolvedValue({ success: true }),
  replayThreadHistory: vi.fn().mockResolvedValue(undefined),
  getWorkflowStatus: vi.fn().mockResolvedValue({ can_reconnect: false, status: 'completed' }),
  reconnectToWorkflowStream: vi.fn().mockResolvedValue({ disconnected: false, aborted: false }),
  streamSubagentTaskEvents: vi.fn(),
  fetchThreadTurns: vi.fn().mockResolvedValue({ turns: [], retry_checkpoint_id: null }),
  submitFeedback: vi.fn(),
  removeFeedback: vi.fn(),
  getThreadFeedback: vi.fn().mockResolvedValue([]),
  watchThread: vi.fn().mockReturnValue({ abort: new AbortController() }),
}));

import { getWorkflowStatus, reconnectToWorkflowStream, replayThreadHistory } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';

const mockStatus = getWorkflowStatus as Mock;
const mockReconnect = reconnectToWorkflowStream as Mock;
const mockReplay = replayThreadHistory as Mock;

/** Flush the mount effect's status-fetch → history-load → branch decision. */
async function settleMountEffect() {
  for (let i = 0; i < 2; i++) {
    await act(async () => {
      await new Promise((r) => setTimeout(r, 0));
    });
  }
}

describe('useChatMessages — reconnect-on-reactivation (cached view, run started while hidden)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockReconnect.mockResolvedValue({ disconnected: false, aborted: false });
  });

  it('attaches to a newer live run when the re-shown view had missed it', async () => {
    // Mount shows a completed/idle thread (the prior turn) → no reconnect on load.
    mockStatus.mockResolvedValue({
      can_reconnect: false,
      status: 'completed',
      pending_report_back: false,
      active_tasks: [],
    });

    const { result } = renderHookWithProviders(() => useChatMessages('ws', 'th'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalled());
    await settleMountEffect();
    // The view loaded history but never reconnected — it shows the prior turn.
    expect(mockReconnect).not.toHaveBeenCalled();

    // A second round dispatched a follow-up run into THIS thread; it is now live.
    mockStatus.mockResolvedValue({
      can_reconnect: true,
      status: 'running',
      run_id: 'run-2',
      pending_report_back: false,
      active_tasks: [],
    });

    // Reactivation (the become-active effect calls this on inactive→active).
    await act(async () => {
      await result.current.reconnectIfStaleRun();
    });

    // It attaches to the live run, replaying from the start of its per-run key.
    expect(mockReconnect).toHaveBeenCalledTimes(1);
    expect(mockReconnect.mock.calls[0][0]).toBe('th');
    expect(mockReconnect.mock.calls[0][1]).toBe('run-2');
    expect(mockReconnect.mock.calls[0][2]).toBeNull();

    // Reactivating again with the SAME live run on screen is a no-op (idempotent).
    await act(async () => {
      await result.current.reconnectIfStaleRun();
    });
    expect(mockReconnect).toHaveBeenCalledTimes(1);
  });

  it('does nothing when the thread is idle (no live run to attach to)', async () => {
    mockStatus.mockResolvedValue({
      can_reconnect: false,
      status: 'completed',
      pending_report_back: false,
      active_tasks: [],
    });

    const { result } = renderHookWithProviders(() => useChatMessages('ws', 'th'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalled());
    await settleMountEffect();

    // /status still reports idle (run_id absent) — nothing newer than what's shown.
    await act(async () => {
      await result.current.reconnectIfStaleRun();
    });
    expect(mockReconnect).not.toHaveBeenCalled();
  });
});
