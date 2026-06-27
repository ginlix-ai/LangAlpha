/**
 * Regression: cross-thread navigation must reconnect to the LANDED thread's
 * active run, not the prior thread's stale run.
 *
 * Within one SPA session the `useChatMessages` instance is reused as the user
 * navigates threads (e.g. clicking a dispatched-PTC card in a flash thread to
 * jump into the running PTC thread). `currentRunIdRef` keeps the last stream's
 * run_id, and `lastEventIdRef` keeps its per-stream-key cursor. On the new
 * thread's load, the reconnect path must repoint at that thread's CURRENT run
 * (from `/status`) and rewind the cursor — otherwise it attaches to a dead key
 * `workflow:stream:{newThread}:{priorRun}`, receives zero live events, and the
 * turn only appears on a later refetch (the live-render bug this guards).
 *
 * We use the REAL hook internals and mock only the api module — the thread-load
 * reconnect path touches getWorkflowStatus / replayThreadHistory /
 * fetchThreadTurns / reconnectToWorkflowStream, none of which need real network.
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
  watchThread: vi.fn(() => ({ abort: new AbortController() })),
}));

import { getWorkflowStatus, reconnectToWorkflowStream } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';

const mockStatus = getWorkflowStatus as Mock;
const mockReconnect = reconnectToWorkflowStream as Mock;

describe('useChatMessages — cross-thread navigation reconnect', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("reconnect targets the landed thread's run, not the prior thread's stale run", async () => {
    // Thread A is live with run-A.
    mockStatus.mockResolvedValue({ can_reconnect: true, status: 'running', run_id: 'run-A', active_tasks: [] });

    let tid = 'th-A';
    const { rerender } = renderHookWithProviders(() => useChatMessages('ws', tid));

    // Thread A load reconnects to its own run, seeding currentRunIdRef with run-A.
    await waitFor(() => expect(mockReconnect).toHaveBeenCalledTimes(1));
    expect(mockReconnect.mock.calls[0][0]).toBe('th-A');
    expect(mockReconnect.mock.calls[0][1]).toBe('run-A');
    // Let the thread-A reconnect's finally (cleanupAfterStreamEnd) reset
    // isStreamingRef so the thread-B load effect isn't blocked.
    await act(async () => { await new Promise((r) => setTimeout(r, 0)); });

    // Navigate to thread B — a DIFFERENT running dispatched run.
    mockStatus.mockResolvedValue({ can_reconnect: true, status: 'running', run_id: 'run-B', active_tasks: [] });
    tid = 'th-B';
    await act(async () => {
      rerender();
      await new Promise((r) => setTimeout(r, 0));
    });

    await waitFor(() => expect(mockReconnect).toHaveBeenCalledTimes(2));
    expect(mockReconnect.mock.calls[1][0]).toBe('th-B');
    expect(mockReconnect.mock.calls[1][1]).toBe('run-B'); // NOT the stale 'run-A'
    expect(mockReconnect.mock.calls[1][2]).toBeNull(); // fresh per-stream-key cursor
  });

  it('supersedes an in-flight stream on the prior thread so the navigated-to thread still loads', async () => {
    // Thread A's reconnect HANGS — it keeps streaming (isStreamingRef stays true).
    // This mimics a flash report-back streaming on the flash thread when the user
    // clicks the dispatch card to jump into the running PTC thread. Before the
    // fix, the thread-load effect's isStreamingRef guard would skip thread B's
    // load and it would appear blank.
    let resolveA: (() => void) | undefined;
    mockReconnect.mockImplementationOnce(
      () =>
        new Promise<{ disconnected: boolean; aborted: boolean }>((res) => {
          resolveA = () => res({ disconnected: false, aborted: false });
        }),
    );
    mockStatus.mockResolvedValue({ can_reconnect: true, status: 'running', run_id: 'run-A', active_tasks: [] });

    let tid = 'th-A';
    const { rerender } = renderHookWithProviders(() => useChatMessages('ws', tid));

    await waitFor(() => expect(mockReconnect).toHaveBeenCalledTimes(1));
    expect(mockReconnect.mock.calls[0][1]).toBe('run-A');

    // Thread A is STILL streaming (its reconnect promise never resolved). Navigate
    // to thread B — its load must supersede A's stream and reconnect to run-B.
    mockReconnect.mockResolvedValue({ disconnected: false, aborted: false });
    mockStatus.mockResolvedValue({ can_reconnect: true, status: 'running', run_id: 'run-B', active_tasks: [] });
    tid = 'th-B';
    await act(async () => {
      rerender();
      await new Promise((r) => setTimeout(r, 0));
    });

    await waitFor(() => expect(mockReconnect).toHaveBeenCalledTimes(2));
    expect(mockReconnect.mock.calls[1][0]).toBe('th-B');
    expect(mockReconnect.mock.calls[1][1]).toBe('run-B');

    // Let the superseded stream unwind; its cleanup must be a no-op (it is no
    // longer the active stream), so thread B's state is left intact.
    resolveA?.();
    await act(async () => { await new Promise((r) => setTimeout(r, 0)); });
    expect(mockReconnect).toHaveBeenCalledTimes(2);
  });

  it("a superseded reconnect unwinding does not clear the new thread's spinner", async () => {
    // Two reconnects that both HANG, so we can read isReconnecting across the
    // supersede. Thread A streams; navigating to B supersedes A and starts B's
    // own (still-pending) reconnect. When A's superseded reconnect finally
    // unwinds, its `finally` must NOT clear isReconnecting — that spinner now
    // belongs to B. Pre-fix the clear was unconditional and hid B's spinner.
    let resolveA: (() => void) | undefined;
    let resolveB: (() => void) | undefined;
    mockReconnect
      .mockImplementationOnce(
        () =>
          new Promise<{ disconnected: boolean; aborted: boolean }>((res) => {
            resolveA = () => res({ disconnected: false, aborted: false });
          }),
      )
      .mockImplementationOnce(
        () =>
          new Promise<{ disconnected: boolean; aborted: boolean }>((res) => {
            resolveB = () => res({ disconnected: false, aborted: false });
          }),
      );
    mockStatus.mockResolvedValue({ can_reconnect: true, status: 'running', run_id: 'run-A', active_tasks: [] });

    let tid = 'th-A';
    const { result, rerender } = renderHookWithProviders(() => useChatMessages('ws', tid));

    await waitFor(() => expect(mockReconnect).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(result.current.isReconnecting).toBe(true));

    // Navigate to B while A still streams → supersede A, start B's reconnect.
    mockStatus.mockResolvedValue({ can_reconnect: true, status: 'running', run_id: 'run-B', active_tasks: [] });
    tid = 'th-B';
    await act(async () => {
      rerender();
      await new Promise((r) => setTimeout(r, 0));
    });
    await waitFor(() => expect(mockReconnect).toHaveBeenCalledTimes(2));
    // B is reconnecting now (it set the spinner after the supersede cleared it).
    expect(result.current.isReconnecting).toBe(true);

    // A's superseded reconnect unwinds. Its finally is guarded by stillActive
    // (false post-supersede), so it must leave B's spinner on.
    await act(async () => {
      resolveA?.();
      await new Promise((r) => setTimeout(r, 0));
    });
    expect(result.current.isReconnecting).toBe(true);

    resolveB?.();
    await act(async () => { await new Promise((r) => setTimeout(r, 0)); });
  });
});
