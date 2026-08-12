/**
 * Reconnect guards: the post-await guard in `reconnectIfStaleRun` (D1) and the
 * per-instance isolation the render-gate identity check leans on (D2).
 *
 * D1 — a history reload can begin DURING reconnectIfStaleRun's /status await;
 * the post-await re-check must mirror the pre-await guard and bail so the
 * stale-run reconnect can't race the reload for the message state. (The
 * positive path — reconnecting when nothing is in flight — is covered by the
 * reconnect-on-reactivate suite.)
 *
 * D2 — production ChatView is multi-instance (useChatViewCache keys one hook
 * instance per workspace+thread), so real isolation is per-instance hook state
 * + the currentRunIdRef dedup; this proves a run targeting thread A never leaks
 * into a separate thread-B instance.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import { settleMountEffect, threadStatus } from './chatHookHarness';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (k: string) => k }),
}));

vi.mock('@/lib/supabase', () => ({ supabase: null }));

vi.mock('../utils/threadStorage', () => ({
  getStoredThreadId: vi.fn().mockReturnValue(null),
  setStoredThreadId: vi.fn(),
  removeStoredThreadId: vi.fn(),
}));

vi.mock('../../utils/api', async () => (await import('./chatHookHarness')).apiMockModule());

import { getWorkflowStatus, reconnectToWorkflowStream, replayThreadHistory } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';

const mockStatus = getWorkflowStatus as Mock;
const mockReconnect = reconnectToWorkflowStream as Mock;
const mockReplay = replayThreadHistory as Mock;

const IDLE = threadStatus();

/** Externally-resolvable promise so a test can hold an await open then release it. */
function deferred<T>() {
  let resolve!: (v: T) => void;
  const promise = new Promise<T>((r) => { resolve = r; });
  return { promise, resolve };
}

describe('useChatMessages — reconnect guards (D1 post-await load guard, D2 instance isolation)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockReplay.mockResolvedValue(undefined);
    mockReconnect.mockResolvedValue({ disconnected: false, aborted: false });
  });

  it('D1: bails (no reconnect) when a history reload starts DURING the /status await', async () => {
    // Mount idle: history load settles, no reconnect.
    mockStatus.mockResolvedValue(IDLE);

    let wsId = 'ws';
    const { result, rerender } = renderHookWithProviders(() => useChatMessages(wsId, 'th'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalled());
    await settleMountEffect();
    expect(result.current.isLoadingHistory).toBe(false);
    expect(mockReconnect).not.toHaveBeenCalled();

    // A live run now exists on this thread.
    const live = threadStatus({ can_reconnect: true, status: 'running', run_id: 'run-2' });

    // The NEXT history load (workspace-key change below) hangs on replay, so
    // historyLoadingRef stays TRUE — a reload genuinely in flight.
    mockReplay.mockImplementation(() => new Promise(() => {}));

    // Default /status → live; but DEFER the very next call (reconnectIfStaleRun's)
    // so we can flip historyLoadingRef true while it awaits.
    mockStatus.mockResolvedValue(live);
    const d = deferred<typeof live>();
    mockStatus.mockImplementationOnce(() => d.promise);

    let staleRunPromise: Promise<unknown>;
    await act(async () => {
      // Passes the pre-await guard (not loading yet) and parks on the deferred /status.
      staleRunPromise = result.current.reconnectIfStaleRun();
      // Kick off a concurrent reload by changing the workspace key: the load
      // effect fires loadConversationHistory → historyLoadingRef=true → parks on
      // the hanging replay.
      wsId = 'ws2';
      rerender();
      await new Promise((r) => setTimeout(r, 0));
    });

    await waitFor(() => expect(result.current.isLoadingHistory).toBe(true));

    // The stale-run /status resolves with a live run. The POST-await guard must
    // observe historyLoadingRef and BAIL — otherwise it would request a redundant
    // history reload on top of the one already in flight (and, pre-guard, raced
    // the reload for the message state).
    await act(async () => {
      d.resolve(live);
      await staleRunPromise;
    });

    expect(mockReconnect).not.toHaveBeenCalled();
  });

  it('D2: a run targeting thread A does not leak into a separate thread-B instance', async () => {
    // Two hook instances, one per stable threadId (mirrors useChatViewCache).
    mockStatus.mockResolvedValue(IDLE);

    const a = renderHookWithProviders(() => useChatMessages('ws', 'A'));
    const b = renderHookWithProviders(() => useChatMessages('ws', 'B'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalled());
    await settleMountEffect();
    expect(a.result.current.messages).toHaveLength(0);
    expect(b.result.current.messages).toHaveLength(0);
    expect(mockReconnect).not.toHaveBeenCalled();

    // A live run starts on thread A; its reconnect streams a text chunk into A.
    mockStatus.mockImplementation((tid: string) =>
      Promise.resolve(
        tid === 'A' ? threadStatus({ can_reconnect: true, status: 'running', run_id: 'run-A' }) : IDLE,
      ),
    );
    mockReconnect.mockImplementation(
      (tid: string, _runId: unknown, _lastId: unknown, onEvent: (e: Record<string, unknown>) => void) => {
        if (tid === 'A') {
          onEvent({ event: 'message_chunk', content_type: 'text', content: 'A-only content' });
        }
        return Promise.resolve({ disconnected: false, aborted: false });
      },
    );

    // Reactivate A → the identity gate passes for A's OWN threadId (inert here,
    // as in production) → requests a history reload, whose reconnect branch
    // attaches run-A → streams into A's per-instance state.
    await act(async () => {
      await a.result.current.reconnectIfStaleRun();
    });

    await waitFor(() => expect(mockReconnect).toHaveBeenCalledTimes(1));
    expect(mockReconnect.mock.calls[0][0]).toBe('A');
    expect(JSON.stringify(a.result.current.messages)).toContain('A-only content');

    // Nothing leaked into B: separate instance, separate state, never reconnected.
    expect(b.result.current.messages).toHaveLength(0);
    expect(mockReconnect.mock.calls.some((c) => c[0] === 'B')).toBe(false);
  });
});
