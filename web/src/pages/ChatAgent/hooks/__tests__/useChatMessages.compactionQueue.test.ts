/**
 * Tests for the queued-send UX: a message the user presses Send on while the
 * agent is compacting its context must be held (not steered / not started),
 * then auto-sent once compaction finishes — and dropped if the user stops.
 *
 * Mirrors the backend admission gate, which 409s a POST that arrives
 * mid-compaction.
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

vi.mock('@/components/ui/use-toast', () => ({
  toast: vi.fn(),
  useToast: () => ({ toast: vi.fn() }),
}));

vi.mock('../utils/threadStorage', () => ({
  getStoredThreadId: vi.fn().mockReturnValue(null),
  setStoredThreadId: vi.fn(),
  removeStoredThreadId: vi.fn(),
}));

vi.mock('../../utils/api', () => ({
  fetchMarketWatch: vi.fn().mockResolvedValue({ thread_id: 't', symbols: [] }),
  sendChatMessageStream: vi.fn(),
  sendHitlResponse: vi.fn(),
  cancelWorkflow: vi.fn().mockResolvedValue({ success: true }),
  replayThreadHistory: vi.fn(),
  getWorkflowStatus: vi.fn().mockResolvedValue({ can_reconnect: false, status: 'completed' }),
  reconnectToWorkflowStream: vi.fn(),
  openThreadMuxStream: vi.fn(() => new Promise<void>(() => {})),
  fetchThreadTurns: vi.fn(),
  submitFeedback: vi.fn(),
  removeFeedback: vi.fn(),
  getThreadFeedback: vi.fn(),
}));

import { sendChatMessageStream, cancelWorkflow } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';

const mockSendStream = sendChatMessageStream as Mock;
const mockCancelWorkflow = cancelWorkflow as Mock;

type OnEvent = (e: Record<string, unknown>) => void;

interface Deferred<T> {
  promise: Promise<T>;
  resolve: (v: T) => void;
}

/** A promise whose resolution we control, to keep a turn "running". */
function deferred<T>(): Deferred<T> {
  let resolve!: (v: T) => void;
  const promise = new Promise<T>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

/**
 * First send() hangs (emitting thread_id so history-load short-circuits) so
 * isLoading stays true; later sends (the steering POST) resolve immediately.
 * Returns the deferred controlling the first stream so the test can release it.
 */
function mockHangFirstStream(threadId: string): Deferred<{ disconnected: boolean }> {
  const firstHang = deferred<{ disconnected: boolean }>();
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
        onEvent({ event: 'thread_id', thread_id: threadId });
        return firstHang.promise;
      }
      return { disconnected: false };
    },
  );
  return firstHang;
}

/** Resolve the SSE stream immediately with no events. */
function mockEmptyStream() {
  mockSendStream.mockImplementation(
    async (
      _msg: string,
      _ws: string,
      _tid: string | null,
      _hist: unknown[],
      _plan: boolean,
      _onEvent: (e: Record<string, unknown>) => void,
    ) => {
      return { disconnected: false };
    },
  );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('useChatMessages – queued send during compaction', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockEmptyStream();
  });

  it('holds a Send issued while compacting instead of starting a turn', async () => {
    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    act(() => {
      result.current.setIsCompacting('summarize');
    });

    await act(async () => {
      await result.current.handleSendMessage('hello while compacting');
    });

    // No turn started — the message is parked, not sent.
    expect(mockSendStream).not.toHaveBeenCalled();
    expect(result.current.queuedSend).toBe('hello while compacting');

    // Parked message renders as an optimistic (shimmer) user bubble.
    const queuedBubble = result.current.messages.find(
      (m) => m.role === 'user' && (m as { queued?: boolean }).queued,
    );
    expect(queuedBubble?.content).toBe('hello while compacting');
  });

  it('keeps only the latest of two messages queued during compaction', async () => {
    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    act(() => {
      result.current.setIsCompacting('summarize');
    });

    // Two Sends arrive while compacting; only the latest is held (the earlier
    // optimistic bubble is removed via the prevQueuedId filter).
    await act(async () => {
      await result.current.handleSendMessage('first queued');
    });
    await act(async () => {
      await result.current.handleSendMessage('second queued');
    });

    // Neither send started a turn.
    expect(mockSendStream).not.toHaveBeenCalled();

    // Exactly one queued shimmer bubble survives, showing the SECOND message.
    const queuedBubbles = result.current.messages.filter(
      (m) => m.role === 'user' && (m as { queued?: boolean }).queued,
    );
    expect(queuedBubbles).toHaveLength(1);
    expect(queuedBubbles[0].content).toBe('second queued');

    // The preview chip tracks the second message. queuedSendRef is internal
    // (not on the hook's return surface), so its payload is verified below by
    // what the flush actually replays.
    expect(result.current.queuedSend).toBe('second queued');

    // Compaction finishes → the flush effect replays the held payload. No turn
    // was running, so ONLY the second message is sent (not steered), proving
    // queuedSendRef held the second payload (not the first, not both).
    await act(async () => {
      result.current.setIsCompacting(false);
    });

    await waitFor(() => {
      expect(mockSendStream).toHaveBeenCalledTimes(1);
    });
    expect(mockSendStream.mock.calls[0][0]).toBe('second queued');
    expect(result.current.queuedSend).toBe(false);
  });

  it('auto-sends the queued message once compaction finishes', async () => {
    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    act(() => {
      result.current.setIsCompacting('summarize');
    });
    await act(async () => {
      await result.current.handleSendMessage('deferred question');
    });
    expect(mockSendStream).not.toHaveBeenCalled();

    // Compaction completes → the flush effect starts a fresh turn (no turn was
    // running, so it is sent rather than steered).
    await act(async () => {
      result.current.setIsCompacting(false);
    });

    await waitFor(() => {
      expect(mockSendStream).toHaveBeenCalledTimes(1);
    });
    expect(mockSendStream.mock.calls[0][0]).toBe('deferred question');
    expect(result.current.queuedSend).toBe(false);
  });

  it('drops the queued message when the user stops mid-compaction', async () => {
    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    act(() => {
      result.current.setIsCompacting('summarize');
    });
    await act(async () => {
      await result.current.handleSendMessage('should be dropped');
    });
    expect(result.current.queuedSend).toBe('should be dropped');

    // Stopping clears isCompacting AND the queue in the same synchronous block,
    // so the flush effect sees an empty ref and must not replay the message.
    await act(async () => {
      await result.current.stopWorkflow();
    });

    expect(result.current.queuedSend).toBe(false);
    expect(mockSendStream).not.toHaveBeenCalled();
    // The optimistic shimmer bubble is removed on stop.
    expect(
      result.current.messages.some(
        (m) => m.role === 'user' && (m as { queued?: boolean }).queued,
      ),
    ).toBe(false);
  });

  it('stopCompaction clears state, drops the queue, and cancels the backend', async () => {
    // Manual /compact has isLoading=false, so the chat-input stop button isn't
    // shown — the compacting banner's own stop must drive stopCompaction, which
    // cancels the in-flight backend compaction and clears local state.
    const { result } = renderHookWithProviders(() =>
      useChatMessages('ws-test', 'thread-stop-1'),
    );

    act(() => {
      result.current.setIsCompacting('summarize');
    });
    await act(async () => {
      await result.current.handleSendMessage('queued during compaction');
    });
    expect(result.current.queuedSend).toBe('queued during compaction');

    await act(async () => {
      await result.current.stopCompaction();
    });

    expect(result.current.isCompacting).toBe(false);
    expect(result.current.queuedSend).toBe(false);
    expect(mockCancelWorkflow).toHaveBeenCalledWith('thread-stop-1');
    // The parked message is dropped, not replayed.
    expect(mockSendStream).not.toHaveBeenCalled();
    // The optimistic shimmer bubble is removed on stop.
    expect(
      result.current.messages.some(
        (m) => m.role === 'user' && (m as { queued?: boolean }).queued,
      ),
    ).toBe(false);
  });

  it('queues (not steers) a Send while a turn is running AND compacting', async () => {
    // The motivating case: an auto Tier-2 summarize fires mid-turn, so the
    // turn is STILL running (isLoading true) AND isCompacting is set. The
    // isCompacting check must win over the isLoading steering branch —
    // steering now would corrupt the in-flight context rewrite.
    const firstHang = mockHangFirstStream('thread-q-1');
    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    let firstSend: Promise<unknown> = Promise.resolve();
    await act(async () => {
      firstSend = result.current.handleSendMessage('running turn');
      await Promise.resolve();
    });
    await waitFor(() => expect(result.current.isLoading).toBe(true));
    expect(mockSendStream).toHaveBeenCalledTimes(1);

    act(() => {
      result.current.setIsCompacting('summarize');
    });
    await act(async () => {
      await result.current.handleSendMessage('deferred');
    });

    // Parked, not steered: no second POST, and the bubble is queued (shimmer)
    // rather than steered into the running turn.
    expect(result.current.queuedSend).toBe('deferred');
    expect(mockSendStream).toHaveBeenCalledTimes(1);
    expect(
      result.current.messages.some(
        (m) => m.role === 'user' && (m as { steering?: boolean }).steering,
      ),
    ).toBe(false);
    const queuedBubble = result.current.messages.find(
      (m) => m.role === 'user' && (m as { queued?: boolean }).queued,
    );
    expect(queuedBubble?.content).toBe('deferred');

    firstHang.resolve({ disconnected: false });
    await act(async () => {
      await firstSend.catch(() => undefined);
    });
  });

  it('clears the parked queue + compacting flag on thread switch (no cross-thread replay)', async () => {
    // A message parked during compaction on thread A must not survive a switch
    // to thread B: the queue payload, the preview chip, and the compacting flag
    // are all reset, so the flush effect can never replay A's message into B.
    let tid: string | undefined = 'thread-A';
    const { result, rerender } = renderHookWithProviders(() =>
      useChatMessages('ws-test', tid),
    );

    act(() => {
      result.current.setIsCompacting('summarize');
    });
    await act(async () => {
      await result.current.handleSendMessage('parked on thread A');
    });
    expect(result.current.queuedSend).toBe('parked on thread A');

    // Switch to thread B before compaction finishes.
    await act(async () => {
      tid = 'thread-B';
      rerender();
      await Promise.resolve();
    });

    expect(result.current.queuedSend).toBe(false);
    expect(result.current.isCompacting).toBe(false);
    expect(
      result.current.messages.some(
        (m) => m.role === 'user' && (m as { queued?: boolean }).queued,
      ),
    ).toBe(false);

    // Proof of no cross-thread replay: a fresh compaction cycle on thread B
    // must not flush thread A's stranded payload. Separate act blocks so the
    // intermediate 'summarize' state is committed before resetting to false —
    // otherwise the two updates batch into one commit and the flush effect
    // (keyed on isCompacting) never fires, making this check vacuous.
    await act(async () => {
      result.current.setIsCompacting('summarize');
      await Promise.resolve();
    });
    await act(async () => {
      result.current.setIsCompacting(false);
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(mockSendStream).not.toHaveBeenCalled();
  });

  it('steers the queued message when a turn is still running at flush', async () => {
    // Auto Tier-2 summarize completes while the turn is STILL running → the
    // flush effect must steer into it (handleSendSteering), not start a fresh
    // turn. Covers the flush effect's isLoading branch.
    const firstHang = mockHangFirstStream('thread-q-2');
    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    let firstSend: Promise<unknown> = Promise.resolve();
    await act(async () => {
      firstSend = result.current.handleSendMessage('running turn');
      await Promise.resolve();
    });
    await waitFor(() => expect(result.current.isLoading).toBe(true));

    act(() => {
      result.current.setIsCompacting('summarize');
    });
    await act(async () => {
      await result.current.handleSendMessage('deferred');
    });
    expect(result.current.queuedSend).toBe('deferred');

    // Compaction finishes; turn still running → steer.
    await act(async () => {
      result.current.setIsCompacting(false);
      await Promise.resolve();
      await Promise.resolve();
    });

    await waitFor(() => {
      expect(mockSendStream).toHaveBeenCalledTimes(2);
    });
    const userMsgs = result.current.messages.filter((m) => m.role === 'user');
    const lastUser = userMsgs[userMsgs.length - 1] as { steering?: boolean };
    expect(lastUser?.steering).toBe(true);
    expect(result.current.queuedSend).toBe(false);

    firstHang.resolve({ disconnected: false });
    await act(async () => {
      await firstSend.catch(() => undefined);
      await Promise.resolve();
    });
  });

  it('preserves widget snapshots on the steered bubble after compaction flush', async () => {
    // A message queued mid-compaction carries inline context (widget snapshots /
    // chart selections). When the flush routes through steering (a turn is still
    // running), those cards must survive on the rebuilt bubble — not get dropped
    // because handleSendSteering only received message + attachmentMeta.
    const firstHang = mockHangFirstStream('thread-q-3');
    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    let firstSend: Promise<unknown> = Promise.resolve();
    await act(async () => {
      firstSend = result.current.handleSendMessage('running turn');
      await Promise.resolve();
    });
    await waitFor(() => expect(result.current.isLoading).toBe(true));

    const snapshot = { id: 'w1', title: 'Watchlist' } as unknown as never;
    act(() => {
      result.current.setIsCompacting('summarize');
    });
    await act(async () => {
      await result.current.handleSendMessage('deferred with widget', false, null, null, {
        widgetSnapshots: [snapshot],
      });
    });
    expect(result.current.queuedSend).toBe('deferred with widget');

    // Compaction finishes; turn still running → steer (the flush isLoading branch).
    await act(async () => {
      result.current.setIsCompacting(false);
      await Promise.resolve();
      await Promise.resolve();
    });

    await waitFor(() => {
      expect(mockSendStream).toHaveBeenCalledTimes(2);
    });
    const userMsgs = result.current.messages.filter((m) => m.role === 'user');
    const steered = userMsgs[userMsgs.length - 1] as {
      steering?: boolean;
      widgetSnapshots?: unknown[];
    };
    expect(steered.steering).toBe(true);
    expect(steered.widgetSnapshots).toHaveLength(1);

    firstHang.resolve({ disconnected: false });
    await act(async () => {
      await firstSend.catch(() => undefined);
      await Promise.resolve();
    });
  });
});
