/**
 * Regression: reconnect a cached, re-shown view to a run that started while it
 * was hidden. ChatView instances stay MOUNTED in an LRU cache (useChatViewCache)
 * with a stable key, so revisiting a thread does NOT remount or re-fire the
 * thread-load effect — a follow-up turn dispatched into an already-visited PTC
 * thread kept showing the PRIOR turn until a full refresh. `reconnectIfStaleRun`
 * (called by useForeignRunCatchUp when the view is shown) closes that gap by
 * re-checking /status; on a live run it differs from what's on screen it
 * requests a FULL history reload (which replays /messages and then reconnects)
 * — a bare stream attach is not enough, because live streams carry no
 * user_message event, so the dispatched turn's query row (and any turns
 * completed while hidden) only render via the replay. /status only carries
 * run_id while a run is live, so an idle thread is a no-op.
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

import { fetchThreadTurns, getThreadFeedback, getWorkflowStatus, reconnectToWorkflowStream, replayThreadHistory, sendChatMessageStream } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';

const mockStatus = getWorkflowStatus as Mock;
const mockReconnect = reconnectToWorkflowStream as Mock;
const mockReplay = replayThreadHistory as Mock;
const mockFeedback = getThreadFeedback as Mock;
const mockSend = sendChatMessageStream as Mock;
const mockTurns = fetchThreadTurns as Mock;

describe('useChatMessages — reconnect-on-reactivation (cached view, run started while hidden)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockReplay.mockResolvedValue(undefined);
    mockReconnect.mockResolvedValue({ disconnected: false, aborted: false });
  });

  it('reloads history, then attaches to a newer live run the re-shown view had missed', async () => {
    // Mount shows a completed/idle thread → no reconnect on load.
    mockStatus.mockResolvedValue(threadStatus());

    const { result } = renderHookWithProviders(() => useChatMessages('ws', 'th'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalled());
    await settleMountEffect();
    expect(mockReconnect).not.toHaveBeenCalled();
    expect(mockReplay).toHaveBeenCalledTimes(1);

    // A second round dispatched a follow-up run into THIS thread; it is now live.
    mockStatus.mockResolvedValue(threadStatus({ can_reconnect: true, status: 'running', run_id: 'run-2' }));
    // Deliver one event so this models a REAL attach: a zero-content end
    // deliberately releases the run-id latch for a bounded retry, so an
    // event-less mock would re-attach and break the idempotency assertion below
    // for the wrong reason.
    mockReconnect.mockImplementation((...args: unknown[]) => {
      const onEvent = args[3] as (e: Record<string, unknown>) => void;
      onEvent({ event: 'message_chunk', role: 'assistant', agent: 'main', content_type: 'text', content: 'live…' });
      return Promise.resolve({ disconnected: false, aborted: false });
    });

    // Reactivation (useForeignRunCatchUp calls this on inactive→active).
    await act(async () => {
      await result.current.reconnectIfStaleRun();
    });

    // NOT a bare attach: the stale live run triggers a full history reload
    // first (replay re-fetched), and the reload flow then attaches to the live
    // run, replaying from the start of its per-run key.
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(mockReconnect).toHaveBeenCalledTimes(1));
    await settleMountEffect();
    expect(mockReconnect.mock.calls[0][0]).toBe('th');
    expect(mockReconnect.mock.calls[0][1]).toBe('run-2');
    expect(mockReconnect.mock.calls[0][2]).toBeNull();
    // The reconnect happens AFTER the reload's replay, never before it.
    expect(mockReplay.mock.invocationCallOrder[1]).toBeLessThan(mockReconnect.mock.invocationCallOrder[0]);

    // Reactivating again with the SAME live run on screen is a no-op (the
    // reload's reconnect latched run-2, closing the stale-run gate).
    await act(async () => {
      await result.current.reconnectIfStaleRun();
    });
    await settleMountEffect();
    expect(mockReconnect).toHaveBeenCalledTimes(1);
    expect(mockReplay).toHaveBeenCalledTimes(2);
  });

  it('renders the dispatched turn\'s user bubble (via replay) ahead of the streaming assistant bubble', async () => {
    // Mount: one completed turn on screen.
    mockStatus.mockResolvedValue(threadStatus());
    mockReplay.mockImplementation((_tid: string, onEvent: (e: Record<string, unknown>) => void) => {
      onEvent({ event: 'user_message', turn_index: 0, content: 'earlier question', role: 'user' });
      return Promise.resolve();
    });

    const { result } = renderHookWithProviders(() => useChatMessages('ws', 'th'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalled());
    await settleMountEffect();
    expect(result.current.messages.some((m) => m.id === 'history-user-0')).toBe(true);
    // The dispatched turn's query row is NOT on screen yet — the view was
    // hidden when the turn was dispatched.
    expect(result.current.messages.some((m) => m.id === 'history-user-1')).toBe(false);

    // While hidden: a new turn was dispatched into this thread (its query row is
    // now part of persisted history) and its run is live.
    mockStatus.mockResolvedValue(threadStatus({ can_reconnect: true, status: 'running', run_id: 'run-2' }));
    mockReplay.mockImplementation((_tid: string, onEvent: (e: Record<string, unknown>) => void) => {
      onEvent({ event: 'user_message', turn_index: 0, content: 'earlier question', role: 'user' });
      onEvent({ event: 'user_message', turn_index: 1, content: 'placeholder follow-up instruction', role: 'user' });
      return Promise.resolve();
    });
    mockReconnect.mockImplementation((...args: unknown[]) => {
      const onEvent = args[3] as (e: Record<string, unknown>) => void;
      onEvent({ event: 'message_chunk', role: 'assistant', agent: 'main', content_type: 'text', content: 'streamed answer chunk' });
      return Promise.resolve({ disconnected: false, aborted: false });
    });

    // Reactivation.
    await act(async () => {
      await result.current.reconnectIfStaleRun();
    });

    // (a) the replay is re-fetched…
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    // (c) …and the reconnect attaches the live run AFTER the reload.
    await waitFor(() => expect(mockReconnect).toHaveBeenCalledTimes(1));
    await settleMountEffect();
    expect(mockReconnect.mock.calls[0][1]).toBe('run-2');
    expect(mockReplay.mock.invocationCallOrder[1]).toBeLessThan(mockReconnect.mock.invocationCallOrder[0]);

    // (b) the dispatched turn's user bubble renders, AHEAD of the streaming
    // assistant bubble (deterministic history ids dedupe the re-replayed turn 0).
    const msgs = result.current.messages;
    expect(msgs.filter((m) => m.id === 'history-user-0')).toHaveLength(1);
    const userIdx = msgs.findIndex((m) => m.id === 'history-user-1');
    expect(userIdx).toBeGreaterThan(-1);
    expect(msgs[userIdx].content).toBe('placeholder follow-up instruction');
    const liveIdx = msgs.findIndex(
      (m) => typeof m.id === 'string' && m.id.startsWith('assistant-reconnect-'),
    );
    expect(liveIdx).toBeGreaterThan(userIdx);
    expect(JSON.stringify(msgs[liveIdx])).toContain('streamed answer chunk');
  });

  it('reloads history when the missed run already FINISHED while hidden (terminal staleness)', async () => {
    // Mount: one completed turn on screen; backend watermark agrees (turn 0).
    mockStatus.mockResolvedValue(threadStatus({ latest_turn_index: 0 }));
    mockReplay.mockImplementation((_tid: string, onEvent: (e: Record<string, unknown>) => void) => {
      onEvent({ event: 'user_message', turn_index: 0, content: 'earlier question', role: 'user' });
      onEvent({ event: 'message_chunk', turn_index: 0, role: 'assistant', agent: 'main', content_type: 'text', content: 'earlier answer' });
      return Promise.resolve();
    });

    const { result } = renderHookWithProviders(() => useChatMessages('ws', 'th'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalled());
    await settleMountEffect();
    expect(result.current.messages.some((m) => m.id === 'history-user-0')).toBe(true);
    expect(result.current.messages.some((m) => m.id === 'history-user-1')).toBe(false);

    // While hidden: a dispatched turn ran AND COMPLETED. /status is terminal —
    // can_reconnect=false, no reconnectable run — only the persisted turn
    // watermark says the view is stale.
    mockStatus.mockResolvedValue(threadStatus({ latest_turn_index: 1 }));
    mockReplay.mockImplementation((_tid: string, onEvent: (e: Record<string, unknown>) => void) => {
      onEvent({ event: 'user_message', turn_index: 0, content: 'earlier question', role: 'user' });
      onEvent({ event: 'message_chunk', turn_index: 0, role: 'assistant', agent: 'main', content_type: 'text', content: 'earlier answer' });
      onEvent({ event: 'user_message', turn_index: 1, content: 'placeholder follow-up instruction', role: 'user' });
      onEvent({ event: 'message_chunk', turn_index: 1, role: 'assistant', agent: 'main', content_type: 'text', content: 'placeholder completed answer' });
      return Promise.resolve();
    });

    // Reactivation.
    await act(async () => {
      await result.current.reconnectIfStaleRun();
    });

    // The replay is re-fetched and the completed turn's user + assistant
    // bubbles render; there is no live run, so nothing attaches.
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await settleMountEffect();
    expect(mockReconnect).not.toHaveBeenCalled();
    const msgs = result.current.messages;
    const userIdx = msgs.findIndex((m) => m.id === 'history-user-1');
    expect(userIdx).toBeGreaterThan(-1);
    expect(msgs[userIdx].content).toBe('placeholder follow-up instruction');
    expect(JSON.stringify(msgs)).toContain('placeholder completed answer');
    // Deterministic history ids dedupe the re-replayed turn 0.
    expect(msgs.filter((m) => m.id === 'history-user-0')).toHaveLength(1);

    // Reactivating again is a no-op: the reload's replay re-recorded the
    // watermark (turn 1), closing the gate — no reload loop.
    await act(async () => {
      await result.current.reconnectIfStaleRun();
    });
    await settleMountEffect();
    expect(mockReplay).toHaveBeenCalledTimes(2);
  });

  it('reloads when the watermark exceeds the server (fork/edit elsewhere truncated turns)', async () => {
    // Mount: two turns on screen; watermark records turn 1.
    mockStatus.mockResolvedValue(threadStatus({ latest_turn_index: 1 }));
    mockReplay.mockImplementation((_tid: string, onEvent: (e: Record<string, unknown>) => void) => {
      onEvent({ event: 'user_message', turn_index: 0, content: 'earlier question', role: 'user' });
      onEvent({ event: 'user_message', turn_index: 1, content: 'second question', role: 'user' });
      return Promise.resolve();
    });

    const { result } = renderHookWithProviders(() => useChatMessages('ws', 'th'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalled());
    await settleMountEffect();
    expect(result.current.messages.some((m) => m.id === 'history-user-1')).toBe(true);

    // While hidden: another tab/device edited turn 1 (fork truncates rows >= 1
    // and its regenerated run already completed). Server MAX drops BELOW this
    // view's watermark — the strict '>' check would never fire here.
    mockStatus.mockResolvedValue(threadStatus({ latest_turn_index: 0 }));
    mockReplay.mockImplementation((_tid: string, onEvent: (e: Record<string, unknown>) => void) => {
      onEvent({ event: 'user_message', turn_index: 0, content: 'earlier question', role: 'user' });
      return Promise.resolve();
    });

    await act(async () => {
      await result.current.reconnectIfStaleRun();
    });
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await settleMountEffect();
    // The truncated turn is gone and the gate re-closed at the new watermark.
    expect(result.current.messages.some((m) => m.id === 'history-user-1')).toBe(false);
    await act(async () => {
      await result.current.reconnectIfStaleRun();
    });
    await settleMountEffect();
    expect(mockReplay).toHaveBeenCalledTimes(2);
  });

  it('does NOT reload on reactivation when the view already rendered the latest turn', async () => {
    // Mount: turn 0 on screen, watermark agrees.
    mockStatus.mockResolvedValue(threadStatus({ latest_turn_index: 0 }));
    mockReplay.mockImplementation((_tid: string, onEvent: (e: Record<string, unknown>) => void) => {
      onEvent({ event: 'user_message', turn_index: 0, content: 'earlier question', role: 'user' });
      return Promise.resolve();
    });

    const { result } = renderHookWithProviders(() => useChatMessages('ws', 'th'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalled());
    await settleMountEffect();
    expect(mockReplay).toHaveBeenCalledTimes(1);

    // Reactivation with an unchanged watermark — nothing was missed.
    await act(async () => {
      await result.current.reconnectIfStaleRun();
    });
    await settleMountEffect();
    expect(mockReconnect).not.toHaveBeenCalled();
    expect(mockReplay).toHaveBeenCalledTimes(1);
  });

  it('does nothing when the thread is idle (no live run to attach to)', async () => {
    mockStatus.mockResolvedValue(threadStatus());

    const { result } = renderHookWithProviders(() => useChatMessages('ws', 'th'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalled());
    await settleMountEffect();
    expect(mockReplay).toHaveBeenCalledTimes(1);

    // /status still reports idle (run_id absent) — nothing newer than what's shown.
    await act(async () => {
      await result.current.reconnectIfStaleRun();
    });
    await settleMountEffect();
    expect(mockReconnect).not.toHaveBeenCalled();
    // No spurious reload either.
    expect(mockReplay).toHaveBeenCalledTimes(1);
  });

  const checkWith = async (result: { current: { reconnectIfStaleRun: () => Promise<boolean> } }) => {
    let read: boolean | undefined;
    await act(async () => {
      read = await result.current.reconnectIfStaleRun();
    });
    return read;
  };
  const httpError = (status: number) => Object.assign(new Error(`HTTP ${status}`), { response: { status } });
  const replayTurns = (...turns: number[]) => (_tid: string, onEvent: (e: Record<string, unknown>) => void) => {
    for (const turn of turns) onEvent({ event: 'user_message', turn_index: turn, content: `question ${turn}`, role: 'user' });
    return Promise.resolve();
  };
  /** Sends a turn whose stream stays open until the returned end() is called. */
  const sendHeldTurn = async (result: { current: { handleSendMessage: (text: string) => Promise<unknown> } }) => {
    let finish!: () => void;
    mockSend.mockImplementationOnce((...args: unknown[]) => {
      const onRunIdResolved = (args[3] as { onRunIdResolved: (runId: string, threadId: string | null) => void }).onRunIdResolved;
      onRunIdResolved('run-own', 'th');
      return new Promise((resolve) => { finish = () => resolve({ disconnected: false, aborted: false }); });
    });
    let sent: Promise<unknown> = Promise.resolve();
    await act(async () => {
      sent = result.current.handleSendMessage('a question sent mid-load');
      await new Promise((r) => setTimeout(r, 0));
    });
    return () => act(async () => { finish(); await sent; });
  };

  it('says whether it could read the thread\'s status, so a failure that can pass is asked again', async () => {
    mockStatus.mockResolvedValue(threadStatus());
    const { result } = renderHookWithProviders(() => useChatMessages('ws', 'th'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalled());
    await settleMountEffect();
    const check = () => checkWith(result);

    // Down, signed out mid-refresh, timed out, rate limited: each can pass.
    for (const status of [503, 401, 408, 429]) {
      mockStatus.mockRejectedValueOnce(httpError(status));
      expect(await check()).toBe(false);
    }
    mockStatus.mockRejectedValueOnce(new Error('Network Error'));
    expect(await check()).toBe(false);
    // Gone or not the caller's: asking again gets the same answer.
    mockStatus.mockRejectedValueOnce(httpError(404));
    expect(await check()).toBe(true);
    expect(mockReplay).toHaveBeenCalledTimes(1);
    // Bounded: a hung read would hold every later check.
    expect(mockStatus).toHaveBeenLastCalledWith('th', { timeout: expect.any(Number) });

    // Read, and the reload it asks for stays owed until a later check finds it took.
    mockStatus.mockResolvedValue(threadStatus({ can_reconnect: true, status: 'running', run_id: 'run-2' }));
    expect(await check()).toBe(false);
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
  });

  it('reloads a view whose load never settled, and asks again until one does', async () => {
    mockStatus.mockResolvedValue(threadStatus({ status: 'idle' }));
    mockReplay.mockRejectedValueOnce(new Error('HTTP 500')).mockRejectedValueOnce(new Error('HTTP 500'));
    const errors = vi.spyOn(console, 'error').mockImplementation(() => {});
    const { result } = renderHookWithProviders(() => useChatMessages('ws', 'th'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(1));
    await settleMountEffect();

    // A load is the check, and one that fails leaves it owed.
    expect(await checkWith(result)).toBe(false);
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await settleMountEffect();
    expect(await checkWith(result)).toBe(false);
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(3));
    await settleMountEffect();
    // Settled now, so the next check reads /status instead.
    expect(await checkWith(result)).toBe(true);
    expect(mockReplay).toHaveBeenCalledTimes(3);
    errors.mockRestore();
  });

  it('asks again when a view with turns cannot read the turn count', async () => {
    mockStatus.mockResolvedValue(threadStatus({ latest_turn_index: 0 }));
    mockReplay.mockImplementation((_tid: string, onEvent: (e: Record<string, unknown>) => void) => {
      onEvent({ event: 'user_message', turn_index: 0, content: 'earlier question', role: 'user' });
      return Promise.resolve();
    });
    const { result } = renderHookWithProviders(() => useChatMessages('ws', 'th'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalled());
    await settleMountEffect();

    // A thread with turns has rows, so a missing count is a read that failed
    // and may be hiding the turn this view missed.
    mockStatus.mockResolvedValue(threadStatus({ latest_turn_index: null }));
    expect(await checkWith(result)).toBe(false);
    expect(mockReplay).toHaveBeenCalledTimes(1);

    mockStatus.mockResolvedValue(threadStatus({ latest_turn_index: 1 }));
    mockReplay.mockImplementation(replayTurns(0, 1));
    expect(await checkWith(result)).toBe(false);
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await settleMountEffect();
    expect(await checkWith(result)).toBe(true);
    expect(mockReplay).toHaveBeenCalledTimes(2);
  });

  it('settles on a backend that sends no turn count field at all', async () => {
    mockStatus.mockResolvedValue(threadStatus({ latest_turn_index: 0 }));
    mockReplay.mockImplementation(replayTurns(0));
    const { result } = renderHookWithProviders(() => useChatMessages('ws', 'th'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalled());
    await settleMountEffect();

    // Asking again can't make such a backend send one.
    mockStatus.mockResolvedValue(threadStatus());
    expect(await checkWith(result)).toBe(true);
    expect(mockReplay).toHaveBeenCalledTimes(1);
  });

  it('takes a missing turn count at its word only on a thread that never ran', async () => {
    mockStatus.mockResolvedValue(threadStatus({ status: 'idle', latest_turn_index: null }));
    const { result } = renderHookWithProviders(() => useChatMessages('ws', 'th'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalled());
    await settleMountEffect();

    expect(await checkWith(result)).toBe(true);
    expect(mockReplay).toHaveBeenCalledTimes(1);

    // Its first turn ran elsewhere and finished, and the count failed to read.
    mockStatus.mockResolvedValue(threadStatus({ latest_turn_index: null }));
    expect(await checkWith(result)).toBe(false);
    expect(mockReplay).toHaveBeenCalledTimes(1);

    mockStatus.mockResolvedValue(threadStatus({ latest_turn_index: 0 }));
    mockReplay.mockImplementation(replayTurns(0));
    expect(await checkWith(result)).toBe(false);
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await settleMountEffect();
    expect(await checkWith(result)).toBe(true);
    expect(mockReplay).toHaveBeenCalledTimes(2);
  });

  it.each([
    ['a turn that finished', threadStatus({ latest_turn_index: 1 })],
    ['a run still live', threadStatus({ can_reconnect: true, status: 'running', run_id: 'run-2', latest_turn_index: 1 })],
  ])('asks for the reload again when the one it asked for fails (%s)', async (_case, missed) => {
    mockStatus.mockResolvedValue(threadStatus({ latest_turn_index: 0 }));
    mockReplay.mockImplementation(replayTurns(0));
    mockReconnect.mockImplementation((...args: unknown[]) => {
      const onEvent = args[3] as (e: Record<string, unknown>) => void;
      onEvent({ event: 'message_chunk', role: 'assistant', agent: 'main', content_type: 'text', content: 'live…' });
      return Promise.resolve({ disconnected: false, aborted: false });
    });
    const errors = vi.spyOn(console, 'error').mockImplementation(() => {});
    const { result } = renderHookWithProviders(() => useChatMessages('ws', 'th'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(1));
    await settleMountEffect();

    mockStatus.mockResolvedValue(missed);
    mockReplay.mockRejectedValueOnce(new Error('HTTP 500'));
    expect(await checkWith(result)).toBe(false);
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await settleMountEffect();

    mockReplay.mockImplementation(replayTurns(0, 1));
    expect(await checkWith(result)).toBe(false);
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(3));
    await settleMountEffect();
    expect(result.current.messages.some((m) => m.id === 'history-user-1')).toBe(true);

    expect(await checkWith(result)).toBe(true);
    expect(mockReplay).toHaveBeenCalledTimes(3);
    errors.mockRestore();
  });

  it('declines until the whole load settles, so the caller asks again', async () => {
    mockStatus.mockResolvedValue(threadStatus({ status: 'idle' }));
    let finishReplay!: () => void;
    mockReplay.mockImplementationOnce(() => new Promise<void>((resolve) => { finishReplay = resolve; }));
    let finishFeedback!: () => void;
    mockFeedback.mockImplementationOnce(() => new Promise((resolve) => { finishFeedback = () => resolve([]); }));
    const { result } = renderHookWithProviders(() => useChatMessages('ws', 'th'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalled());

    expect(await checkWith(result)).toBe(false);
    // The replay is done but the load is not: a reload requested here would
    // restart a load that has not recorded itself as settled yet.
    await act(async () => { finishReplay(); });
    expect(result.current.isLoadingHistory).toBe(false);
    expect(result.current.isLoadingThread).toBe(true);
    expect(await checkWith(result)).toBe(false);

    await act(async () => { finishFeedback(); });
    await settleMountEffect();
    expect(result.current.isLoadingThread).toBe(false);
    expect(await checkWith(result)).toBe(true);
    expect(mockReplay).toHaveBeenCalledTimes(1);
  });

  it('runs a reload a send overtook in its status read once that turn ends, not over it', async () => {
    mockStatus.mockResolvedValue(threadStatus({ latest_turn_index: 0 }));
    mockReplay.mockImplementation(replayTurns(0));
    const { result } = renderHookWithProviders(() => useChatMessages('ws', 'th'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(1));
    await settleMountEffect();

    let answerStatus!: () => void;
    mockStatus
      .mockResolvedValueOnce(threadStatus({ latest_turn_index: 1 }))
      .mockImplementationOnce(() => new Promise((resolve) => { answerStatus = () => resolve(threadStatus({ latest_turn_index: 1 })); }));
    expect(await checkWith(result)).toBe(false);
    await waitFor(() => expect(mockStatus).toHaveBeenCalledTimes(3));

    const endTurn = await sendHeldTurn(result);
    await act(async () => { answerStatus(); });
    await settleMountEffect();
    // The status predates the turn, so replaying it would run over that turn.
    expect(mockReplay).toHaveBeenCalledTimes(1);

    mockReplay.mockImplementation(replayTurns(0, 1, 2));
    await endTurn();
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
  });

  it('leaves a turn sent after the reload\'s replay alone, reconnecting to no run the status named before it', async () => {
    mockStatus.mockResolvedValue(threadStatus({ status: 'idle' }));
    const { result } = renderHookWithProviders(() => useChatMessages('ws', 'th'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(1));
    await settleMountEffect();

    mockStatus.mockResolvedValue(threadStatus({ can_reconnect: true, status: 'running', run_id: 'run-elsewhere' }));
    let finishFeedback!: () => void;
    mockFeedback.mockImplementationOnce(() => new Promise((resolve) => { finishFeedback = () => resolve([]); }));
    expect(await checkWith(result)).toBe(false);
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(result.current.isLoadingHistory).toBe(false));

    mockStatus.mockResolvedValue(threadStatus({ status: 'idle' }));
    const endTurn = await sendHeldTurn(result);
    await act(async () => { finishFeedback(); });
    await settleMountEffect();
    expect(mockReconnect).not.toHaveBeenCalled();

    await endTurn();
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(3));
    await settleMountEffect();
    expect(mockReconnect).not.toHaveBeenCalled();
  });

  it('runs a reload again when a turn sent in its status read has already ended', async () => {
    mockStatus.mockResolvedValue(threadStatus({ status: 'idle' }));
    const { result } = renderHookWithProviders(() => useChatMessages('ws', 'th'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(1));
    await settleMountEffect();

    let answerStatus!: () => void;
    const elsewhere = threadStatus({ can_reconnect: true, status: 'running', run_id: 'run-elsewhere' });
    mockStatus
      .mockResolvedValueOnce(elsewhere)
      .mockImplementationOnce(() => new Promise((resolve) => { answerStatus = () => resolve(elsewhere); }))
      .mockResolvedValue(threadStatus({ status: 'idle' }));
    expect(await checkWith(result)).toBe(false);
    await waitFor(() => expect(mockStatus).toHaveBeenCalledTimes(3));

    const endTurn = await sendHeldTurn(result);
    await endTurn();
    await act(async () => { answerStatus(); });
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await settleMountEffect();
    expect(mockReconnect).not.toHaveBeenCalled();
  });

  it('runs a reload again when a turn sent after its replay has already ended, reconnecting to no run the status named before it', async () => {
    mockStatus.mockResolvedValue(threadStatus({ status: 'idle' }));
    const { result } = renderHookWithProviders(() => useChatMessages('ws', 'th'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(1));
    await settleMountEffect();

    mockStatus.mockResolvedValue(threadStatus({ can_reconnect: true, status: 'running', run_id: 'run-elsewhere' }));
    let finishFeedback!: () => void;
    mockFeedback.mockImplementationOnce(() => new Promise((resolve) => { finishFeedback = () => resolve([]); }));
    expect(await checkWith(result)).toBe(false);
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(result.current.isLoadingHistory).toBe(false));

    mockStatus.mockResolvedValue(threadStatus({ status: 'idle' }));
    const endTurn = await sendHeldTurn(result);
    await endTurn();
    await act(async () => { finishFeedback(); });
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(3));
    await settleMountEffect();
    expect(mockReconnect).not.toHaveBeenCalled();
  });

  describe('an edit that lands in a reload', () => {
    const CHECKPOINTS = { turns: [0, 1].map((i) => ({ turn_index: i, edit_checkpoint_id: `cp-edit-${i}`, regenerate_checkpoint_id: `cp-regen-${i}` })) };

    /** Mounts on two turns and starts a reload whose status read waits for answer(). */
    const reloadHeldInStatus = async () => {
      mockStatus.mockResolvedValue(threadStatus({ latest_turn_index: 1 }));
      mockReplay.mockImplementation(replayTurns(0, 1));
      const hook = renderHookWithProviders(() => useChatMessages('ws', 'th'));
      await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(1));
      await settleMountEffect();

      let answer!: () => void;
      const elsewhere = threadStatus({ can_reconnect: true, status: 'running', run_id: 'run-elsewhere', latest_turn_index: 2 });
      mockStatus
        .mockResolvedValueOnce(elsewhere)
        .mockImplementationOnce(() => new Promise((resolve) => { answer = () => resolve(elsewhere); }))
        .mockResolvedValue(threadStatus({ status: 'idle', latest_turn_index: 1 }));
      expect(await checkWith(hook.result)).toBe(false);
      await waitFor(() => expect(mockStatus).toHaveBeenCalledTimes(3));
      return { ...hook, answer: () => act(async () => { answer(); }) };
    };

    /** Starts an edit of the last question whose checkpoint read waits for answer(). */
    const editHeldInPreflight = async (result: { current: ReturnType<typeof useChatMessages> }) => {
      let answer!: (data: unknown) => void;
      let fail!: (error: Error) => void;
      mockTurns.mockImplementationOnce(() => new Promise((resolve, reject) => { answer = resolve; fail = reject; }));
      const lastQuestion = result.current.messages.filter((m) => m.role === 'user').at(-1)!;
      let edited: Promise<unknown> = Promise.resolve();
      await act(async () => {
        edited = result.current.handleEditMessage(lastQuestion.id as string, 'an edited question');
        await new Promise((r) => setTimeout(r, 0));
      });
      return {
        answer: (data: unknown) => act(async () => { answer(data); await new Promise((r) => setTimeout(r, 0)); }),
        fail: (error: Error) => act(async () => { fail(error); await new Promise((r) => setTimeout(r, 0)); }),
        settled: () => edited,
      };
    };

    it('waits for the fork it cut the transcript for, not replaying over the cut', async () => {
      const { result, answer: answerStatus } = await reloadHeldInStatus();
      const edit = await editHeldInPreflight(result);

      await answerStatus();
      await settleMountEffect();
      expect(mockReplay).toHaveBeenCalledTimes(1);
      expect(mockReconnect).not.toHaveBeenCalled();

      let finish!: () => void;
      mockSend.mockImplementationOnce(() => new Promise((resolve) => { finish = () => resolve({ disconnected: false, aborted: false }); }));
      await edit.answer(CHECKPOINTS);
      await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(1));
      await settleMountEffect();
      expect(mockReplay).toHaveBeenCalledTimes(1);

      await act(async () => { finish(); await edit.settled(); });
      await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
      await settleMountEffect();
      expect(mockReconnect).not.toHaveBeenCalled();
    });

    it('runs the reload that waited once a failed checkpoint read hands the transcript back', async () => {
      const { result, answer: answerStatus } = await reloadHeldInStatus();
      const lastQuestion = result.current.messages.filter((m) => m.role === 'user').at(-1)!;
      const edit = await editHeldInPreflight(result);
      await answerStatus();
      await settleMountEffect();

      await edit.fail(new Error('network down'));
      await edit.settled();
      await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
      await waitFor(() => expect(result.current.isLoadingHistory).toBe(false));
      expect(mockSend).not.toHaveBeenCalled();
      expect(result.current.messages.some((m) => m.content === 'an edited question')).toBe(false);
      // The edit given back stays open over its message through the reload.
      expect(result.current.editDrafts.get({ turnIndex: lastQuestion.turnIndex as number, original: lastQuestion.content as string })).toBe('an edited question');
    });

    it('is refused while a replay is rebuilding the transcript it would cut', async () => {
      mockStatus.mockResolvedValue(threadStatus({ latest_turn_index: 1 }));
      mockReplay.mockImplementation(replayTurns(0, 1));
      const { result } = renderHookWithProviders(() => useChatMessages('ws', 'th'));
      await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(1));
      await settleMountEffect();
      const lastQuestion = result.current.messages.filter((m) => m.role === 'user').at(-1)!;

      let finishReplay!: () => void;
      mockReplay.mockImplementationOnce((tid: string, onEvent: (e: Record<string, unknown>) => void) => {
        replayTurns(0, 1)(tid, onEvent);
        return new Promise<void>((resolve) => { finishReplay = resolve; });
      });
      mockStatus.mockResolvedValue(threadStatus({ latest_turn_index: 2 }));
      expect(await checkWith(result)).toBe(false);
      await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
      expect(result.current.messages.some((m) => m.id === lastQuestion.id)).toBe(true);

      await act(async () => {
        await result.current.handleEditMessage(lastQuestion.id as string, 'an edited question');
      });
      expect(mockTurns).not.toHaveBeenCalled();
      expect(result.current.messages.some((m) => m.content === 'an edited question')).toBe(false);
      // The edit waits in its box rather than vanishing.
      expect(result.current.editDrafts.get({ turnIndex: lastQuestion.turnIndex as number, original: lastQuestion.content as string })).toBe('an edited question');
      await act(async () => { finishReplay(); });
    });
  });
});

describe('useChatMessages — an edit open while the view follows a turn started elsewhere', () => {
  const replayOf = (questions: string[]) => (_tid: string, onEvent: (e: Record<string, unknown>) => void) => {
    questions.forEach((content, turn) => onEvent({ event: 'user_message', turn_index: turn, content, role: 'user' }));
    return Promise.resolve();
  };

  beforeEach(() => {
    vi.clearAllMocks();
    mockReconnect.mockResolvedValue({ disconnected: false, aborted: false });
    mockStatus.mockResolvedValue(threadStatus());
  });

  /** Mounts on two turns with an edit typed over question 1, then follows a
   *  run started elsewhere whose reload shows `after`. */
  async function followWithEditOpen(after: string[], text = 'question 1 half typed') {
    mockReplay.mockImplementation(replayOf(['question 0', 'question 1']));
    const hook = renderHookWithProviders(() => useChatMessages('ws', 'th'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(1));
    await settleMountEffect();
    act(() => hook.result.current.editDrafts.put({ turnIndex: 1, original: 'question 1', text }));

    mockReplay.mockImplementation(replayOf(after));
    mockStatus.mockResolvedValue(threadStatus({ can_reconnect: true, status: 'running', run_id: 'run-elsewhere' }));
    await act(async () => {
      await hook.result.current.reconnectIfStaleRun();
    });
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(hook.result.current.isLoadingHistory).toBe(false));
    return hook;
  }

  it('keeps the edit open over its message through the reload', async () => {
    const { result } = await followWithEditOpen(['question 0', 'question 1', 'question 2']);
    expect(result.current.editDrafts.get({ turnIndex: 1, original: 'question 1' })).toBe('question 1 half typed');
    expect(result.current.returnedEdit).toBeNull();
    expect(result.current.messageError).toBeNull();
  });

  it('gives the composer an edit whose message an edit elsewhere replaced', async () => {
    const { result } = await followWithEditOpen(['question 0', 'question 1 from elsewhere']);
    expect(result.current.editDrafts.get({ turnIndex: 1, original: 'question 1' })).toBeUndefined();
    expect(result.current.returnedEdit).toBe('question 1 half typed');
    expect(result.current.messageError).toEqual({ message: 'chat.forkStaleEdit', kind: 'notice' });
  });

  it('lets go of a box left as it opened when its message is gone', async () => {
    const { result } = await followWithEditOpen(['question 0', 'question 1 from elsewhere'], 'question 1');
    expect(result.current.editDrafts.get({ turnIndex: 1, original: 'question 1' })).toBeUndefined();
    expect(result.current.returnedEdit).toBeNull();
    expect(result.current.messageError).toBeNull();
  });

  it("keeps a thread's edit off another thread whose messages read the same", async () => {
    mockReplay.mockImplementation(replayOf(['question 0', 'question 1']));
    let threadId = 'th';
    const { result, rerender } = renderHookWithProviders(() => useChatMessages('ws', threadId));
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(1));
    await settleMountEffect();
    act(() => result.current.editDrafts.put({ turnIndex: 1, original: 'question 1', text: 'written on th' }));

    threadId = 'th-2';
    rerender();
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(result.current.isLoadingHistory).toBe(false));
    expect(result.current.messages.some((m) => m.id === 'history-user-1')).toBe(true);
    expect(result.current.editDrafts.get({ turnIndex: 1, original: 'question 1' })).toBeUndefined();
    expect(result.current.returnedEdit).toBeNull();

    threadId = 'th';
    rerender();
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(3));
    await waitFor(() => expect(result.current.isLoadingHistory).toBe(false));
    expect(result.current.editDrafts.get({ turnIndex: 1, original: 'question 1' })).toBe('written on th');
  });
});
