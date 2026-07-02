/**
 * Regression: reconnect a cached, re-shown view to a run that started while it
 * was hidden. ChatView instances stay MOUNTED in an LRU cache (useChatViewCache)
 * with a stable key, so revisiting a thread does NOT remount or re-fire the
 * thread-load effect — a follow-up turn dispatched into an already-visited PTC
 * thread kept showing the PRIOR turn until a full refresh. `reconnectIfStaleRun`
 * (called by ChatView's become-active effect) closes that gap by re-checking
 * /status; on a live run it differs from what's on screen it requests a FULL
 * history reload (which replays /messages and then reconnects) — a bare stream
 * attach is not enough, because live streams carry no user_message event, so
 * the dispatched turn's query row (and any turns completed while hidden) only
 * render via the replay. /status only carries run_id while a run is live, so an
 * idle thread is a no-op.
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

    // Reactivation (the become-active effect calls this on inactive→active).
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
});
