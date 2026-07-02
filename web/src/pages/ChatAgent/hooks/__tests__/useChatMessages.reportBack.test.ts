/**
 * Report-back watch (PTC dispatch → flash report-back turns).
 *
 * After a PTC dispatch the backend fires a follow-up flash "report-back"
 * workflow per completed analysis, named via a pub/sub wake (run_id payload)
 * and durably via `/status.report_back_run_id` / `recent_report_back_run_ids`.
 * These tests drive the REAL hook internals (mirroring the sibling stop suite)
 * with the api module mocked, covering: arming (load / approve / activation),
 * wake + catch-up attach paths, the FIFO wake latch, chained-attach ownership
 * (Bug A), the idle watchdog + terminality gate, dedup release on zero-content
 * ends, and the backstop give-up cap.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import { settleMountEffect, threadStatus, captureWatchCalls } from './chatHookHarness';

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

import { getWorkflowStatus, getReportBackStatus, replayThreadHistory, reconnectToWorkflowStream, watchThread, sendChatMessageStream } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';
import { REPORT_BACK_IDLE_MAX_REARMS, REPORT_BACK_MAX_POLLS } from '../useReportBackWatch';

/** One captured reconnect reader: the run it targeted, its onEvent sink, its signal. */
interface CapturedReconnect {
  rid: string;
  onEvent: (event: Record<string, unknown>) => void;
  signal: AbortSignal;
}

/**
 * A mock reconnect reader that HANGS (resolves only when its signal aborts),
 * mirroring the server keeping a per-run stream open with no terminal sentinel.
 */
function hangingReconnect(captured: CapturedReconnect[]) {
  return (
    _tid: string,
    rid: string,
    _leid: unknown,
    onEvent: (event: Record<string, unknown>) => void,
    signal: AbortSignal,
  ) =>
    new Promise((resolve) => {
      captured.push({ rid, onEvent, signal });
      if (signal?.aborted) return resolve({ disconnected: false, aborted: true });
      signal?.addEventListener('abort', () => resolve({ disconnected: false, aborted: true }));
    });
}

/**
 * A mock reconnect reader that streams ONE chunk and resolves — a SUCCESSFUL
 * attach. Exact-count assertions need this: a zero-content end deliberately
 * releases the run-id dedup latch for a bounded retry, so an event-less mock
 * reads as a FAILED attach and legitimately re-attaches once.
 */
function streamedReconnect() {
  return (...args: unknown[]) => {
    const onEvent = args[3] as (e: Record<string, unknown>) => void;
    onEvent({ event: 'message_chunk', role: 'assistant', agent: 'main', content_type: 'text', content: 'summary…' });
    return Promise.resolve({ disconnected: false, aborted: false });
  };
}

/**
 * Like {@link streamedReconnect} but HELD open per run id: streams one chunk,
 * then resolves only when the test invokes the closer captured under that run.
 */
function heldReconnect(closers: Map<string, () => void>) {
  return (...args: unknown[]) => {
    const rid = args[1] as string;
    const onEvent = args[3] as (e: Record<string, unknown>) => void;
    onEvent({ event: 'message_chunk', role: 'assistant', agent: 'main', content_type: 'text', content: `[${rid}]` });
    return new Promise((resolve) => {
      closers.set(rid, () => resolve({ disconnected: false, aborted: false }));
    });
  };
}

const mockStatus = getWorkflowStatus as Mock;
const mockReportBackStatus = getReportBackStatus as Mock;
const mockReplay = replayThreadHistory as Mock;
const mockReconnect = reconnectToWorkflowStream as Mock;
const mockWatch = watchThread as Mock;
const mockSend = sendChatMessageStream as Mock;

const captureWatch = () => captureWatchCalls(mockWatch);

/**
 * Settle the mount effect under FAKE timers (the 60s backstop `setInterval` must
 * be fake-clock controllable); advancing 0ms drains the async chain without
 * firing a backstop tick.
 */
async function settleMountEffectFake() {
  for (let i = 0; i < 5; i++) {
    await act(async () => {
      await Promise.resolve();
      await vi.advanceTimersByTimeAsync(0);
    });
  }
}

/** Fire exactly one backstop `reconcile('poll')` tick (60s of fake clock). */
async function backstopTick() {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(60_000);
  });
}

// Mirrors REPORT_BACK_IDLE_ABORT_MS in useReportBackWatch (kept in sync by hand
// — not exported to avoid widening the hook's surface for a test).
const IDLE_MS = 4000;

describe('useChatMessages — report-back watch (PTC → flash report-back)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    // The cheap report-back slice is a strict subset of the full status, so
    // delegating lets one mockStatus.mockResolvedValue(...) per test feed both
    // the load-time read AND the watch's reconcile.
    mockReportBackStatus.mockImplementation((...args: unknown[]) => mockStatus(...args));
  });

  it('keeps approve/reconnect callback identities stable across re-renders', async () => {
    // handleApprovePTCAgent flows into memo()'d transcript components
    // (MessageBubble receives onApprovePTCAgent), so a per-render identity
    // would re-render every bubble on every stream chunk. Guards the
    // useReportBackWatch stable-facade contract: a bare re-render must not
    // mint new callbacks.
    mockStatus.mockResolvedValue(threadStatus());
    const { result, rerender } = renderHookWithProviders(() => useChatMessages('ws-rb', 'th-rb'));
    await settleMountEffect();

    const approveBefore = result.current.handleApprovePTCAgent;
    const reconnectBefore = result.current.reconnectIfStaleRun;
    rerender();
    expect(result.current.handleApprovePTCAgent).toBe(approveBefore);
    expect(result.current.reconnectIfStaleRun).toBe(reconnectBefore);
  });

  it('arms the report-back watch on load and the wake payload drives a direct reconnect', async () => {
    // PTC turn done (can_reconnect:false) but a report-back is still pending.
    mockStatus.mockResolvedValue(threadStatus({ pending_report_back: true }));
    const watchCalls = captureWatch();

    renderHookWithProviders(() => useChatMessages('ws-rb', 'th-rb'));

    // Armed: persistent watch signature (tid, onWorkflowStarted, onClosed,
    // onResubscribed); no reconnect yet (the PTC turn was already complete).
    await waitFor(() => expect(mockWatch).toHaveBeenCalledTimes(1));
    expect(mockWatch).toHaveBeenCalledWith('th-rb', expect.any(Function), expect.any(Function), expect.any(Function));
    expect(mockReconnect).not.toHaveBeenCalled();

    // The wake names the run → attach to exactly that run, fresh cursor, no
    // /status round-trip.
    await act(async () => {
      await watchCalls[0].cb({ run_id: 'rb-run-1' });
    });

    expect(mockReconnect).toHaveBeenCalledTimes(1);
    // Signature: reconnectToWorkflowStream(threadId, runId, lastEventId, onEvent, signal)
    expect(mockReconnect.mock.calls[0][0]).toBe('th-rb');
    expect(mockReconnect.mock.calls[0][1]).toBe('rb-run-1');
    expect(mockReconnect.mock.calls[0][2]).toBeNull();
  });

  it('does NOT duplicate the transcript when a staleness reload follows a live-streamed report-back', async () => {
    // The flash-thread round-trip repro: report-back turns stream in LIVE,
    // then the user jumps to the PTC thread and back. The live turns never
    // advanced the rendered-turn watermark, so reconnectIfStaleRun requests a
    // corrective reload whose replay covers those same turns. The attach's
    // success finalize must have marked them isHistory so the loader clears
    // them — otherwise the replay renders their twins (duplicated transcript).
    mockStatus.mockResolvedValue(threadStatus({ pending_report_back: true, latest_turn_index: 0 }));
    mockReplay.mockImplementation((_tid: string, onEvent: (e: Record<string, unknown>) => void) => {
      onEvent({ event: 'user_message', turn_index: 0, content: 'dispatch instruction', role: 'user' });
      onEvent({ event: 'message_chunk', turn_index: 0, role: 'assistant', agent: 'main', content_type: 'text', content: 'dispatched both analyses' });
      return Promise.resolve();
    });
    mockReconnect.mockImplementation(streamedReconnect());
    const watchCalls = captureWatch();

    const { result } = renderHookWithProviders(() => useChatMessages('ws-rb', 'th-rb'));
    await waitFor(() => expect(mockWatch).toHaveBeenCalledTimes(1));
    await settleMountEffect();

    // The wake attaches the report-back run; its stream delivers the summary
    // live and completes (turn 1 is persisted server-side by construction).
    await act(async () => {
      await watchCalls[0].cb({ run_id: 'rb-run-1' });
      await new Promise((r) => setTimeout(r, 0));
    });
    expect(mockReconnect).toHaveBeenCalledTimes(1);
    expect(JSON.stringify(result.current.messages)).toContain('summary…');

    // Jump away and back: the report-back turn advanced the server watermark
    // (turn 1) but not this view's; the drain also finished (idle, recents).
    mockStatus.mockResolvedValue(threadStatus({
      latest_turn_index: 1,
      recent_report_back_run_ids: ['rb-run-1'],
    }));
    mockReplay.mockImplementation((_tid: string, onEvent: (e: Record<string, unknown>) => void) => {
      onEvent({ event: 'user_message', turn_index: 0, content: 'dispatch instruction', role: 'user' });
      onEvent({ event: 'message_chunk', turn_index: 0, role: 'assistant', agent: 'main', content_type: 'text', content: 'dispatched both analyses' });
      // The report-back turn's own query row (type='system') replays too.
      onEvent({ event: 'user_message', turn_index: 1, content: 'report back instruction', role: 'user' });
      onEvent({ event: 'message_chunk', turn_index: 1, role: 'assistant', agent: 'main', content_type: 'text', content: 'summary…' });
      return Promise.resolve();
    });

    await act(async () => {
      await result.current.reconnectIfStaleRun();
    });
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await settleMountEffect();

    // Every turn renders exactly once: the reload REPLACED the live-rendered
    // bubbles instead of appending replayed twins after them.
    const serialized = result.current.messages.map((m) => JSON.stringify(m));
    expect(serialized.filter((s) => s.includes('dispatch instruction'))).toHaveLength(1);
    expect(serialized.filter((s) => s.includes('dispatched both analyses'))).toHaveLength(1);
    expect(serialized.filter((s) => s.includes('summary…'))).toHaveLength(1);
  });

  it('keeps the SENT user bubble when a staleness reload replays a turn the user typed', async () => {
    // Second half of the round-trip repro: the user TYPES a message (its
    // content lands in the recently-sent dedup tracker), the send finalizes,
    // then a jump-away/back triggers a corrective reload. The reload clears
    // the now-isHistory optimistic bubble, so the replay must be allowed to
    // re-render the user message — the finalize has to release the dedup
    // tracker, or the replayed user_message is skipped as a "duplicate" of a
    // bubble that no longer exists (vanished user bubble).
    mockStatus.mockResolvedValue(threadStatus({ latest_turn_index: 0 }));
    mockReplay.mockImplementation((_tid: string, onEvent: (e: Record<string, unknown>) => void) => {
      onEvent({ event: 'user_message', turn_index: 0, content: 'earlier question', role: 'user' });
      onEvent({ event: 'message_chunk', turn_index: 0, role: 'assistant', agent: 'main', content_type: 'text', content: 'earlier answer' });
      return Promise.resolve();
    });

    const { result } = renderHookWithProviders(() => useChatMessages('ws-rb', 'th-rb'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(1));
    await settleMountEffect();

    // Live send: streams and success-finalizes (turn 1 persisted server-side).
    mockSend.mockImplementation(async (...args: unknown[]) => {
      const onEvent = args[5] as (e: Record<string, unknown>) => void;
      onEvent({ event: 'message_chunk', role: 'assistant', agent: 'main', content_type: 'text', content: 'resuming both threads' });
      return { disconnected: false };
    });
    await act(async () => {
      await result.current.handleSendMessage('resume both analyses', false);
    });
    expect(JSON.stringify(result.current.messages)).toContain('resume both analyses');

    // Jump away and back: another turn (a report-back) advanced the server
    // watermark past this view's → corrective reload; its replay now covers
    // the typed turn too.
    mockStatus.mockResolvedValue(threadStatus({ latest_turn_index: 2 }));
    mockReplay.mockImplementation((_tid: string, onEvent: (e: Record<string, unknown>) => void) => {
      onEvent({ event: 'user_message', turn_index: 0, content: 'earlier question', role: 'user' });
      onEvent({ event: 'message_chunk', turn_index: 0, role: 'assistant', agent: 'main', content_type: 'text', content: 'earlier answer' });
      onEvent({ event: 'user_message', turn_index: 1, content: 'resume both analyses', role: 'user' });
      onEvent({ event: 'message_chunk', turn_index: 1, role: 'assistant', agent: 'main', content_type: 'text', content: 'resuming both threads' });
      onEvent({ event: 'user_message', turn_index: 2, content: 'report back instruction', role: 'user' });
      onEvent({ event: 'message_chunk', turn_index: 2, role: 'assistant', agent: 'main', content_type: 'text', content: 'late summary' });
      return Promise.resolve();
    });

    await act(async () => {
      await result.current.reconnectIfStaleRun();
    });
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await settleMountEffect();

    // The typed user message survives the reload exactly once — neither
    // vanished (dedup ate the replay) nor twinned (optimistic bubble kept).
    const users = result.current.messages.filter((m) => m.role === 'user' && JSON.stringify(m).includes('resume both analyses'));
    expect(users).toHaveLength(1);
    const serialized = result.current.messages.map((m) => JSON.stringify(m));
    expect(serialized.filter((s) => s.includes('resuming both threads'))).toHaveLength(1);
  });

  it('replaces an admitted STOPPED turn exactly once when a staleness reload replays it', async () => {
    // Stop-path half of the transcript invariant: an ADMITTED stop (run id
    // latched) is persisted server-side as a user-cancelled "Stopped" turn, so
    // stopWorkflow must mark its bubbles isHistory and release the dedup like
    // a success finalize. Before the fix a later corrective reload kept the
    // unmarked live bubbles (mis-ordered after newer turns), appended a
    // replayed twin of the partial answer, and the still-armed recently-sent
    // dedup ate the replayed user message.
    mockStatus.mockResolvedValue(threadStatus({ latest_turn_index: 0 }));
    mockReplay.mockImplementation((_tid: string, onEvent: (e: Record<string, unknown>) => void) => {
      onEvent({ event: 'user_message', turn_index: 0, content: 'earlier question', role: 'user' });
      onEvent({ event: 'message_chunk', turn_index: 0, role: 'assistant', agent: 'main', content_type: 'text', content: 'earlier answer' });
      return Promise.resolve();
    });

    const { result } = renderHookWithProviders(() => useChatMessages('ws-rb', 'th-rb'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(1));
    await settleMountEffect();

    // Live send: latches the run id (admission), streams ONE partial chunk,
    // then hangs until stopWorkflow aborts it — a mid-stream user stop.
    mockSend.mockImplementation((...args: unknown[]) => {
      const onEvent = args[5] as (e: Record<string, unknown>) => void;
      const onRunIdResolved = args[16] as (runId: string, threadId: string | null) => void;
      const signal = args[17] as AbortSignal | null;
      onRunIdResolved('run-stopped-1', 'th-rb');
      onEvent({ event: 'message_chunk', role: 'assistant', agent: 'main', content_type: 'text', content: 'partial answer' });
      return new Promise((resolve) => {
        if (signal?.aborted) return resolve({ disconnected: false, aborted: true });
        signal?.addEventListener('abort', () => resolve({ disconnected: false, aborted: true }));
      });
    });
    let sendPromise: Promise<unknown> = Promise.resolve();
    await act(async () => {
      sendPromise = result.current.handleSendMessage('stopped question', false);
      await new Promise((r) => setTimeout(r, 0));
    });
    expect(JSON.stringify(result.current.messages)).toContain('partial answer');
    await act(async () => {
      await result.current.stopWorkflow();
      await sendPromise;
    });

    // The backend persisted the stopped partial turn (turn 1); a report-back
    // lands as turn 2 and its watermark divergence triggers a corrective
    // reload whose replay covers the stopped turn too.
    mockStatus.mockResolvedValue(threadStatus({ latest_turn_index: 2 }));
    mockReplay.mockImplementation((_tid: string, onEvent: (e: Record<string, unknown>) => void) => {
      onEvent({ event: 'user_message', turn_index: 0, content: 'earlier question', role: 'user' });
      onEvent({ event: 'message_chunk', turn_index: 0, role: 'assistant', agent: 'main', content_type: 'text', content: 'earlier answer' });
      onEvent({ event: 'user_message', turn_index: 1, content: 'stopped question', role: 'user' });
      onEvent({ event: 'message_chunk', turn_index: 1, role: 'assistant', agent: 'main', content_type: 'text', content: 'partial answer' });
      onEvent({ event: 'user_message', turn_index: 2, content: 'report back instruction', role: 'user' });
      onEvent({ event: 'message_chunk', turn_index: 2, role: 'assistant', agent: 'main', content_type: 'text', content: 'late summary' });
      return Promise.resolve();
    });
    await act(async () => {
      await result.current.reconnectIfStaleRun();
    });
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await settleMountEffect();

    // The stopped turn renders exactly once — user bubble not dedup-eaten,
    // answer not twinned — and in turn order, before the later report-back.
    const serialized = result.current.messages.map((m) => JSON.stringify(m));
    expect(result.current.messages.filter((m) => m.role === 'user' && JSON.stringify(m).includes('stopped question'))).toHaveLength(1);
    expect(serialized.filter((s) => s.includes('partial answer'))).toHaveLength(1);
    const stoppedIdx = serialized.findIndex((s) => s.includes('stopped question'));
    const reportBackIdx = serialized.findIndex((s) => s.includes('report back instruction'));
    expect(stoppedIdx).toBeGreaterThan(-1);
    expect(stoppedIdx).toBeLessThan(reportBackIdx);
  });

  it('keeps the bubbles of a PRE-ADMISSION stop across a staleness reload', async () => {
    // Guard for the other half of the stop fork: a stop BEFORE the run id is
    // latched has no turn row server-side, so replay cannot reproduce those
    // bubbles — stopWorkflow must NOT mark them, or a corrective reload
    // vanishes the typed message and its partial answer.
    mockStatus.mockResolvedValue(threadStatus({ latest_turn_index: 0 }));
    mockReplay.mockImplementation((_tid: string, onEvent: (e: Record<string, unknown>) => void) => {
      onEvent({ event: 'user_message', turn_index: 0, content: 'earlier question', role: 'user' });
      onEvent({ event: 'message_chunk', turn_index: 0, role: 'assistant', agent: 'main', content_type: 'text', content: 'earlier answer' });
      return Promise.resolve();
    });

    const { result } = renderHookWithProviders(() => useChatMessages('ws-rb', 'th-rb'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(1));
    await settleMountEffect();

    // Live send that is stopped before admission: streams a chunk but never
    // resolves a run id (e.g. stopped during sandbox bringup).
    mockSend.mockImplementation((...args: unknown[]) => {
      const onEvent = args[5] as (e: Record<string, unknown>) => void;
      const signal = args[17] as AbortSignal | null;
      onEvent({ event: 'message_chunk', role: 'assistant', agent: 'main', content_type: 'text', content: 'partial answer' });
      return new Promise((resolve) => {
        if (signal?.aborted) return resolve({ disconnected: false, aborted: true });
        signal?.addEventListener('abort', () => resolve({ disconnected: false, aborted: true }));
      });
    });
    let sendPromise: Promise<unknown> = Promise.resolve();
    await act(async () => {
      sendPromise = result.current.handleSendMessage('stopped question', false);
      await new Promise((r) => setTimeout(r, 0));
    });
    await act(async () => {
      await result.current.stopWorkflow();
      await sendPromise;
    });

    // TWO report-backs land as turns 1-2; the reload's replay does NOT cover
    // the never-admitted stopped turn. (Two, because the send optimistically
    // bumped this view's watermark to 1 — the pre-admission stop leaves it
    // over-counted, so the server must reach 2 before `!==` sees divergence.)
    mockStatus.mockResolvedValue(threadStatus({ latest_turn_index: 2 }));
    mockReplay.mockImplementation((_tid: string, onEvent: (e: Record<string, unknown>) => void) => {
      onEvent({ event: 'user_message', turn_index: 0, content: 'earlier question', role: 'user' });
      onEvent({ event: 'message_chunk', turn_index: 0, role: 'assistant', agent: 'main', content_type: 'text', content: 'earlier answer' });
      onEvent({ event: 'user_message', turn_index: 1, content: 'report back instruction', role: 'user' });
      onEvent({ event: 'message_chunk', turn_index: 1, role: 'assistant', agent: 'main', content_type: 'text', content: 'late summary' });
      onEvent({ event: 'user_message', turn_index: 2, content: 'second report back', role: 'user' });
      onEvent({ event: 'message_chunk', turn_index: 2, role: 'assistant', agent: 'main', content_type: 'text', content: 'second summary' });
      return Promise.resolve();
    });
    await act(async () => {
      await result.current.reconnectIfStaleRun();
    });
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await settleMountEffect();

    // The unpersisted stop survives the reload — not cleared, not twinned.
    const serialized = result.current.messages.map((m) => JSON.stringify(m));
    expect(result.current.messages.filter((m) => m.role === 'user' && JSON.stringify(m).includes('stopped question'))).toHaveLength(1);
    expect(serialized.filter((s) => s.includes('partial answer'))).toHaveLength(1);
    expect(serialized.filter((s) => s.includes('late summary'))).toHaveLength(1);
  });

  it('discovers a DRAINED report-back via recent_report_back_run_ids when the wake was missed, attaches it, THEN tears down', async () => {
    // BUG B, deterministic for fast tasks: the wake fired with zero /watch
    // subscribers (pub/sub has no replay) and the turn DRAINED before this
    // client reconciled. A drained turn's live pointer is deleted server-side,
    // so recent_report_back_run_ids is the ONLY discovery path — and an idle
    // signal with unrendered recents must attach them BEFORE tearing down.
    mockStatus.mockResolvedValue(threadStatus({ run_id: 'dispatch-run', pending_report_back: true }));
    mockReconnect.mockImplementation(streamedReconnect());
    const watchCalls = captureWatch();

    const { result } = renderHookWithProviders(() => useChatMessages('ws-rb', 'th-rb'));
    await waitFor(() => expect(mockWatch).toHaveBeenCalledTimes(1));

    // Drained while nobody was subscribed: no longer pending, live pointer gone,
    // only the recents slice names the run (its events stay buffered ~15 min).
    mockStatus.mockResolvedValue(threadStatus({
      run_id: null,
      report_back_run_id: null,
      recent_report_back_run_ids: ['rb-drained'],
    }));

    await act(async () => {
      await watchCalls[0].cb();
      await new Promise((r) => setTimeout(r, 0));
    });

    // Attached the drained run from the recents list, fresh cursor...
    expect(mockReconnect).toHaveBeenCalledTimes(1);
    expect(mockReconnect.mock.calls[0][0]).toBe('th-rb');
    expect(mockReconnect.mock.calls[0][1]).toBe('rb-drained');
    expect(mockReconnect.mock.calls[0][2]).toBeNull();
    // ...THEN tore down: idle signal + every recent rendered + empty queue.
    await waitFor(() => expect(result.current.awaitingReportBack).toBe(false));
    expect(watchCalls[0].controller.signal.aborted).toBe(true);

    // The run is recorded rendered: a stray late wake re-naming it is a no-op.
    await act(async () => {
      await watchCalls[0].cb({ run_id: 'rb-drained' });
    });
    expect(mockReconnect).toHaveBeenCalledTimes(1);
  });

  it('does NOT attach when no report-back run is ever named (PTC dispatch failed)', async () => {
    // Report-back pending on load → arm the watch.
    mockStatus.mockResolvedValue(threadStatus({ run_id: 'dispatch-run', pending_report_back: true }));
    const watchCalls = captureWatch();

    renderHookWithProviders(() => useChatMessages('ws-rb', 'th-rb'));
    await waitFor(() => expect(mockWatch).toHaveBeenCalledTimes(1));

    // PTC dispatch failed: no report-back run is ever created, so /status never
    // names one. Attaching to anything here would re-stream "Dispatched."
    mockStatus.mockResolvedValue(threadStatus({ run_id: 'dispatch-run', report_back_run_id: null }));

    await act(async () => {
      await watchCalls[0].cb();
    });

    expect(mockReconnect).not.toHaveBeenCalled();
  });

  it('does NOT attach a stale wake after the user navigated to another thread', async () => {
    // A flash wake firing LATE, after the user jumped into the PTC thread, must
    // not attach the report-back onto the PTC thread — that would race the PTC
    // reconnect for the stream.
    mockStatus.mockResolvedValue(threadStatus({ pending_report_back: true }));
    const watchCalls = captureWatch();

    let tid = 'th-rb';
    const { rerender } = renderHookWithProviders(() => useChatMessages('ws-rb', tid));
    await waitFor(() => expect(mockWatch).toHaveBeenCalledTimes(1));
    expect(watchCalls[0].tid).toBe('th-rb');

    // Navigate to a different thread with nothing pending (no new watch armed).
    mockStatus.mockResolvedValue(threadStatus());
    tid = 'th-other';
    await act(async () => {
      rerender();
      await new Promise((r) => setTimeout(r, 0));
    });

    // The th-rb wake fires now, naming its run — but we're on th-other. Must bail.
    await act(async () => {
      await watchCalls[0].cb({ run_id: 'rb-late' });
    });

    expect(mockReconnect).not.toHaveBeenCalled();
  });

  it('arms the report-back watch on refresh even when the flash thread reconnects to an active run', async () => {
    // Refresh right as the report-back becomes due: /status reports the thread
    // ACTIVE and a report-back pending. The load takes the reconnect branch —
    // but must ALSO arm the watch, so if that one reconnect misses the
    // report-back run the watch still catches it via /status.report_back_run_id.
    mockStatus.mockResolvedValue(threadStatus({
      can_reconnect: true,
      status: 'active',
      run_id: 'active-run',
      pending_report_back: true,
      report_back_run_id: 'rb-run',
    }));
    mockReconnect.mockResolvedValue({ disconnected: false, aborted: false });
    captureWatch();

    renderHookWithProviders(() => useChatMessages('ws-rb', 'th-flash'));

    // The active run is reconnected to...
    await waitFor(() => expect(mockReconnect.mock.calls.some((c) => c[0] === 'th-flash')).toBe(true));
    // ...AND the report-back watch is armed as the reliable catch.
    await waitFor(() =>
      expect(mockWatch).toHaveBeenCalledWith('th-flash', expect.any(Function), expect.any(Function), expect.any(Function)),
    );
  });

  it('supersedes a streaming report-back when the user jumps into the live PTC thread', async () => {
    // A report-back is STILL streaming on the flash thread when the user clicks
    // the dispatch card. The flash reader owns isStreamingRef; navigation must
    // SUPERSEDE it so the PTC thread loads and reconnects to its own live run.
    mockStatus.mockResolvedValue(threadStatus({ pending_report_back: true }));
    const watchCalls = captureWatch();

    // Flash reconnect HOLDS the stream open; PTC reconnect resolves normally.
    mockReconnect.mockImplementation((threadId: string) => {
      if (threadId === 'th-flash') return new Promise(() => {});
      return Promise.resolve({ disconnected: false, aborted: false });
    });

    let tid = 'th-flash';
    const { rerender } = renderHookWithProviders(() => useChatMessages('ws-rb', tid));
    await waitFor(() => expect(mockWatch).toHaveBeenCalledTimes(1));

    // Wake attaches on the flash thread; don't await the never-resolving reader.
    await act(async () => {
      void watchCalls[0].cb({ run_id: 'rb-run' });
      await new Promise((r) => setTimeout(r, 0));
    });
    expect(mockReconnect.mock.calls.some((c) => c[0] === 'th-flash')).toBe(true);

    // PTC thread is live; user jumps into it.
    mockStatus.mockResolvedValue(threadStatus({ can_reconnect: true, status: 'running', run_id: 'ptc-run' }));
    tid = 'th-ptc';
    await act(async () => {
      rerender();
      await new Promise((r) => setTimeout(r, 0));
    });

    // PTC must reconnect to its own run. If supersede fails, this never happens.
    const ptcCall = mockReconnect.mock.calls.find((c) => c[0] === 'th-ptc');
    expect(ptcCall).toBeTruthy();
    expect(ptcCall![1]).toBe('ptc-run');
  });

  it('supersedes the in-flight flash dispatch SEND when the user jumps into the live PTC thread', async () => {
    // Same jump, but the flash DISPATCH TURN itself is still streaming (a send,
    // not a reconnect). The send must claim stream ownership, or supersede can't
    // fire and the isStreamingRef guard blocks the PTC thread from ever loading.
    mockStatus.mockResolvedValue(threadStatus());
    captureWatch();

    // The flash send HOLDS across the navigation; the PTC reconnect resolves.
    mockSend.mockImplementation(() => new Promise(() => {}));
    mockReconnect.mockImplementation((threadId: string) =>
      threadId === 'th-ptc'
        ? Promise.resolve({ disconnected: false, aborted: false })
        : new Promise(() => {}),
    );

    let tid = 'th-flash';
    const { result, rerender } = renderHookWithProviders(() => useChatMessages('ws-flash', tid));
    await settleMountEffect();

    await act(async () => {
      void result.current.handleSendMessage('dispatch a ptc analysis');
      await new Promise((r) => setTimeout(r, 0));
    });

    mockStatus.mockResolvedValue(threadStatus({ can_reconnect: true, status: 'running', run_id: 'ptc-run' }));
    tid = 'th-ptc';
    await act(async () => {
      rerender();
      await new Promise((r) => setTimeout(r, 0));
    });

    const ptcCall = mockReconnect.mock.calls.find((c) => c[0] === 'th-ptc');
    expect(ptcCall).toBeTruthy();
    expect(ptcCall![1]).toBe('ptc-run');
  });

  it('keeps the pending flash report-back alive across a jump into the live PTC thread, then streams it on return', async () => {
    // THE simultaneity contract: PTC streams live on the jumped-into thread AND
    // the keyed flash watch survives the navigation (holding wakes captured
    // while away) so the report-back still streams on return.
    mockStatus.mockResolvedValue(threadStatus({ pending_report_back: true }));
    mockReconnect.mockResolvedValue({ disconnected: false, aborted: false });
    const watchCalls = captureWatch();

    let tid = 'th-flash';
    const { rerender } = renderHookWithProviders(() => useChatMessages('ws-rb', tid));

    await waitFor(() => expect(mockWatch).toHaveBeenCalledTimes(1));
    expect(watchCalls[0].tid).toBe('th-flash');
    expect(mockReconnect).not.toHaveBeenCalled();

    // Jump into the live PTC thread.
    mockStatus.mockResolvedValue(threadStatus({ can_reconnect: true, status: 'running', run_id: 'ptc-run' }));
    tid = 'th-ptc';
    await act(async () => {
      rerender();
      await new Promise((r) => setTimeout(r, 0));
    });

    // PTC streams live...
    const ptcCall = mockReconnect.mock.calls.find((c) => c[0] === 'th-ptc');
    expect(ptcCall).toBeTruthy();
    expect(ptcCall![1]).toBe('ptc-run');
    // ...AND the flash watch SURVIVED the jump: not re-armed, not aborted.
    expect(mockWatch).toHaveBeenCalledTimes(1);
    expect(watchCalls[0].controller.signal.aborted).toBe(false);

    // The wake fires while the user is away: the watch latches the run id but
    // must NOT attach onto th-ptc.
    await act(async () => {
      await watchCalls[0].cb({ run_id: 'rb-run' });
    });
    expect(mockReconnect.mock.calls.some((c) => c[0] === 'th-flash')).toBe(false);

    // Return to the flash thread (idempotent re-arm keeps the same watch).
    mockStatus.mockResolvedValue(threadStatus({ pending_report_back: true, report_back_run_id: 'rb-run' }));
    tid = 'th-flash';
    await act(async () => {
      rerender();
      await new Promise((r) => setTimeout(r, 0));
    });

    // The next reconcile (payload-less callback) streams the REMEMBERED run id.
    await act(async () => {
      await watchCalls[0].cb();
    });

    const rbCall = mockReconnect.mock.calls.find((c) => c[0] === 'th-flash');
    expect(rbCall).toBeTruthy();
    expect(rbCall![1]).toBe('rb-run');
    expect(rbCall![2]).toBeNull();
    // Only ever ONE watch — keyed and persistent, never re-armed per navigation.
    expect(mockWatch).toHaveBeenCalledTimes(1);
  });

  it('idle watchdog: a report-back reconnect whose stream never closes still clears the spinner and tears down', async () => {
    // The stuck "Reconnecting…" spinner: the per-run stream has no terminal
    // sentinel, so a hung reader strands isReconnecting + isLoading +
    // isStreamingRef — and the backstop reconcile bails on isStreamingRef, so
    // the watch can never self-recover. Here the idle-window probe reports the
    // queue DRAINED, so the gate finalizes on the first window.
    mockStatus.mockResolvedValue(threadStatus({ pending_report_back: true, report_back_run_id: null }));
    const watchCalls = captureWatch();

    // The reader resolves ONLY when the client aborts it — the production bug.
    mockReconnect.mockImplementation(
      (_tid: string, _rid: string, _leid: unknown, _onEvent: unknown, signal: AbortSignal) =>
        new Promise((resolve) => {
          if (signal?.aborted) return resolve({ disconnected: false, aborted: true });
          signal?.addEventListener('abort', () => resolve({ disconnected: false, aborted: true }));
        }),
    );

    const { result } = renderHookWithProviders(() => useChatMessages('ws-rb', 'th-rb'));
    await waitFor(() => expect(mockWatch).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(result.current.awaitingReportBack).toBe(true));

    vi.useFakeTimers();
    try {
      // Wake attaches; the reconnect hangs (server never closes).
      await act(async () => {
        void watchCalls[0].cb({ run_id: 'rb-stuck' });
        await Promise.resolve();
      });
      // Spinner + loading up, reader hung. PRE-FIX this is permanent.
      expect(result.current.isReconnecting).toBe(true);
      expect(result.current.isLoading).toBe(true);
      expect(mockReconnect).toHaveBeenCalledTimes(1);

      // The run drained server-side while its stream sat idle.
      mockStatus.mockResolvedValue(threadStatus({ report_back_run_id: null }));

      // Idle watchdog fires → aborts the hung reader → teardown runs.
      await act(async () => {
        await vi.advanceTimersByTimeAsync(IDLE_MS + 50);
      });
    } finally {
      vi.useRealTimers();
    }

    // Spinner + loading cleared, and the watch drained (isStreamingRef was
    // released — otherwise the streamEnd-poke reconcile would have bailed).
    await waitFor(() => expect(result.current.isReconnecting).toBe(false));
    expect(result.current.isLoading).toBe(false);
    await waitFor(() => expect(result.current.awaitingReportBack).toBe(false));
  });

  it('idle gate: a still-pending report-back re-arms (not dismissed), finalizing only once a newer head run supersedes it', async () => {
    // THE bug: two report-backs finishing close together. The OLD watchdog
    // aborted blindly on a quiet window, and the ensuing reconcile re-targeted
    // the watch to run #2 — dismissing a slow-but-live run #1 mid-stream. The
    // gate must probe /status: same pending head → RE-ARM; finalize only once
    // the backend drained #1 and advanced the head.
    // Unnamed on load so the watch arms but does not seed-attach under real
    // timers; the wake below drives the gated reconnect.
    mockStatus.mockResolvedValue(threadStatus({ pending_report_back: true, report_back_run_id: null }));
    const watchCalls = captureWatch();

    const reconnects: CapturedReconnect[] = [];
    mockReconnect.mockImplementation(hangingReconnect(reconnects));

    const { result } = renderHookWithProviders(() => useChatMessages('ws-rb', 'th-rb'));
    await waitFor(() => expect(mockWatch).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(result.current.awaitingReportBack).toBe(true));

    vi.useFakeTimers();
    try {
      // Wake attaches run #1; its stream hangs with no events (slow first token).
      await act(async () => {
        void watchCalls[0].cb({ run_id: 'rb-1' });
        await Promise.resolve();
      });
      expect(reconnects).toHaveLength(1);
      expect(reconnects[0].rid).toBe('rb-1');

      // /status still names rb-1 as the pending head → RE-ARM, not dismissed.
      mockStatus.mockResolvedValue(threadStatus({ pending_report_back: true, report_back_run_id: 'rb-1' }));
      await act(async () => {
        await vi.advanceTimersByTimeAsync(IDLE_MS + 50);
      });
      expect(reconnects).toHaveLength(1);
      expect(reconnects[0].signal.aborted).toBe(false);

      // The backend drains rb-1 and advances the head → FINALIZE → the
      // stream-end reconcile attaches the new head rb-2.
      mockStatus.mockResolvedValue(threadStatus({ pending_report_back: true, report_back_run_id: 'rb-2' }));
      await act(async () => {
        await vi.advanceTimersByTimeAsync(IDLE_MS + 50);
      });
    } finally {
      vi.useRealTimers();
    }

    expect(reconnects[0].signal.aborted).toBe(true); // released only once superseded
    await waitFor(() => expect(reconnects.length).toBeGreaterThanOrEqual(2));
    expect(reconnects.some((r) => r.rid === 'rb-2')).toBe(true);
  });

  it('idle gate: a transient /status blip re-arms (never finalizes on one blip) and force-releases after the cap', async () => {
    // A single probe failure must never finalize (that re-opens the
    // dismiss-run-#1 bug on a flaky network), but a persistently failing probe
    // must still force-release after the bounded budget.
    mockStatus.mockResolvedValue(threadStatus({ pending_report_back: true, report_back_run_id: null }));
    // Load arms via getWorkflowStatus; make ONLY the cheap slice (the gate
    // probe) reject, so every idle-window probe is a blip.
    mockReportBackStatus.mockReset();
    mockReportBackStatus.mockRejectedValue(new Error('report-back status unavailable'));
    const watchCalls = captureWatch();

    const reconnects: CapturedReconnect[] = [];
    mockReconnect.mockImplementation(hangingReconnect(reconnects));

    const { result } = renderHookWithProviders(() => useChatMessages('ws-rb', 'th-rb'));
    await waitFor(() => expect(mockWatch).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(result.current.awaitingReportBack).toBe(true));

    vi.useFakeTimers();
    try {
      await act(async () => {
        void watchCalls[0].cb({ run_id: 'rb-blip' });
        await Promise.resolve();
      });
      expect(reconnects).toHaveLength(1);

      // One idle window: probe rejects → unknown → RE-ARM, reader NOT released.
      await act(async () => {
        await vi.advanceTimersByTimeAsync(IDLE_MS + 50);
      });
      expect(reconnects[0].signal.aborted).toBe(false);

      // Keep blipping: after the bounded budget the reader force-releases.
      await act(async () => {
        await vi.advanceTimersByTimeAsync(REPORT_BACK_IDLE_MAX_REARMS * IDLE_MS + 50);
      });
    } finally {
      vi.useRealTimers();
    }

    expect(reconnects[0].signal.aborted).toBe(true); // released after the cap
    // No re-attach (the stream-end reconcile's /status read also blips) — the
    // release doesn't spin a fresh reader on a dead endpoint.
    expect(reconnects).toHaveLength(1);
    // Spinner + loading released; the watch stays armed to retry.
    await waitFor(() => expect(result.current.isReconnecting).toBe(false));
    expect(result.current.isLoading).toBe(false);
    expect(result.current.awaitingReportBack).toBe(true);
  });

  it('idle gate: a wedged same-run report-back force-releases after the cap re-arms (spinner not stranded)', async () => {
    // /status keeps naming the SAME head pending forever (stuck RUNNING / never
    // started). Indistinguishable from merely-slow on any single probe, so the
    // gate re-arms — but only up to the cap, then force-releases (teardown frees
    // currentRunIdRef and the stream-end reconcile re-attaches the same head).
    mockStatus.mockResolvedValue(threadStatus({ pending_report_back: true, report_back_run_id: null }));
    const watchCalls = captureWatch();

    const reconnects: CapturedReconnect[] = [];
    mockReconnect.mockImplementation(hangingReconnect(reconnects));

    const { result } = renderHookWithProviders(() => useChatMessages('ws-rb', 'th-rb'));
    await waitFor(() => expect(mockWatch).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(result.current.awaitingReportBack).toBe(true));

    vi.useFakeTimers();
    try {
      await act(async () => {
        void watchCalls[0].cb({ run_id: 'rb-wedge' });
        await Promise.resolve();
      });
      expect(reconnects).toHaveLength(1);
      expect(reconnects[0].rid).toBe('rb-wedge');

      mockStatus.mockResolvedValue(threadStatus({ pending_report_back: true, report_back_run_id: 'rb-wedge' }));

      // One window: still ours → RE-ARM, reader not released.
      await act(async () => {
        await vi.advanceTimersByTimeAsync(IDLE_MS + 50);
      });
      expect(reconnects[0].signal.aborted).toBe(false);

      // Burn the rest of the budget → force-release.
      await act(async () => {
        await vi.advanceTimersByTimeAsync(REPORT_BACK_IDLE_MAX_REARMS * IDLE_MS + 50);
      });
    } finally {
      vi.useRealTimers();
    }

    expect(reconnects[0].signal.aborted).toBe(true); // bounded, not stranded
    await waitFor(() => expect(reconnects.length).toBeGreaterThanOrEqual(2));
    expect(reconnects[1].rid).toBe('rb-wedge'); // re-attached the same still-pending head
  });

  it('does NOT arm the watch when pending_report_back is false', async () => {
    mockStatus.mockResolvedValue(threadStatus());
    captureWatch();

    renderHookWithProviders(() => useChatMessages('ws-rb', 'th-rb'));

    await waitFor(() => expect(mockReplay).toHaveBeenCalled());
    await settleMountEffect();

    expect(mockWatch).not.toHaveBeenCalled();
    expect(mockReconnect).not.toHaveBeenCalled();
  });

  it('a report-back named on load attaches immediately (seed), before any wake', async () => {
    // /status already names report_back_run_id on load: the watch seeds it and
    // pokes an immediate reconcile — no backstop wait, no wake required.
    mockStatus.mockResolvedValue(threadStatus({ pending_report_back: true, report_back_run_id: 'rb-seed' }));
    mockReconnect.mockImplementation(streamedReconnect());
    captureWatch();

    renderHookWithProviders(() => useChatMessages('ws-rb', 'th-rb'));

    // Attached straight from the load-time seed (no wake callback was invoked).
    await waitFor(() => expect(mockReconnect).toHaveBeenCalledTimes(1));
    expect(mockReconnect.mock.calls[0][1]).toBe('rb-seed');
    expect(mockReconnect.mock.calls[0][2]).toBeNull();
  });

  it('the load-time seed does NOT preempt a live run: the watch still arms, and the active run attaches FIRST', async () => {
    // can_reconnect:true → the load reconnects to the active run AND arms the
    // watch (the reliable catch if that reconnect misses the report-back run).
    // The seed's immediate poke is gated on !can_reconnect so it can't jump
    // ahead of (and double-attach alongside) the live run.
    mockStatus.mockResolvedValue(threadStatus({
      can_reconnect: true,
      status: 'active',
      run_id: 'active-run',
      pending_report_back: true,
      report_back_run_id: 'rb-held',
    }));
    mockReconnect.mockResolvedValue({ disconnected: false, aborted: false });
    captureWatch();

    renderHookWithProviders(() => useChatMessages('ws-flash', 'th-flash'));

    await waitFor(() => expect(mockReconnect).toHaveBeenCalled());
    await settleMountEffect();
    // Armed as the reliable catch...
    expect(mockWatch).toHaveBeenCalledWith('th-flash', expect.any(Function), expect.any(Function), expect.any(Function));
    // ...and ordering is the invariant: the live run attaches before the held
    // report-back ever could.
    expect(mockReconnect.mock.calls[0][1]).toBe('active-run');
    const activeIdx = mockReconnect.mock.calls.findIndex((c) => c[1] === 'active-run');
    const heldIdx = mockReconnect.mock.calls.findIndex((c) => c[1] === 'rb-held');
    if (heldIdx !== -1) expect(heldIdx).toBeGreaterThan(activeIdx);
  });

  it('stream-end poke: a queued next report-back attaches without a second wake', async () => {
    // When run-1's stream ends, run-2 may already be queued; the stream-end poke
    // must discover and attach it via /status — no second wake needed.
    mockStatus.mockResolvedValue(threadStatus({ pending_report_back: true }));
    mockReconnect.mockResolvedValue({ disconnected: false, aborted: false });
    const watchCalls = captureWatch();

    renderHookWithProviders(() => useChatMessages('ws-rb', 'th-rb'));
    await waitFor(() => expect(mockWatch).toHaveBeenCalledTimes(1));

    // /status already names run-2 as the next head; run-1's wake attaches run-1,
    // and its stream end pokes the reconcile that discovers run-2.
    mockStatus.mockResolvedValue(threadStatus({ pending_report_back: true, report_back_run_id: 'rb-2' }));
    await act(async () => {
      await watchCalls[0].cb({ run_id: 'rb-1' });
    });

    const runIds = mockReconnect.mock.calls.map((c) => c[1]);
    expect(runIds).toContain('rb-1');
    expect(runIds).toContain('rb-2'); // discovered by the stream-end poke
  });

  it('onClosed re-subscribes and reconciles the gap (push watch dropped, then a run is named)', async () => {
    // The backend caps the persistent watch (~30 min). onClosed must
    // re-subscribe AND reconcile once, so a report-back that became due during
    // the gap is recovered without waiting for the backstop.
    mockStatus.mockResolvedValue(threadStatus({ pending_report_back: true }));
    mockReconnect.mockResolvedValue({ disconnected: false, aborted: false });
    const watchCalls = captureWatch();

    renderHookWithProviders(() => useChatMessages('ws-rb', 'th-rb'));
    await waitFor(() => expect(mockWatch).toHaveBeenCalledTimes(1));

    // A run becomes due during the drop, then the watch closes non-deliberately.
    mockStatus.mockResolvedValue(threadStatus({ pending_report_back: true, report_back_run_id: 'rb-gap' }));
    await act(async () => {
      watchCalls[0].onClosed?.();
      await new Promise((r) => setTimeout(r, 0));
    });

    // Re-subscribed (a 2nd watch) AND the gap was reconciled (rb-gap streamed).
    expect(mockWatch).toHaveBeenCalledTimes(2);
    expect(mockReconnect.mock.calls.some((c) => c[1] === 'rb-gap')).toBe(true);
  });

  it('activation: re-entering a cached flash thread with a pending report-back arms + attaches', async () => {
    // The become-active transition of a cached view routes through
    // reconnectIfStaleRun, NOT loadAndMaybeReconnect: a report-back that became
    // due while the view was hidden must arm the watch and attach.
    mockStatus.mockResolvedValue(threadStatus());
    mockReconnect.mockResolvedValue({ disconnected: false, aborted: false });
    captureWatch();

    const { result } = renderHookWithProviders(() => useChatMessages('ws-rb', 'th-rb'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalled());
    await settleMountEffect();
    // Nothing pending on load → no watch, no attach yet.
    expect(mockWatch).not.toHaveBeenCalled();

    // While away, a report-back completed: /status now names it (thread idle).
    mockStatus.mockResolvedValue(threadStatus({ pending_report_back: true, report_back_run_id: 'rb-active' }));
    await act(async () => {
      await result.current.reconnectIfStaleRun();
      await new Promise((r) => setTimeout(r, 0));
    });

    // Armed the keyed watch and streamed the named run.
    expect(mockWatch).toHaveBeenCalledWith('th-rb', expect.any(Function), expect.any(Function), expect.any(Function));
    expect(mockReconnect.mock.calls.some((c) => c[1] === 'rb-active')).toBe(true);
    await waitFor(() => expect(result.current.awaitingReportBack).toBe(true));
  });

  it('arms the watch AT PTC approve (subscribe-at-dispatch), before any stream end', async () => {
    // BUG B's other half: a subscription opened only at the dispatch turn's
    // stream END has zero subscribers when a fast PTC wakes mid-turn (pub/sub,
    // no replay). Approving must open the keyed watch immediately.
    mockStatus.mockResolvedValue(threadStatus());
    captureWatch();

    const { result } = renderHookWithProviders(() => useChatMessages('ws-rb', 'th-rb'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalled());
    await settleMountEffect();
    expect(mockWatch).not.toHaveBeenCalled();

    // Approve: the watch opens NOW, and nothing attaches (no run exists yet —
    // approval is what dispatches).
    await act(async () => {
      result.current.handleApprovePTCAgent({ tool_call_id: 'tc-1' }, undefined, 'prop-1', 'int-1');
      await new Promise((r) => setTimeout(r, 0));
    });
    expect(mockWatch).toHaveBeenCalledTimes(1);
    expect(mockWatch).toHaveBeenCalledWith('th-rb', expect.any(Function), expect.any(Function), expect.any(Function));
    expect(result.current.awaitingReportBack).toBe(true);
    expect(mockReconnect).not.toHaveBeenCalled();
  });

  it('does NOT arm the watch when the approval explicitly disables report_back', async () => {
    mockStatus.mockResolvedValue(threadStatus());
    captureWatch();

    const { result } = renderHookWithProviders(() => useChatMessages('ws-rb', 'th-rb'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalled());
    await settleMountEffect();

    await act(async () => {
      result.current.handleApprovePTCAgent({ tool_call_id: 'tc-1' }, { report_back: false }, 'prop-1', 'int-1');
      await new Promise((r) => setTimeout(r, 0));
    });
    expect(mockWatch).not.toHaveBeenCalled();
    expect(result.current.awaitingReportBack).toBe(false);
  });

  it('BUG A: a report-back chain-attached synchronously at stream end keeps ownership — the chain ends with isLoading false', async () => {
    // The stuck-stop-button wedge. The dispatch reader's finally →
    // cleanupAfterStreamEnd → onStreamEnd poke → the reconcile SYNCHRONOUSLY
    // attaches the latched run, registering a fresh AbortController in
    // mainStreamAbortRef before the outer finally resumes. The old code nulled
    // the ref from its STALE snapshot, orphaning the new stream: un-stoppable,
    // its own finally skipped cleanup, isLoading + isStreamingRef wedged forever.
    const closers = new Map<string, () => void>();
    mockReconnect.mockImplementation(heldReconnect(closers));

    // Load: dispatch turn still streaming AND a report-back pending.
    mockStatus.mockResolvedValue(threadStatus({ can_reconnect: true, status: 'running', run_id: 'dispatch-run', pending_report_back: true }));
    const watchCalls = captureWatch();

    const { result } = renderHookWithProviders(() => useChatMessages('ws-rb', 'th-rb'));
    await waitFor(() => expect(closers.has('dispatch-run')).toBe(true));
    await waitFor(() => expect(result.current.isLoading).toBe(true));

    // A fast PTC finishes MID-TURN: the wake latches rb-1 without attaching.
    await act(async () => {
      await watchCalls[0].cb({ run_id: 'rb-1' });
    });
    expect(closers.has('rb-1')).toBe(false);

    // Dispatch turn ends → cleanup chain-attaches rb-1 synchronously.
    await act(async () => {
      closers.get('dispatch-run')!();
      await new Promise((r) => setTimeout(r, 0));
    });
    await waitFor(() => expect(closers.has('rb-1')).toBe(true));
    expect(result.current.isLoading).toBe(true); // the chained stream owns loading

    // rb-1 drains the queue; its stream ends. Pre-fix its finally saw the
    // nulled abort ref, skipped cleanup, and isLoading stayed true forever.
    mockStatus.mockResolvedValue(threadStatus({ report_back_run_id: null }));
    await act(async () => {
      closers.get('rb-1')!();
      await new Promise((r) => setTimeout(r, 0));
    });

    // Ownership was NOT orphaned: cleanup ran and the watch drained.
    await waitFor(() => expect(result.current.isLoading).toBe(false));
    await waitFor(() => expect(result.current.awaitingReportBack).toBe(false));
    expect(watchCalls[0].controller.signal.aborted).toBe(true);
  });

  it('FIFO: two wakes latched while the dispatch turn streams attach IN ORDER at stream end (no overwrite)', async () => {
    // The old single-slot latch let wake #2 overwrite un-attached wake #1. Both
    // must latch (ordered, deduped) and attach head-first — one per reconcile,
    // each stream-end poking the next — on ONE persistent watch.
    const closers = new Map<string, () => void>();
    mockReconnect.mockImplementation(heldReconnect(closers));

    mockStatus.mockResolvedValue(threadStatus({ can_reconnect: true, status: 'running', run_id: 'dispatch-run', pending_report_back: true }));
    const watchCalls = captureWatch();

    const { result } = renderHookWithProviders(() => useChatMessages('ws-rb', 'th-rb'));
    await waitFor(() => expect(closers.has('dispatch-run')).toBe(true));

    // Both wakes land mid-turn, in order; a duplicate redelivery of rb-1
    // collapses to one queue entry.
    await act(async () => {
      await watchCalls[0].cb({ run_id: 'rb-1' });
      await watchCalls[0].cb({ run_id: 'rb-2' });
      await watchCalls[0].cb({ run_id: 'rb-1' });
    });
    expect(closers.has('rb-1')).toBe(false);
    expect(closers.has('rb-2')).toBe(false);

    // Dispatch ends → rb-1 (the FIFO head) attaches; rb-2 stays queued.
    await act(async () => {
      closers.get('dispatch-run')!();
      await new Promise((r) => setTimeout(r, 0));
    });
    await waitFor(() => expect(closers.has('rb-1')).toBe(true));
    expect(closers.has('rb-2')).toBe(false);

    // rb-1 ends → its stream-end poke attaches rb-2 straight off the queue.
    await act(async () => {
      closers.get('rb-1')!();
      await new Promise((r) => setTimeout(r, 0));
    });
    await waitFor(() => expect(closers.has('rb-2')).toBe(true));

    // rb-2 ends with everything drained → the watch tears down.
    mockStatus.mockResolvedValue(threadStatus({ report_back_run_id: null }));
    await act(async () => {
      closers.get('rb-2')!();
      await new Promise((r) => setTimeout(r, 0));
    });
    await waitFor(() => expect(result.current.awaitingReportBack).toBe(false));

    // In-order, no overwrite, exactly once each, on a single persistent watch.
    const rids = mockReconnect.mock.calls.map((c) => c[1]);
    expect(rids).toEqual(['dispatch-run', 'rb-1', 'rb-2']);
    expect(mockWatch).toHaveBeenCalledTimes(1);
  });

  it('idle with every recent run already rendered by the history load tears down without attaching', async () => {
    // markRunsRendered seeding: a reload's replay rendered every persisted turn
    // and recorded that load's recents slice. A later idle reconcile whose
    // recents name ONLY those runs must tear down WITHOUT duplicate attaches.
    mockStatus.mockResolvedValue(threadStatus({
      pending_report_back: true, // one dispatch still due → arm on load
      report_back_run_id: null,
      recent_report_back_run_ids: ['rb-old'], // drained + replayed by THIS load
    }));
    const watchCalls = captureWatch();

    const { result } = renderHookWithProviders(() => useChatMessages('ws-rb', 'th-rb'));
    await waitFor(() => expect(mockWatch).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(result.current.awaitingReportBack).toBe(true));
    // The rendered recent was never attached by the load-time poke.
    expect(mockReconnect).not.toHaveBeenCalled();

    // The remaining dispatch was cancelled server-side: flash_watch drains with
    // no new run — recents still name only the already-rendered turn.
    mockStatus.mockResolvedValue(threadStatus({
      report_back_run_id: null,
      recent_report_back_run_ids: ['rb-old'],
    }));
    await act(async () => {
      await watchCalls[0].cb();
      await new Promise((r) => setTimeout(r, 0));
    });

    // Idle + all recents rendered + empty queue → teardown, zero attaches.
    expect(mockReconnect).not.toHaveBeenCalled();
    await waitFor(() => expect(result.current.awaitingReportBack).toBe(false));
    expect(watchCalls[0].controller.signal.aborted).toBe(true);
  });

  it('a zero-content attach releases the run-id dedup so the named run can re-attach (bounded retry)', async () => {
    // Dedup un-poisoning: a failed first attach (404/410 silently discarded,
    // thrown fetch, idle-close before any event) ends with zero content. The old
    // code released the latch only on the idle-close flavor; every other
    // zero-content end left the stale id latched, so attach() deduped forever
    // and the still-pending summary only surfaced on reload.
    mockStatus.mockResolvedValue(threadStatus({ pending_report_back: true, report_back_run_id: null }));
    const watchCalls = captureWatch();

    renderHookWithProviders(() => useChatMessages('ws-rb', 'th-rb'));
    await waitFor(() => expect(mockWatch).toHaveBeenCalledTimes(1));

    // First attach: the per-run stream is dead — resolves with NO events.
    mockReconnect.mockResolvedValue({ disconnected: false, aborted: false });
    await act(async () => {
      await watchCalls[0].cb({ run_id: 'rb-x' });
      await new Promise((r) => setTimeout(r, 0));
    });
    expect(mockReconnect).toHaveBeenCalledTimes(1);
    expect(mockReconnect.mock.calls[0][1]).toBe('rb-x');

    // The run is re-announced. The latch must have been released — the retry
    // attaches and streams this time.
    mockReconnect.mockImplementation(streamedReconnect());
    await act(async () => {
      await watchCalls[0].cb({ run_id: 'rb-x' });
      await new Promise((r) => setTimeout(r, 0));
    });
    expect(mockReconnect).toHaveBeenCalledTimes(2);
    expect(mockReconnect.mock.calls[1][1]).toBe('rb-x');
  });

  it('resubscribe catch-up: in-loop /watch recovery reconciles as a NON-poll source (attaches; never burns the give-up cap)', async () => {
    // watchThread's own retry loop can re-subscribe after a transient error;
    // wakes fired during that gap are lost. The recovery callback must run a
    // catch-up reconcile — and NEVER count toward the backstop give-up cap, or
    // a flaky connection would burn the watch budget with no poll ever firing.
    mockStatus.mockResolvedValue(threadStatus({ pending_report_back: true, report_back_run_id: null }));
    mockReconnect.mockImplementation(streamedReconnect());
    const watchCalls = captureWatch();

    const { result } = renderHookWithProviders(() => useChatMessages('ws-rb', 'th-rb'));
    await waitFor(() => expect(mockWatch).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(result.current.awaitingReportBack).toBe(true));

    // Spam recoveries far past the give-up cap while the backend can only
    // return the non-confirming unknown sentinel: the watch must stay armed.
    mockStatus.mockResolvedValue(threadStatus({ pending_report_back: null, report_back_run_id: null }));
    for (let i = 0; i < REPORT_BACK_MAX_POLLS + 3; i++) {
      await act(async () => {
        watchCalls[0].onResubscribed?.();
        await new Promise((r) => setTimeout(r, 0));
      });
    }
    expect(result.current.awaitingReportBack).toBe(true);
    expect(watchCalls[0].controller.signal.aborted).toBe(false);
    expect(mockReconnect).not.toHaveBeenCalled();

    // A wake WAS lost during one of those gaps: /status names the run, and the
    // next recovery's catch-up discovers and attaches it — no wake needed.
    mockStatus.mockResolvedValue(threadStatus({ pending_report_back: true, report_back_run_id: 'rb-gap' }));
    await act(async () => {
      watchCalls[0].onResubscribed?.();
      await new Promise((r) => setTimeout(r, 0));
    });
    expect(mockReconnect.mock.calls.some((c) => c[1] === 'rb-gap')).toBe(true);
  });

  it('backstop give-up cap: confirmed-pending ticks reset the budget; only CONSECUTIVE non-confirming ticks release', async () => {
    // Pre-fix, EVERY backstop tick counted toward the cap, so any dispatch
    // outlasting ~10 ticks silently lost its live stream. Now the cap is
    // measured from the LAST confirmation: `pending` ticks reset it, and only a
    // full consecutive run of `unknown` (the backend's own Redis read failing)
    // ticks releases the watch. Fake timers drive the 60s backstop; nothing is
    // ever named, so no attach can occur.
    const PENDING = threadStatus({ pending_report_back: true, report_back_run_id: null });
    const UNKNOWN = { ...PENDING, pending_report_back: null };

    mockStatus.mockResolvedValue(PENDING);
    const watchCalls = captureWatch();

    vi.useFakeTimers();
    try {
      const { result } = renderHookWithProviders(() => useChatMessages('ws-rb', 'th-rb'));
      await settleMountEffectFake();
      expect(mockWatch).toHaveBeenCalledTimes(1);
      expect(result.current.awaitingReportBack).toBe(true);

      // A healthy long-running dispatch: well past the cap, every tick
      // confirming pending — still armed.
      for (let i = 0; i < REPORT_BACK_MAX_POLLS + 3; i++) await backstopTick();
      expect(result.current.awaitingReportBack).toBe(true);

      // (cap - 1) consecutive unknown ticks: one short of giving up.
      mockStatus.mockResolvedValue(UNKNOWN);
      for (let i = 0; i < REPORT_BACK_MAX_POLLS - 1; i++) await backstopTick();
      expect(result.current.awaitingReportBack).toBe(true);

      // One confirmed-pending tick RESETS the budget...
      mockStatus.mockResolvedValue(PENDING);
      await backstopTick();
      // ...so another (cap - 1) unknown ticks still leave the watch armed, even
      // though the total unknown count far exceeds the cap.
      mockStatus.mockResolvedValue(UNKNOWN);
      for (let i = 0; i < REPORT_BACK_MAX_POLLS - 1; i++) await backstopTick();
      expect(result.current.awaitingReportBack).toBe(true);
      expect(watchCalls[0].controller.signal.aborted).toBe(false);

      // The cap-th consecutive unknown tick SINCE the reset finally releases.
      await backstopTick();
      expect(result.current.awaitingReportBack).toBe(false);
      expect(watchCalls[0].controller.signal.aborted).toBe(true);
      // Never re-armed; nothing was ever named, so nothing ever attached.
      expect(mockWatch).toHaveBeenCalledTimes(1);
      expect(mockReconnect).not.toHaveBeenCalled();
    } finally {
      vi.useRealTimers();
    }
  });
});
