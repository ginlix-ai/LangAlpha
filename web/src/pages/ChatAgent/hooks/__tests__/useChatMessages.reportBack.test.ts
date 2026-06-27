/**
 * Regression: report-back watch re-arm on load (PTC → flash report-back).
 *
 * After a PTC dispatch, the backend fires a follow-up flash "report-back"
 * workflow once the PTC analysis completes. If the user navigated away and
 * came back, `/status` returns `pending_report_back=true` (and, because the
 * PTC turn itself is done, `can_reconnect=false`). On load the hook must:
 *  (a) ARM the lightweight watch (`startReportBackWatch` → `watchThread`), and
 *  (b) attach to the report-back run the BACKEND NAMES — either the run_id the
 *      wake carries (in-session fast path) or `/status.report_back_run_id` (after
 *      a reload). It must NOT attach to the stale dispatch/resume run held in
 *      currentRunIdRef: reconnecting there hits a drained stream key and yields
 *      zero events, so the report-back turn would only show on a later refetch
 *      (the live-render bug this guards against).
 *
 * Inverse: with `pending_report_back=false` the watch is NOT armed; and when no
 * report-back run is ever named (the PTC dispatch failed) it does NOT attach.
 *
 * We use the REAL hook internals (no streamEventHandlers stubs, mirroring the
 * sibling stop suite) but mock the api module — the report-back path only
 * touches getWorkflowStatus / watchThread / replayThreadHistory /
 * reconnectToWorkflowStream, none of which need real network.
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
  watchThread: vi.fn(),
}));

import { getWorkflowStatus, replayThreadHistory, reconnectToWorkflowStream, watchThread, sendChatMessageStream } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';

const mockStatus = getWorkflowStatus as Mock;
const mockReplay = replayThreadHistory as Mock;
const mockReconnect = reconnectToWorkflowStream as Mock;
const mockWatch = watchThread as Mock;
const mockSend = sendChatMessageStream as Mock;

/**
 * Flush the mount effect's status-fetch → history-load → branch decision.
 * Every awaited call in that chain (getWorkflowStatus, replayThreadHistory) is
 * a resolved mock, and the report-back arm decision is synchronous right after
 * — so flushing micro + macro tasks settles it deterministically.
 */
async function settleMountEffect() {
  for (let i = 0; i < 2; i++) {
    await act(async () => {
      await new Promise((r) => setTimeout(r, 0));
    });
  }
}

describe('useChatMessages — report-back watch (PTC → flash report-back)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('arms the report-back watch on load and the wake payload drives a direct reconnect', async () => {
    // PTC turn is done (can_reconnect:false) but a report-back is still pending.
    mockStatus.mockResolvedValue({
      can_reconnect: false,
      status: 'completed',
      pending_report_back: true,
      active_tasks: [],
    });

    const watchCalls: Array<{ tid: string; cb: (p?: { run_id?: string | null }) => void | Promise<void> }> = [];
    mockWatch.mockImplementation((tid: string, cb: (p?: { run_id?: string | null }) => void | Promise<void>) => {
      watchCalls.push({ tid, cb });
      return { abort: new AbortController() };
    });

    renderHookWithProviders(() => useChatMessages('ws-rb', 'th-rb'));

    // (a) Armed: a watch was opened for this thread with a callback, and no
    // reconnect has fired yet (the PTC turn was already complete).
    await waitFor(() => expect(mockWatch).toHaveBeenCalledTimes(1));
    expect(mockWatch).toHaveBeenCalledWith('th-rb', expect.any(Function));
    expect(mockReconnect).not.toHaveBeenCalled();

    // (b) The wake names the report-back run directly → the callback attaches to
    // that exact run (not a stale one) and reads its stream from the start
    // (lastEventId=null), with no /status round-trip.
    await act(async () => {
      await watchCalls[0].cb({ run_id: 'rb-run-1' });
    });

    expect(mockReconnect).toHaveBeenCalledTimes(1);
    // Signature: reconnectToWorkflowStream(threadId, runId, lastEventId, onEvent, signal)
    expect(mockReconnect.mock.calls[0][0]).toBe('th-rb');
    expect(mockReconnect.mock.calls[0][1]).toBe('rb-run-1'); // run named by the wake, not stale
    expect(mockReconnect.mock.calls[0][2]).toBeNull(); // fresh per-stream-key cursor
  });

  it('attaches via /status.report_back_run_id when the wake was missed (reload / payload-less wake)', async () => {
    // On load the report-back is still pending → arm the watch.
    mockStatus.mockResolvedValue({
      can_reconnect: false,
      status: 'completed',
      run_id: 'dispatch-run',
      pending_report_back: true,
      active_tasks: [],
    });

    const watchCalls: Array<{ tid: string; cb: (p?: { run_id?: string | null }) => void | Promise<void> }> = [];
    mockWatch.mockImplementation((tid: string, cb: (p?: { run_id?: string | null }) => void | Promise<void>) => {
      watchCalls.push({ tid, cb });
      return { abort: new AbortController() };
    });

    renderHookWithProviders(() => useChatMessages('ws-rb', 'th-rb'));

    await waitFor(() => expect(mockWatch).toHaveBeenCalledTimes(1));

    // The callback fires WITHOUT a run_id (payload-less wake, or a reload poll) →
    // it falls back to /status, which now NAMES the report-back run via
    // report_back_run_id. Its events are still buffered on the per-run stream, so
    // the watch must ATTACH and replay them (streaming the summary in) rather than
    // reload history (which duplicates the dispatch card) or sit idle.
    mockStatus.mockResolvedValue({
      can_reconnect: false,
      status: 'completed',
      run_id: 'rb-run-done',
      pending_report_back: false,
      report_back_run_id: 'rb-run-done',
      active_tasks: [],
    });

    await act(async () => {
      await watchCalls[0].cb();
    });

    expect(mockReconnect).toHaveBeenCalledTimes(1);
    expect(mockReconnect.mock.calls[0][0]).toBe('th-rb');
    expect(mockReconnect.mock.calls[0][1]).toBe('rb-run-done'); // the run /status named, replayed
    expect(mockReconnect.mock.calls[0][2]).toBeNull(); // fresh per-stream-key cursor
  });

  it('does NOT attach when no report-back run is ever named (PTC dispatch failed)', async () => {
    // Report-back pending on load → arm the watch.
    mockStatus.mockResolvedValue({
      can_reconnect: false,
      status: 'completed',
      run_id: 'dispatch-run',
      pending_report_back: true,
      active_tasks: [],
    });

    const watchCalls: Array<{ tid: string; cb: (p?: { run_id?: string | null }) => void | Promise<void> }> = [];
    mockWatch.mockImplementation((tid: string, cb: (p?: { run_id?: string | null }) => void | Promise<void>) => {
      watchCalls.push({ tid, cb });
      return { abort: new AbortController() };
    });

    renderHookWithProviders(() => useChatMessages('ws-rb', 'th-rb'));
    await waitFor(() => expect(mockWatch).toHaveBeenCalledTimes(1));

    // PTC dispatch failed: no report-back run is ever created, so /status never
    // populates report_back_run_id (and eventually stops reporting it pending).
    // Attaching to anything here would re-stream "Dispatched." — so the watch
    // must NOT attach.
    mockStatus.mockResolvedValue({
      can_reconnect: false,
      status: 'completed',
      run_id: 'dispatch-run',
      pending_report_back: false,
      report_back_run_id: null, // never named
      active_tasks: [],
    });

    await act(async () => {
      await watchCalls[0].cb();
    });

    expect(mockReconnect).not.toHaveBeenCalled();
  });

  it('does NOT attach a stale wake after the user navigated to another thread', async () => {
    // Regression: the wake fast-path attaches immediately. If the user dispatched
    // PTC on the flash thread and then jumped into the PTC thread, a flash wake
    // firing LATE must not attach the report-back onto the PTC thread — that would
    // race the PTC reconnect for the stream and the PTC turn would stop streaming.
    mockStatus.mockResolvedValue({
      can_reconnect: false,
      status: 'completed',
      pending_report_back: true,
      active_tasks: [],
    });

    const watchCalls: Array<{ tid: string; cb: (p?: { run_id?: string | null }) => void | Promise<void> }> = [];
    mockWatch.mockImplementation((tid: string, cb: (p?: { run_id?: string | null }) => void | Promise<void>) => {
      watchCalls.push({ tid, cb });
      return { abort: new AbortController() };
    });

    let tid = 'th-rb';
    const { rerender } = renderHookWithProviders(() => useChatMessages('ws-rb', tid));
    await waitFor(() => expect(mockWatch).toHaveBeenCalledTimes(1));
    expect(watchCalls[0].tid).toBe('th-rb');

    // Navigate to a different thread with nothing pending (no new watch armed).
    mockStatus.mockResolvedValue({
      can_reconnect: false,
      status: 'completed',
      pending_report_back: false,
      active_tasks: [],
    });
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

  it('supersedes a streaming report-back when the user jumps into the live PTC thread', async () => {
    // The live race that keeps breaking PTC: on the flash thread a report-back
    // attaches (fast path) and is STILL streaming when the user clicks the
    // dispatch card to jump into the running PTC thread. The flash report-back
    // owns isStreamingRef/streamingThreadIdRef; navigation must SUPERSEDE it so
    // the PTC thread loads and reconnects to its own live run.
    mockStatus.mockResolvedValue({
      can_reconnect: false,
      status: 'completed',
      pending_report_back: true,
      active_tasks: [],
    });

    const watchCalls: Array<{ tid: string; cb: (p?: { run_id?: string | null }) => void | Promise<void> }> = [];
    mockWatch.mockImplementation((tid: string, cb: (p?: { run_id?: string | null }) => void | Promise<void>) => {
      watchCalls.push({ tid, cb });
      return { abort: new AbortController() };
    });

    // Flash report-back reconnect HOLDS the stream open (never resolves) so it
    // keeps ownership when navigation happens. PTC reconnect resolves normally.
    mockReconnect.mockImplementation((threadId: string) => {
      if (threadId === 'th-flash') return new Promise(() => {}); // hold forever
      return Promise.resolve({ disconnected: false, aborted: false });
    });

    let tid = 'th-flash';
    const { rerender } = renderHookWithProviders(() => useChatMessages('ws-rb', tid));
    await waitFor(() => expect(mockWatch).toHaveBeenCalledTimes(1));

    // Wake names the run → report-back attaches on the flash thread (fast path).
    // Don't await the callback to completion — its reconnect promise never
    // resolves; just let the synchronous attach + ownership claim run.
    await act(async () => {
      void watchCalls[0].cb({ run_id: 'rb-run' });
      await new Promise((r) => setTimeout(r, 0));
    });
    expect(mockReconnect.mock.calls.some((c) => c[0] === 'th-flash')).toBe(true);

    // PTC thread is live (can_reconnect:true). User jumps into it.
    mockStatus.mockResolvedValue({
      can_reconnect: true,
      status: 'running',
      run_id: 'ptc-run',
      active_tasks: [],
      pending_report_back: false,
    });
    tid = 'th-ptc';
    await act(async () => {
      rerender();
      await new Promise((r) => setTimeout(r, 0));
    });

    // PTC must reconnect to its own run. If supersede fails, this is never called.
    const ptcCall = mockReconnect.mock.calls.find((c) => c[0] === 'th-ptc');
    expect(ptcCall).toBeTruthy();
    expect(ptcCall![1]).toBe('ptc-run');
  });

  it('supersedes the in-flight flash dispatch SEND when the user jumps into the live PTC thread', async () => {
    // The other half of the live flow: the user clicks "jump to thread" while the
    // flash DISPATCH TURN ITSELF is still streaming (a send, not a reconnect).
    // A send sets isStreamingRef=true; if it does NOT also claim stream ownership,
    // streamingThreadIdRef stays null, supersede can't fire, and the load guard
    // (isStreamingRef) blocks the PTC thread from ever loading → PTC never streams.
    // This is the regression that kept breaking PTC live while unit tests passed.
    mockStatus.mockResolvedValue({
      can_reconnect: false,
      status: 'completed',
      pending_report_back: false,
      active_tasks: [],
    });
    mockWatch.mockImplementation(() => ({ abort: new AbortController() }));

    // The flash send HOLDS (never resolves) so it stays in-progress across the
    // navigation. The PTC reconnect resolves normally.
    mockSend.mockImplementation(() => new Promise(() => {}));
    mockReconnect.mockImplementation((threadId: string) =>
      threadId === 'th-ptc'
        ? Promise.resolve({ disconnected: false, aborted: false })
        : new Promise(() => {}),
    );

    let tid = 'th-flash';
    const { result, rerender } = renderHookWithProviders(() => useChatMessages('ws-flash', tid));
    await settleMountEffect();

    // Start a send on the flash thread; it holds → isStreamingRef stays true.
    await act(async () => {
      void result.current.handleSendMessage('dispatch a ptc analysis');
      await new Promise((r) => setTimeout(r, 0));
    });

    // PTC thread is live; user jumps into it.
    mockStatus.mockResolvedValue({
      can_reconnect: true,
      status: 'running',
      run_id: 'ptc-run',
      active_tasks: [],
      pending_report_back: false,
    });
    tid = 'th-ptc';
    await act(async () => {
      rerender();
      await new Promise((r) => setTimeout(r, 0));
    });

    const ptcCall = mockReconnect.mock.calls.find((c) => c[0] === 'th-ptc');
    expect(ptcCall).toBeTruthy();
    expect(ptcCall![1]).toBe('ptc-run');
  });

  it('arms the report-back watch on refresh even when the flash thread reconnects to an active run', async () => {
    // Refresh on the flash thread right as the report-back becomes due: /status
    // reports the thread ACTIVE (can_reconnect:true) AND a report-back pending.
    // The load takes the reconnect branch — but it must ALSO arm the watch, so if
    // that one reconnect doesn't land on the report-back run (stale/drained run,
    // or a short summary that finishes before attach), the watch still catches it
    // via /status.report_back_run_id. Without this the report-back only surfaces
    // on a LATER history-replay refresh (static, not live) — the reported bug.
    mockStatus.mockResolvedValue({
      can_reconnect: true,
      status: 'active',
      run_id: 'active-run',
      pending_report_back: true,
      report_back_run_id: 'rb-run',
      active_tasks: [],
    });
    mockReconnect.mockResolvedValue({ disconnected: false, aborted: false });

    const watchCalls: Array<{ tid: string }> = [];
    mockWatch.mockImplementation((tid: string) => {
      watchCalls.push({ tid });
      return { abort: new AbortController() };
    });

    renderHookWithProviders(() => useChatMessages('ws-rb', 'th-flash'));

    // The active run is reconnected to...
    await waitFor(() => expect(mockReconnect.mock.calls.some((c) => c[0] === 'th-flash')).toBe(true));
    // ...AND the report-back watch is armed as the reliable catch.
    await waitFor(() => expect(mockWatch).toHaveBeenCalledWith('th-flash', expect.any(Function)));
  });

  it('keeps the pending flash report-back alive across a jump into the live PTC thread, then streams it on return', async () => {
    // THE simultaneity contract every prior fix oscillated on. A flash
    // report-back is PENDING (its PTC run still live, the report-back not yet
    // started) when the user jumps flash → PTC. Both must hold at once:
    //   • PTC streams live on the jumped-into thread, and
    //   • the keyed flash watch SURVIVES the navigation (not torn down), so the
    //     report-back still streams when the user returns.
    // The old code tore the watch down on navigation (supersede + thread-change
    // cleanup), so the report-back was lost — this test fails against that code.
    mockStatus.mockResolvedValue({
      can_reconnect: false,
      status: 'completed',
      pending_report_back: true,
      active_tasks: [],
    });
    mockReconnect.mockResolvedValue({ disconnected: false, aborted: false });

    const watchCalls: Array<{ tid: string; cb: (p?: { run_id?: string | null }) => void | Promise<void>; controller: AbortController }> = [];
    mockWatch.mockImplementation((tid: string, cb: (p?: { run_id?: string | null }) => void | Promise<void>) => {
      const controller = new AbortController();
      watchCalls.push({ tid, cb, controller });
      return { abort: controller };
    });

    let tid = 'th-flash';
    const { rerender } = renderHookWithProviders(() => useChatMessages('ws-rb', tid));

    // Armed once for the flash thread; nothing streaming yet (PTC turn is done).
    await waitFor(() => expect(mockWatch).toHaveBeenCalledTimes(1));
    expect(watchCalls[0].tid).toBe('th-flash');
    expect(mockReconnect).not.toHaveBeenCalled();

    // User jumps into the live PTC thread.
    mockStatus.mockResolvedValue({
      can_reconnect: true,
      status: 'running',
      run_id: 'ptc-run',
      pending_report_back: false,
      active_tasks: [],
    });
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

    // The report-back STARTS while the user is on the PTC thread: the flash wake
    // fires naming its run. The keyed watch captures the run id but must NOT
    // attach onto th-ptc (its render gate keeps it off the visible PTC thread).
    await act(async () => {
      await watchCalls[0].cb({ run_id: 'rb-run' });
    });
    expect(mockReconnect.mock.calls.some((c) => c[0] === 'th-flash')).toBe(false);

    // User returns to the flash thread (still pending → idempotent re-arm no-op,
    // so the same persistent watch keeps the run id it captured while away).
    mockStatus.mockResolvedValue({
      can_reconnect: false,
      status: 'completed',
      pending_report_back: true,
      report_back_run_id: 'rb-run',
      active_tasks: [],
    });
    tid = 'th-flash';
    await act(async () => {
      rerender();
      await new Promise((r) => setTimeout(r, 0));
    });

    // The persistent watch is on its flash thread again; its next reconcile (a
    // poll tick, simulated here with a payload-less callback) streams the run id
    // it REMEMBERED — no fresh run_id needed — live into a new bubble.
    await act(async () => {
      await watchCalls[0].cb();
    });

    const rbCall = mockReconnect.mock.calls.find((c) => c[0] === 'th-flash');
    expect(rbCall).toBeTruthy();
    expect(rbCall![1]).toBe('rb-run');
    expect(rbCall![2]).toBeNull(); // fresh per-stream-key cursor
    // Only ever ONE watch — keyed and persistent, never re-armed per navigation.
    expect(mockWatch).toHaveBeenCalledTimes(1);
  });

  it('does NOT arm the watch when pending_report_back is false', async () => {
    mockStatus.mockResolvedValue({
      can_reconnect: false,
      status: 'completed',
      pending_report_back: false,
      active_tasks: [],
    });
    mockWatch.mockImplementation(() => ({ abort: new AbortController() }));

    renderHookWithProviders(() => useChatMessages('ws-rb', 'th-rb'));

    // Let the load + status check fully settle.
    await waitFor(() => expect(mockReplay).toHaveBeenCalled());
    await settleMountEffect();

    expect(mockWatch).not.toHaveBeenCalled();
    expect(mockReconnect).not.toHaveBeenCalled();
  });
});
