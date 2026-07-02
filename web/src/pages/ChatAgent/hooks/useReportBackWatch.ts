/**
 * Report-back watch, extracted from useChatMessages. After a PTC dispatch the
 * backend fires a follow-up flash "report-back" workflow per completed PTC
 * analysis; this hook drives the flash thread to each report-back turn via a
 * persistent SSE watch (Redis pub/sub wake), catch-up pulls on load /
 * re-activation / re-subscribe / stream-end, and a slow safety backstop.
 *
 * The watch is KEYED to the flash thread and survives navigation into the
 * dispatched PTC thread, attaching only while its flash thread is on screen and
 * idle — that is how a flash report-back and a live PTC stream coexist. The
 * host (useChatMessages) injects the shared stream primitives and calls `arm`
 * at load AND on PTC approve (subscribe-at-dispatch: a wake fired mid-turn is
 * latched, pub/sub has no replay), `markRunsRendered` after each history load,
 * `onStreamEnd` at dispatch-turn end, and `reconnectIfStaleRun` on the
 * cached-view become-active transition.
 */
import { useCallback, useEffect, useMemo, useRef, useState, type MutableRefObject } from 'react';
import {
  getReportBackStatus,
  getWorkflowStatus,
  watchThread,
  type ReportBackStatusResponse,
  type WorkflowStatusResponse,
} from '../utils/api';
// From the dependency-free signal module (not `../utils/api`) so decoding still
// works where the hook tests mock `../utils/api`.
import { decodeReportBackSignal, shouldArmReportBack } from '../utils/reportBackSignal';

/**
 * Slow SAFETY backstop interval — the push wake and the event-driven catch-up
 * pulls do the real work; this only recovers gaps they missed.
 */
const REPORT_BACK_BACKSTOP_MS = 60_000;

/**
 * Give up after this many CONSECUTIVE non-confirming backstop ticks (`unknown`,
 * `none`, or a thrown probe). A `pending` tick is the backend affirmatively
 * confirming a report-back is still due, so it RESETS the budget — a healthy
 * long-running PTC analysis never loses its live stream. The budget also resets
 * on each successful attach.
 */
export const REPORT_BACK_MAX_POLLS = 10;

/**
 * Cap onClosed-driven re-subscribes: watchThread returns instantly on a non-ok
 * response (no backoff), which would otherwise spin onClosed→subscribe→onClosed.
 * Resets on each successful attach; once spent the backstop timer is the sole
 * recovery path until the next re-arm.
 */
const REPORT_BACK_MAX_RESUBSCRIBES = 5;

/**
 * Max attach attempts per run id within one watch generation. A zero-content
 * stream end releases the run for ONE retry (a failed first attach must not
 * poison the dedup), but a run whose stream never yields must not re-attach
 * forever.
 */
const REPORT_BACK_MAX_ATTACH_ATTEMPTS = 2;

/**
 * Idle cap for a report-back catch-up reconnect. The per-run stream has no
 * terminal sentinel (it stays open ~8s after the summary, forever if the run is
 * wedged), and a reader that never resolves strands the spinner + isStreamingRef
 * — unrecoverably, since the backstop reconcile bails on isStreamingRef. Chosen
 * well above flash inter-token + first-event gaps so a healthy summary is never
 * truncated.
 */
const REPORT_BACK_IDLE_ABORT_MS = 4_000;

/**
 * Max idle-watchdog re-arms before force-releasing a report-back reconnect. The
 * watchdog probes `/status` on each idle window and re-arms while the run is
 * still ours (a quiet window is not proof of terminality); this bounds a
 * genuinely wedged run to ~24s. Consumed by the gate in useChatMessages'
 * reconnectToStream (co-located with REPORT_BACK_IDLE_ABORT_MS).
 */
export const REPORT_BACK_IDLE_MAX_REARMS = 6;

/** Options accepted by the host's shared reconnect reader. */
interface ReconnectToStreamOptions {
  activeTasks?: string[];
  runId?: string | null;
  resetCursor?: boolean;
  idleAbortMs?: number;
}

export interface UseReportBackWatchParams {
  /** The visible thread id (prop). */
  threadId: string;
  /** The workspace id (prop). */
  workspaceId: string;
  /** Ref-based visible thread id (latched from Content-Location for a new chat). */
  threadIdRef: MutableRefObject<string>;
  /** True while a main/reconnect stream is live. */
  isStreamingRef: MutableRefObject<boolean>;
  /** The active run id on screen (the attach dedup key). */
  currentRunIdRef: MutableRefObject<string | null>;
  /**
   * Highest turn_index the host view has rendered (null = no history load
   * yet). Compared against `/status.latest_turn_index` on reactivation to
   * catch turns whose run finished while the view was hidden (terminal runs
   * carry no reconnectable run_id to compare).
   */
  lastRenderedTurnIndexRef: MutableRefObject<number | null>;
  /** Set once this instance's initial history load settled. */
  historyLoadedKeyRef: MutableRefObject<string | null>;
  /** True while a history load is in flight. */
  historyLoadingRef: MutableRefObject<boolean>;
  /** The host's shared reconnect reader. */
  reconnectToStream: (opts?: ReconnectToStreamOptions) => Promise<void>;
  /**
   * Bump the host's reload trigger, re-running the full load-then-reconnect
   * flow (/status → history replay → reconnect to the live run).
   */
  requestHistoryReload: () => void;
}

export interface ReportBackWatch {
  /** React-state mirror of the watch-armed flag, for render (chat-input tip). */
  awaitingReportBack: boolean;
  /**
   * High-level arm: mark awaiting, open/keep the keyed watch, seed the
   * backend-named run, and optionally poke a catch-up reconcile. Seeds AFTER
   * arming, since a fresh arm tears down + rebuilds and would drop the seed.
   */
  arm: (
    flashThreadId: string | null | undefined,
    reportBackRunId: string | null | undefined,
    pokeSource: string | null,
  ) => void;
  /**
   * Record run ids whose turns are already on screen (a fresh history replay
   * rendered every persisted turn — and a drained run is always persisted before
   * it enters the recents list), so the recent-runs catch-up never re-attaches
   * them as duplicates. Call after each successful history load with that load's
   * `/status.recent_report_back_run_ids`.
   */
  markRunsRendered: (runIds: string[] | null | undefined) => void;
  /**
   * Re-arm at the dispatch turn's stream end (no-op if a watch is already live)
   * and poke a catch-up reconcile — the next ordered report-back may be queued.
   * Keyed to the watch's flash thread so a stream-end while the user is on the
   * dispatched PTC thread never re-points the watch at the PTC thread.
   */
  onStreamEnd: () => void;
  /** Reconnect a re-shown cached view to a run/report-back started while hidden. */
  reconnectIfStaleRun: () => Promise<void>;
}

/**
 * Owns the report-back watch's dedicated refs, constants and lifecycle. All
 * closures are recreated each render (capturing the latest injected deps),
 * matching the original inline behavior in useChatMessages — but the RETURNED
 * methods are identity-stable facades over a latest-impl ref, so hosts can
 * list them in useCallback/useEffect deps without churning per render.
 */
export function useReportBackWatch(params: UseReportBackWatchParams): ReportBackWatch {
  const {
    threadId,
    workspaceId,
    threadIdRef,
    isStreamingRef,
    currentRunIdRef,
    lastRenderedTurnIndexRef,
    historyLoadedKeyRef,
    historyLoadingRef,
    reconnectToStream,
    requestHistoryReload,
  } = params;

  const awaitingReportBackRef = useRef(false);
  // React-state mirror of awaitingReportBackRef for the chat-input tip; the ref
  // stays the synchronous source of truth.
  const [awaitingReportBack, setAwaitingReportBackState] = useState(false);
  const setAwaiting = useCallback((v: boolean) => {
    awaitingReportBackRef.current = v;
    setAwaitingReportBackState(v);
  }, []);
  const reportBackWatchAbortRef = useRef<AbortController | null>(null);
  const reportBackPollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  // The active watch's reconcile fn, exposed so gap events outside the watch
  // closure (thread-load, re-activation, stream-end) can poke an immediate
  // catch-up. Stale pokes are ignored by the closure's own epoch check.
  const reportBackReconcileRef = useRef<
    ((source: string, wakeRunId?: string | null) => Promise<void>) | null
  >(null);
  // The flash thread the active watch is keyed to (NOT the visible thread) —
  // this is what lets the watch survive navigation into the dispatched PTC
  // thread and render only when its flash thread is back on screen.
  const reportBackWatchThreadIdRef = useRef<string | null>(null);
  // Ordered FIFO of backend-named run ids not yet attached. A flash thread can
  // dispatch N concurrent PTCs, and two wakes landing while the dispatch turn
  // still streams must BOTH survive to attach at stream end (a single-slot latch
  // let wake #2 overwrite wake #1). Deduped on enqueue, drained head-first (one
  // attach per reconcile; the stream-end poke fires the next), dropped whole on
  // teardown.
  const reportBackRunQueueRef = useRef<string[]>([]);
  // Run ids already on screen this session (attached report-backs, foreground
  // runs, history-replayed turns). Filters the recents catch-up so a drained but
  // already-rendered run never re-attaches as a duplicate. Deliberately
  // SESSION-scoped — survives watch teardown/re-arm, like the rendered bubbles.
  const attachedRunIdsRef = useRef<Set<string>>(new Set());

  // Latch a named run into the FIFO, collapsing redundant deliveries (a wake
  // plus /status naming the same run) to a single entry.
  const enqueueReportBackRun = (runId: string | null | undefined) => {
    if (!runId) return;
    if (attachedRunIdsRef.current.has(runId)) return;
    if (runId === currentRunIdRef.current) return;
    if (reportBackRunQueueRef.current.includes(runId)) return;
    reportBackRunQueueRef.current.push(runId);
  };

  // See {@link ReportBackWatch.markRunsRendered}. Also purges the queue so a
  // re-replay (reloadTrigger) can't leave a now-rendered run queued.
  const markRunsRendered = (runIds: string[] | null | undefined) => {
    if (!runIds || runIds.length === 0) return;
    for (const rid of runIds) attachedRunIdsRef.current.add(rid);
    reportBackRunQueueRef.current = reportBackRunQueueRef.current.filter(
      (rid) => !attachedRunIdsRef.current.has(rid),
    );
  };
  // Generation token, bumped on every teardown so in-flight wake/poll callbacks
  // bail. (Can't key off threadIdRef: its two writers — prop sync and the stream
  // metadata handler — disagree transiently on a fresh thread.)
  const reportBackWatchEpochRef = useRef(0);

  const stopReportBackWatch = () => {
    reportBackWatchEpochRef.current += 1;
    reportBackWatchThreadIdRef.current = null;
    // Drop the pending queue; attachedRunIdsRef survives on purpose (what's
    // rendered stays rendered).
    reportBackRunQueueRef.current = [];
    reportBackReconcileRef.current = null;
    if (reportBackWatchAbortRef.current) {
      reportBackWatchAbortRef.current.abort();
      reportBackWatchAbortRef.current = null;
    }
    if (reportBackPollRef.current) {
      clearInterval(reportBackPollRef.current);
      reportBackPollRef.current = null;
    }
  };

  // Drive the flash thread to each PTC report-back turn as the PTCs complete.
  // The backend serializes N concurrent dispatches into ordered turns, so the
  // watch is PERSISTENT: it attaches the current head run, and the next
  // reconcile (after that turn's stream ends) discovers the next head, until
  // /status reports pending_report_back=false.
  //
  // The backend names each report-back run explicitly: a fire-and-forget Redis
  // pub/sub wake (`thread:wake:{tid}`) carries the run_id, and `/status`
  // (`report_back_run_id`) durably records it for clients that missed the wake.
  const startReportBackWatch = (flashThreadId?: string | null) => {
    const tid = flashThreadId ?? threadIdRef.current;
    if (!tid || tid === '__default__') return;

    // Idempotent: a watch for this flash thread is already running. Re-arming on
    // return must NOT tear it down — that would drop its captured run ids.
    if (reportBackWatchAbortRef.current && reportBackWatchThreadIdRef.current === tid) return;

    stopReportBackWatch();
    reportBackWatchThreadIdRef.current = tid;

    const epoch = reportBackWatchEpochRef.current;

    let consumed = false;
    let inFlight = false;
    let polls = 0;
    let resubscribes = 0;
    // Per-run zero-content attach failures — see REPORT_BACK_MAX_ATTACH_ATTEMPTS.
    const attachFailures = new Map<string, number>();

    // Attach to the named run's per-run stream (which replays buffered events
    // even after completion) so the summary streams into a fresh bubble — no
    // full-history reload, which would also duplicate the live dispatch card.
    // Skips the run already on screen.
    const attach = async (runId: string | null, activeTasks: string[]) => {
      if (!runId || runId === currentRunIdRef.current) return false;
      polls = 0; // progress — reset the give-up + re-subscribe budgets
      resubscribes = 0;
      // Record BEFORE streaming so a racing reconcile / recents read can't
      // re-enqueue this run; un-recorded below if the stream delivered nothing.
      attachedRunIdsRef.current.add(runId);
      // idleAbortMs self-limits the catch-up: the per-run stream has no terminal
      // sentinel, so the idle watchdog ends the reader once the summary streamed
      // (or never started). See REPORT_BACK_IDLE_ABORT_MS.
      await reconnectToStream({ activeTasks, runId, resetCursor: true, idleAbortMs: REPORT_BACK_IDLE_ABORT_MS });
      // A zero-content stream end releases currentRunIdRef in the reader's
      // teardown — read that as "never actually rendered" and un-record the run
      // for a bounded retry.
      if (currentRunIdRef.current === null) {
        const failures = (attachFailures.get(runId) ?? 0) + 1;
        attachFailures.set(runId, failures);
        if (failures < REPORT_BACK_MAX_ATTACH_ATTEMPTS) attachedRunIdsRef.current.delete(runId);
      }
      return true;
    };

    // Drain ONE attach off the FIFO head; heads that dedup are dropped and the
    // next tried. Returns false only with an emptied queue, so the idle-teardown
    // check below can trust "no attach" to mean "nothing un-attached remains".
    const attachQueueHead = async (activeTasks: string[]): Promise<boolean> => {
      while (reportBackRunQueueRef.current.length > 0) {
        const head = reportBackRunQueueRef.current.shift()!;
        if (attachedRunIdsRef.current.has(head) || head === currentRunIdRef.current) continue;
        if (await attach(head, activeTasks)) return true;
      }
      return false;
    };

    const reconcile = async (source: string, wakeRunId?: string | null) => {
      // Stale generation (re-armed for another thread / hard-stopped / unmounted).
      if (reportBackWatchEpochRef.current !== epoch) return;
      if (consumed) return;

      // The run on screen right now is rendered by definition — record it so the
      // recents catch-up never re-attaches it after a later run replaces
      // currentRunIdRef.
      if (currentRunIdRef.current) attachedRunIdsRef.current.add(currentRunIdRef.current);

      // Latch the named run even while off-thread or mid-stream (a wake fires
      // once; pub/sub has no replay) so it attaches at stream end / return.
      enqueueReportBackRun(wakeRunId);

      // Ownership guard. In production each ChatView is its own hook instance
      // with a stable threadId, so `threadIdRef !== tid` is a belt-and-suspenders
      // identity check (real isolation is per-instance state + the
      // currentRunIdRef dedup); it only fires in the single-instance test
      // harness. isStreamingRef/inFlight prevent double-attaching.
      if (threadIdRef.current !== tid || isStreamingRef.current || inFlight) return;

      // On the flash thread and idle: attach the queued head immediately. One
      // attach per reconcile; the attached stream's end pokes the next.
      if (await attachQueueHead([])) return;

      inFlight = true;
      let status: ReportBackStatusResponse;
      try {
        // Cheap report-back-only slice — skips the checkpoint / background-task /
        // share reads the full /status does.
        status = await getReportBackStatus(tid);
      } catch {
        inFlight = false;
        // A transient /status error is NOT a drained queue (the backend returns
        // a null sentinel, never `false`, when its own read fails) — but a
        // persistently-failing endpoint counts toward the give-up cap.
        if (source === 'poll' && ++polls >= REPORT_BACK_MAX_POLLS) {
          consumed = true;
          setAwaiting(false);
          stopReportBackWatch();
        }
        return;
      }
      inFlight = false;
      // Re-check generation/ownership after the await: the watch may have been
      // torn down or a stream may have claimed the slot while /status was in flight.
      if (consumed || reportBackWatchEpochRef.current !== epoch) return;
      if (threadIdRef.current !== tid || isStreamingRef.current) return;

      enqueueReportBackRun(status.report_back_run_id);
      // Recent-runs catch-up (drained-run discovery): once a report-back turn
      // drains, its live pointer is deleted server-side — report_back_run_id can
      // never name it again. recent_report_back_run_ids (newest first, ~10,
      // 15-min TTL) keeps drained runs discoverable, covering wakes that fired
      // with zero subscribers. Enqueue oldest-first so turns render in dispatch
      // order; enqueue dedup skips everything already attached/rendered/queued.
      const recents = status.recent_report_back_run_ids;
      if (recents && recents.length > 0) {
        for (const rid of [...recents].reverse()) enqueueReportBackRun(rid);
      }
      // Flash report-backs carry no subagent tasks (no sandbox) → empty task list.
      if (await attachQueueHead([])) return;

      const signal = decodeReportBackSignal(status.pending_report_back);

      // `idle` (the backend's explicit false) → every dispatched report-back has
      // drained. Tear down — safe with respect to unrendered runs because a
      // non-empty queue would have attached above and returned; an idle signal
      // alone never discards turns that haven't rendered.
      if (signal === 'idle') {
        consumed = true;
        setAwaiting(false);
        stopReportBackWatch();
        return;
      }

      // Still pending but no run named yet, or none coming (dispatch failed).
      // Only non-confirming backstop ticks (`unknown`/`none`) burn the give-up
      // budget; a `pending` tick affirmatively confirms and resets it.
      if (source === 'poll') {
        if (signal === 'pending') {
          polls = 0;
        } else if (++polls >= REPORT_BACK_MAX_POLLS) {
          consumed = true;
          setAwaiting(false);
          stopReportBackWatch();
        }
      }
    };

    // Subscribe to the push wake stream. The backend caps it (~30 min) and
    // transient drops happen, so onClosed re-subscribes event-first and
    // reconciles once to recover the gap — bounded by `resubscribes` against a
    // hard-failing endpoint (watchThread returns instantly, no backoff).
    const subscribe = () => {
      if (reportBackWatchEpochRef.current !== epoch) return;
      const { abort } = watchThread(
        tid,
        async (payload) => {
          const wakeRunId = payload?.run_id ?? null;
          if (wakeRunId) {
            await reconcile('wake', wakeRunId);
            return;
          }
          // Payload-less wake (older backend / malformed): /status reconcile
          // after a short delay so the report-back run can register.
          await new Promise((r) => setTimeout(r, 500));
          await reconcile('wake');
        },
        () => {
          // Non-deliberate close. Bail if this generation was torn down, or the
          // live abort ref is no longer ours (a newer subscribe replaced it).
          if (reportBackWatchEpochRef.current !== epoch) return;
          if (reportBackWatchAbortRef.current !== abort) return;
          reportBackWatchAbortRef.current = null;
          // Re-subscribe FIRST so a wake arriving during the gap lands on the
          // fresh connection, THEN reconcile once to recover the gap.
          if (++resubscribes <= REPORT_BACK_MAX_RESUBSCRIBES) {
            subscribe();
            void reconcile('close');
          }
        },
        () => {
          // watchThread's in-loop retry re-subscribed after a transient error;
          // wakes fired during the gap are lost, so reconcile once. NOT a 'poll'
          // source: a catch-up must never count toward the give-up cap.
          void reconcile('resubscribe');
        },
      );
      reportBackWatchAbortRef.current = abort;
    };

    subscribe();
    reportBackReconcileRef.current = reconcile;
    // Slow SAFETY backstop — only catches the rare case every event path missed.
    reportBackPollRef.current = setInterval(() => reconcile('poll'), REPORT_BACK_BACKSTOP_MS);
  };

  // See {@link ReportBackWatch.arm}.
  const arm = (
    flashThreadId: string | null | undefined,
    reportBackRunId: string | null | undefined,
    pokeSource: string | null,
  ) => {
    setAwaiting(true);
    startReportBackWatch(flashThreadId);
    // Seed AFTER arming: a fresh arm tears down + rebuilds and would drop it.
    enqueueReportBackRun(reportBackRunId);
    if (pokeSource) void reportBackReconcileRef.current?.(pokeSource);
  };

  // Re-arm at the dispatch turn's stream end, then poke a catch-up reconcile
  // (the arm is a no-op when a watch is already live; the poke is what discovers
  // the next queued head).
  const onStreamEnd = () => {
    if (!awaitingReportBackRef.current) return;
    startReportBackWatch(reportBackWatchThreadIdRef.current ?? threadIdRef.current);
    void reportBackReconcileRef.current?.('streamEnd');
  };

  // Reconnect a cached, re-shown view to a run that started while it was hidden.
  // ChatView instances stay mounted in an LRU cache (useChatViewCache), so
  // revisiting a thread does NOT remount or re-fire the thread-load effect;
  // ChatView's become-active effect calls this on the inactive→active
  // transition. /status only carries a reconnectable run_id while a run is
  // live, so the first branch is purely the live-run path; a run that already
  // FINISHED while hidden is caught by the turn-watermark branch below it.
  const reconnectIfStaleRun = async () => {
    if (!workspaceId || !threadId || threadId === '__default__') return;
    // Only after this instance's initial load settled; never mid-stream or -load.
    if (historyLoadedKeyRef.current === null || isStreamingRef.current || historyLoadingRef.current) return;
    const status = (await getWorkflowStatus(threadId).catch(() => null)) as WorkflowStatusResponse | null;
    if (!status) return;
    // Re-check after the await, mirroring the pre-await guard — including
    // historyLoadingRef, so a load that starts DURING the /status fetch can't
    // race this reconnect for the message state.
    if (isStreamingRef.current || historyLoadingRef.current || threadIdRef.current !== threadId) return;
    if (status.can_reconnect && status.run_id && status.run_id !== currentRunIdRef.current) {
      // Full reload, NOT a bare stream attach: a live stream carries no
      // user_message event, so only a history replay can render the missed
      // turn's query row (and any turns completed while hidden). The reload
      // flow ends by reconnecting to status.run_id, which latches
      // currentRunIdRef and closes this gate against re-entry.
      requestHistoryReload();
      return;
    }
    // Terminal staleness: the missed run already FINISHED, so the branch above
    // never opens (can_reconnect=false, no reconnectable run_id). The persisted
    // turn counter diverging from what this view rendered is the only remaining
    // signal that the transcript changed. Divergence in EITHER direction is
    // stale: higher means turns were missed while hidden; lower means a fork/
    // edit elsewhere truncated rows this view still renders. The reload's
    // replay re-records the watermark, closing this gate against re-entry (no
    // reload loop). A null watermark means no load settled yet — not-stale by
    // definition (and the pre-await guard already requires a settled load).
    if (
      typeof status.latest_turn_index === 'number' &&
      lastRenderedTurnIndexRef.current !== null &&
      status.latest_turn_index !== lastRenderedTurnIndexRef.current
    ) {
      requestHistoryReload();
      return;
    }
    // No live run, but this re-activated flash thread still has a report-back
    // pending (or the backend can't say — `unknown` also arms; draining is only
    // ever an explicit `false`). Without this, a report-back finished while
    // hidden would not stream until the next slow backstop tick.
    if (shouldArmReportBack(decodeReportBackSignal(status.pending_report_back))) {
      arm(threadId, status.report_back_run_id, 'activate');
      return;
    }
    // Everything drained while hidden (explicit idle) but the recents list names
    // runs this instance never attached nor rendered — the watch died early and
    // the turns are missing from the transcript. Arm + poke: the reconcile's
    // idle-with-recents path attaches them in order, then tears down.
    const recents = status.recent_report_back_run_ids;
    if (
      recents?.some(
        (rid) => !attachedRunIdsRef.current.has(rid) && rid !== currentRunIdRef.current,
      )
    ) {
      arm(threadId, status.report_back_run_id, 'activate');
    }
  };

  // The watch survives thread navigation, so the host's thread-load effect can't
  // own its teardown. Stop it only when the chat hook itself unmounts.
  useEffect(() => {
    return () => {
      stopReportBackWatch();
      awaitingReportBackRef.current = false;
    };
  }, []);

  // The closures above are recreated each render ON PURPOSE (fresh injected
  // deps), but their identities must not leak: the host lists what this hook
  // returns in useCallback deps whose results flow into memo()'d transcript
  // components, so a per-render identity re-renders every bubble on every
  // stream chunk. Delegate through a latest-impl ref behind stable facades.
  const implRef = useRef({ arm, markRunsRendered, onStreamEnd, reconnectIfStaleRun });
  implRef.current = { arm, markRunsRendered, onStreamEnd, reconnectIfStaleRun };

  const armStable = useCallback(
    (
      flashThreadId: string | null | undefined,
      reportBackRunId: string | null | undefined,
      pokeSource: string | null,
    ) => implRef.current.arm(flashThreadId, reportBackRunId, pokeSource),
    [],
  );
  const markRunsRenderedStable = useCallback(
    (runIds: string[] | null | undefined) => implRef.current.markRunsRendered(runIds),
    [],
  );
  const onStreamEndStable = useCallback(() => implRef.current.onStreamEnd(), []);
  const reconnectIfStaleRunStable = useCallback(() => implRef.current.reconnectIfStaleRun(), []);

  return useMemo(
    () => ({
      awaitingReportBack,
      arm: armStable,
      markRunsRendered: markRunsRenderedStable,
      onStreamEnd: onStreamEndStable,
      reconnectIfStaleRun: reconnectIfStaleRunStable,
    }),
    [awaitingReportBack, armStable, markRunsRenderedStable, onStreamEndStable, reconnectIfStaleRunStable],
  );
}
