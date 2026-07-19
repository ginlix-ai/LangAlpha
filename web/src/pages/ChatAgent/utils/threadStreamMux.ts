/**
 * Client for the multiplexed thread stream, v2 contract
 * (`GET /threads/{id}/stream?contract=v2` — STREAM_CONTRACT_V2.md).
 *
 * One socket carries a run-scoped channel per open run (main lane + every
 * subagent task run) plus the watch relay. Channels are keyed by `run_id`;
 * frames carry their cursor in-band (`data.seq` = the run stream's entry id),
 * so a socket drop reconnects with `run:<run_id>#<entry_id>` cursors and the
 * server resumes each channel exclusively after them.
 *
 * Delivery is at-least-once: a channel re-pushed in replay mode (new socket,
 * resync, reconcile rescan) resends from 0, and the per-run applied
 * high-water drops everything already handed to the sink. Closure is
 * positive — `run_end` / `chan_close {reason:"terminal"}` from row truth —
 * never inferred from socket loss or retry exhaustion.
 *
 * The consumer is one thread-level sink, not per-task subscriptions: task
 * frames are self-describing (`agent: "task:<id>"` routes inside the hook),
 * so frames arriving before any card exists apply immediately instead of
 * being buffered.
 */
import { openThreadMuxStream } from './api';

type SSEEventObj = Record<string, unknown>;

export interface ThreadMuxSink {
  /** A task-lane content frame, shaped like a v1 SSE event object
   * (`event`, `agent`, `_eventId`, …) for the hook's dispatch switchboard. */
  onTaskEvent: (event: SSEEventObj) => void;
  /** The task's current run reached terminal (run_end or ledger-row truth)
   * and no other run of the task is open. Fires only from server-side
   * closure — never from detach or socket loss. */
  onTaskRunClosed: (taskId: string, outcome: string | null) => void;
  /** The client's knowledge horizon is beyond the server's catch-up window
   * (e.g. a tab asleep past the since_age cap): stream catch-up can't be
   * trusted — reload the projection from history. */
  onResyncRequired?: () => void;
}

interface RunChannel {
  runId: string;
  lane: string; // "main" | "task:<taskId>"
  cursor: string | null; // last entry id received (reconnect resume point)
  applied: string | null; // last entry id delivered to the sink (dedup)
  closed: boolean;
  outcome: string | null; // from run_end payload or chan_close row truth
  drain: boolean; // opened in drain mode: a superseded predecessor's backlog
  // Server-declared run start (epoch ms, ledger row truth; 0 = undeclared).
  // Outcome voting orders by this — close order never decides.
  startedAt: number;
}

interface ParsedFrame {
  id: string | null;
  event: string | null;
  data: string | null;
}

const CONTROL_EVENTS = new Set([
  'chan_open',
  'chan_close',
  'resync_required',
  'transport_error',
  'timeout',
  'watch_snapshot',
  'workflow_started',
  'error',
]);

// Declared when a discarding tear left no horizon floor: far past the
// server's since_age cap, so it answers with a thread-scoped resync.
const HORIZON_UNTRUSTED_AGE_S = 24 * 3600;

/** Numeric major-minor comparison of Redis stream entry ids ("1784-3"). */
function entryAfter(a: string, b: string): boolean {
  const [aMaj, aMin] = a.split('-').map(Number);
  const [bMaj, bMin] = b.split('-').map(Number);
  if (aMaj !== bMaj) return aMaj > bMaj;
  return (aMin || 0) > (bMin || 0);
}

function taskIdFromLane(lane: string): string | null {
  return lane.startsWith('task:') ? lane.slice(5) || null : null;
}

export class ThreadStreamMux {
  private runs = new Map<string, RunChannel>();
  private sink: ThreadMuxSink | null = null;
  private controller: AbortController | null = null;
  private running = false;
  private retry = 0;
  private disposed = false;
  // Set by a resync_required close: the socket abort is a cursor-reset
  // reconnect, not a teardown.
  private forceReconnect = false;
  // Set by a transport_error frame: the server closed this connection over a
  // failed transport (Redis outage), so its normal HTTP resolution must not
  // reset the reconnect backoff — otherwise an outage hammers at 1 rps.
  private connFailed = false;
  // Latest-STARTED run's terminal outcome per task, keyed by the
  // server-declared run start. Close order is NOT run order — a
  // predecessor's replay backlog can close after a short successor under
  // batched reads, and after an outage every channel re-opens as drain —
  // so the recorded outcome only yields to an equal-or-later-started run's.
  private latestRunOutcome = new Map<
    string,
    { startedAt: number; outcome: string }
  >();
  // The client's knowledge horizon (epoch ms): seeded by attach with the
  // status/history snapshot time, advanced by every received SSE line. Sent
  // as ?since_age_s so the server's recent-terminal window covers runs that
  // settled between the snapshot (or an outage gap) and this socket.
  private knownAt: number | null = null;
  // knownAt as of the current connection's start — the rollback floor when
  // this mux tears its own connection and discards buffered lines (every
  // discarded line was generated after the connection began, so a window
  // anchored at the floor covers whatever was dropped).
  private connStartKnownAt: number | null = null;
  // A discarding tear had no rollback floor: declare an over-cap age so the
  // server answers with a thread-scoped resync (projection reload).
  private horizonUntrusted = false;

  constructor(
    private threadId: string,
    private onDispose: () => void,
  ) {}

  /** Register (or replace) the thread-level sink and keep the socket up.
   * The socket stays open while attached even with zero channels — the
   * control lane is what discovers newly spawned runs push-style.
   * `snapshotAtMs` is when the caller's view of the thread (workflow
   * status / history) was captured — the true knowledge horizon, which can
   * predate the socket by a slow history load. */
  attach(sink: ThreadMuxSink, snapshotAtMs?: number): void {
    if (this.disposed) return;
    this.sink = sink;
    if (snapshotAtMs != null) {
      this.knownAt =
        this.knownAt == null
          ? snapshotAtMs
          : Math.min(this.knownAt, snapshotAtMs);
    }
    this.ensureRunning();
  }

  /** Client teardown (navigation/unmount). Never marks anything completed. */
  detach(): void {
    this.sink = null;
    this.dispose();
  }

  /** Short task ids with an open (non-closed) run channel. */
  openTaskIds(): Set<string> {
    const ids = new Set<string>();
    for (const chan of this.runs.values()) {
      if (chan.closed) continue;
      const taskId = taskIdFromLane(chan.lane);
      if (taskId) ids.add(taskId);
    }
    return ids;
  }

  private ensureRunning(): void {
    if (this.running || this.disposed) return;
    this.running = true;
    void this.loop();
  }

  private async loop(): Promise<void> {
    while (!this.disposed && this.sink) {
      this.controller = new AbortController();
      const aborted = this.controller.signal;
      this.connFailed = false;
      this.connStartKnownAt = this.knownAt;
      // Sticky until the server's thread resync actually reaches the sink
      // (cleared in onControl) — a connect that dies or EOFs before
      // delivering it re-declares on the next attempt.
      const sinceAgeS = this.horizonUntrusted
        ? HORIZON_UNTRUSTED_AGE_S
        : this.knownAt == null
          ? 0
          : Math.max(0, (Date.now() - this.knownAt) / 1000);
      try {
        await openThreadMuxStream(
          this.threadId,
          this.cursorParam(),
          (line) => this.onLine(line),
          aborted,
          sinceAgeS,
        );
        // A transport_error close resolves the HTTP request normally but is
        // a failure — let the backoff keep growing across an outage.
        if (!this.connFailed) this.retry = 0;
      } catch (err: unknown) {
        const e = err as { name?: string; status?: number };
        if (e?.name !== 'AbortError') {
          console.warn(`[mux:${this.threadId}]`, err);
        }
        // A definitive HTTP rejection (bad request/permission/gone) can't
        // heal by retrying — dispose so a later attach starts fresh instead
        // of leaving an inert registry entry. 401 stays retryable: a
        // backgrounded tab reconnects with an expired token before Supabase
        // refreshes it, and every retry re-reads the session.
        if (
          e?.status &&
          e.status >= 400 &&
          e.status <= 404 &&
          e.status !== 401
        ) {
          this.dispose();
          break;
        }
      }
      this.flushBlock();
      if (this.disposed || !this.sink) break;
      if (aborted.aborted && !this.forceReconnect) break;
      this.forceReconnect = false;
      this.retry += 1;
      await new Promise((r) =>
        setTimeout(r, Math.min(1000 * 2 ** (this.retry - 1), 16000)),
      );
    }
    this.running = false;
    if (!this.sink) this.dispose();
  }

  private dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.controller?.abort();
    this.onDispose();
  }

  /** A mux-initiated tear discards this connection's buffered lines —
   * including control announces whose runs we then know nothing about.
   * Receipt time of the line that triggered the tear is NOT a valid
   * watermark for them, so roll the horizon back to the connection-start
   * floor (everything discarded was generated after it); with no floor,
   * declare the horizon untrusted and let the server force a reload. */
  private poisonHorizon(): void {
    this.knownAt = this.connStartKnownAt;
    if (this.knownAt == null) this.horizonUntrusted = true;
  }

  private cursorParam(): string | null {
    const parts: string[] = [];
    for (const chan of this.runs.values()) {
      if (!chan.closed && chan.cursor) {
        parts.push(`run:${chan.runId}#${chan.cursor}`);
      }
    }
    return parts.length ? parts.join(',') : null;
  }

  // ---- SSE block assembly ------------------------------------------------

  private block: ParsedFrame = { id: null, event: null, data: null };

  private onLine(line: string): void {
    // A torn connection's remaining buffered lines are dead: they must not
    // advance the knowledge horizon (a discarded control announce was never
    // applied — claiming it would leave that run undiscoverable) and their
    // frames must not be assembled.
    if (this.controller?.signal.aborted) return;
    // Any processed line — keepalive included — proves the control lane was
    // live at receipt time; the horizon backs the reconnect catch-up window.
    this.knownAt = Date.now();
    if (line.startsWith(':')) return; // keepalive comment
    if (line.startsWith('id: ')) {
      this.block.id = line.slice(4).trim();
    } else if (line.startsWith('event: ')) {
      this.block.event = line.slice(7).trim();
    } else if (line.startsWith('data: ')) {
      this.block.data = line.slice(6);
    } else if (line.trim() === '') {
      this.flushBlock();
    }
  }

  private flushBlock(): void {
    const { id, event, data } = this.block;
    this.block = { id: null, event: null, data: null };
    // A torn connection (sink-throw poison or teardown) must not act on its
    // remaining buffered frames: a run_end here would acknowledge — and
    // close past — an entry the sink never applied.
    if (this.controller?.signal.aborted) return;
    if (data == null) return;
    let payload: SSEEventObj;
    try {
      payload = JSON.parse(data) as SSEEventObj;
    } catch {
      return;
    }
    if (event && CONTROL_EVENTS.has(event) && id == null) {
      this.onControl(event, payload);
      return;
    }
    if (typeof payload.run_id === 'string' && typeof payload.seq === 'string') {
      this.onRunFrame(payload);
    }
  }

  // ---- frame handling ----------------------------------------------------

  private onRunFrame(frame: SSEEventObj): void {
    const runId = frame.run_id as string;
    const entryId = frame.seq as string;
    const lane = typeof frame.lane === 'string' ? frame.lane : '';
    const chan = this.getOrCreateRun(runId, lane);
    const ftype = typeof frame.type === 'string' ? frame.type : 'message_chunk';
    if (ftype === 'run_end') {
      chan.cursor = entryId;
      const p = frame.payload as { outcome?: unknown } | null;
      if (p && typeof p.outcome === 'string') chan.outcome = p.outcome;
      return; // closure is delivered via the chan_close that follows
    }
    if (ftype === 'lane_open') {
      chan.cursor = entryId;
      return; // channel metadata, not content
    }
    if (chan.lane === 'main') {
      chan.cursor = entryId;
      return; // foreground POST owns the main lane
    }
    const taskId = taskIdFromLane(chan.lane);
    if (!taskId) {
      chan.cursor = entryId;
      return;
    }
    // At-least-once transport: drop frames at or below the applied
    // high-water (a replayed channel resends from 0 after resync/re-attach).
    if (chan.applied && !entryAfter(entryId, chan.applied)) {
      chan.cursor = entryId;
      return;
    }
    // The payload is the captured record {seq, event, data}; project it to
    // the v1 SSE event shape the hook's handlers consume.
    const record = frame.payload as
      | { seq?: unknown; event?: unknown; data?: unknown }
      | null;
    const ev: SSEEventObj = {
      ...((record?.data as SSEEventObj) ?? {}),
      event: ftype,
      thread_id: this.threadId,
    };
    if (typeof record?.seq === 'number') ev._eventId = record.seq;
    // Drain channels carry a settled run's backlog; the flag lets the sink
    // skip content the client already projected from history, and the run
    // start (server row truth) lets it do so at run granularity — a stale
    // durable-debt drain must not replay into a task a successor keeps live.
    if (chan.drain) {
      ev._drain = true;
      if (chan.startedAt) ev._runStartedMs = chan.startedAt;
    }
    try {
      this.sink?.onTaskEvent(ev);
      // Cursor and high-water advance only after successful delivery: the
      // exclusive-resume cursor must not acknowledge a frame the sink never
      // applied, and dedup must leave it re-deliverable by replay.
      chan.applied = entryId;
      chan.cursor = entryId;
    } catch (e) {
      console.warn(`[mux:${this.threadId}] onTaskEvent threw for ${taskId}`, e);
      // Poison this connection: later frames — a run_end above all — would
      // advance the cursor past the undelivered entry and close the channel
      // over it. Reconnect resumes after the last applied entry, so the
      // server resends the failed frame; a deterministic sink bug degrades
      // to a backoff-bounded reconnect loop, never silent content loss.
      this.forceReconnect = true;
      this.poisonHorizon();
      this.controller?.abort();
    }
  }

  private onControl(event: string, data: SSEEventObj): void {
    if (event === 'chan_open') {
      const runId = runIdFromChanName(data.chan);
      if (!runId) return;
      const lane = typeof data.lane === 'string' ? data.lane : '';
      const chan = this.getOrCreateRun(runId, lane);
      chan.closed = false;
      chan.drain = data.mode === 'drain';
      if (typeof data.started === 'number') chan.startedAt = data.started;
      return;
    }
    if (event === 'resync_required' && typeof data.chan !== 'string') {
      // Thread-scoped (chan-less) resync: the declared knowledge horizon is
      // beyond the server's catch-up window — stream catch-up alone cannot
      // reconstruct what settled in the gap. (Chan-scoped resync arrives as
      // chan_close {reason: resync_required} and is handled there.)
      // An untrusted-horizon declaration is answered by exactly this frame,
      // so only its delivery to a sink discharges the flag.
      if (this.sink) {
        this.horizonUntrusted = false;
        try {
          this.sink.onResyncRequired?.();
        } catch (e) {
          console.warn(`[mux:${this.threadId}] onResyncRequired threw`, e);
        }
      }
      return;
    }
    if (event === 'transport_error') {
      this.connFailed = true;
      return;
    }
    if (event === 'chan_close') {
      const runId = runIdFromChanName(data.chan);
      if (!runId) return;
      const chan = this.runs.get(runId);
      if (!chan || chan.closed) return;
      if (data.reason === 'resync_required') {
        // Our cursor points below a lost head. Drop the cursor and force one
        // reconnect: the channel re-attaches in replay mode from 0, and the
        // kept applied high-water turns the replay into dedup — a bounded
        // hole at worst, never duplicated content.
        chan.closed = true;
        chan.cursor = null;
        this.forceReconnect = true;
        this.poisonHorizon(); // this tear discards buffered lines too
        this.controller?.abort();
        return;
      }
      if (data.reason === 'unknown_run') {
        this.runs.delete(runId);
        return;
      }
      if (data.reason === 'terminal') {
        chan.closed = true;
        if (typeof data.outcome === 'string') chan.outcome = data.outcome;
        const taskId = taskIdFromLane(chan.lane);
        if (!taskId) return;
        // The task's outcome is its latest-STARTED run's — never whichever
        // channel happened to close last. A predecessor's backlog (live
        // replay or drain) can close after a short successor under batched
        // reads, and after an outage every channel re-opens as drain, so
        // ordering comes from the server-declared run start, not liveness.
        if (typeof chan.outcome === 'string') {
          const rec = this.latestRunOutcome.get(taskId);
          if (!rec || chan.startedAt >= rec.startedAt) {
            this.latestRunOutcome.set(taskId, {
              startedAt: chan.startedAt,
              outcome: chan.outcome,
            });
          }
        }
        // A predecessor run closing while the task's successor is still
        // open is not task-terminal.
        if (!this.openTaskIds().has(taskId)) {
          const outcome =
            this.latestRunOutcome.get(taskId)?.outcome ?? chan.outcome;
          try {
            this.sink?.onTaskRunClosed(taskId, outcome);
          } catch (e) {
            console.warn(
              `[mux:${this.threadId}] onTaskRunClosed threw for ${taskId}`,
              e,
            );
          }
        }
      }
      return;
    }
    // resync_required (bare notice), transport_error, timeout: the server
    // closes the socket after these — the loop reconnects with cursors.
    // watch_snapshot / workflow_started / error: watch relay, unconsumed
    // here (the report-back watch has its own transport until M8).
  }

  private getOrCreateRun(runId: string, lane: string): RunChannel {
    let chan = this.runs.get(runId);
    if (!chan) {
      chan = {
        runId,
        lane,
        cursor: null,
        applied: null,
        closed: false,
        outcome: null,
        drain: false,
        startedAt: 0,
      };
      this.runs.set(runId, chan);
    } else if (lane && !chan.lane) {
      chan.lane = lane;
    }
    return chan;
  }
}

function runIdFromChanName(chan: unknown): string | null {
  if (typeof chan !== 'string' || !chan.startsWith('run:')) return null;
  return chan.slice(4) || null;
}

// ---- per-thread registry --------------------------------------------------

const muxByThread = new Map<string, ThreadStreamMux>();

export function getThreadMux(threadId: string): ThreadStreamMux {
  let mux = muxByThread.get(threadId);
  if (!mux) {
    mux = new ThreadStreamMux(threadId, () => {
      if (muxByThread.get(threadId) === mux) muxByThread.delete(threadId);
    });
    muxByThread.set(threadId, mux);
  }
  return mux;
}

/** Existing mux for the thread, without creating one (for passive reads). */
export function peekThreadMux(threadId: string): ThreadStreamMux | null {
  return muxByThread.get(threadId) ?? null;
}
