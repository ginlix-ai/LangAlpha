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
}

interface RunChannel {
  runId: string;
  lane: string; // "main" | "task:<taskId>"
  cursor: string | null; // last entry id received (reconnect resume point)
  applied: string | null; // last entry id delivered to the sink (dedup)
  closed: boolean;
  outcome: string | null; // from run_end payload or chan_close row truth
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

  constructor(
    private threadId: string,
    private onDispose: () => void,
  ) {}

  /** Register (or replace) the thread-level sink and keep the socket up.
   * The socket stays open while attached even with zero channels — the
   * control lane is what discovers newly spawned runs push-style. */
  attach(sink: ThreadMuxSink): void {
    if (this.disposed) return;
    this.sink = sink;
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
      try {
        await openThreadMuxStream(
          this.threadId,
          this.cursorParam(),
          (line) => this.onLine(line),
          aborted,
        );
        this.retry = 0;
      } catch (err: unknown) {
        const e = err as { name?: string; status?: number };
        if (e?.name !== 'AbortError') {
          console.warn(`[mux:${this.threadId}]`, err);
        }
        // A definitive HTTP rejection (bad request/auth/gone) can't heal by
        // retrying; transient failures and server-side timeouts reconnect.
        if (e?.status && e.status >= 400 && e.status <= 404) break;
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
    chan.cursor = entryId;
    const ftype = typeof frame.type === 'string' ? frame.type : 'message_chunk';
    if (ftype === 'run_end') {
      const p = frame.payload as { outcome?: unknown } | null;
      if (p && typeof p.outcome === 'string') chan.outcome = p.outcome;
      return; // closure is delivered via the chan_close that follows
    }
    if (ftype === 'lane_open') return; // channel metadata, not content
    if (chan.lane === 'main') return; // foreground POST owns the main lane
    const taskId = taskIdFromLane(chan.lane);
    if (!taskId) return;
    // At-least-once transport: drop frames at or below the applied
    // high-water (a replayed channel resends from 0 after resync/re-attach).
    if (chan.applied && !entryAfter(entryId, chan.applied)) return;
    chan.applied = entryId;
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
    try {
      this.sink?.onTaskEvent(ev);
    } catch (e) {
      console.warn(`[mux:${this.threadId}] onTaskEvent threw for ${taskId}`, e);
    }
  }

  private onControl(event: string, data: SSEEventObj): void {
    if (event === 'chan_open') {
      const runId = runIdFromChanName(data.chan);
      if (!runId) return;
      const lane = typeof data.lane === 'string' ? data.lane : '';
      const chan = this.getOrCreateRun(runId, lane);
      chan.closed = false;
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
        // A drained predecessor run closing while the task's live successor
        // is open is not task-terminal.
        if (taskId && !this.openTaskIds().has(taskId)) {
          try {
            this.sink?.onTaskRunClosed(taskId, chan.outcome);
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
