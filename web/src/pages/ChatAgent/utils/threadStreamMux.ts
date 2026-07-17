/**
 * Client for the multiplexed thread stream (`GET /threads/{id}/stream`).
 *
 * One socket carries every background-task channel (and the watch channel,
 * unconsumed until the client watch migrates). Task subscriptions are
 * promises that resolve ONLY on `chan_close {reason:"terminal"}` — a socket
 * drop reconnects with per-channel `(epoch, entryId)` cursors instead of
 * being mistaken for task completion, which is the false-terminal bug the
 * per-task-socket transport had by construction.
 *
 * Cursor discipline: cursors are recorded only from live task frames'
 * composite ids (`chan@epoch#entry#logical`); control frames never advance
 * them, and nothing from history replay can reach this map.
 */
import { openThreadMuxStream } from './api';

export function muxStreamEnabled(): boolean {
  return import.meta.env.VITE_MUX_STREAM === '1';
}

type SSEEventObj = Record<string, unknown>;
type ProcessEvent = (event: SSEEventObj) => void;

interface TaskChannel {
  processEvent: ProcessEvent;
  epoch: string | null;
  entryId: string | null;
  closed: boolean;
  resolve: () => void;
  promise: Promise<void>;
}

interface ParsedFrame {
  id: string | null;
  event: string | null;
  data: string | null;
}

/** Frames arriving before the client subscribes to their task (server-side
 * nudge discovery can beat the artifact card): buffered per task, flushed on
 * subscribe, dropped past the cap (history replay reconciles). */
const PRE_SUB_BUFFER_MAX = 200;
const MAX_RETRIES = 10;
const CONTROL_EVENTS = new Set([
  'chan_open',
  'chan_close',
  'stream_gap',
  'transport_error',
  'timeout',
  'watch_snapshot',
  'workflow_started',
  'error',
]);

export class ThreadStreamMux {
  private channels = new Map<string, TaskChannel>();
  private preSubBuffers = new Map<string, SSEEventObj[]>();
  // Tasks whose terminal close beat the subscriber (fast-settling task while
  // another channel keeps the socket alive): the server sends nothing further
  // for them on this socket, so a late openTask must flush the buffer and
  // resolve immediately instead of waiting forever.
  private closedPreSub = new Set<string>();
  private controller: AbortController | null = null;
  private running = false;
  private retry = 0;
  private disposed = false;

  constructor(
    private threadId: string,
    private onDispose: () => void,
  ) {}

  /** Subscribe to a task channel. Resolves on terminal close (or abort). */
  openTask(
    taskId: string,
    processEvent: ProcessEvent,
    signal: AbortSignal,
  ): Promise<void> {
    const existing = this.channels.get(taskId);
    if (existing) return existing.promise;
    if (this.closedPreSub.has(taskId)) {
      this.closedPreSub.delete(taskId);
      const buffered = this.preSubBuffers.get(taskId);
      this.preSubBuffers.delete(taskId);
      for (const ev of buffered ?? []) {
        try {
          processEvent(ev);
        } catch (e) {
          console.warn(`[mux:${this.threadId}] processEvent threw for ${taskId}`, e);
        }
      }
      return Promise.resolve();
    }
    let resolve!: () => void;
    const promise = new Promise<void>((r) => {
      resolve = r;
    });
    const chan: TaskChannel = {
      processEvent,
      epoch: null,
      entryId: null,
      closed: false,
      resolve,
      promise,
    };
    this.channels.set(taskId, chan);
    const buffered = this.preSubBuffers.get(taskId);
    if (buffered) {
      this.preSubBuffers.delete(taskId);
      for (const ev of buffered) this.deliver(taskId, chan, ev);
    }
    signal.addEventListener('abort', () => this.leaveTask(taskId), {
      once: true,
    });
    this.ensureRunning();
    return promise;
  }

  /** Client-side unsubscribe (navigation/teardown): resolves the task's
   * promise WITHOUT marking anything completed — the caller distinguishes
   * abort from terminal via its own signal. */
  leaveTask(taskId: string): void {
    const chan = this.channels.get(taskId);
    if (!chan) return;
    this.channels.delete(taskId);
    chan.closed = true;
    chan.resolve();
    if (this.channels.size === 0) this.controller?.abort();
  }

  private ensureRunning(): void {
    if (this.running || this.disposed) return;
    this.running = true;
    void this.loop();
  }

  private async loop(): Promise<void> {
    while (!this.disposed && this.channels.size > 0) {
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
        if ((err as { name?: string })?.name !== 'AbortError') {
          console.warn(`[mux:${this.threadId}]`, err);
        }
      }
      this.flushBlock();
      if (this.disposed || this.channels.size === 0) break;
      if (aborted.aborted) break;
      this.retry += 1;
      if (this.retry > MAX_RETRIES) {
        // Degraded parity with the old transport: give up and let callers
        // treat the tasks as done rather than spinning forever.
        console.error(
          `[mux:${this.threadId}] reconnect budget exhausted; resolving ${this.channels.size} channel(s)`,
        );
        for (const taskId of [...this.channels.keys()]) this.leaveTask(taskId);
        break;
      }
      await new Promise((r) =>
        setTimeout(r, Math.min(1000 * 2 ** (this.retry - 1), 16000)),
      );
    }
    this.running = false;
    if (this.channels.size === 0) this.dispose();
  }

  private dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.controller?.abort();
    this.preSubBuffers.clear();
    this.closedPreSub.clear();
    this.onDispose();
  }

  private cursorParam(): string | null {
    const parts: string[] = [];
    for (const [taskId, chan] of this.channels) {
      if (chan.epoch && chan.entryId) {
        parts.push(`task:${taskId}@${chan.epoch}#${chan.entryId}`);
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
    const composite = id ? parseCompositeId(id) : null;
    if (!composite) return; // unroutable frame
    payload.event = event || 'message_chunk';
    if (composite.logical !== null) payload._eventId = composite.logical;
    const chan = this.channels.get(composite.taskId);
    if (chan) {
      chan.epoch = composite.epoch;
      chan.entryId = composite.entryId;
      this.deliver(composite.taskId, chan, payload);
    } else {
      const buf = this.preSubBuffers.get(composite.taskId) ?? [];
      if (buf.length < PRE_SUB_BUFFER_MAX) {
        buf.push(payload);
        this.preSubBuffers.set(composite.taskId, buf);
      }
    }
  }

  private deliver(taskId: string, chan: TaskChannel, ev: SSEEventObj): void {
    try {
      chan.processEvent(ev);
    } catch (e) {
      console.warn(`[mux:${this.threadId}] processEvent threw for ${taskId}`, e);
    }
  }

  private onControl(event: string, data: SSEEventObj): void {
    if (event === 'chan_open') {
      const taskId = taskIdFromChan(data.chan);
      if (!taskId) return;
      const chan = this.channels.get(taskId);
      if (chan && typeof data.epoch === 'string') {
        if (chan.epoch !== null && chan.epoch !== data.epoch) {
          // Stream re-incarnated (task resume): stale cursor is unusable.
          chan.entryId = null;
        }
        chan.epoch = data.epoch;
      }
      return;
    }
    if (event === 'chan_close') {
      const taskId = taskIdFromChan(data.chan);
      if (!taskId) return;
      const chan = this.channels.get(taskId);
      if (data.reason === 'terminal' && !chan) {
        // Terminal beat the subscriber: keep the buffered frames for the late
        // openTask (which flushes them and resolves without waiting).
        this.closedPreSub.add(taskId);
        return;
      }
      this.preSubBuffers.delete(taskId);
      if (chan && data.reason === 'terminal') {
        this.channels.delete(taskId);
        chan.closed = true;
        chan.resolve();
        if (this.channels.size === 0) this.controller?.abort();
      }
      return;
    }
    if (event === 'stream_gap') {
      console.info(`[mux:${this.threadId}] stream_gap`, data);
      return;
    }
    // transport_error / timeout: the server closes the socket after these —
    // the read loop ends and the reconnect loop re-attaches with cursors.
    // watch_snapshot / workflow_started / error: watch channel frames,
    // unconsumed until the client watch migrates onto the mux (M2b).
  }
}

function taskIdFromChan(chan: unknown): string | null {
  if (typeof chan !== 'string' || !chan.startsWith('task:')) return null;
  return chan.slice(5) || null;
}

function parseCompositeId(
  id: string,
): { taskId: string; epoch: string; entryId: string; logical: number | null } | null {
  // task:<id>@<epoch>#<entryId>#<logical|->
  if (!id.startsWith('task:')) return null;
  const at = id.indexOf('@');
  if (at < 0) return null;
  const taskId = id.slice(5, at);
  const rest = id.slice(at + 1);
  const [epoch, entryId, logicalRaw] = rest.split('#');
  if (!taskId || !epoch || !entryId) return null;
  const logical =
    logicalRaw && logicalRaw !== '-' ? parseInt(logicalRaw, 10) : NaN;
  return {
    taskId,
    epoch,
    entryId,
    logical: Number.isNaN(logical) ? null : logical,
  };
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
