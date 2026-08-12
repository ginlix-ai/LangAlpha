/**
 * v2 mux client contract locks (STREAM_CONTRACT_V2.md, client side): frame
 * projection, applied high-water dedup across replays, positive closure
 * (drain predecessor vs live successor), resync semantics, main-lane
 * silence, and detach never signalling completion.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

interface Conn {
  threadId: string;
  cursors: string | null;
  sinceAge: number;
  push: (sse: string) => void;
  close: () => void;
  signal: AbortSignal;
}
const conns: Conn[] = [];
// When set, the next connection attempts reject with this error instead of
// connecting (drives the HTTP-status disposal contract).
let nextError: { status?: number } | null = null;

vi.mock('../../../utils/api', () => ({
  openThreadMuxStream: (
    threadId: string,
    cursors: string | null,
    onLine: (line: string) => void,
    signal: AbortSignal,
    sinceAgeS?: number,
  ) => {
    if (nextError) return Promise.reject(nextError);
    return new Promise<void>((resolve) => {
      // The real fetch rejects on abort; resolving is close enough for the
      // loop (abort is checked via the signal, not the settle path).
      signal.addEventListener('abort', () => resolve());
      conns.push({
        threadId,
        cursors,
        sinceAge: sinceAgeS ?? 0,
        push: (sse) => sse.split('\n').forEach(onLine),
        close: resolve,
        signal,
      });
    });
  },
}));

import { getThreadMux, peekThreadMux, type ThreadMuxSink } from '../threadStreamMux';

const chanOpen = (rid: string, lane: string, mode = 'replay', started?: number) =>
  `event: chan_open\ndata: ${JSON.stringify({ chan: `run:${rid}`, lane, mode, ...(started != null ? { started } : {}) })}\n\n`;

const chanClose = (rid: string, reason: string, extra: Record<string, unknown> = {}) =>
  `event: chan_close\ndata: ${JSON.stringify({ chan: `run:${rid}`, reason, ...extra })}\n\n`;

const taskFrame = (rid: string, lane: string, entry: string, seq: number, content: string) =>
  `id: run:${rid}#${entry}\nevent: message_chunk\ndata: ${JSON.stringify({
    run_id: rid,
    seq: entry,
    lane,
    type: 'message_chunk',
    payload: { seq, event: 'message_chunk', data: { agent: lane, content } },
  })}\n\n`;

const runEnd = (rid: string, lane: string, entry: string, outcome: string) =>
  `id: run:${rid}#${entry}\nevent: run_end\ndata: ${JSON.stringify({
    run_id: rid,
    seq: entry,
    lane,
    type: 'run_end',
    payload: { outcome },
  })}\n\n`;

function makeSink() {
  const events: Array<Record<string, unknown>> = [];
  const closures: Array<{ taskId: string; outcome: string | null }> = [];
  const resyncs = { count: 0 };
  const sink: ThreadMuxSink = {
    onTaskEvent: (ev) => events.push(ev),
    onTaskRunClosed: (taskId, outcome) => closures.push({ taskId, outcome }),
    onResyncRequired: () => {
      resyncs.count += 1;
    },
  };
  return { sink, events, closures, resyncs };
}

async function connected(atLeast: number): Promise<Conn> {
  // Reconnects sit behind the loop's real-timer backoff (1s first retry).
  for (let i = 0; i < 60 && conns.length < atLeast; i++) {
    await new Promise((r) => setTimeout(r, 100));
  }
  expect(conns.length).toBeGreaterThanOrEqual(atLeast);
  return conns[atLeast - 1];
}

let threadN = 0;
const freshThread = () => `t-${++threadN}`;

beforeEach(() => {
  conns.length = 0;
  nextError = null;
});

describe('ThreadStreamMux (v2 contract client)', () => {
  it('projects task frames to the v1 SSE event shape and closes positively', async () => {
    const tid = freshThread();
    const { sink, events, closures } = makeSink();
    const mux = getThreadMux(tid);
    mux.attach(sink);
    const c = await connected(1);
    expect(c.cursors).toBeNull();

    c.push(chanOpen('r1', 'task:t1'));
    expect(mux.openTaskIds().has('t1')).toBe(true);
    c.push(taskFrame('r1', 'task:t1', '5-0', 1, 'hello'));
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      event: 'message_chunk',
      agent: 'task:t1',
      content: 'hello',
      thread_id: tid,
      _eventId: 1,
    });

    c.push(runEnd('r1', 'task:t1', '6-0', 'completed'));
    expect(events).toHaveLength(1); // run_end is transport, not content
    c.push(chanClose('r1', 'terminal'));
    expect(closures).toEqual([{ taskId: 't1', outcome: 'completed' }]);
    expect(mux.openTaskIds().size).toBe(0);
    mux.detach();
  });

  it('reconnects with the run cursor and dedups a replay-from-0', async () => {
    const tid = freshThread();
    const { sink, events } = makeSink();
    const mux = getThreadMux(tid);
    mux.attach(sink);
    const c1 = await connected(1);
    c1.push(chanOpen('r1', 'task:t1'));
    c1.push(taskFrame('r1', 'task:t1', '1-0', 1, 'a'));
    c1.push(taskFrame('r1', 'task:t1', '2-0', 2, 'b'));
    c1.close(); // torn socket

    const c2 = await connected(2);
    expect(c2.cursors).toBe('run:r1#2-0');
    // Server replays from 0 anyway (e.g. cursor ignored after re-seed):
    // already-applied entries must not reach the sink twice.
    c2.push(chanOpen('r1', 'task:t1'));
    c2.push(taskFrame('r1', 'task:t1', '1-0', 1, 'a'));
    c2.push(taskFrame('r1', 'task:t1', '2-0', 2, 'b'));
    c2.push(taskFrame('r1', 'task:t1', '3-0', 3, 'c'));
    expect(events.map((e) => e.content)).toEqual(['a', 'b', 'c']);
    mux.detach();
  }, 15000);

  it('drain-closing a predecessor run is not task-terminal while the successor is open', async () => {
    const tid = freshThread();
    const { sink, events, closures } = makeSink();
    const mux = getThreadMux(tid);
    mux.attach(sink);
    const c = await connected(1);
    c.push(chanOpen('rA', 'task:t1', 'drain', 1000));
    c.push(chanOpen('rB', 'task:t1', 'replay', 2000));
    // Drain-channel frames are stamped so the sink can skip content the
    // client already projected from history; live frames are not. The
    // server-declared run start rides along so the sink can drop runs at
    // or before its history watermark.
    c.push(taskFrame('rA', 'task:t1', '1-0', 1, 'backlog'));
    c.push(taskFrame('rB', 'task:t1', '1-0', 1, 'live'));
    expect(
      events.map((e) => [e.content, e._drain === true, e._runStartedMs]),
    ).toEqual([
      ['backlog', true, 1000],
      ['live', false, undefined],
    ]);
    c.push(chanClose('rA', 'terminal', { outcome: 'completed' }));
    expect(closures).toEqual([]); // successor still open
    c.push(chanClose('rB', 'terminal', { outcome: 'cancelled' }));
    expect(closures).toEqual([{ taskId: 't1', outcome: 'cancelled' }]);
    mux.detach();
  });

  it('drops main-lane frames (the foreground POST owns that transcript)', async () => {
    const tid = freshThread();
    const { sink, events } = makeSink();
    const mux = getThreadMux(tid);
    mux.attach(sink);
    const c = await connected(1);
    c.push(chanOpen('rm', 'main'));
    c.push(taskFrame('rm', 'main', '1-0', 1, 'main content'));
    expect(events).toEqual([]);
    expect(mux.openTaskIds().size).toBe(0); // main lane is not a task
    mux.detach();
  });

  it('resync_required reconnects without the cursor but keeps the dedup high-water', async () => {
    const tid = freshThread();
    const { sink, events } = makeSink();
    const mux = getThreadMux(tid);
    mux.attach(sink);
    const c1 = await connected(1);
    c1.push(chanOpen('r1', 'task:t1'));
    c1.push(taskFrame('r1', 'task:t1', '4-0', 4, 'd'));
    c1.push(chanClose('r1', 'resync_required'));
    c1.close();

    const c2 = await connected(2);
    expect(c2.cursors).toBeNull(); // cursor dropped
    c2.push(chanOpen('r1', 'task:t1'));
    // Replay reaches only entries the stream still has; anything at or
    // below the applied high-water dedups (bounded hole, never duplicates).
    c2.push(taskFrame('r1', 'task:t1', '4-0', 4, 'd'));
    c2.push(taskFrame('r1', 'task:t1', '9-0', 9, 'e'));
    expect(events.map((e) => e.content)).toEqual(['d', 'e']);
    mux.detach();
  }, 15000);

  it("task outcome is the latest live run's, even when a predecessor drain closes last", async () => {
    const tid = freshThread();
    const { sink, closures } = makeSink();
    const mux = getThreadMux(tid);
    mux.attach(sink);
    const c = await connected(1);
    c.push(chanOpen('rA', 'task:t1', 'drain', 1000));
    c.push(chanOpen('rB', 'task:t1', 'replay', 2000));
    // Short successor settles first; its closure is suppressed while the
    // predecessor backlog drains — but its outcome must still win.
    c.push(chanClose('rB', 'terminal', { outcome: 'cancelled' }));
    expect(closures).toEqual([]);
    c.push(chanClose('rA', 'terminal', { outcome: 'completed' }));
    expect(closures).toEqual([{ taskId: 't1', outcome: 'cancelled' }]);
    mux.detach();
  });

  it("start order decides the outcome when every channel is a drain (reconnect after both settled)", async () => {
    const tid = freshThread();
    const { sink, closures } = makeSink();
    const mux = getThreadMux(tid);
    mux.attach(sink);
    const c = await connected(1);
    // Post-outage shape: both runs settled while disconnected, so both
    // re-open as drain — liveness can't order them, run start must.
    c.push(chanOpen('rA', 'task:t1', 'drain', 1000));
    c.push(chanOpen('rB', 'task:t1', 'drain', 2000));
    c.push(chanClose('rB', 'terminal', { outcome: 'cancelled' }));
    expect(closures).toEqual([]);
    c.push(chanClose('rA', 'terminal', { outcome: 'completed' }));
    expect(closures).toEqual([{ taskId: 't1', outcome: 'cancelled' }]);
    mux.detach();
  });

  it('a sink throw poisons the connection: later frames are ignored and the mux reconnects to redeliver', async () => {
    const tid = freshThread();
    const events: Array<Record<string, unknown>> = [];
    const closures: Array<{ taskId: string; outcome: string | null }> = [];
    let poisoned = true;
    const sink: ThreadMuxSink = {
      onTaskEvent: (ev) => {
        if (ev.content === 'poison' && poisoned) {
          poisoned = false;
          throw new Error('reducer bug');
        }
        events.push(ev);
      },
      onTaskRunClosed: (taskId, outcome) => closures.push({ taskId, outcome }),
    };
    const mux = getThreadMux(tid);
    mux.attach(sink);
    const c1 = await connected(1);
    c1.push(chanOpen('r1', 'task:t1'));
    c1.push(taskFrame('r1', 'task:t1', '4-0', 4, 'ok'));
    c1.push(taskFrame('r1', 'task:t1', '5-0', 5, 'poison')); // throws → self-poison
    expect(events.map((e) => e.content)).toEqual(['ok']);
    expect(c1.signal.aborted).toBe(true); // the mux tore the socket itself
    // The torn connection's tail — a run_end above all — must not advance
    // the cursor past the undelivered entry or close the channel over it.
    c1.push(runEnd('r1', 'task:t1', '6-0', 'completed'));
    c1.push(chanClose('r1', 'terminal'));
    expect(closures).toEqual([]);

    const c2 = await connected(2);
    // Exclusive resume must NOT acknowledge the undelivered entry — the
    // server has to resend it, and delivery then succeeds.
    expect(c2.cursors).toBe('run:r1#4-0');
    // The discarded tail may have held control announces this mux never
    // applied: with no pre-connection horizon floor to roll back to, the
    // reconnect declares an untrusted (over-cap) age so the server answers
    // with a thread resync.
    expect(c2.sinceAge).toBeGreaterThan(600);
    c2.push(chanOpen('r1', 'task:t1', 'resume'));
    c2.push(taskFrame('r1', 'task:t1', '5-0', 5, 'poison'));
    expect(events.map((e) => e.content)).toEqual(['ok', 'poison']);
    c2.push(runEnd('r1', 'task:t1', '6-0', 'completed'));
    c2.push(chanClose('r1', 'terminal'));
    expect(closures).toEqual([{ taskId: 't1', outcome: 'completed' }]);
    mux.detach();
  }, 15000);

  it('an untrusted declaration survives an empty EOF and discharges only on the delivered thread resync', async () => {
    const tid = freshThread();
    let poisoned = true;
    const resyncs = { count: 0 };
    const sink: ThreadMuxSink = {
      onTaskEvent: () => {
        if (poisoned) {
          poisoned = false;
          throw new Error('reducer bug');
        }
      },
      onTaskRunClosed: () => {},
      onResyncRequired: () => {
        resyncs.count += 1;
      },
    };
    const mux = getThreadMux(tid);
    mux.attach(sink); // no snapshot → a tear here is floorless
    const c1 = await connected(1);
    c1.push(chanOpen('r1', 'task:t1'));
    c1.push(taskFrame('r1', 'task:t1', '1-0', 1, 'x')); // throws → poison
    expect(c1.signal.aborted).toBe(true);

    // The reconnect declares the untrusted age — but this connection EOFs
    // cleanly before any SSE line (e.g. a proxy ended the body early).
    const c2 = await connected(2);
    expect(c2.sinceAge).toBeGreaterThan(600);
    c2.close();

    // An unanswered declaration is not spent: it must re-arm.
    const c3 = await connected(3);
    expect(c3.sinceAge).toBeGreaterThan(600);
    // Only the server's delivered thread resync discharges it…
    c3.push(`event: resync_required\ndata: ${JSON.stringify({ scope: 'thread' })}\n\n`);
    expect(resyncs.count).toBe(1);
    c3.close();

    // …after which the next connect is back to a normal age.
    const c4 = await connected(4);
    expect(c4.sinceAge).toBeLessThan(600);
    mux.detach();
  }, 20000);

  it('4xx disposes the mux, but 401 keeps retrying and heals', async () => {
    const t403 = freshThread();
    nextError = { status: 403 };
    const m403 = getThreadMux(t403);
    m403.attach(makeSink().sink);
    for (let i = 0; i < 40 && peekThreadMux(t403); i++) {
      await new Promise((r) => setTimeout(r, 50));
    }
    expect(peekThreadMux(t403)).toBeNull(); // permanent rejection → disposed

    const t401 = freshThread();
    nextError = { status: 401 };
    const m401 = getThreadMux(t401);
    m401.attach(makeSink().sink);
    await new Promise((r) => setTimeout(r, 300));
    expect(peekThreadMux(t401)).toBe(m401); // transient auth → still alive
    nextError = null; // token refreshed
    await connected(1); // …and the loop reconnects on its own
    m401.detach();
  }, 15000);

  it("a later-started live run's outcome wins even when the predecessor's backlog closes last", async () => {
    const tid = freshThread();
    const { sink, closures } = makeSink();
    const mux = getThreadMux(tid);
    mux.attach(sink);
    const c = await connected(1);
    // Both channels are LIVE (e.g. a seeded-open predecessor with a large
    // replay backlog, and its successor announced after it). Batched reads
    // let the short successor close first; the predecessor's stale outcome
    // must not overwrite it when its backlog finally drains.
    c.push(chanOpen('rA', 'task:t1', 'replay', 1000));
    c.push(chanOpen('rB', 'task:t1', 'replay', 2000));
    c.push(chanClose('rB', 'terminal', { outcome: 'cancelled' }));
    expect(closures).toEqual([]); // predecessor still draining
    c.push(chanClose('rA', 'terminal', { outcome: 'completed' }));
    expect(closures).toEqual([{ taskId: 't1', outcome: 'cancelled' }]);
    mux.detach();
  });

  it('thread-scoped resync_required asks the sink to reload; chan-scoped does not', async () => {
    const tid = freshThread();
    const { sink, resyncs } = makeSink();
    const mux = getThreadMux(tid);
    mux.attach(sink);
    const c = await connected(1);
    // Chan-scoped bare notice (precedes a chan_close) is not a reload ask.
    c.push(chanOpen('r1', 'task:t1'));
    c.push(`event: resync_required\ndata: ${JSON.stringify({ chan: 'run:r1' })}\n\n`);
    expect(resyncs.count).toBe(0);
    // Thread-scoped (chan-less): the horizon outran the catch-up window.
    c.push(`event: resync_required\ndata: ${JSON.stringify({ scope: 'thread' })}\n\n`);
    expect(resyncs.count).toBe(1);
    mux.detach();
  });

  it('carries the knowledge-horizon age to the server on connect', async () => {
    const tid = freshThread();
    const mux = getThreadMux(tid);
    // The caller's status/history snapshot was 30s before the socket —
    // the server must widen its settled-run window by that lag.
    mux.attach(makeSink().sink, Date.now() - 30_000);
    const c = await connected(1);
    expect(c.sinceAge).toBeGreaterThanOrEqual(29);
    expect(c.sinceAge).toBeLessThan(120);
    mux.detach();
  });

  it('detach never signals completion and disposes the registry entry', async () => {
    const tid = freshThread();
    const { sink, closures } = makeSink();
    const mux = getThreadMux(tid);
    mux.attach(sink);
    const c = await connected(1);
    c.push(chanOpen('r1', 'task:t1'));
    mux.detach();
    expect(closures).toEqual([]);
    expect(c.signal.aborted).toBe(true);
    expect(peekThreadMux(tid)).toBeNull();
  });
});
