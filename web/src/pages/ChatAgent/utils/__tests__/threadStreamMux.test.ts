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
  push: (sse: string) => void;
  close: () => void;
  signal: AbortSignal;
}
const conns: Conn[] = [];

vi.mock('../api', () => ({
  openThreadMuxStream: (
    threadId: string,
    cursors: string | null,
    onLine: (line: string) => void,
    signal: AbortSignal,
  ) =>
    new Promise<void>((resolve) => {
      conns.push({
        threadId,
        cursors,
        push: (sse) => sse.split('\n').forEach(onLine),
        close: resolve,
        signal,
      });
    }),
}));

import { getThreadMux, peekThreadMux, type ThreadMuxSink } from '../threadStreamMux';

const chanOpen = (rid: string, lane: string, mode = 'replay') =>
  `event: chan_open\ndata: ${JSON.stringify({ chan: `run:${rid}`, lane, mode })}\n\n`;

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
  const sink: ThreadMuxSink = {
    onTaskEvent: (ev) => events.push(ev),
    onTaskRunClosed: (taskId, outcome) => closures.push({ taskId, outcome }),
  };
  return { sink, events, closures };
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
    const { sink, closures } = makeSink();
    const mux = getThreadMux(tid);
    mux.attach(sink);
    const c = await connected(1);
    c.push(chanOpen('rA', 'task:t1', 'drain'));
    c.push(chanOpen('rB', 'task:t1'));
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
