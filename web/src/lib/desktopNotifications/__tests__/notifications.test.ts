// @vitest-environment node
/**
 * The desktop notification rules: what a feed event becomes in the shell, when
 * a posted notification comes down, and what the dock badge counts.
 */
import { describe, expect, it } from 'vitest';
import {
  buildAttention,
  fate,
  readFeedNotification,
  STARTED_NOTICE_GRACE_MS,
  startBehindResult,
  startedDeadline,
  toDesktopNotification,
  type PostedNotification,
  type ThreadView,
} from '../notifications';

const base = { thread_id: 't1', workspace_id: 'w1', run_id: 'r1', run_seq: 4, status: 'completed', origin: 'turn' };

describe('readFeedNotification', () => {
  it('reads a notification event', () => {
    expect(readFeedNotification({ ...base, type: 'notification', notification: { kind: 'turn', title: 'Q3 notes', body: 'Done' } })).toEqual({
      threadId: 't1',
      runId: 'r1',
      runSeq: 4,
      kind: 'turn',
      title: 'Q3 notes',
      body: 'Done',
    });
  });

  it('reads the start a background run_started carries', () => {
    const n = readFeedNotification({
      ...base,
      type: 'run_started',
      status: 'running',
      origin: 'wakeup',
      notification: { kind: 'started', title: 'NVDA watch', body: 'Checking back in…' },
    });
    expect(n).toMatchObject({ kind: 'started', title: 'NVDA watch', body: 'Checking back in…', runSeq: 4 });
  });

  it('is no notification for a plain run_started, an unknown kind, or no thread', () => {
    expect(readFeedNotification({ ...base, type: 'run_started' })).toBeNull();
    expect(readFeedNotification({ ...base, type: 'notification', notification: { kind: 'confetti', title: '', body: '' } })).toBeNull();
    expect(readFeedNotification({ ...base, thread_id: '', type: 'notification', notification: { kind: 'turn' } })).toBeNull();
    // A start only rides its run_started; a result never does.
    expect(readFeedNotification({ ...base, type: 'notification', notification: { kind: 'started' } })).toBeNull();
    expect(readFeedNotification({ ...base, type: 'run_started', notification: { kind: 'turn' } })).toBeNull();
  });
});

describe('toDesktopNotification', () => {
  it('keys by thread and opens the thread', () => {
    const n = readFeedNotification({ ...base, type: 'notification', notification: { kind: 'needs_input', title: 'Rebalance', body: 'Approve?' } })!;
    expect(toDesktopNotification(n)).toEqual({
      id: 't1',
      kind: 'needs_input',
      title: 'Rebalance',
      body: 'Approve?',
      path: '/chat/t/t1',
    });
  });

  it('a start is silent, and a missing title falls back to the app name', () => {
    const n = readFeedNotification({ ...base, type: 'run_started', notification: { kind: 'started', title: '', body: 'x' } })!;
    expect(toDesktopNotification(n)).toMatchObject({ silent: true, title: 'LangAlpha' });
  });
});

const NOW = 1_000_000;

const view = (over: Partial<ThreadView> = {}): ThreadView => ({
  known: true,
  seenThrough: 0,
  runSeq: 4,
  status: 'completed',
  running: false,
  needsInput: false,
  ...over,
});

const posted = (over: Partial<PostedNotification> = {}): PostedNotification => ({
  kind: 'turn',
  runSeq: 4,
  runId: 'run-4',
  known: true,
  clearAt: null,
  ...over,
});

describe('fate', () => {
  it('comes down once the thread is read at or past its run', () => {
    expect(fate(posted(), view({ seenThrough: 3 }), NOW)).toBe('keep');
    expect(fate(posted(), view({ seenThrough: 4 }), NOW)).toBe('clear');
    expect(fate(posted({ kind: 'started' }), view({ seenThrough: 4 }), NOW)).toBe('clear');
  });

  it('a question comes down once answered, never before the store has its run', () => {
    const question = posted({ kind: 'needs_input' });
    expect(fate(question, view({ runSeq: 3 }), NOW)).toBe('keep');
    expect(fate(question, view({ needsInput: true }), NOW)).toBe('keep');
    expect(fate(question, view({ runSeq: 5 }), NOW)).toBe('clear');
    expect(fate(question, view({ needsInput: false }), NOW)).toBe('clear');
    // Delivered before the run_settled it follows: the store still has the run going.
    expect(fate(question, view({ status: 'running', running: true }), NOW)).toBe('keep');
    // Only a question: a turn's result stays up through a new run until read.
    expect(fate(posted(), view({ runSeq: 5 }), NOW)).toBe('keep');
  });

  it('comes down when a thread it knew is gone', () => {
    expect(fate(posted(), view({ known: false }), NOW)).toBe('clear');
    expect(fate(posted({ known: false }), view({ known: false }), NOW)).toBe('keep');
  });

  it('comes down when its own Mark as read was pressed', () => {
    expect(fate(posted({ dismissed: true }), view(), NOW)).toBe('clear');
  });

  it('a start comes down at its deadline, not before', () => {
    const started = posted({ kind: 'started', clearAt: NOW + 5 });
    expect(fate(started, view(), NOW)).toBe('keep');
    expect(fate(started, view(), NOW + 5)).toBe('clear');
  });
});

describe('startedDeadline', () => {
  const started = posted({ kind: 'started' });
  const running = (over: Partial<ThreadView> = {}) => view({ status: 'running', running: true, ...over });

  it('stays up while its run is underway, or before the store has the run', () => {
    expect(startedDeadline(started, running(), NOW)).toBeNull();
    expect(startedDeadline(started, running({ status: 'stopping' }), NOW)).toBeNull();
    expect(startedDeadline(started, running({ runSeq: 3, status: 'completed' }), NOW)).toBeNull();
    expect(startedDeadline(started, running({ runSeq: undefined, status: undefined }), NOW)).toBeNull();
  });

  it('outlives a finished run by the grace its result needs to replace it, counted once', () => {
    expect(STARTED_NOTICE_GRACE_MS).toBeGreaterThan(8_000);
    for (const status of ['completed', 'failed', 'interrupted'] as const) {
      expect(startedDeadline(started, running({ status }), NOW)).toBe(NOW + STARTED_NOTICE_GRACE_MS);
    }
    // A later look keeps the deadline the first one set.
    const stamped = { ...started, clearAt: NOW + 7 };
    expect(startedDeadline(stamped, running({ status: 'completed' }), NOW + 3)).toBe(NOW + 7);
  });

  it('counts a run the store no longer has running as over, whatever its last status said', () => {
    // A snapshot found the run over after its settle was lost (a sleep, a dropped stream).
    expect(startedDeadline(started, view({ status: 'running', running: false }), NOW)).toBe(NOW + STARTED_NOTICE_GRACE_MS);
  });

  it('goes at once for a cancelled run or one a newer run overtook', () => {
    expect(startedDeadline(started, running({ status: 'cancelled' }), NOW)).toBe(NOW);
    expect(startedDeadline(started, running({ runSeq: 5 }), NOW)).toBe(NOW);
    expect(fate(started, running({ status: 'cancelled' }), NOW)).toBe('clear');
  });

  it('is only for a start with a run to match', () => {
    expect(startedDeadline(posted(), running({ status: 'cancelled' }), NOW)).toBeNull();
    expect(startedDeadline(posted({ kind: 'started', runSeq: null }), running({ status: 'cancelled' }), NOW)).toBeNull();
  });
});

describe('startBehindResult', () => {
  const start = (runId: string | null = 'run-4') =>
    readFeedNotification({ ...base, run_id: runId, type: 'run_started', notification: { kind: 'started', title: 't', body: 'b' } })!;

  it("drops a start that lands after its own run's result", () => {
    expect(startBehindResult(start(), posted({ kind: 'turn', runId: 'run-4' }))).toBe(true);
  });

  it('lets a start through for a new run, over an earlier start, or with nothing up', () => {
    expect(startBehindResult(start('run-5'), posted({ kind: 'turn', runId: 'run-4' }))).toBe(false);
    expect(startBehindResult(start(), posted({ kind: 'started', runId: 'run-4' }))).toBe(false);
    expect(startBehindResult(start(), undefined)).toBe(false);
    expect(startBehindResult(start(null), posted({ kind: 'turn', runId: null }))).toBe(false);
  });
});

describe('buildAttention', () => {
  it('counts unread and waiting, lists waiting then unread then running', () => {
    const attention = buildAttention(
      { running: new Set(['run']), needsInput: new Set(['ask']), unseen: new Set(['u1', 'u2']) },
      (id) => `title ${id}`,
    );
    expect(attention.count).toBe(3);
    expect(attention.threads.map((th) => [th.id, th.state])).toEqual([
      ['ask', 'needs_input'],
      ['u1', 'unread'],
      ['u2', 'unread'],
      ['run', 'running'],
    ]);
    expect(attention.threads[0]).toMatchObject({ title: 'title ask', path: '/chat/t/ask' });
  });

  it('caps the list but not the count', () => {
    const unseen = new Set(Array.from({ length: 30 }, (_, i) => `t${i}`));
    const attention = buildAttention({ running: new Set(), needsInput: new Set(), unseen }, () => '');
    expect(attention.count).toBe(30);
    expect(attention.threads).toHaveLength(20);
  });
});
