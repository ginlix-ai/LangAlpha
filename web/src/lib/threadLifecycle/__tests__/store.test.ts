/**
 * Settled-contract lock for the lifecycle store's watermark algebra: which
 * absences from a snapshot are allowed to switch an indicator off, and which
 * are merely "not proven" (the v5 bug this design replaced erased unseen dots
 * that had only been clipped below the unseen cap).
 */
import { renderHook } from '@testing-library/react';
import { beforeEach, describe, expect, it } from 'vitest';
import {
  applyFeedEvent,
  applySnapshot,
  pruneThread,
  resetThreadLifecycle,
  setActiveThread,
  type SnapshotEntry,
  type SnapshotFrame,
  useThreadNeedsInput,
  useThreadRunning,
  useThreadRunStatus,
  useThreadUnseen,
} from '../store';

interface Flags {
  running: boolean;
  needsInput: boolean;
  unseen: boolean;
  status: string;
}

/** Read the derived flags for one thread through the public hooks. */
function read(threadId: string): Flags {
  const { result, unmount } = renderHook(() => ({
    running: useThreadRunning(threadId),
    needsInput: useThreadNeedsInput(threadId),
    unseen: useThreadUnseen(threadId),
    status: useThreadRunStatus(threadId),
  }));
  const flags = { ...result.current };
  unmount();
  return flags;
}

function entry(over: Partial<SnapshotEntry> & { thread_id: string }): SnapshotEntry {
  return {
    workspace_id: 'w1',
    run_id: `r-${over.thread_id}`,
    run_seq: 1,
    status: 'completed',
    last_seen_run_seq: 0,
    ...over,
  };
}

function frame(over: Partial<SnapshotFrame>): SnapshotFrame {
  return {
    as_of_seq: 0,
    oldest_included_unseen_seq: 0,
    live: [],
    unseen: [],
    ...over,
  };
}

/** Feed a settled/started run in, the way the SSE feed does. */
function feed(
  threadId: string,
  status: string,
  runSeq: number,
  extra: { run_id?: string; interrupt_reason?: string | null } = {},
): void {
  applyFeedEvent({
    type: status === 'running' ? 'run_started' : 'run_settled',
    thread_id: threadId,
    run_id: extra.run_id ?? `r-${threadId}-${runSeq}`,
    run_seq: runSeq,
    status,
    interrupt_reason: extra.interrupt_reason ?? null,
  });
}

beforeEach(() => {
  resetThreadLifecycle();
});

describe('threadLifecycleStore — snapshot watermarks', () => {
  it('kills a live indicator on absence while keeping a clipped unseen dot', () => {
    feed('live-thread', 'running', 5);
    feed('done-thread', 'completed', 3);
    expect(read('live-thread').running).toBe(true);
    expect(read('done-thread').unseen).toBe(true);

    // Truncated snapshot: the live set is complete (absence proves), the
    // unseen set only reaches back to seq 7 (absence below it proves nothing).
    applySnapshot(
      frame({ as_of_seq: 12, oldest_included_unseen_seq: 7, unseen: [entry({ thread_id: 'other', run_seq: 9 })] }),
    );

    expect(read('live-thread').running).toBe(false);
    expect(read('done-thread').unseen).toBe(true);
  });

  it('proves seen for a terminal absence at or above the cutoff', () => {
    feed('above', 'completed', 9);
    feed('below', 'completed', 3);

    applySnapshot(
      frame({ as_of_seq: 12, oldest_included_unseen_seq: 7, unseen: [entry({ thread_id: 'other', run_seq: 8 })] }),
    );

    expect(read('above').unseen).toBe(false);
    expect(read('below').unseen).toBe(true);
  });

  it('rebuilds the clipped dot once the unseen set is complete again', () => {
    feed('clipped', 'completed', 3);
    applySnapshot(frame({ as_of_seq: 12, oldest_included_unseen_seq: 7 }));
    expect(read('clipped').unseen).toBe(true);

    // Later snapshot is untruncated (cutoff 0) — now absence does prove seen.
    applySnapshot(frame({ as_of_seq: 14 }));
    expect(read('clipped').unseen).toBe(false);
  });

  it('a live absence must not erase the dot for the settle that follows it', () => {
    feed('t1', 'running', 5);
    applySnapshot(frame({ as_of_seq: 10 }));
    expect(read('t1').running).toBe(false);

    // The settle event lands after the frame was read. A single status-agnostic
    // watermark (the v5 bug) would swallow this dot.
    feed('t1', 'completed', 5);
    expect(read('t1').unseen).toBe(true);
  });

  it('a terminal absence must not silence a late-delivered run start', () => {
    feed('t1', 'completed', 5);
    applySnapshot(frame({ as_of_seq: 10 }));
    expect(read('t1').unseen).toBe(false);

    // run_started for seq 7 arrives after the frame — the seen watermark has
    // no business gating the spinner.
    feed('t1', 'running', 7);
    expect(read('t1').running).toBe(true);
  });

  it('ignores absences for observations newer than the frame', () => {
    feed('ahead', 'running', 20);
    // Stale frame (as_of 10) can't speak for a seq-20 observation.
    applySnapshot(frame({ as_of_seq: 10 }));
    expect(read('ahead').running).toBe(true);
  });

  it('lets a newer run re-arm both indicators past their watermarks', () => {
    feed('t1', 'running', 5);
    feed('t2', 'completed', 5);
    applySnapshot(frame({ as_of_seq: 10 }));
    expect(read('t1').running).toBe(false);
    expect(read('t2').unseen).toBe(false);

    feed('t1', 'running', 12);
    feed('t2', 'completed', 12);
    expect(read('t1').running).toBe(true);
    expect(read('t2').unseen).toBe(true);
  });

  it('snapshot presence releases a watermark that outran a late-committing run', () => {
    // run_seq is allocated at INSERT and committed later: a snapshot can
    // publish as_of=10 while run 8 is still uncommitted, suppressing it.
    feed('t1', 'running', 8);
    feed('t2', 'completed', 8);
    applySnapshot(frame({ as_of_seq: 10 }));
    expect(read('t1').running).toBe(false);
    expect(read('t2').unseen).toBe(false);

    // The run commits; the NEXT snapshot lists it. Positive presence must
    // beat the earlier absence inference — a raise-only watermark would
    // suppress this run forever.
    applySnapshot(
      frame({
        as_of_seq: 11,
        live: [entry({ thread_id: 't1', run_seq: 8, status: 'running' })],
        unseen: [entry({ thread_id: 't2', run_seq: 8 })],
      }),
    );
    expect(read('t1').running).toBe(true);
    expect(read('t2').unseen).toBe(true);
  });

  it('suppresses interrupted with the live watermark, not the seen one', () => {
    applyFeedEvent({
      type: 'run_settled',
      thread_id: 'hitl',
      run_id: 'r-hitl',
      run_seq: 5,
      status: 'interrupted',
      interrupt_reason: 'ask_user',
    });
    expect(read('hitl').needsInput).toBe(true);

    // Interrupted rows ride the UNCAPPED live set, so absence always proves —
    // the unseen cutoff must not gate it.
    applySnapshot(frame({ as_of_seq: 10, oldest_included_unseen_seq: 7 }));
    expect(read('hitl').needsInput).toBe(false);
    expect(read('hitl').unseen).toBe(false);
  });

  it('raises the seen cursor from snapshot rows, keeping a later settle quiet', () => {
    feed('t1', 'completed', 5);
    expect(read('t1').unseen).toBe(true);

    // Another tab is watching t1: its latest run is interrupted at seq 7 and
    // already stamped seen there.
    applySnapshot(
      frame({
        as_of_seq: 7,
        live: [
          entry({
            thread_id: 't1',
            run_seq: 7,
            status: 'interrupted',
            last_seen_run_seq: 7,
          }),
        ],
      }),
    );
    expect(read('t1').needsInput).toBe(true);
    expect(read('t1').unseen).toBe(false);

    // That run settles — the durable cursor already covers seq 7, no dot.
    feed('t1', 'completed', 7);
    expect(read('t1').unseen).toBe(false);
  });
});

describe('threadLifecycleStore — activeThreadId gate', () => {
  it('withholds the dot for the thread the user is viewing, and restores it on leave', () => {
    feed('t1', 'completed', 5);
    expect(read('t1').unseen).toBe(true);

    setActiveThread('t1');
    expect(read('t1').unseen).toBe(false);
    expect(read('t1').status).toBe('completed'); // gate is display-only

    setActiveThread('t2');
    expect(read('t1').unseen).toBe(true);
  });

  it('does not gate live indicators', () => {
    feed('t1', 'running', 5);
    setActiveThread('t1');
    expect(read('t1').running).toBe(true);
  });
});

describe('threadLifecycleStore — prune', () => {
  it('drops every layer and watermark for a pruned thread', () => {
    feed('t1', 'completed', 5);
    expect(read('t1').unseen).toBe(true);

    pruneThread('t1');

    expect(read('t1')).toEqual({
      running: false,
      needsInput: false,
      unseen: false,
      status: 'idle',
    });
  });
});
