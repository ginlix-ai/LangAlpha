/**
 * A failed /status read looks the same as nothing to catch up on. The hook used
 * to drop its check before that read resolved, so a run announced (or started
 * while the view was hidden) stayed off screen until a reload when the read
 * failed. The check now stays owed and is retried with backoff while shown,
 * and is held while the view is busy.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, renderHook } from '@testing-library/react';

let feedRunId: string | null = null;
vi.mock('@/lib/threadLifecycle/store', () => ({
  useThreadFeedRunId: () => feedRunId,
}));

import { useForeignRunCatchUp } from '../useForeignRunCatchUp';

interface Props {
  isActive: boolean;
  busy: boolean;
  threadId?: string;
  /** The report-back watch is armed (a Home thread waiting on a hand-off). */
  awaiting?: boolean;
}

type CatchUp = (watched: readonly string[] | null) => Promise<boolean>;

function mount(catchUp: CatchUp, initial: Props = { isActive: true, busy: false }) {
  const isOwnRun = (runId: string) => runId === 'own-run';
  return renderHook(
    ({ isActive, busy, threadId = 't-1', awaiting = false }: Props) =>
      useForeignRunCatchUp({ threadId, isActive, busy, awaitingReportBack: awaiting, isOwnRun, catchUp }),
    { initialProps: initial },
  );
}

/** Mounts hidden, then shows the view, which owes one check. */
async function mountShown(catchUp: CatchUp) {
  const view = mount(catchUp, { isActive: false, busy: false });
  view.rerender({ isActive: true, busy: false });
  await flush();
  return view;
}

/** A catchUp whose first check stays in flight until the test settles it. */
function heldFirstCheck() {
  let settle!: (read: boolean) => void;
  const catchUp = vi
    .fn<CatchUp>()
    .mockImplementationOnce(() => new Promise((resolve) => { settle = resolve; }))
    .mockResolvedValue(true);
  return { catchUp, settle: (read: boolean) => act(async () => { settle(read); }) };
}

const flush = () => act(async () => {});
const advance = (ms: number) => act(async () => { await vi.advanceTimersByTimeAsync(ms); });

beforeEach(() => {
  vi.useFakeTimers();
  feedRunId = null;
});

afterEach(() => {
  vi.useRealTimers();
});

describe('useForeignRunCatchUp', () => {
  it('checks nothing on mount, since the first load reads the thread', async () => {
    const catchUp = vi.fn<() => Promise<boolean>>().mockResolvedValue(true);
    mount(catchUp);
    await advance(60_000);
    expect(catchUp).not.toHaveBeenCalled();
  });

  it('checks an announced run again after a failed read, backing off to a cap, until one succeeds', async () => {
    const catchUp = vi.fn<() => Promise<boolean>>().mockResolvedValue(false);
    const view = mount(catchUp);
    feedRunId = 'run-from-elsewhere';
    view.rerender({ isActive: true, busy: false });
    await flush();
    let calls = 1;
    expect(catchUp).toHaveBeenCalledTimes(calls);

    // The cap bounds how long a view stays stale after the link comes back.
    for (const gap of [2_000, 4_000, 8_000, 16_000, 30_000, 30_000]) {
      await advance(gap - 1);
      expect(catchUp).toHaveBeenCalledTimes(calls);
      await advance(1);
      expect(catchUp).toHaveBeenCalledTimes(++calls);
    }

    catchUp.mockResolvedValue(true);
    await advance(30_000);
    expect(catchUp).toHaveBeenCalledTimes(++calls);
    await advance(60_000);
    expect(catchUp).toHaveBeenCalledTimes(calls);
  });

  it('starts the backoff over once a check succeeds', async () => {
    const catchUp = vi
      .fn<() => Promise<boolean>>()
      .mockResolvedValueOnce(false)
      .mockResolvedValueOnce(false)
      .mockResolvedValueOnce(true)
      .mockResolvedValueOnce(false)
      .mockResolvedValue(true);
    const view = mount(catchUp);
    feedRunId = 'run-1';
    view.rerender({ isActive: true, busy: false });
    await flush();
    await advance(2_000);
    await advance(4_000);
    expect(catchUp).toHaveBeenCalledTimes(3);

    feedRunId = 'run-2';
    view.rerender({ isActive: true, busy: false });
    await flush();
    expect(catchUp).toHaveBeenCalledTimes(4);
    await advance(1_999);
    expect(catchUp).toHaveBeenCalledTimes(4);
    await advance(1);
    expect(catchUp).toHaveBeenCalledTimes(5);
  });

  it('checks again after a failed read when the view is shown again', async () => {
    const catchUp = vi.fn<() => Promise<boolean>>().mockResolvedValueOnce(false).mockResolvedValue(true);
    await mountShown(catchUp);
    expect(catchUp).toHaveBeenCalledTimes(1);
    await advance(2_000);
    expect(catchUp).toHaveBeenCalledTimes(2);
    await advance(60_000);
    expect(catchUp).toHaveBeenCalledTimes(2);
  });

  it('stops retrying once the view is hidden', async () => {
    const catchUp = vi.fn<() => Promise<boolean>>().mockResolvedValue(false);
    const view = await mountShown(catchUp);
    expect(catchUp).toHaveBeenCalledTimes(1);

    view.rerender({ isActive: false, busy: false });
    await advance(60_000);
    expect(catchUp).toHaveBeenCalledTimes(1);
  });

  it('holds an announced run while busy and passes over the view\'s own', async () => {
    const catchUp = vi.fn<() => Promise<boolean>>().mockResolvedValue(true);
    const view = mount(catchUp);

    feedRunId = 'own-run';
    view.rerender({ isActive: true, busy: true });
    await flush();
    view.rerender({ isActive: true, busy: false });
    await flush();
    expect(catchUp).not.toHaveBeenCalled();

    feedRunId = 'run-from-elsewhere';
    view.rerender({ isActive: true, busy: true });
    await flush();
    expect(catchUp).not.toHaveBeenCalled();
    view.rerender({ isActive: true, busy: false });
    await flush();
    expect(catchUp).toHaveBeenCalledTimes(1);
  });

  it('holds a view shown while busy, with a run announced meanwhile, for one check once it settles', async () => {
    const catchUp = vi.fn<() => Promise<boolean>>().mockResolvedValue(true);
    const view = mount(catchUp, { isActive: false, busy: true });

    view.rerender({ isActive: true, busy: true });
    await flush();
    feedRunId = 'run-from-elsewhere';
    view.rerender({ isActive: true, busy: true });
    await flush();
    expect(catchUp).not.toHaveBeenCalled();
    view.rerender({ isActive: true, busy: false });
    await flush();
    expect(catchUp).toHaveBeenCalledTimes(1);
    await advance(60_000);
    expect(catchUp).toHaveBeenCalledTimes(1);
  });

  it('holds a failed check\'s retry while the view is busy', async () => {
    const { catchUp, settle } = heldFirstCheck();
    const view = await mountShown(catchUp);

    view.rerender({ isActive: true, busy: true });
    await flush();
    await settle(false);
    await advance(60_000);
    expect(catchUp).toHaveBeenCalledTimes(1);
    view.rerender({ isActive: true, busy: false });
    await flush();
    expect(catchUp).toHaveBeenCalledTimes(2);
  });

  it('waits out a failed check\'s backoff when the view goes busy and idle meanwhile', async () => {
    // The load a check asks for makes that edge, and checking on it would
    // reload back to back. A run announced meanwhile waits too.
    const catchUp = vi.fn<() => Promise<boolean>>().mockResolvedValueOnce(false).mockResolvedValue(true);
    const view = await mountShown(catchUp);
    expect(catchUp).toHaveBeenCalledTimes(1);

    view.rerender({ isActive: true, busy: true });
    await flush();
    feedRunId = 'run-from-elsewhere';
    view.rerender({ isActive: true, busy: false });
    await flush();
    await advance(1_999);
    expect(catchUp).toHaveBeenCalledTimes(1);
    await advance(1);
    expect(catchUp).toHaveBeenCalledTimes(2);
    await advance(60_000);
    expect(catchUp).toHaveBeenCalledTimes(2);
  });

  it('keeps a failed check owed when the view\'s own run is announced during the backoff', async () => {
    // The own run's stream carries only its own turn, so dropping the debt
    // would leave the announced run off screen.
    const catchUp = vi.fn<() => Promise<boolean>>().mockResolvedValueOnce(false).mockResolvedValue(true);
    const view = mount(catchUp);
    feedRunId = 'run-from-elsewhere';
    view.rerender({ isActive: true, busy: false });
    await flush();
    expect(catchUp).toHaveBeenCalledTimes(1);

    feedRunId = 'own-run';
    view.rerender({ isActive: true, busy: true });
    await flush();
    view.rerender({ isActive: true, busy: false });
    await flush();
    await advance(2_000);
    expect(catchUp).toHaveBeenCalledTimes(2);
    await advance(60_000);
    expect(catchUp).toHaveBeenCalledTimes(2);
  });

  it('keeps a failed check owed when the view\'s own run is announced while it was in flight', async () => {
    const { catchUp, settle } = heldFirstCheck();
    const view = mount(catchUp);
    feedRunId = 'run-from-elsewhere';
    view.rerender({ isActive: true, busy: false });
    await flush();

    feedRunId = 'own-run';
    view.rerender({ isActive: true, busy: false });
    await flush();
    await settle(false);
    await advance(2_000);
    expect(catchUp).toHaveBeenCalledTimes(2);
  });

  it('checks a run announced while a check was in flight once that check settles', async () => {
    const { catchUp, settle } = heldFirstCheck();
    const view = await mountShown(catchUp);

    feedRunId = 'run-from-elsewhere';
    view.rerender({ isActive: true, busy: false });
    await flush();
    expect(catchUp).toHaveBeenCalledTimes(1);
    await settle(true);
    await advance(0);
    expect(catchUp).toHaveBeenCalledTimes(2);
  });

  it('checks a run announced on the next thread while the last thread\'s check was in flight', async () => {
    const { catchUp, settle } = heldFirstCheck();
    const view = await mountShown(catchUp);

    view.rerender({ isActive: true, busy: false, threadId: 't-2' });
    await flush();
    feedRunId = 'run-on-t-2';
    view.rerender({ isActive: true, busy: false, threadId: 't-2' });
    await flush();
    expect(catchUp).toHaveBeenCalledTimes(1);
    await settle(false);
    await advance(0);
    expect(catchUp).toHaveBeenCalledTimes(2);
  });

  it('keeps checking after a check throws, without retrying the throw', async () => {
    const logged = vi.spyOn(console, 'error').mockImplementation(() => {});
    const catchUp = vi
      .fn<() => Promise<boolean>>()
      .mockImplementationOnce(() => { throw new Error('boom'); })
      .mockResolvedValue(true);
    const view = await mountShown(catchUp);
    await advance(60_000);
    expect(catchUp).toHaveBeenCalledTimes(1);

    feedRunId = 'run-from-elsewhere';
    view.rerender({ isActive: true, busy: false });
    await flush();
    expect(catchUp).toHaveBeenCalledTimes(2);
    expect(logged).toHaveBeenCalled();
    logged.mockRestore();
  });
});

describe('useForeignRunCatchUp: a run announced while the report-back watch is armed', () => {
  // The watch attaches the thread's report-back runs itself, so the check
  // leaves those be; any other run (a steer its run missed, started as a turn
  // of its own) the watch never attaches, and only the check's read can tell
  // the two apart. So the run is checked, named as the watch's to judge.
  const armed = { isActive: true, busy: false, awaiting: true };

  it('checks it, naming the runs the watch may own', async () => {
    const catchUp = vi.fn<CatchUp>().mockResolvedValue(true);
    const view = mount(catchUp, armed);

    feedRunId = 'run-from-elsewhere';
    view.rerender(armed);
    await flush();
    expect(catchUp).toHaveBeenCalledTimes(1);
    expect(catchUp).toHaveBeenLastCalledWith(['run-from-elsewhere']);
  });

  it('passes over the view\'s own run', async () => {
    const catchUp = vi.fn<CatchUp>().mockResolvedValue(true);
    const view = mount(catchUp, armed);

    feedRunId = 'own-run';
    view.rerender(armed);
    await flush();
    await advance(60_000);
    expect(catchUp).not.toHaveBeenCalled();
  });

  it('holds a run announced while busy for one check once the view settles', async () => {
    const catchUp = vi.fn<CatchUp>().mockResolvedValue(true);
    const view = mount(catchUp, armed);

    feedRunId = 'run-from-elsewhere';
    view.rerender({ ...armed, busy: true });
    await flush();
    expect(catchUp).not.toHaveBeenCalled();
    view.rerender(armed);
    await flush();
    expect(catchUp).toHaveBeenCalledTimes(1);
    expect(catchUp).toHaveBeenLastCalledWith(['run-from-elsewhere']);
  });

  it('names every run announced while busy, in order, passing over the view\'s own', async () => {
    // A turn started for a missed steer, then a report-back: naming only the
    // last would leave the first, which the watch never attaches, off screen.
    const catchUp = vi.fn<CatchUp>().mockResolvedValue(true);
    const view = mount(catchUp, armed);

    for (const runId of ['run-a', 'own-run', 'run-b', 'run-a']) {
      feedRunId = runId;
      view.rerender({ ...armed, busy: true });
      await flush();
    }
    expect(catchUp).not.toHaveBeenCalled();
    view.rerender(armed);
    await flush();
    expect(catchUp).toHaveBeenCalledTimes(1);
    expect(catchUp).toHaveBeenLastCalledWith(['run-a', 'run-b']);
  });

  it('checks a run announced while a watched check was in flight once that check settles', async () => {
    const { catchUp, settle } = heldFirstCheck();
    const view = mount(catchUp, armed);

    feedRunId = 'run-a';
    view.rerender(armed);
    await flush();
    expect(catchUp).toHaveBeenCalledTimes(1);
    feedRunId = 'run-b';
    view.rerender(armed);
    await flush();
    await settle(true);
    await advance(0);
    expect(catchUp).toHaveBeenCalledTimes(2);
    expect(catchUp).toHaveBeenLastCalledWith(['run-b']);
  });

  it('runs a full check when the view is owed one anyway', async () => {
    const catchUp = vi.fn<CatchUp>().mockResolvedValue(true);
    const view = mount(catchUp, { ...armed, isActive: false });

    feedRunId = 'run-from-elsewhere';
    view.rerender(armed);
    await flush();
    expect(catchUp).toHaveBeenCalledTimes(1);
    expect(catchUp).toHaveBeenLastCalledWith(null);
  });

  it('checks a run announced with the watch disarmed in full', async () => {
    const catchUp = vi.fn<CatchUp>().mockResolvedValue(true);
    const view = mount(catchUp);

    feedRunId = 'run-from-elsewhere';
    view.rerender({ isActive: true, busy: false });
    await flush();
    expect(catchUp).toHaveBeenLastCalledWith(null);
  });

  it('keeps the runs owed after a failed read, and checks them again after the backoff', async () => {
    const catchUp = vi.fn<CatchUp>().mockResolvedValueOnce(false).mockResolvedValue(true);
    const view = mount(catchUp, armed);

    feedRunId = 'run-from-elsewhere';
    view.rerender(armed);
    await flush();
    expect(catchUp).toHaveBeenCalledTimes(1);
    await advance(2_000);
    expect(catchUp).toHaveBeenCalledTimes(2);
    expect(catchUp).toHaveBeenLastCalledWith(['run-from-elsewhere']);
    await advance(60_000);
    expect(catchUp).toHaveBeenCalledTimes(2);
  });
});
