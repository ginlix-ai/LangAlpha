/**
 * The welcome flow's screenshots stay staged while it is open: each stages
 * again, unseen, before the server lets it go; Start stages again and waits
 * for any whose id a sleeping tab let go stale; and a screenshot removed, or
 * the flow left, stages nothing more.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import type { Mock } from 'vitest';

vi.mock('@/pages/ChatAgent/utils/attachmentUpload', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/pages/ChatAgent/utils/attachmentUpload')>()),
  stageAttachment: vi.fn(),
  discardAttachment: vi.fn(),
}));

import {
  discardAttachment,
  RESTAGE_AFTER_MS,
  stageAttachment,
} from '@/pages/ChatAgent/utils/attachmentUpload';
import { ScreenshotStaging, screenshotAttachment, type Screenshot } from '../welcome/screenshots';

const stage = stageAttachment as Mock;
const discard = discardAttachment as Mock;

/** Stagings the test finishes by hand. */
function heldStaging() {
  const going: Array<{ signal: AbortSignal; resolve: (id: string) => void; reject: (e: unknown) => void }> = [];
  stage.mockImplementation((_file: File, { signal }: { signal: AbortSignal }) =>
    new Promise((resolve, reject) => {
      going.push({ signal, resolve, reject });
      signal.addEventListener('abort', () => reject(new Error('aborted')));
    }));
  return going;
}

/** A flow's list, as the keeper reads and writes it. */
function flow(shots: Screenshot[]) {
  const list = { shots };
  const staging = new ScreenshotStaging(
    () => list.shots,
    (id, attachmentId, readyAt) => {
      list.shots = list.shots.map((s) => (s.id === id ? { ...s, attachmentId, readyAt } : s));
    },
  );
  staging.track(list.shots);
  return { list, staging };
}

const shot = (id: string, readyAt = Date.now()): Screenshot => ({
  id,
  file: new File(['png'], `${id}.png`, { type: 'image/png' }),
  attachmentId: `att-${id}`,
  readyAt,
});

beforeEach(() => {
  vi.useFakeTimers();
  vi.resetAllMocks();
});

afterEach(() => {
  vi.useRealTimers();
});

describe('ScreenshotStaging', () => {
  it('stages a screenshot again before the server lets it go, then forgets the old copy', async () => {
    const going = heldStaging();
    const { list, staging } = flow([shot('s1')]);

    await vi.advanceTimersByTimeAsync(RESTAGE_AFTER_MS - 1);
    expect(going).toHaveLength(0);
    await vi.advanceTimersByTimeAsync(1);
    expect(going).toHaveLength(1);
    // Still the old copy while the new one goes.
    expect(list.shots[0].attachmentId).toBe('att-s1');

    going[0].resolve('att-s1b');
    await vi.advanceTimersByTimeAsync(0);
    expect(list.shots[0]).toMatchObject({ attachmentId: 'att-s1b', readyAt: Date.now() });
    expect(screenshotAttachment(list.shots[0]).readyAt).toBe(Date.now());
    expect(discard).toHaveBeenCalledExactlyOnceWith('att-s1');

    // And again a re-stage period after that.
    await vi.advanceTimersByTimeAsync(RESTAGE_AFTER_MS);
    expect(going).toHaveLength(2);
    staging.close();
  });

  it('on Start, stages again and waits for a screenshot gone stale while the tab slept', async () => {
    const going = heldStaging();
    const { list, staging } = flow([shot('s1'), shot('s2')]);
    // A tab asleep for a day: the clock moved, no timer fired.
    vi.setSystemTime(Date.now() + 25 * 60 * 60 * 1000);
    list.shots = [...list.shots, shot('s3')];
    staging.track(list.shots);

    let done = false;
    const freshened = staging.freshen().then(() => { done = true; });
    expect(going).toHaveLength(2);
    await vi.advanceTimersByTimeAsync(0);
    expect(done).toBe(false);

    going[0].resolve('att-s1b');
    going[1].resolve('att-s2b');
    await freshened;
    expect(list.shots.map((s) => s.attachmentId)).toEqual(['att-s1b', 'att-s2b', 'att-s3']);
    expect(discard.mock.calls.map(([id]) => id).sort()).toEqual(['att-s1', 'att-s2']);
    staging.close();
  });

  it('has nothing to wait for on Start when every screenshot is fresh', async () => {
    heldStaging();
    const { staging } = flow([shot('s1')]);
    await staging.freshen();
    expect(stage).not.toHaveBeenCalled();
    staging.close();
  });

  it('stages nothing more for a screenshot removed, or once the flow is left', async () => {
    const going = heldStaging();
    const { list, staging } = flow([shot('s1'), shot('s2')]);
    await vi.advanceTimersByTimeAsync(RESTAGE_AFTER_MS);
    expect(going).toHaveLength(2);

    // s1 removed while it goes again: the staging ends, its copy unused.
    list.shots = list.shots.filter((s) => s.id !== 's1');
    staging.track(list.shots);
    expect(going[0].signal.aborted).toBe(true);

    staging.close();
    expect(going[1].signal.aborted).toBe(true);
    await vi.advanceTimersByTimeAsync(3 * RESTAGE_AFTER_MS);
    expect(going).toHaveLength(2);
    expect(vi.getTimerCount()).toBe(0);
    expect(list.shots.map((s) => s.attachmentId)).toEqual(['att-s2']);
  });
});
