import { act } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { getChanges } from '@/api/drive';
import { renderHookWithProviders } from '@/test/utils';
import type { DriveChanges, DriveEntry } from '@/types/drive';
import { CHANGES_POLL_MS, useDriveChanges } from '../useDrive';

vi.mock('@/api/drive', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/drive')>()),
  getChanges: vi.fn(),
}));

const entry = { id: 'e1', path: 'a.md' } as DriveEntry;
const answer = (cursor: number, entries: DriveEntry[] = [], resync = false): DriveChanges => ({ cursor, entries, resync });

let visibility: DocumentVisibilityState = 'visible';

/** Hides or shows the tab the way a browser does: the state, then the event. */
function setVisible(visible: boolean) {
  visibility = visible ? 'visible' : 'hidden';
  document.dispatchEvent(new Event('visibilitychange'));
  window.dispatchEvent(new Event('visibilitychange'));
}

/**
 * Lets timers run and every promise they start settle. A poll's answer
 * reaches the page a few ticks after it arrives, well inside a poll's beat.
 */
async function advance(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms + SETTLE_MS);
  });
}
const SETTLE_MS = 50;

function follow(startSeq: number | null) {
  const onChanged = vi.fn();
  const view = renderHookWithProviders(() => useDriveChanges('personal', startSeq, onChanged));
  const invalidate = vi.spyOn(view.queryClient, 'invalidateQueries');
  return { ...view, onChanged, invalidate };
}

const sinces = () => vi.mocked(getChanges).mock.calls.map((call) => call[1]);

beforeEach(() => {
  vi.useFakeTimers();
  visibility = 'visible';
  Object.defineProperty(document, 'visibilityState', { configurable: true, get: () => visibility });
  vi.mocked(getChanges).mockReset();
});

afterEach(() => {
  vi.useRealTimers();
});

describe('useDriveChanges', () => {
  it("starts from the space's change_seq, hands over what changed, then asks from the new cursor", async () => {
    vi.mocked(getChanges).mockResolvedValueOnce(answer(11, [entry])).mockResolvedValue(answer(11));
    const { onChanged, invalidate } = follow(10);
    await advance(0);
    expect(sinces()).toEqual([10]);
    expect(onChanged).toHaveBeenCalledWith([entry], false);
    expect(invalidate).toHaveBeenCalled();

    await advance(CHANGES_POLL_MS);
    expect(sinces()).toEqual([10, 11]);
    expect(onChanged).toHaveBeenCalledTimes(1);
  });

  it('without a cursor yet, takes the current one and applies nothing', async () => {
    vi.mocked(getChanges).mockResolvedValueOnce(answer(20, [], true)).mockResolvedValue(answer(20));
    const { onChanged } = follow(null);
    await advance(0);
    expect(sinces()).toEqual([Number.MAX_SAFE_INTEGER]);
    expect(onChanged).not.toHaveBeenCalled();
    await advance(CHANGES_POLL_MS);
    expect(sinces()).toEqual([Number.MAX_SAFE_INTEGER, 20]);
  });

  it('hands a resync over with no entries, since they are not the whole story', async () => {
    vi.mocked(getChanges).mockResolvedValueOnce(answer(50, [entry], true)).mockResolvedValue(answer(50));
    const { onChanged, invalidate } = follow(10);
    await advance(0);
    expect(onChanged).toHaveBeenCalledWith([], true);
    expect(invalidate).toHaveBeenCalled();
    await advance(CHANGES_POLL_MS);
    expect(sinces()).toEqual([10, 50]);
  });

  it('reads nothing again while the cursor stays where it was', async () => {
    vi.mocked(getChanges).mockResolvedValue(answer(10));
    const { onChanged, invalidate } = follow(10);
    await advance(0);
    await advance(CHANGES_POLL_MS * 2);
    expect(sinces()).toEqual([10, 10, 10]);
    expect(invalidate).not.toHaveBeenCalled();
    expect(onChanged).not.toHaveBeenCalled();
  });

  it('reads the space again, once, when the cursor moves with no entry behind it (a trash emptied elsewhere)', async () => {
    vi.mocked(getChanges).mockResolvedValueOnce(answer(14)).mockResolvedValue(answer(14));
    const { onChanged, invalidate } = follow(10);
    await advance(0);
    await advance(CHANGES_POLL_MS * 2);
    expect(sinces()).toEqual([10, 14, 14]);
    // The space's lists, trash, entries and versions, and the spaces with their usage.
    expect(invalidate.mock.calls.map(([f]) => f?.queryKey)).toEqual([['drive', 'space', 'personal'], ['drive', 'spaces']]);
    // Nothing for the page to follow or drop: no entry moved, and it is no resync.
    expect(onChanged).not.toHaveBeenCalled();
  });

  it('reads nothing again on the first batch, which only takes the current cursor', async () => {
    vi.mocked(getChanges).mockResolvedValueOnce(answer(20)).mockResolvedValue(answer(20));
    const { invalidate } = follow(null);
    await advance(0);
    await advance(CHANGES_POLL_MS);
    expect(invalidate).not.toHaveBeenCalled();
  });

  it('asks again from the same cursor after a failed poll', async () => {
    vi.mocked(getChanges)
      .mockRejectedValueOnce(new Error('Request failed with status code 502'))
      .mockResolvedValueOnce(answer(12, [entry]))
      .mockResolvedValue(answer(12));
    const { onChanged } = follow(10);
    await advance(0);
    expect(sinces()).toEqual([10]);
    expect(onChanged).not.toHaveBeenCalled();
    await advance(CHANGES_POLL_MS);
    expect(sinces()).toEqual([10, 10]);
    expect(onChanged).toHaveBeenCalledWith([entry], false);
  });

  it('does not poll a hidden tab, and catches up at once when it is shown again', async () => {
    vi.mocked(getChanges).mockResolvedValueOnce(answer(10)).mockResolvedValueOnce(answer(13, [entry])).mockResolvedValue(answer(13));
    const { onChanged } = follow(10);
    await advance(0);
    expect(sinces()).toEqual([10]);

    setVisible(false);
    await advance(CHANGES_POLL_MS * 4);
    expect(sinces()).toEqual([10]);

    setVisible(true);
    await advance(0);
    expect(sinces()).toEqual([10, 10]);
    expect(onChanged).toHaveBeenCalledWith([entry], false);
  });

  it('stops when unmounted', async () => {
    vi.mocked(getChanges).mockResolvedValue(answer(10));
    const { unmount } = follow(10);
    await advance(0);
    unmount();
    await advance(CHANGES_POLL_MS * 3);
    expect(sinces()).toEqual([10]);
  });
});
