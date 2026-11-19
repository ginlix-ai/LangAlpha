/**
 * A thread load does not wait for its ratings, so two reads can be out at
 * once. Only the latest one may apply, and only it may end the loading state.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import type { ThreadFeedback } from '../../utils/api';

const answers: Array<(rows: ThreadFeedback[]) => void> = [];

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (k: string) => k }),
}));

vi.mock('../../utils/api', () => ({
  getThreadFeedback: vi.fn(
    () => new Promise<ThreadFeedback[]>((resolve) => { answers.push(resolve); }),
  ),
  submitFeedback: vi.fn(),
  removeFeedback: vi.fn(),
  followUpFeedback: vi.fn(),
}));

import { useChatFeedback } from '../useChatFeedback';

const rated = (turn_index: number, rating: string) => ({ turn_index, rating }) as ThreadFeedback;

async function startTwoReads(load: () => Promise<void>) {
  let older!: Promise<void>;
  let newer!: Promise<void>;
  act(() => {
    older = load();
    newer = load();
  });
  const [answerOlder, answerNewer] = answers;
  return { older, newer, answerOlder, answerNewer };
}

describe('useChatFeedback load order', () => {
  beforeEach(() => { answers.length = 0; });

  it('keeps the newer read when the older one answers last', async () => {
    const { result } = renderHook(() => useChatFeedback('thread-A'));
    const reads = await startTwoReads(() => result.current.loadFeedback());

    await act(async () => {
      reads.answerNewer([rated(0, 'thumbs_up')]);
      await reads.newer;
    });
    expect(result.current.feedbackByTurn[0]?.rating).toBe('thumbs_up');

    await act(async () => {
      reads.answerOlder([rated(0, 'thumbs_down')]);
      await reads.older;
    });
    expect(result.current.feedbackByTurn[0]?.rating).toBe('thumbs_up');
  });

  it('stays loading while the newer read is out', async () => {
    const { result } = renderHook(() => useChatFeedback('thread-A'));
    const reads = await startTwoReads(() => result.current.loadFeedback());

    await act(async () => {
      reads.answerOlder([rated(0, 'thumbs_down')]);
      await reads.older;
    });
    expect(result.current.feedbackLoading).toBe(true);
    expect(result.current.feedbackByTurn[0]).toBeUndefined();

    await act(async () => {
      reads.answerNewer([]);
      await reads.newer;
    });
    expect(result.current.feedbackLoading).toBe(false);
  });
});
