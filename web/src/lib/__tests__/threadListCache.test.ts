import { describe, expect, it } from 'vitest';
import { QueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { patchThreadTitle } from '@/lib/threadListCache';

describe('patchThreadTitle', () => {
  it('reaches the hand-off cards\' and the roster\'s liveness reads, which stop polling once a run settles', () => {
    const qc = new QueryClient();
    const cardKey = queryKeys.threads.dispatchLiveness({ threads: ['t-1', 't-2'], runs: ['r-1'], steers: [] });
    const rosterKey = queryKeys.threads.dispatchActivity(['t-1', 't-2'], 'rev-1');
    const read = () => ({
      liveness: [
        { thread_id: 't-1', status: 'completed', title: 'Build a demo-ready' },
        { thread_id: 't-2', status: 'running', title: 'Other' },
      ],
      runs: [{ run_id: 'r-1' }],
    });
    qc.setQueryData(cardKey, read());
    qc.setQueryData(rosterKey, read());

    patchThreadTitle(qc, 't-1', 'Credit Cycle Dashboard', '2026-10-14T00:00:00Z');

    const expected = {
      liveness: [
        { thread_id: 't-1', status: 'completed', title: 'Credit Cycle Dashboard' },
        { thread_id: 't-2', status: 'running', title: 'Other' },
      ],
      runs: [{ run_id: 'r-1' }],
    };
    expect(qc.getQueryData(cardKey)).toEqual(expected);
    expect(qc.getQueryData(rosterKey)).toEqual(expected);
  });

  it('leaves a read that does not hold the thread as it was', () => {
    const qc = new QueryClient();
    const key = queryKeys.threads.dispatchLiveness({ threads: ['t-2'], runs: [], steers: [] });
    const before = { liveness: [{ thread_id: 't-2', title: 'Other' }], runs: [] };
    qc.setQueryData(key, before);

    patchThreadTitle(qc, 't-1', 'Credit Cycle Dashboard');

    expect(qc.getQueryData(key)).toBe(before);
  });
});
