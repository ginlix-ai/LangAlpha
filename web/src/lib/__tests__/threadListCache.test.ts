import { describe, expect, it } from 'vitest';
import { QueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { runAuthResets } from '@/lib/authResets';
import { holdThreadTitle, patchThreadTitle } from '@/lib/threadListCache';

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

describe('holdThreadTitle', () => {
  const fetchDetail = (qc: QueryClient, title: string) =>
    qc.fetchQuery({
      queryKey: queryKeys.threads.detail('t-1'),
      queryFn: async () => ({ thread_id: 't-1', title }),
      staleTime: 0,
    });

  it('lands the title being saved on a read that answers with the old one', async () => {
    const qc = new QueryClient();
    const hold = holdThreadTitle(qc, 't-1', 'New');
    await fetchDetail(qc, 'Old');
    expect(qc.getQueryData(queryKeys.threads.detail('t-1'))).toMatchObject({ title: 'New' });
    hold.release();
    await fetchDetail(qc, 'Server');
    expect(qc.getQueryData(queryKeys.threads.detail('t-1'))).toMatchObject({ title: 'Server' });
  });

  // A read with no observer, as a prefetch is, that went out before the
  // rename and answers after the write did.
  const staleRead = (qc: QueryClient) => {
    let answer: (title: string) => void = () => {};
    const read = qc.fetchQuery({
      queryKey: queryKeys.threads.detail('t-1'),
      queryFn: () => new Promise<{ thread_id: string; title: string }>((resolve) => {
        answer = (title) => resolve({ thread_id: 't-1', title });
      }),
      staleTime: 0,
    });
    return { read, answer: (title: string) => answer(title) };
  };

  it('outlasts a saved write until the reads out then have landed', async () => {
    const qc = new QueryClient();
    const hold = holdThreadTitle(qc, 't-1', 'New');
    const stale = staleRead(qc);
    hold.saved();
    stale.answer('Old');
    await stale.read;
    expect(qc.getQueryData(queryKeys.threads.detail('t-1'))).toMatchObject({ title: 'New' });
    // Drained: the next read is the server's.
    await fetchDetail(qc, 'Server');
    expect(qc.getQueryData(queryKeys.threads.detail('t-1'))).toMatchObject({ title: 'Server' });
  });

  it('leaves alone the read an invalidation starts in place of one it cancelled', async () => {
    const qc = new QueryClient();
    qc.setQueryData(queryKeys.threads.detail('t-1'), { thread_id: 't-1', title: 'Old' });
    const hold = holdThreadTitle(qc, 't-1', 'New');
    const stale = staleRead(qc);
    hold.saved();
    // With data cached, a refetch cancels the read in flight and starts its
    // own, which reads the server after the write: another tab renamed it.
    const query = qc.getQueryCache().find({ queryKey: queryKeys.threads.detail('t-1') })!;
    const fresh = query.fetch(
      { queryKey: queryKeys.threads.detail('t-1'), queryFn: async () => ({ thread_id: 't-1', title: 'Other tab' }) },
      { cancelRefetch: true },
    );
    stale.answer('Old');
    await fresh;
    await stale.read.catch(() => {});
    expect(qc.getQueryData(queryKeys.threads.detail('t-1'))).toMatchObject({ title: 'Other tab' });
  });

  it('lets a read from before a failed write land, since its title is still the saved one', async () => {
    const qc = new QueryClient();
    const hold = holdThreadTitle(qc, 't-1', 'New');
    const stale = staleRead(qc);
    hold.release();
    stale.answer('Old');
    await stale.read;
    expect(qc.getQueryData(queryKeys.threads.detail('t-1'))).toMatchObject({ title: 'Old' });
  });

  it('keeps the title on a liveness read that lands during the hold', async () => {
    const qc = new QueryClient();
    const key = queryKeys.threads.dispatchLiveness({ threads: ['t-1', 't-2'], runs: [], steers: [] });
    const hold = holdThreadTitle(qc, 't-1', 'New');
    await qc.fetchQuery({
      queryKey: key,
      queryFn: async () => ({
        liveness: [{ thread_id: 't-1', title: 'Old' }, { thread_id: 't-2', title: 'Other' }],
        runs: [],
      }),
    });
    expect(qc.getQueryData(key)).toEqual({
      liveness: [{ thread_id: 't-1', title: 'New' }, { thread_id: 't-2', title: 'Other' }],
      runs: [],
    });
    hold.release();
  });

  it('lets go at sign-out, so the next session reads the server\'s title', async () => {
    const qc = new QueryClient();
    holdThreadTitle(qc, 't-1', 'Unsaved');
    runAuthResets();
    await fetchDetail(qc, 'Server');
    expect(qc.getQueryData(queryKeys.threads.detail('t-1'))).toMatchObject({ title: 'Server' });
  });
});
