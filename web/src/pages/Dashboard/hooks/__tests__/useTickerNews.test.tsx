import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import type { Mock } from 'vitest';
import type { ReactNode } from 'react';
import { renderHook, act, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { useTickerNews } from '../useTickerNews';

vi.mock('../../utils/api', () => ({ getNews: vi.fn() }));
import { getNews } from '../../utils/api';

const mockGetNews = getNews as Mock;

// Fresh client per test so cache from one case can't suppress another's fetch.
function makeWrapper() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return function Wrapper({ children }: { children: ReactNode }) {
    return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
  };
}

const rowsFor = (...syms: string[]) => syms.map((symbol) => ({ symbol }));

describe('useTickerNews (React Query)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockGetNews.mockResolvedValue({ results: [] });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('eagerly fetches once with the provider, then re-polls every 60s', async () => {
    vi.useFakeTimers();
    renderHook(() => useTickerNews(rowsFor('AAPL'), 'portfolio', 'tickertick'), { wrapper: makeWrapper() });

    await act(async () => { await vi.advanceTimersByTimeAsync(0); });
    expect(mockGetNews).toHaveBeenCalledTimes(1);
    expect(mockGetNews).toHaveBeenCalledWith({ tickers: ['AAPL'], limit: 50, provider: 'tickertick' });

    await act(async () => { await vi.advanceTimersByTimeAsync(60000); });
    expect(mockGetNews).toHaveBeenCalledTimes(2);

    await act(async () => { await vi.advanceTimersByTimeAsync(60000); });
    expect(mockGetNews).toHaveBeenCalledTimes(3);
  });

  it('does not fetch when there are no tickers', async () => {
    const { result } = renderHook(() => useTickerNews([], 'portfolio', 'tickertick'), {
      wrapper: makeWrapper(),
    });
    await act(async () => { await Promise.resolve(); });

    expect(mockGetNews).not.toHaveBeenCalled();
    expect(result.current.items).toEqual([]);
    expect(result.current.loading).toBe(false);
  });

  it('stops polling after unmount', async () => {
    vi.useFakeTimers();
    const { unmount } = renderHook(() => useTickerNews(rowsFor('MSFT'), 'watchlist', 'tickertick'), {
      wrapper: makeWrapper(),
    });

    await act(async () => { await vi.advanceTimersByTimeAsync(0); });
    expect(mockGetNews).toHaveBeenCalledTimes(1);

    unmount();
    await act(async () => { await vi.advanceTimersByTimeAsync(180000); });
    expect(mockGetNews).toHaveBeenCalledTimes(1); // no further polls
  });

  it('keeps Classic (no provider) and Custom (tickertick) feeds in separate cache entries', async () => {
    // Same QueryClient + same tickers/cacheKey, differing only by provider. The
    // provider is part of the query key, so the two must NOT serve each other's
    // articles — the invariant a prior review flagged as critical.
    const Wrapper = makeWrapper();
    mockGetNews.mockImplementation(({ provider }: { provider?: string }) =>
      Promise.resolve({ results: [{ id: provider ?? 'classic', title: provider ?? 'classic' }] }),
    );
    const rows = rowsFor('AAPL');
    const classic = renderHook(() => useTickerNews(rows, 'portfolio'), { wrapper: Wrapper });
    const custom = renderHook(() => useTickerNews(rows, 'portfolio', 'tickertick'), { wrapper: Wrapper });

    await waitFor(() => expect(classic.result.current.loading).toBe(false));
    await waitFor(() => expect(custom.result.current.loading).toBe(false));

    expect(mockGetNews).toHaveBeenCalledWith({ tickers: ['AAPL'], limit: 50, provider: undefined });
    expect(mockGetNews).toHaveBeenCalledWith({ tickers: ['AAPL'], limit: 50, provider: 'tickertick' });
    expect(classic.result.current.items[0].id).toBe('classic');
    expect(custom.result.current.items[0].id).toBe('tickertick');
  });

  it('keys cache by cacheKey so portfolio and watchlist feeds stay separate', async () => {
    // Same provider + tickers but different cacheKey → two distinct cache
    // entries (two fetches), not deduped into one.
    const Wrapper = makeWrapper();
    const rows = rowsFor('AAPL');
    renderHook(() => useTickerNews(rows, 'portfolio', 'tickertick'), { wrapper: Wrapper });
    renderHook(() => useTickerNews(rows, 'watchlist', 'tickertick'), { wrapper: Wrapper });

    await waitFor(() => expect(mockGetNews).toHaveBeenCalledTimes(2));
  });

  it('maps raw results into the normalized item shape with field fallbacks', async () => {
    mockGetNews.mockResolvedValue({
      results: [{ id: '1', title: 'T', published_at: null, has_sentiment: true, author: null, source: undefined }],
    });
    const { result } = renderHook(() => useTickerNews(rowsFor('AAPL'), 'portfolio', 'tickertick'), {
      wrapper: makeWrapper(),
    });
    await waitFor(() => expect(result.current.items).toHaveLength(1));

    expect(result.current.items[0]).toMatchObject({
      id: '1', title: 'T', isHot: true, author: null,
      source: '', favicon: null, image: null, keywords: [], publishedAt: null, time: '',
    });
  });

  it('surfaces an empty list when the fetch yields no results', async () => {
    mockGetNews.mockResolvedValue({ results: [], count: 0, next_cursor: null });
    const { result } = renderHook(() => useTickerNews(rowsFor('AAPL'), 'portfolio', 'tickertick'), {
      wrapper: makeWrapper(),
    });
    await waitFor(() => expect(result.current.loading).toBe(false));

    expect(mockGetNews).toHaveBeenCalledTimes(1);
    expect(result.current.items).toEqual([]);
  });

  it('keeps the last good list when a poll fails (getNews rejects)', async () => {
    vi.useFakeTimers();
    mockGetNews
      .mockResolvedValueOnce({ results: [{ id: 'good', title: 'g' }] })
      .mockRejectedValue(new Error('boom'));
    const { result } = renderHook(() => useTickerNews(rowsFor('AAPL'), 'portfolio', 'tickertick'), {
      wrapper: makeWrapper(),
    });

    await act(async () => { await vi.advanceTimersByTimeAsync(0); });
    expect(result.current.items.map((i) => i.id)).toEqual(['good']);

    // 60s poll fails — the feed must NOT blank; React Query retains last data.
    await act(async () => { await vi.advanceTimersByTimeAsync(60000); });
    expect(mockGetNews).toHaveBeenCalledTimes(2);
    expect(result.current.items.map((i) => i.id)).toEqual(['good']);
  });
});
