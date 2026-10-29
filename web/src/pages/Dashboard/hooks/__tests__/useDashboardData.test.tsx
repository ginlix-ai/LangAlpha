import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { renderHookWithProviders } from '../../../../test/utils';
import { useDashboardData } from '../useDashboardData';
import { waitFor } from '@testing-library/react';

// Partial mock: real pure helpers (INDEX_SYMBOLS, normalizeIndexSymbol,
// buildIndexData, fallbackIndex) stay real; only network calls are mocked.
vi.mock('../../utils/api', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../utils/api')>();
  return {
    ...actual,
    getNews: vi.fn(),
    getIndex: vi.fn(),
  };
});

// Index quotes flow through the shared quote layer's batcher, which hits the
// snapshot primitives in lib/quotes — mock those to feed useQuotes.
vi.mock('@/lib/quotes/snapshotApi', () => ({
  getSnapshotIndexes: vi.fn(),
  getSnapshotStocks: vi.fn(),
}));

// Partial mock so the real normalizeIndexKey (used by the batcher + snapshotApi)
// survives; only fetchMarketStatus is stubbed.
vi.mock('@/lib/marketUtils', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/marketUtils')>()),
  fetchMarketStatus: vi.fn(),
}));

import { getNews, getIndex } from '../../utils/api';
import { getSnapshotIndexes } from '@/lib/quotes/snapshotApi';
import { fetchMarketStatus } from '@/lib/marketUtils';

const mockFetchMarketStatus = fetchMarketStatus as Mock;
const mockGetSnapshotIndexes = getSnapshotIndexes as Mock;
const mockGetIndex = getIndex as Mock;
const mockGetNews = getNews as Mock;

describe('useDashboardData', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockFetchMarketStatus.mockResolvedValue({ market: 'open', afterHours: false, earlyHours: false });
    mockGetSnapshotIndexes.mockResolvedValue({
      snapshots: [
        { symbol: 'GSPC', name: 'S&P 500', price: 5000, change: 50, change_percent: 1.0 },
      ],
    });
    mockGetIndex.mockResolvedValue({ sparklineData: [], asOfDate: undefined });
    mockGetNews.mockResolvedValue({ results: [], count: 0 });
  });

  it('returns marketStatus from the fetched data', async () => {
    const { result } = renderHookWithProviders(() => useDashboardData());

    await waitFor(() => expect(result.current.marketStatus).not.toBeNull());
    expect(result.current.marketStatus!.market).toBe('open');
  });

  it('returns indices data', async () => {
    const { result } = renderHookWithProviders(() => useDashboardData());

    await waitFor(() => expect(result.current.indices).toBeDefined());
    // Indices should eventually resolve (either from query or placeholderData)
    expect(Array.isArray(result.current.indices)).toBe(true);
  });

  it('returns newsItems as an empty array when no news', async () => {
    mockGetNews.mockResolvedValue({ results: [] });

    const { result } = renderHookWithProviders(() => useDashboardData());

    await waitFor(() => expect(result.current.newsLoading).toBe(false));
    expect(result.current.newsItems).toEqual([]);
  });

  it('transforms news results into formatted items', async () => {
    mockGetNews.mockResolvedValue({
      results: [
        {
          id: 'n-1',
          title: 'Markets rally',
          published_at: new Date().toISOString(),
          has_sentiment: true,
          source: { name: 'Reuters', favicon_url: 'https://favicon.com/r.ico' },
          image_url: 'https://img.com/1.jpg',
          tickers: ['AAPL'],
        },
      ],
      count: 1,
    });

    const { result } = renderHookWithProviders(() => useDashboardData());

    await waitFor(() => expect(result.current.newsItems.length).toBe(1));
    const item = result.current.newsItems[0];
    expect(item.id).toBe('n-1');
    expect(item.title).toBe('Markets rally');
    expect(item.source).toBe('Reuters');
    expect(item.isHot).toBe(true);
    expect(item.tickers).toEqual(['AAPL']);
  });

  it('market feed paginates by cursor, de-duping across pages', async () => {
    const row = (id: string) => ({
      id,
      title: `Story ${id}`,
      published_at: new Date().toISOString(),
      source: { name: 'Reuters' },
      tickers: [],
    });
    // getNews serves both the market and curated queries — dispatch on args.
    mockGetNews.mockImplementation(async ({ provider, cursor }: { provider?: string; cursor?: string }) => {
      if (provider === 'tickertick') return { results: [], count: 0, next_cursor: null };
      if (!cursor) return { results: [row('n-1'), row('n-2')], count: 2, next_cursor: 'n-2' };
      return { results: [row('n-2'), row('n-3')], count: 2, next_cursor: null };
    });

    const { result } = renderHookWithProviders(() => useDashboardData());

    await waitFor(() => expect(result.current.newsItems.length).toBe(2));
    expect(result.current.newsHasNextPage).toBe(true);

    result.current.newsFetchNextPage();

    // Page 2's duplicate n-2 (feed rotation) is dropped; n-3 appends.
    await waitFor(() => expect(result.current.newsItems.length).toBe(3));
    expect(result.current.newsItems.map((i) => i.id)).toEqual(['n-1', 'n-2', 'n-3']);
    expect(result.current.newsHasNextPage).toBe(false);
    expect(mockGetNews).toHaveBeenCalledWith(
      expect.objectContaining({ limit: 50, cursor: 'n-2' }),
    );
  });

  it('market feed offers no next page when the feed serves no cursor', async () => {
    mockGetNews.mockResolvedValue({ results: [], count: 0, next_cursor: null });

    const { result } = renderHookWithProviders(() => useDashboardData());

    await waitFor(() => expect(result.current.newsLoading).toBe(false));
    expect(result.current.newsHasNextPage).toBe(false);
  });

  it('provides a marketStatusRef for backward compatibility', async () => {
    const { result } = renderHookWithProviders(() => useDashboardData());

    await waitFor(() => expect(result.current.marketStatus).not.toBeNull());
    expect(result.current.marketStatusRef).toBeDefined();
    expect(result.current.marketStatusRef.current).toEqual(result.current.marketStatus);
  });
});
