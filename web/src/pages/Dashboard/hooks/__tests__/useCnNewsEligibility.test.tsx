import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '../../../../test/utils';
import { isCnNewsEligible } from '../useCnNewsEligibility';
import { useDashboardData } from '../useDashboardData';

vi.mock('../../utils/api', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../utils/api')>();
  return {
    ...actual,
    getNews: vi.fn(),
    getIndex: vi.fn(),
  };
});

vi.mock('@/lib/quotes/snapshotApi', () => ({
  getSnapshotIndexes: vi.fn(),
  getSnapshotStocks: vi.fn(),
}));

vi.mock('@/lib/marketUtils', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/marketUtils')>()),
  fetchMarketStatus: vi.fn(),
}));

// Eligibility is unit-tested via the pure function below; for the widget
// provider-selection test we mock the hook to isolate the wiring.
vi.mock('../useCnNewsEligibility', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../useCnNewsEligibility')>()),
  useCnNewsEligibility: vi.fn(),
}));

import { getNews, getIndex } from '../../utils/api';
import { getSnapshotIndexes } from '@/lib/quotes/snapshotApi';
import { fetchMarketStatus } from '@/lib/marketUtils';
import { useCnNewsEligibility } from '../useCnNewsEligibility';

const mockGetNews = getNews as Mock;
const mockEligible = useCnNewsEligibility as Mock;

describe('isCnNewsEligible', () => {
  it('requires both the a_share_pack feature and a zh locale', () => {
    expect(isCnNewsEligible('zh-CN', true)).toBe(true);
    expect(isCnNewsEligible('zh-TW', true)).toBe(true);
    expect(isCnNewsEligible('ZH-cn', true)).toBe(true);

    expect(isCnNewsEligible('en-US', true)).toBe(false);
    expect(isCnNewsEligible('zh-CN', false)).toBe(false);
    expect(isCnNewsEligible(null, true)).toBe(false);
    expect(isCnNewsEligible(undefined, false)).toBe(false);
  });
});

describe('useDashboardData news provider selection', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    (fetchMarketStatus as Mock).mockResolvedValue({ market: 'open' });
    (getSnapshotIndexes as Mock).mockResolvedValue({ snapshots: [] });
    (getIndex as Mock).mockResolvedValue({ sparklineData: [], asOfDate: undefined });
    mockGetNews.mockResolvedValue({ results: [], count: 0 });
  });

  it('targets provider=tushare for eligible users', async () => {
    mockEligible.mockReturnValue(true);

    const { result } = renderHookWithProviders(() => useDashboardData());
    await waitFor(() => expect(result.current.newsLoading).toBe(false));

    const calls = mockGetNews.mock.calls.map(([opts]) => opts);
    expect(calls).toContainEqual({ limit: 50, provider: 'tushare' });
  });

  it('fetches no feed while eligibility is undecided, then only the CN one', async () => {
    mockEligible.mockReturnValue(null);
    const feedCalls = () =>
      mockGetNews.mock.calls.map(([opts]) => opts).filter((o) => o.provider !== 'tickertick');

    const { result, rerender } = renderHookWithProviders(() => useDashboardData());
    await waitFor(() => expect(result.current.curatedLoading).toBe(false));
    expect(result.current.newsLoading).toBe(true);
    expect(feedCalls()).toEqual([]);

    mockEligible.mockReturnValue(true);
    rerender();
    await waitFor(() => expect(result.current.newsLoading).toBe(false));
    expect(feedCalls()).toEqual([{ limit: 50, provider: 'tushare' }]);
  });

  it('uses the default feed for ineligible users', async () => {
    mockEligible.mockReturnValue(false);

    const { result } = renderHookWithProviders(() => useDashboardData());
    await waitFor(() => expect(result.current.newsLoading).toBe(false));

    const calls = mockGetNews.mock.calls.map(([opts]) => opts);
    expect(calls).toContainEqual({ limit: 50, provider: undefined });
    expect(calls).not.toContainEqual({ limit: 50, provider: 'tushare' });
  });
});
