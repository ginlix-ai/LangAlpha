import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { Mock } from 'vitest';
import { waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '../../../../test/utils';
import { usePortfolioData } from '../usePortfolioData';

vi.mock('@/components/ui/use-toast', () => ({
  useToast: () => ({ toast: vi.fn() }),
}));

vi.mock('../../utils/api', () => ({
  addPortfolioHolding: vi.fn(),
  deletePortfolioHolding: vi.fn(),
  getPortfolio: vi.fn(),
  updatePortfolioHolding: vi.fn(),
}));

// Quotes flow through the shared quote layer, whose batcher hits the snapshot
// primitives in lib/quotes — mock those (not getStockPrices) to feed useQuotes.
vi.mock('@/lib/quotes/snapshotApi', () => ({
  getSnapshotStocks: vi.fn(),
  getSnapshotIndexes: vi.fn(),
}));

import { getPortfolio } from '../../utils/api';
import { getSnapshotStocks } from '@/lib/quotes/snapshotApi';

const mockGetPortfolio = getPortfolio as Mock;
const mockGetSnapshotStocks = getSnapshotStocks as Mock;

describe('usePortfolioData', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('marks missing quotes unavailable and skips fake NAV math', async () => {
    mockGetPortfolio.mockResolvedValue({
      holdings: [
        {
          user_portfolio_id: '1',
          symbol: 'AAPL',
          quantity: 2,
          average_cost: 10,
          currency: 'USD',
        },
        {
          user_portfolio_id: '2',
          symbol: 'MSFT',
          quantity: 3,
          average_cost: 20,
          currency: 'USD',
        },
      ],
    });
    // Raw snapshot rows — the quote layer's batcher hits getSnapshotStocks and
    // adapts rows via snapshotToStockPrice. MSFT is absent (dropped upstream).
    mockGetSnapshotStocks.mockResolvedValue({
      snapshots: [
        { symbol: 'AAPL', price: 12, change: 0.5, change_percent: 1.5 },
      ],
    });

    const { result } = renderHookWithProviders(() => usePortfolioData());

    await waitFor(() => expect(result.current.loading).toBe(false));

    expect(result.current.rows).toHaveLength(2);

    const priced = result.current.rows.find((row) => row.symbol === 'AAPL');
    const missing = result.current.rows.find((row) => row.symbol === 'MSFT');

    expect(priced).toMatchObject({
      price: 12,
      marketValue: 24,
      quoteAvailable: true,
    });
    expect(priced?.unrealizedPlPercent).toBeCloseTo(20);

    expect(missing).toMatchObject({
      price: 0,
      marketValue: null,
      quoteAvailable: false,
      unrealizedPlPercent: null,
    });
  });
});
