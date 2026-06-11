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
  getStockPrices: vi.fn(),
  updatePortfolioHolding: vi.fn(),
}));

import { getPortfolio, getStockPrices } from '../../utils/api';

const mockGetPortfolio = getPortfolio as Mock;
const mockGetStockPrices = getStockPrices as Mock;

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
    mockGetStockPrices.mockResolvedValue([
      {
        symbol: 'AAPL',
        price: 12,
        change: 0.5,
        changePercent: 1.5,
        isPositive: true,
      },
    ]);

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
