import { describe, it, expect, vi } from 'vitest';
import { fireEvent, render, screen, within } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

vi.mock('@/lib/marketUtils', async (importActual) => ({
  ...(await importActual<typeof import('@/lib/marketUtils')>()),
  marketStatusQuery: () => ({ queryKey: ['test-market-status'], queryFn: async () => null }),
}));
vi.mock('@/pages/Dashboard/hooks/useWatchlistData', () => ({
  useWatchlistData: () => ({ rows: [], loading: false, modalOpen: false, setModalOpen: vi.fn(), handleAdd: vi.fn(), handleDelete: vi.fn(), currentWatchlistId: null }),
}));
vi.mock('@/pages/Dashboard/hooks/usePortfolioData', () => ({
  usePortfolioData: () => ({
    rows: [{
      user_portfolio_id: 'p1', symbol: 'ACME', quantity: 10, average_cost: 100, currency: 'USD', costCurrency: 'USD',
      price: 100, change: 0, changePercent: 0, quoteAvailable: true, places: 2,
    }],
    loading: false,
    modalOpen: false,
    setModalOpen: vi.fn(),
    handleAdd: vi.fn(),
    handleDelete: vi.fn(),
  }),
}));
// A down day on the socket for a holding that is up since it was bought.
vi.mock('@/pages/MarketView/contexts/MarketDataWSContext', () => ({
  useMarketDataWSContext: () => ({
    prices: new Map([['ACME', { symbol: 'ACME', price: 120, change: -3, changePercent: -2.44 }]]),
    connectionStatus: 'connected',
    subscribe: vi.fn(),
    unsubscribe: vi.fn(),
  }),
}));
vi.mock('@/pages/Dashboard/components/AddWatchlistItemDialog', () => ({ default: () => null }));
vi.mock('@/pages/Dashboard/components/AddPortfolioHoldingDialog', () => ({ default: () => null }));
vi.mock('@/pages/Dashboard/components/ConfirmDialog', () => ({ default: () => null }));

import { QuoteList } from '../QuoteList';

describe('QuoteList holdings rows', () => {
  it("shows the day's move and the unrealized P/L apart, each in its own tone, valued at the live price", () => {
    render(
      <QueryClientProvider client={new QueryClient()}>
        <QuoteList activeSymbol={null} onPick={vi.fn()} />
      </QueryClientProvider>,
    );
    fireEvent.click(screen.getByRole('button', { name: /Holdings/ }));

    const row = screen.getByRole('button', { name: /^ACME/ });
    expect(within(row).getByText('-2.44%')).toHaveClass('quote-list-down');
    expect(within(row).getByText('+20.00%')).toHaveClass('quote-list-up');
    expect(within(row).getByText('$1,200')).toBeInTheDocument();
  });
});
