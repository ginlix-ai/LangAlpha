import { describe, it, expect, vi } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router';

vi.mock('@/hooks/useIsMobile', () => ({ useIsMobile: () => true }));
vi.mock('@/pages/Dashboard/hooks/useWatchlistData', () => ({
  useWatchlistData: () => ({ rows: [], loading: false, modalOpen: false, setModalOpen: vi.fn(), handleAdd: vi.fn(), handleDelete: vi.fn(), currentWatchlistId: null }),
}));
vi.mock('@/pages/Dashboard/hooks/usePortfolioData', () => ({
  usePortfolioData: () => ({
    rows: [{ user_portfolio_id: 'p1', symbol: 'ACME', price: 100, unrealizedPlPercent: 20, isPositive: true, quoteAvailable: true, places: 2 }],
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

import MarketSidebarPanel from '../MarketSidebarPanel';

describe('MarketSidebarPanel portfolio rows', () => {
  it("colors a holding by its unrealized P/L, not the day's move", () => {
    render(
      <MemoryRouter>
        <MarketSidebarPanel activeSymbol={null} marketStatus={null} />
      </MemoryRouter>,
    );
    fireEvent.click(screen.getByRole('button', { name: 'Portfolio' }));

    const change = screen.getByText('+20.00%');
    expect(change).toHaveClass('market-sidebar-row-change--positive');
    expect(screen.getByText('120.00')).toBeInTheDocument();
  });
});
