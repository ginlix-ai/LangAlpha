/**
 * A watchlist mixes venues and indexes, so each row prices itself: in the
 * currency its quote came in, and an index level with no currency at all.
 */
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router';

import { WatchlistRowItem } from '../_holdingsPrimitives';
import type { WatchlistRow } from '../../../hooks/useWatchlistData';

function row(over: Partial<WatchlistRow>): WatchlistRow {
  return {
    watchlist_item_id: 'w1', symbol: 'ACME', price: 100, change: 1, changePercent: 1,
    isPositive: true, quoteAvailable: true, previousClose: 99,
    earlyTradingChangePercent: null, lateTradingChangePercent: null,
    currency: null, assetClass: null,
    ...over,
  };
}

function renderRow(item: WatchlistRow) {
  return render(
    <MemoryRouter>
      <WatchlistRowItem item={item} index={0} marketStatus={null} />
    </MemoryRouter>,
  );
}

describe('WatchlistRowItem price', () => {
  it('prints an index level bare, whatever currency its row names', () => {
    renderRow(row({ symbol: '^GSPC', price: 5000, currency: 'USD', assetClass: 'index' }));
    expect(screen.getByText('5,000.00')).toBeInTheDocument();
    expect(screen.queryByText(/\$/)).not.toBeInTheDocument();
  });

  it('reads an index from its code when the row carries no asset class', () => {
    renderRow(row({ symbol: '000300.SH', price: 4000, currency: 'CNY' }));
    expect(screen.getByText('4,000.00')).toBeInTheDocument();
    expect(screen.queryByText(/¥/)).not.toBeInTheDocument();
  });

  it('prices a listing in the currency its quote came in', () => {
    renderRow(row({ symbol: '600519.SH', price: 1500, currency: 'CNY', assetClass: 'stock' }));
    expect(screen.getByText('CN¥1,500.00')).toBeInTheDocument();
  });
});
