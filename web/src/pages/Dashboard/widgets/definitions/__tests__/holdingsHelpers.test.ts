import { describe, expect, it } from 'vitest';

import { portfolioSummary } from '../_holdingsHelpers';

describe('portfolioSummary', () => {
  it('excludes holdings without available quotes from NAV and P/L math', () => {
    const summary = portfolioSummary([
      {
        symbol: 'AAPL',
        price: 12,
        quantity: 10,
        average_cost: 10,
        marketValue: 120,
        quoteAvailable: true,
      },
      {
        symbol: '301189.SZ',
        price: 0,
        quantity: 10,
        average_cost: 30,
        marketValue: null,
        quoteAvailable: false,
      },
    ]);

    expect(summary.totalValue).toBe(120);
    expect(summary.totalCost).toBe(100);
    expect(summary.totalPl).toBe(20);
    expect(summary.totalPlPct).toBe(20);
    expect(summary.hasPricedRows).toBe(true);
  });

  it('reports when no holdings have available quotes', () => {
    const summary = portfolioSummary([
      {
        symbol: '301189.SZ',
        price: 0,
        quantity: 10,
        average_cost: 30,
        marketValue: null,
        quoteAvailable: false,
      },
    ]);

    expect(summary.totalValue).toBe(0);
    expect(summary.totalCost).toBe(0);
    expect(summary.totalPl).toBe(0);
    expect(summary.totalPlPct).toBe(0);
    expect(summary.hasPricedRows).toBe(false);
  });
});
