import { describe, expect, it } from 'vitest';

import { portfolioSummary } from '../_holdingsHelpers';

describe('portfolioSummary', () => {
  it('excludes holdings without available quotes from NAV and P/L math', () => {
    const summaries = portfolioSummary([
      {
        symbol: 'AAPL',
        price: 12,
        quantity: 10,
        average_cost: 10,
        currency: 'USD',
        marketValue: 120,
        quoteAvailable: true,
      },
      {
        symbol: '301189.SZ',
        price: 0,
        quantity: 10,
        average_cost: 30,
        currency: 'USD',
        marketValue: null,
        quoteAvailable: false,
      },
    ]);
    const summary = summaries[0];

    expect(summaries).toHaveLength(1);
    expect(summary.currency).toBe('USD');
    expect(summary.totalValue).toBe(120);
    expect(summary.totalCost).toBe(100);
    expect(summary.totalPl).toBe(20);
    expect(summary.totalPlPct).toBe(20);
  });

  it('reports when no holdings have available quotes', () => {
    const summaries = portfolioSummary([
      {
        symbol: '301189.SZ',
        price: 0,
        quantity: 10,
        average_cost: 30,
        currency: 'USD',
        marketValue: null,
        quoteAvailable: false,
      },
    ]);

    expect(summaries).toEqual([]);
  });
});
