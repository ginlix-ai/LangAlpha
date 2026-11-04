// @vitest-environment node
import { describe, expect, it } from 'vitest';

import {
  portfolioRowData,
  portfolioSummary,
  serializePortfolioRowsToMarkdown,
  serializePortfolioRowToMarkdown,
} from '../_holdingsHelpers';
import type { PortfolioRow } from '../../../hooks/usePortfolioData';

describe('portfolioSummary', () => {
  it('excludes holdings without available quotes from NAV and P/L math', () => {
    const summaries = portfolioSummary([
      {
        symbol: 'AAPL',
        price: 12,
        quantity: 10,
        average_cost: 10,
        currency: 'USD',
        costCurrency: 'USD',
        places: 2,
        marketValue: 120,
        quoteAvailable: true,
      },
      {
        symbol: '301189.SZ',
        price: 0,
        quantity: 10,
        average_cost: 30,
        currency: 'USD',
        costCurrency: 'USD',
        places: 2,
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
        costCurrency: 'USD',
        places: 2,
        marketValue: null,
        quoteAvailable: false,
      },
    ]);

    expect(summaries).toEqual([]);
  });
});

const holding = (overrides: Partial<PortfolioRow>): PortfolioRow => ({
  symbol: 'AAPL',
  price: 331.9,
  quantity: 10,
  average_cost: 150,
  currency: 'USD',
  costCurrency: 'USD',
  places: 2,
  marketValue: 3319,
  unrealizedPlPercent: 121.27,
  quoteAvailable: true,
  ...overrides,
});

describe('portfolio serializers', () => {
  it('heads the since-purchase P/L as unrealized, beside what is held and its cost', () => {
    expect(
      serializePortfolioRowsToMarkdown([
        holding({}),
        holding({ symbol: 'NVDA', quantity: 5, average_cost: null, price: 206.69, marketValue: 1033.45, unrealizedPlPercent: null }),
        holding({ symbol: '0700.HK', currency: 'HKD', quantity: 100, average_cost: 50, price: 401.4, marketValue: 40140, unrealizedPlPercent: null }),
      ].map(portfolioRowData)),
    ).toBe(
      [
        '| symbol | currency | shares | avg cost | price | value | unrealized P/L% |',
        '| --- | --- | --- | --- | --- | --- | --- |',
        '| AAPL | USD | 10 | 150.00 | 331.90 | 3319.00 | +121.27% |',
        '| NVDA | USD | 5 |  | 206.69 | 1033.45 |  |',
        '| 0700.HK | HKD | 100 | 50.00 USD | 401.40 | 40140.00 |  |',
      ].join('\n'),
    );
  });

  it('prints a cost far under a cent to its significant digits, never as 0.00', () => {
    expect(serializePortfolioRowToMarkdown(holding({ symbol: 'TINY-USD', average_cost: 4.5e-11 }))).toContain('avg cost USD 0.000000000045');
  });

  it('keeps a sub-cent cost and a one row attach in the same terms', () => {
    expect(
      serializePortfolioRowToMarkdown(holding({ symbol: 'SHIB-USD', quantity: 50000000, average_cost: 0.00001234, price: 0.0000046, places: 4, marketValue: 230, unrealizedPlPercent: -62.72 })),
    ).toBe(
      ['**SHIB-USD**', 'shares 50000000', 'avg cost USD 0.00001234', 'price USD 0.0000046', 'value USD 230.00', 'unrealized P/L -62.72%'].join('\n'),
    );
  });
});
