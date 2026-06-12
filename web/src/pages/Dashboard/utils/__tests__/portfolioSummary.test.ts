import { describe, expect, it } from 'vitest';
import {
  formatPortfolioMoney,
  normalizePortfolioCurrency,
  summarizePortfolioByCurrency,
} from '../portfolioSummary';
import {
  formatPortfolioNavMarkdownLine,
  portfolioSummary,
} from '../../widgets/definitions/_holdingsHelpers';
import { serializeQuoteRowsToMarkdown } from '../../widgets/framework/snapshotSerializers';
import type { PortfolioRow } from '../../hooks/usePortfolioData';

const createPortfolioRow = (overrides: Partial<PortfolioRow>): PortfolioRow => ({
  symbol: 'TEST',
  price: 0,
  quantity: 0,
  average_cost: 0,
  currency: 'USD',
  marketValue: 0,
  ...overrides,
});

describe('portfolioSummary', () => {
  it('keeps portfolio NAV and P/L separated by holding currency', () => {
    const summaries = summarizePortfolioByCurrency([
      createPortfolioRow({ currency: 'USD', marketValue: 120, average_cost: 10, quantity: 10 }),
      createPortfolioRow({ currency: 'hkd', marketValue: 800, average_cost: 70, quantity: 10 }),
      createPortfolioRow({ currency: 'USD', marketValue: 55, average_cost: 50, quantity: 1 }),
    ]);

    expect(summaries).toEqual([
      {
        currency: 'USD',
        totalValue: 175,
        totalCost: 150,
        totalPl: 25,
        totalPlPct: expect.any(Number),
        isPlPositive: true,
      },
      {
        currency: 'HKD',
        totalValue: 800,
        totalCost: 700,
        totalPl: 100,
        totalPlPct: expect.any(Number),
        isPlPositive: true,
      },
    ]);
    expect(summaries[0].totalPlPct).toBeCloseTo(16.67, 2);
    expect(summaries[1].totalPlPct).toBeCloseTo(14.29, 2);
  });

  it('defaults missing or invalid currencies to USD', () => {
    expect(normalizePortfolioCurrency(undefined)).toBe('USD');
    expect(normalizePortfolioCurrency('')).toBe('USD');
    expect(normalizePortfolioCurrency('usd')).toBe('USD');

    const summaries = summarizePortfolioByCurrency([
      createPortfolioRow({ currency: undefined, marketValue: 20, average_cost: 10, quantity: 1 }),
      createPortfolioRow({ currency: 'bad-code', marketValue: 30, average_cost: 15, quantity: 1 }),
    ]);

    expect(summaries).toHaveLength(1);
    expect(summaries[0].currency).toBe('USD');
    expect(summaries[0].totalValue).toBe(50);
  });

  it('formats widget NAV markdown as one line per currency', () => {
    const summaries = portfolioSummary([
      createPortfolioRow({ symbol: 'AAPL', price: 12, currency: 'USD', marketValue: 120, average_cost: 10, quantity: 10 }),
      createPortfolioRow({ symbol: '0700.HK', price: 80, currency: 'HKD', marketValue: 800, average_cost: 70, quantity: 10 }),
    ]);

    expect(formatPortfolioNavMarkdownLine(summaries)).toBe(
      [
        '**NAV (USD)** USD 120.00 (cost USD 100.00, P/L +USD 20.00 / +20.00%)',
        '**NAV (HKD)** HKD 800.00 (cost HKD 700.00, P/L +HKD 100.00 / +14.29%)',
      ].join('\n'),
    );
  });

  it('keeps zero-cost holdings visible in NAV markdown', () => {
    const summaries = portfolioSummary([
      createPortfolioRow({ symbol: 'BONUS', price: 15, currency: 'USD', marketValue: 150, average_cost: 0, quantity: 10 }),
    ]);

    expect(formatPortfolioNavMarkdownLine(summaries)).toBe('**NAV (USD)** USD 150.00');
  });

  it('formats negative P/L in NAV markdown with negative signs', () => {
    const summaries = portfolioSummary([
      createPortfolioRow({ symbol: 'LOSS', price: 8, currency: 'USD', marketValue: 80, average_cost: 10, quantity: 10 }),
    ]);

    expect(formatPortfolioNavMarkdownLine(summaries)).toBe(
      '**NAV (USD)** USD 80.00 (cost USD 100.00, P/L -USD 20.00 / -20.00%)',
    );
  });

  it('omits zero-value summaries from NAV markdown', () => {
    const summaries = portfolioSummary([
      createPortfolioRow({ symbol: 'EMPTY', price: 0, currency: 'USD', marketValue: 0, average_cost: 0, quantity: 0 }),
    ]);

    expect(formatPortfolioNavMarkdownLine(summaries)).toBe('');
  });

  it('excludes unavailable quotes from currency NAV and cost basis', () => {
    const summaries = summarizePortfolioByCurrency([
      createPortfolioRow({ symbol: 'AAPL', currency: 'USD', marketValue: 120, average_cost: 10, quantity: 10 }),
      createPortfolioRow({ symbol: '0700.HK', currency: 'HKD', marketValue: 800, average_cost: 70, quantity: 10 }),
      createPortfolioRow({ symbol: 'MISSING', currency: 'CNY', marketValue: null, average_cost: 50, quantity: 5, quoteAvailable: false }),
    ]);

    expect(summaries.map((summary) => summary.currency)).toEqual(['USD', 'HKD']);
    expect(summaries.find((summary) => summary.currency === 'CNY')).toBeUndefined();
  });

  it('formats portfolio money with the holding currency', () => {
    expect(formatPortfolioMoney(120, 'USD', 'en-US')).toBe('$120.00');
    expect(formatPortfolioMoney(800, 'HKD', 'en-US')).toBe('HK$800.00');
  });

  it('includes currency in serialized quote markdown rows', () => {
    expect(
      serializeQuoteRowsToMarkdown([
        { symbol: '0700.HK', currency: 'HKD', price: 800, changePercent: 1.2 },
      ]),
    ).toBe(
      [
        '| symbol | currency | price | change | change% |',
        '| --- | --- | --- | --- | --- |',
        '| 0700.HK | HKD | 800.00 |  | 1.20% |',
      ].join('\n'),
    );
  });
});
