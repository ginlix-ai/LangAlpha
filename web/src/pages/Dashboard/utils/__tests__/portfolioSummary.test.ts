// @vitest-environment node
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
  costCurrency: overrides.currency ?? 'USD',
  marketValue: 0,
  places: 2,
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
        uncosted: [],
      },
      {
        currency: 'HKD',
        totalValue: 800,
        totalCost: 700,
        totalPl: 100,
        totalPlPct: expect.any(Number),
        isPlPositive: true,
        uncosted: [],
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

    expect(formatPortfolioNavMarkdownLine(summaries, 'en-US')).toBe(
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

    expect(formatPortfolioNavMarkdownLine(summaries, 'en-US')).toBe('**NAV (USD)** USD 150.00');
  });

  it('formats negative P/L in NAV markdown with negative signs', () => {
    const summaries = portfolioSummary([
      createPortfolioRow({ symbol: 'LOSS', price: 8, currency: 'USD', marketValue: 80, average_cost: 10, quantity: 10 }),
    ]);

    expect(formatPortfolioNavMarkdownLine(summaries, 'en-US')).toBe(
      '**NAV (USD)** USD 80.00 (cost USD 100.00, P/L -USD 20.00 / -20.00%)',
    );
  });

  it('omits zero-value summaries from NAV markdown', () => {
    const summaries = portfolioSummary([
      createPortfolioRow({ symbol: 'EMPTY', price: 0, currency: 'USD', marketValue: 0, average_cost: 0, quantity: 0 }),
    ]);

    expect(formatPortfolioNavMarkdownLine(summaries, 'en-US')).toBe('');
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

  it('values a holding with no cost but leaves it out of the cost and P/L', () => {
    const [usd] = summarizePortfolioByCurrency([
      createPortfolioRow({ symbol: 'AAPL', marketValue: 3319, average_cost: 150, quantity: 10 }),
      createPortfolioRow({ symbol: 'NVDA', marketValue: 1033.45, average_cost: null, quantity: 5 }),
    ]);

    expect(usd.totalValue).toBeCloseTo(4352.45, 2);
    expect(usd.totalCost).toBe(1500);
    expect(usd.totalPl).toBe(1819);
    expect(usd.totalPlPct).toBeCloseTo(121.27, 2);
    expect(usd.uncosted).toEqual([{ symbol: 'NVDA', value: 1033.45 }]);
  });

  it('counts a holding entered at zero cost as all gain, not as having no cost', () => {
    const [usd] = summarizePortfolioByCurrency([
      createPortfolioRow({ symbol: 'AAPL', marketValue: 3319, average_cost: 150, quantity: 10 }),
      createPortfolioRow({ symbol: 'BONUS', marketValue: 150, average_cost: 0, quantity: 10 }),
    ]);

    expect(usd.totalCost).toBe(1500);
    expect(usd.totalPl).toBe(1969);
    expect(usd.uncosted).toEqual([]);
  });

  it('leaves out of the P/L a cost entered in another currency than the quote', () => {
    const [hkd] = summarizePortfolioByCurrency([
      createPortfolioRow({ symbol: '0700.HK', currency: 'HKD', marketValue: 80280, average_cost: 380, quantity: 200 }),
      createPortfolioRow({ symbol: '0700.HK', currency: 'HKD', costCurrency: 'USD', marketValue: 40140, average_cost: 50, quantity: 100 }),
    ]);

    expect(hkd.totalValue).toBe(120420);
    expect(hkd.totalCost).toBe(76000);
    expect(hkd.totalPl).toBe(4280);
    expect(hkd.uncosted).toEqual([{ symbol: '0700.HK', value: 40140 }]);
  });

  it('names the holdings a NAV line leaves out of its P/L', () => {
    const summaries = portfolioSummary([
      createPortfolioRow({ symbol: 'AAPL', marketValue: 3319, average_cost: 150, quantity: 10 }),
      createPortfolioRow({ symbol: 'NVDA', marketValue: 1033.45, average_cost: null, quantity: 5 }),
    ]);

    expect(formatPortfolioNavMarkdownLine(summaries, 'en-US')).toBe(
      '**NAV (USD)** USD 4352.45 (cost USD 1500.00, P/L +USD 1819.00 / +121.27%, excluding NVDA (USD 1033.45): no cost in USD)',
    );
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
