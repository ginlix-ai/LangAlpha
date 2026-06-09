import { describe, expect, it } from 'vitest';
import {
  normalizePortfolioCurrency,
  summarizePortfolioByCurrency,
} from '../portfolioSummary';
import {
  formatPortfolioNavMarkdownLine,
  portfolioSummary,
} from '../../widgets/definitions/_holdingsHelpers';

describe('portfolioSummary', () => {
  it('keeps portfolio NAV and P/L separated by holding currency', () => {
    const summaries = summarizePortfolioByCurrency([
      { currency: 'USD', marketValue: 120, average_cost: 10, quantity: 10 },
      { currency: 'hkd', marketValue: 800, average_cost: 70, quantity: 10 },
      { currency: 'USD', marketValue: 55, average_cost: 50, quantity: 1 },
    ]);

    expect(summaries).toEqual([
      {
        currency: 'USD',
        totalValue: 175,
        totalCost: 150,
        totalPl: 25,
        totalPlPct: 16.666666666666664,
        isPlPositive: true,
      },
      {
        currency: 'HKD',
        totalValue: 800,
        totalCost: 700,
        totalPl: 100,
        totalPlPct: 14.285714285714285,
        isPlPositive: true,
      },
    ]);
  });

  it('defaults missing or invalid currencies to USD', () => {
    expect(normalizePortfolioCurrency(undefined)).toBe('USD');
    expect(normalizePortfolioCurrency('')).toBe('USD');
    expect(normalizePortfolioCurrency('usd')).toBe('USD');

    const summaries = summarizePortfolioByCurrency([
      { marketValue: 20, average_cost: 10, quantity: 1 },
      { currency: 'bad-code', marketValue: 30, average_cost: 15, quantity: 1 },
    ]);

    expect(summaries).toHaveLength(1);
    expect(summaries[0].currency).toBe('USD');
    expect(summaries[0].totalValue).toBe(50);
  });

  it('formats widget NAV markdown as one line per currency', () => {
    const summaries = portfolioSummary([
      { symbol: 'AAPL', price: 12, currency: 'USD', marketValue: 120, average_cost: 10, quantity: 10 },
      { symbol: '0700.HK', price: 80, currency: 'HKD', marketValue: 800, average_cost: 70, quantity: 10 },
    ]);

    expect(formatPortfolioNavMarkdownLine(summaries)).toBe(
      [
        '**NAV (USD)** USD 120.00 (cost USD 100.00, P/L +USD 20.00 / +20.00%)',
        '**NAV (HKD)** HKD 800.00 (cost HKD 700.00, P/L +HKD 100.00 / +14.29%)',
      ].join('\n'),
    );
  });
});
