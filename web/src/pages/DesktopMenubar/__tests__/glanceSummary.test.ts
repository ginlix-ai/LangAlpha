// @vitest-environment node
import { describe, expect, it } from 'vitest';
import {
  minutesLeftInPhase,
  signedPercent,
  summarizePortfolioDay,
  type HoldingQuote,
} from '../glanceSummary';

const h = (symbol: string, quantity: number, price: number | null, change: number | null, currency = 'USD'): HoldingQuote => ({
  symbol,
  quantity,
  price,
  change,
  changePercent: null,
  currency,
});

describe('summarizePortfolioDay', () => {
  it('sums quantity times the day change, against yesterday’s value', () => {
    const day = summarizePortfolioDay([h('AAPL', 10, 110, 10), h('MSFT', 5, 200, -4)]);
    expect(day?.currency).toBe('USD');
    expect(day?.totalValue).toBe(2100);
    expect(day?.dayChange).toBe(80);
    // Yesterday: 2100 - 80 = 2020.
    expect(day?.dayChangePct).toBeCloseTo((80 / 2020) * 100, 10);
  });

  it('ranks the primary currency’s holdings by value, with weights', () => {
    const day = summarizePortfolioDay([h('A', 1, 100, 0), h('B', 3, 100, 0), h('C', 1, 200, 0)]);
    expect(day?.holdings.map((r) => r.symbol)).toEqual(['B', 'C', 'A']);
    expect(day?.holdings[0].weight).toBe(50);
  });

  it('keeps dollars primary and names the other currencies without adding them', () => {
    const day = summarizePortfolioDay([
      h('7203.T', 100, 3000, 30, 'JPY'),
      h('AAPL', 1, 100, 1),
      h('SAP.DE', 1, 200, 2, 'EUR'),
    ]);
    expect(day?.currency).toBe('USD');
    expect(day?.totalValue).toBe(100);
    expect(day?.dayChange).toBe(1);
    expect(day?.otherCurrencies).toEqual(['EUR', 'JPY']);
  });

  it('leaves out holdings with no quote, and is null when none has one', () => {
    expect(summarizePortfolioDay([h('X', 1, null, null)])).toBeNull();
    expect(summarizePortfolioDay([])).toBeNull();
    const day = summarizePortfolioDay([h('X', 1, null, null), h('Y', 2, 50, 1)]);
    expect(day?.holdings.map((r) => r.symbol)).toEqual(['Y']);
  });

  it('has no day move before the open, when no holding has moved yet', () => {
    const day = summarizePortfolioDay([h('A', 1, 100, null)]);
    expect(day?.totalValue).toBe(100);
    expect(day?.dayChange).toBeNull();
    expect(day?.dayChangePct).toBeNull();
  });

  it('sums the moves there are, leaving out a holding with none yet', () => {
    const day = summarizePortfolioDay([h('A', 1, 100, 10), h('B', 1, 100, null)]);
    expect(day?.dayChange).toBe(10);
  });

  it('keeps the extended move apart from the day, against the official value', () => {
    const ext = (change: number) => ({ label: 'AH', price: 0, change, pct: 0 });
    const day = summarizePortfolioDay([
      { ...h('A', 2, 100, 5), ext: ext(1) },
      { ...h('B', 1, 200, -2), ext: null },
    ]);
    // The day is the official session's alone; the AH move is 2 x 1 over 400.
    expect(day?.dayChange).toBe(8);
    expect(day?.ext).toEqual({ label: 'AH', pct: 0.5 });
  });

  it('has no extended move outside an extended session', () => {
    expect(summarizePortfolioDay([h('A', 1, 100, 1)])?.ext).toBeNull();
  });
});

describe('signedPercent', () => {
  it('signs with a hyphen-minus', () => {
    expect(signedPercent(-0.5)).toBe('-0.50%');
  });
});

describe('minutesLeftInPhase', () => {
  it('counts down to the close, the open, and the end of after hours', () => {
    expect(minutesLeftInPhase('open', '14:46')).toBe(74);
    expect(minutesLeftInPhase('pre', '08:00')).toBe(90);
    expect(minutesLeftInPhase('after', '19:30')).toBe(30);
  });

  it('is null when closed, past the end, or unreadable', () => {
    expect(minutesLeftInPhase('closed', '12:00')).toBeNull();
    expect(minutesLeftInPhase('open', '16:05')).toBeNull();
    expect(minutesLeftInPhase('open', 'soon')).toBeNull();
  });

  it('reads 24:00 as midnight', () => {
    expect(minutesLeftInPhase('pre', '24:00')).toBe(570);
  });
});
