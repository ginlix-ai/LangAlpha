import { describe, expect, it } from 'vitest';
import { COLUMNS, listSession, nextSort, quoteRow, sortRows, type QuoteListRow } from '../quoteRows';

const row = (symbol: string, pct: number | null, ext: QuoteListRow['ext'] = null): QuoteListRow => ({
  id: symbol, symbol, price: 1, pct, ext, close: { price: 1, pct: null }, places: 2, currency: 'USD', value: null, plPct: null,
});

describe('quote list sorting', () => {
  it('cycles a figure column largest first, then smallest first, then back to the user order', () => {
    const first = nextSort(null, 'pct');
    expect(first).toEqual({ key: 'pct', dir: 'desc' });
    const second = nextSort(first, 'pct');
    expect(second).toEqual({ key: 'pct', dir: 'asc' });
    expect(nextSort(second, 'pct')).toBeNull();
    expect(nextSort(null, 'symbol')).toEqual({ key: 'symbol', dir: 'asc' });
  });

  it('puts a row with no figure last whichever way the column runs', () => {
    const rows = [row('A', null), row('B', 2), row('C', -1)];
    const order = (dir: 'asc' | 'desc') => sortRows(rows, { key: 'pct', dir }, COLUMNS.watchlist).map((r) => r.symbol);
    expect(order('desc')).toEqual(['B', 'C', 'A']);
    expect(order('asc')).toEqual(['C', 'B', 'A']);
  });
});

describe('quote list session', () => {
  it("follows the rows' own extended prints, not the clock", () => {
    expect(listSession([row('A', 1), row('B', 1, 'post')])).toBe('post');
    expect(listSession([row('A', 1, 'pre')])).toBe('pre');
    expect(listSession([row('A', 1)])).toBeNull();
  });
});

describe('quote list row', () => {
  it("re-bases an after-hours move on the previous close, so it is the day's whole move", () => {
    // Closed at 92 from a previous 100 (-8%), then 92.46 after hours (+0.5% on the close).
    const r = quoteRow(
      { symbol: 'ACME', price: 92, change: -8, changePercent: -8, places: 2, session: { previousClose: 100, regularClose: 92, lateTradingChangePercent: 0.5, lastMinuteClose: 92.46 } },
      'h1',
      { market: 'closed', afterHours: true },
      undefined,
    );
    expect(r.ext).toBe('post');
    expect(r.price).toBeCloseTo(92.46);
    expect(r.pct).toBeCloseTo(-7.54);
    expect(r.close.pct).toBeCloseTo(-8);
  });

  it('shows no move after hours when there is no previous close to re-base on', () => {
    const r = quoteRow(
      { symbol: 'ACME', price: 92, change: null, changePercent: null, places: 2, session: { regularClose: 92, lateTradingChangePercent: 0.5, lastMinuteClose: 92.46 } },
      'h1',
      { market: 'closed', afterHours: true },
      undefined,
    );
    expect(r.ext).toBe('post');
    expect(r.pct).toBeNull();
  });
});
