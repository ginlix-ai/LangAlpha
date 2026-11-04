// @vitest-environment node
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { searchStocks, fetchMarketStatus, normalizeSymbolInput, readTypedTicker, sessionQuote } from '../marketUtils';

// Mock the api client
vi.mock('@/api/client', () => ({
  api: {
    get: vi.fn(),
  },
}));

import { api } from '@/api/client';

const mockGet = api.get as Mock;

describe('sessionQuote extended-hours rules', () => {
  const split = (status: Parameters<typeof sessionQuote>[0], session: Record<string, number>, opts: Parameters<typeof sessionQuote>[2] = {}) =>
    sessionQuote(status, { price: 1, change: 0, changePercent: 0, session }, opts);
  const pre = { market: 'open', afterHours: false, earlyHours: true };
  const closed = { market: 'closed', afterHours: false, earlyHours: false };

  it('splits nothing when marketStatus is null', () => {
    expect(split(null, { earlyTradingChangePercent: 1.5, previousClose: 100 }).ext).toBeNull();
  });

  it('returns pre-market info during early hours', () => {
    const { ext } = split(pre, { earlyTradingChangePercent: 2.5, previousClose: 100 });
    expect(ext).toMatchObject({ type: 'pre', label: 'Pre-Market', pct: 2.5 });
  });

  it('returns short label "PM" for pre-market when shortLabels is true', () => {
    const { ext } = split(pre, { earlyTradingChangePercent: 1.0, previousClose: 100 }, { shortLabels: true });
    expect(ext!.label).toBe('PM');
  });

  it('returns after-hours info when market is closed and latePct is available', () => {
    const { ext } = split(closed, { lateTradingChangePercent: -1.2, previousClose: 200, regularClose: 200 });
    expect(ext).toMatchObject({ type: 'post', label: 'After-Hours', pct: -1.2 });
  });

  it('returns short label "AH" for after-hours when shortLabels is true', () => {
    const { ext } = split(closed, { lateTradingChangePercent: 0.5, regularClose: 100 }, { shortLabels: true });
    expect(ext!.label).toBe('AH');
  });

  it('splits nothing during regular open market hours', () => {
    const open = { market: 'open', afterHours: false, earlyHours: false };
    const result = split(open, { earlyTradingChangePercent: 1.0, lateTradingChangePercent: -0.5, previousClose: 100, regularClose: 100 });
    expect(result).toEqual({ headline: { price: 1, change: 0, pct: 0 }, ext: null });
  });

  it('splits nothing after hours when the row has no regular close to anchor on', () => {
    const result = split(closed, { lateTradingChangePercent: 5.0, previousClose: 100 });
    expect(result).toEqual({ headline: { price: 1, change: 0, pct: 0 }, ext: null });
  });

  it('anchors the after-hours move on the regular close, not the previous close', () => {
    // latePct is declared against today's regular close; anchoring it on
    // prevClose was the wrong-basis bug this pins against.
    const { headline, ext } = split(closed, { late_trading_change_percent: -1.0, previous_close: 200, regular_trading_change: -10 });

    expect(headline.price).toBe(190);
    expect(ext!.price).toBeCloseTo(188.1, 6);
    expect(ext!.change).toBeCloseTo(-1.9, 6);
  });

  it('prefers the provider-exact regular_close over the rounded-change derivation', () => {
    // regular_trading_change is served at 1dp; prevClose + (-10) says 190 but
    // the exact close is 189.96 — the exact field must win.
    const { headline, ext } = split(closed, {
      late_trading_change_percent: -1.0,
      previous_close: 200,
      regular_trading_change: -10,
      regular_close: 189.96,
    });

    expect(headline.price).toBe(189.96);
    expect(ext!.price - ext!.change!).toBeCloseTo(189.96, 6);
  });

  it('prefers the exact dollar extended change over the rounded percent', () => {
    const { ext } = split(closed, {
      late_trading_change_percent: -1.1,
      late_trading_change: -1.06,
      previous_close: 200,
      regular_close: 189.96,
    });

    expect(ext!.change).toBe(-1.06);
    expect(ext!.price).toBe(188.9);
    expect(ext!.pct).toBe(-1.1);
  });

  it('prefers the minute-aggregate close and re-derives the whole triple from it', () => {
    // The provider's late change tracks the raw last trade (odd lots
    // included); the aggregate close is the consolidated last sale that the
    // chart shows — it must win, with change and pct recomputed against the
    // anchor so the line is one coherent statement.
    const { ext } = split(closed, {
      late_trading_change_percent: -1.1,
      late_trading_change: -1.06,
      last_minute_close: 189.2,
      previous_close: 200,
      regular_close: 189.96,
    });

    expect(ext!.price).toBe(189.2);
    expect(ext!.change).toBeCloseTo(-0.76, 6);
    expect(ext!.pct).toBeCloseTo((-0.76 / 189.96) * 100, 6);
  });

  it('keeps a sub-dollar extended move unrounded, so its percent is the true one', () => {
    const { ext } = split(closed, { late_trading_change_percent: 2.1, last_minute_close: 0.0493, previous_close: 0.0741, regular_close: 0.0483 });

    expect(ext!.change).toBeCloseTo(0.001, 6);
    expect(ext!.pct).toBeCloseTo(2.07, 2);
  });

  it('anchors the pre-market move on the previous close even when regular_trading_change is present', () => {
    const { headline, ext } = split(pre, { earlyTradingChangePercent: 2.0, previousClose: 100, regularTradingChange: -5 });

    expect(headline.price).toBe(100);
    expect(ext!.price).toBe(102);
    expect(ext!.change).toBe(2);
  });

  it('handles snake_case field names (raw snapshot data)', () => {
    const { headline, ext } = split(pre, { early_trading_change_percent: 3.0, previous_close: 50 });

    expect(ext).toMatchObject({ pct: 3.0, label: 'Pre-Market' });
    expect(headline.price).toBe(50);
  });

  it('splits nothing when the extended price has no close to measure from', () => {
    expect(split(pre, { earlyTradingChangePercent: 2.0 }).ext).toBeNull();
  });
});

describe('searchStocks', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('returns empty result for empty query', async () => {
    const result = await searchStocks('');
    expect(result).toEqual({ query: '', results: [], count: 0 });
    expect(mockGet).not.toHaveBeenCalled();
  });

  it('returns empty result for whitespace-only query', async () => {
    const result = await searchStocks('   ');
    expect(result).toEqual({ query: '', results: [], count: 0 });
    expect(mockGet).not.toHaveBeenCalled();
  });

  it('calls the API with trimmed query and returns data', async () => {
    const mockData = { query: 'AAPL', results: [{ symbol: 'AAPL' }], count: 1 };
    mockGet.mockResolvedValue({ data: mockData });

    const result = await searchStocks(' AAPL ');
    expect(mockGet).toHaveBeenCalledWith('/api/v1/market-data/search/stocks', {
      params: expect.any(URLSearchParams),
    });
    expect(result).toEqual(mockData);
  });

  it('clamps limit between 1 and 100', async () => {
    mockGet.mockResolvedValue({ data: { query: 'X', results: [], count: 0 } });

    await searchStocks('X', 200);
    const params = mockGet.mock.calls[0][1].params as URLSearchParams;
    expect(params.get('limit')).toBe('100');
  });

  it('returns fallback on API failure', async () => {
    mockGet.mockRejectedValue(new Error('Network error'));

    const result = await searchStocks('AAPL');
    expect(result).toEqual({ query: 'AAPL', results: [], count: 0 });
  });
});

describe('fetchMarketStatus', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('returns data from the API', async () => {
    const mockData = { market: 'open', afterHours: false, earlyHours: false };
    mockGet.mockResolvedValue({ data: mockData });

    const result = await fetchMarketStatus();
    expect(mockGet).toHaveBeenCalledWith('/api/v1/market-data/market-status', { signal: undefined });
    expect(result).toEqual(mockData);
  });

  it('returns empty object on API failure', async () => {
    mockGet.mockRejectedValue(new Error('Server error'));

    const result = await fetchMarketStatus();
    expect(result).toEqual({});
  });

  it('re-throws AbortError (CanceledError)', async () => {
    const err = new Error('canceled');
    err.name = 'CanceledError';
    mockGet.mockRejectedValue(err);

    await expect(fetchMarketStatus()).rejects.toThrow();
  });
});

describe('symbol entry', () => {
  it('hands over the display spelling, so a typed or saved .SS opens as .SH', () => {
    expect(normalizeSymbolInput(' 600519.ss ')).toBe('600519.SH');
    expect(normalizeSymbolInput('   ')).toBeNull();
    expect(readTypedTicker('600519.SS')).toBe('600519.SH');
    expect(readTypedTicker('nvda')).toBe('NVDA');
  });

  it('reads a code typed through a Chinese IME as a ticker', () => {
    expect(readTypedTicker('６００５１９。ＳＨ')).toBe('600519.SH');
  });
});

describe('sessionQuote', () => {
  // An after-hours row as the snapshot sends it: the blended change folds the
  // after-hours move into the day's, so a row that printed it beside the AH
  // line counted that move twice.
  const ahRow = {
    previous_close: 338.32, regular_close: 331.9, regular_trading_change: -6.42,
    last_minute_close: 330.4, late_trading_change_percent: -0.449,
  };
  const blended = { price: 331.9, change: -7.91, changePercent: -2.34 };
  const afterHours = { market: 'extended-hours', afterHours: true, earlyHours: false };

  it('after hours, headlines the regular close and its change, with the move off it apart', () => {
    const { headline, ext } = sessionQuote(afterHours, { ...blended, session: ahRow });
    expect(headline.price).toBe(331.9);
    expect(headline.change).toBeCloseTo(-6.42, 6);
    expect(headline.pct).toBeCloseTo((-6.42 / 338.32) * 100, 6);
    expect(ext).toMatchObject({ type: 'post', price: 330.4 });
    expect(ext!.pct).toBeCloseTo(((330.4 - 331.9) / 331.9) * 100, 6);
  });

  it('a live print stands in for the extended price, measured off the same close', () => {
    const { headline, ext } = sessionQuote(afterHours, { ...blended, session: ahRow }, { livePrice: 330.27 });
    expect(headline.price).toBe(331.9);
    expect(ext!.price).toBe(330.27);
    expect(ext!.pct).toBeCloseTo(((330.27 - 331.9) / 331.9) * 100, 6);
  });

  it('before the open, headlines the previous close with no change yet', () => {
    const pre = { market: 'extended-hours', afterHours: false, earlyHours: true };
    const row = { previous_close: 100, early_trading_change_percent: 2, last_minute_close: 102 };
    const { headline, ext } = sessionQuote(pre, { price: 102, change: 2, changePercent: 2, session: row });
    expect(headline).toEqual({ price: 100, change: null, pct: null });
    expect(ext).toMatchObject({ type: 'pre', price: 102 });
  });

  it('splits nothing while the session is unknown, or before the open without an early move', () => {
    const asIs = { headline: { price: 331.9, change: -7.91, pct: -2.34 }, ext: null };
    expect(sessionQuote(null, { ...blended, session: ahRow })).toEqual(asIs);
    const pre = { market: 'extended-hours', afterHours: false, earlyHours: true };
    expect(sessionQuote(pre, { ...blended, session: ahRow })).toEqual(asIs);
  });

  it('in the regular session, keeps the row figures as they are', () => {
    const open = { market: 'open', afterHours: false, earlyHours: false };
    expect(sessionQuote(open, { ...blended, session: ahRow })).toEqual({ headline: { price: 331.9, change: -7.91, pct: -2.34 }, ext: null });
  });
});
