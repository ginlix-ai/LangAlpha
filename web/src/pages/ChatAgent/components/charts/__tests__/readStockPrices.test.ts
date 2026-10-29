/**
 * The detail chart credits the series it draws: the tool may fill the intraday
 * bars from another publisher at another delay than the daily ones the card
 * draws, and says so in `chart_source` / `chart_freshness`.
 */
import { describe, expect, it } from 'vitest';
import { readStockPrices } from '../cardReaders';

const daily = { label: 'live', measured: true, lag_s: 0, source: 'daily-vendor' };
const intraday = { label: 'delayed', measured: true, lag_s: 900, source: 'intraday-vendor' };
const OHLCV = [
  { date: '2026-07-13', close: 100 },
  { date: '2026-07-14', close: 101 },
];
const CHART = [
  { date: '2026-07-14 10:00:00', close: 100.5 },
  { date: '2026-07-14 11:00:00', close: 101 },
];

describe('readStockPrices chart provenance', () => {
  it('credits the intraday series when the chart draws it', () => {
    const prices = readStockPrices({
      symbol: 'ACME', ohlcv: OHLCV, chart_ohlcv: CHART, chart_interval: '1hour',
      source: 'daily-vendor', freshness: daily, chart_source: 'intraday-vendor', chart_freshness: intraday,
    });
    expect(prices.chartSource).toBe('intraday-vendor');
    expect(prices.chartFreshness?.label).toBe('delayed');
    expect(prices.freshness?.label).toBe('live');
  });

  it('falls back to the daily series only when the chart draws it', () => {
    expect(readStockPrices({ symbol: 'ACME', ohlcv: OHLCV, source: 'daily-vendor', freshness: daily }).chartFreshness?.label)
      .toBe('live');
    expect(readStockPrices({ symbol: 'ACME', ohlcv: OHLCV, chart_ohlcv: CHART, freshness: daily }).chartFreshness)
      .toBeNull();
  });
});
