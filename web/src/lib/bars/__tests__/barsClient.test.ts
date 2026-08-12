import { beforeEach, describe, expect, it, vi } from 'vitest';

// Mock the shared axios client at the module boundary (the repo pattern —
// there is no global fetch/network mock).
const apiMock = vi.hoisted(() => ({ get: vi.fn() }));
vi.mock('@/api/client', () => ({ api: apiMock }));

import { BarsNotAvailableError, fetchBarsSeries, headerToMeta, toChartBars } from '../barsClient';

const sampleResponse = {
  series: {
    header: {
      instrument_key: 'AAPL',
      schema: 'ohlcv-1m',
      price_currency: 'USD',
      display_decimals: 2,
      ts_unit: 'ms',
      watermark: 1700000000000,
      revision: 3,
    },
    records: [
      {
        time: Date.UTC(2025, 0, 2, 15, 30),
        ts_event: Date.UTC(2025, 0, 2, 15, 30),
        open: 1,
        high: 2,
        low: 0.5,
        close: 1.5,
        volume: 100,
        is_final: true,
      },
    ],
  },
  page: { next_cursor: null, has_more: false },
  cache: { cached: true, cache_key: 'k' },
};

beforeEach(() => {
  apiMock.get.mockReset();
});

describe('fetchBarsSeries', () => {
  it('parses the series envelope and forwards schema/after/asset_class params', async () => {
    apiMock.get.mockResolvedValueOnce({ data: sampleResponse });

    const res = await fetchBarsSeries('AAPL', 'ohlcv-1m', { after: 1700000000000, assetClass: 'index' });

    expect(res.series.records).toHaveLength(1);
    expect(res.page.has_more).toBe(false);
    expect(res.cache.cached).toBe(true);

    const [url, cfg] = apiMock.get.mock.calls[0];
    expect(url).toBe('/api/v1/market-data/bars/AAPL');
    expect(cfg.params).toEqual({ schema: 'ohlcv-1m', after: '1700000000000', asset_class: 'index' });
  });

  it('omits after/before/asset_class when not provided', async () => {
    apiMock.get.mockResolvedValueOnce({ data: sampleResponse });
    await fetchBarsSeries('AAPL', 'ohlcv-1d');
    expect(apiMock.get.mock.calls[0][1].params).toEqual({ schema: 'ohlcv-1d' });
  });

  it('throws BarsNotAvailableError on a 404 (endpoint not deployed)', async () => {
    apiMock.get.mockRejectedValueOnce({ response: { status: 404 } });
    await expect(fetchBarsSeries('AAPL', 'ohlcv-1m')).rejects.toBeInstanceOf(BarsNotAvailableError);
  });

  it('throws BarsNotAvailableError on a network error (no response)', async () => {
    apiMock.get.mockRejectedValueOnce(new Error('Network Error'));
    await expect(fetchBarsSeries('AAPL', 'ohlcv-1m')).rejects.toBeInstanceOf(BarsNotAvailableError);
  });

  it('treats a malformed body as unavailable', async () => {
    apiMock.get.mockResolvedValueOnce({ data: { nope: true } });
    await expect(fetchBarsSeries('AAPL', 'ohlcv-1m')).rejects.toBeInstanceOf(BarsNotAvailableError);
  });

  it('re-throws non-404 HTTP errors untouched (no false fallback)', async () => {
    const err = { response: { status: 500 } };
    apiMock.get.mockRejectedValueOnce(err);
    await expect(fetchBarsSeries('AAPL', 'ohlcv-1m')).rejects.toBe(err);
  });
});

describe('toChartBars', () => {
  const ET = 'America/New_York';

  it('maps records to chart bars, sorts and dedupes by time (first wins)', () => {
    const bars = toChartBars([
      { time: Date.UTC(2025, 0, 2, 15, 31), open: 2, high: 3, low: 1, close: 2.5, volume: 10 },
      { time: Date.UTC(2025, 0, 2, 15, 30), open: 1, high: 2, low: 0.5, close: 1.5, volume: 5 },
      { time: Date.UTC(2025, 0, 2, 15, 30), open: 9, high: 9, low: 9, close: 9, volume: 1 },
    ], ET);
    expect(bars).toHaveLength(2);
    expect(bars[0].time).toBeLessThan(bars[1].time);
    expect(bars[0].close).toBe(1.5);
  });

  it('returns [] for nullish input', () => {
    expect(toChartBars(undefined, ET)).toEqual([]);
    expect(toChartBars(null, ET)).toEqual([]);
  });

  it('falls back to ts_event when time is absent', () => {
    const bars = toChartBars([
      { ts_event: Date.UTC(2025, 0, 2, 15, 30), open: 1, high: 1, low: 1, close: 1, volume: 1 },
    ], ET);
    expect(bars).toHaveLength(1);
    expect(bars[0].time).toBeGreaterThan(0);
  });

  it('encodes bar times in the venue wall clock, not ET', () => {
    // 01:30 UTC = 09:30 HKT — the HK session open.
    const hkOpenUtc = Date.UTC(2025, 0, 2, 1, 30);
    const [bar] = toChartBars(
      [{ time: hkOpenUtc, open: 1, high: 1, low: 1, close: 1, volume: 1 }],
      'Asia/Hong_Kong',
    );
    // Chart fake-UTC must read back as the HK wall clock: Jan 2, 09:30.
    expect(new Date(bar.time * 1000).toISOString()).toBe('2025-01-02T09:30:00.000Z');
  });

  it('keeps a daily bar anchored at venue midnight on its own trading date', () => {
    // The off-by-one regression pin: a daily bar stamped midnight-venue-local
    // (16:00 UTC the previous day for HKT) must chart under its trading date.
    const hkMidnightUtc = Date.UTC(2025, 6, 2, 16, 0); // = 2025-07-03 00:00 HKT
    const [bar] = toChartBars(
      [{ time: hkMidnightUtc, open: 1, high: 1, low: 1, close: 1, volume: 1 }],
      'Asia/Hong_Kong',
    );
    expect(new Date(bar.time * 1000).toISOString().slice(0, 10)).toBe('2025-07-03');
  });
});

describe('headerToMeta', () => {
  it('normalizes header + cache into LoaderMeta', () => {
    expect(headerToMeta(sampleResponse.series.header, sampleResponse.cache)).toMatchObject({
      watermark: 1700000000000,
      currency: 'USD',
      displayDecimals: 2,
      revision: 3,
      cached: true,
    });
  });

  it('coerces a string watermark and tolerates a missing header', () => {
    expect(headerToMeta({ watermark: '123' }).watermark).toBe(123);
    expect(headerToMeta(null).watermark).toBeNull();
  });

  it('reads the market phase from the cache block (the /bars wire location)', () => {
    const meta = headerToMeta({ watermark: 1 }, { cached: false, cache_key: null, market_phase: 'closed' });
    expect(meta.marketPhase).toBe('closed');
    expect(headerToMeta({ watermark: 1 }, { cached: false, cache_key: null }).marketPhase).toBeNull();
  });

  it('reads next_change_at from the cache block, null when absent or non-numeric', () => {
    const cache = { cached: false, cache_key: null, next_change_at: 1_800_000_000_000 };
    expect(headerToMeta({ watermark: 1 }, cache).nextChangeAt).toBe(1_800_000_000_000);
    expect(headerToMeta({ watermark: 1 }, { cached: false, cache_key: null }).nextChangeAt).toBeNull();
    expect(
      headerToMeta({ watermark: 1 }, { cached: false, cache_key: null, next_change_at: null }).nextChangeAt,
    ).toBeNull();
  });
});
