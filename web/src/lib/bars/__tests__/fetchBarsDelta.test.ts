import { beforeEach, describe, expect, it, vi } from 'vitest';

// Both the protocol client (fetchBarsSeries) and the legacy loader
// (fetchStockData) go through the shared axios client — mock it once and route
// by URL. Supabase is imported at api.ts module scope.
const apiMock = vi.hoisted(() => ({ get: vi.fn(), defaults: { baseURL: '' } }));
vi.mock('@/api/client', () => ({ api: apiMock }));
vi.mock('@/lib/supabase', () => ({ supabase: null }));

import { fetchBarsDelta } from '../chartDataLoaders';

const protocolResponse = {
  series: {
    header: {
      instrument_key: 'AAPL',
      schema: 'ohlcv-1m',
      price_currency: 'USD',
      display_decimals: 2,
      watermark: 1700000000000,
    },
    records: [{ time: Date.UTC(2025, 0, 2, 15, 30), open: 1, high: 2, low: 0.5, close: 1.5, volume: 100 }],
  },
  page: { next_cursor: null, has_more: false },
  cache: { cached: false, cache_key: null },
};

const legacyBar = { time: Date.UTC(2025, 0, 2, 15, 30), open: 1, high: 2, low: 0.5, close: 1.5, volume: 100 };

beforeEach(() => {
  apiMock.get.mockReset();
});

describe('fetchBarsDelta', () => {
  it('hits the protocol endpoint with after= and returns source=protocol', async () => {
    apiMock.get.mockResolvedValueOnce({ data: protocolResponse });

    const res = await fetchBarsDelta('AAPL', '1min', 1699999999999);

    expect(res.source).toBe('protocol');
    expect(res.bars).toHaveLength(1);
    expect(res.meta.watermark).toBe(1700000000000);
    expect(res.meta.currency).toBe('USD');

    const [url, cfg] = apiMock.get.mock.calls[0];
    expect(url).toBe('/api/v1/market-data/bars/AAPL');
    expect(cfg.params.schema).toBe('ohlcv-1m');
    expect(cfg.params.after).toBe('1699999999999');
  });

  it('falls back to one legacy full re-fetch on 404 (source=legacy)', async () => {
    apiMock.get
      .mockRejectedValueOnce({ response: { status: 404 } }) // protocol /bars/
      .mockResolvedValueOnce({ data: { data: [legacyBar], watermark: 1700000000050 } }); // legacy /intraday/

    const res = await fetchBarsDelta('AAPL', '1min', 1700000000000);

    expect(res.source).toBe('legacy');
    expect(res.bars).toHaveLength(1);
    expect(res.meta.watermark).toBe(1700000000050);
    expect(apiMock.get).toHaveBeenCalledTimes(2);
    expect(apiMock.get.mock.calls[0][0]).toContain('/bars/');
    expect(apiMock.get.mock.calls[1][0]).toContain('/intraday/');
  });

  it('maps 1day → ohlcv-1d and sends no after when watermark is null', async () => {
    apiMock.get.mockResolvedValueOnce({ data: protocolResponse });
    await fetchBarsDelta('AAPL', '1day', null);
    expect(apiMock.get.mock.calls[0][1].params.schema).toBe('ohlcv-1d');
    expect(apiMock.get.mock.calls[0][1].params.after).toBeUndefined();
  });
});
