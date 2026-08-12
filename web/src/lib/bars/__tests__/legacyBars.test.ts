import { beforeEach, describe, expect, it, vi } from 'vitest';

// Mock the axios client + supabase (imported at api.ts module scope).
const apiMock = vi.hoisted(() => ({ get: vi.fn(), defaults: { baseURL: '' } }));
vi.mock('@/api/client', () => ({ api: apiMock }));
vi.mock('@/lib/supabase', () => ({ supabase: null }));

import { fetchStockData } from '../legacyBars';

const bar = { time: Date.UTC(2025, 0, 2, 15, 30), open: 1, high: 2, low: 0.5, close: 1.5, volume: 100 };

beforeEach(() => {
  apiMock.get.mockReset();
});

describe('fetchStockData — metadata passthrough', () => {
  it('surfaces the previously-discarded envelope metadata', async () => {
    apiMock.get.mockResolvedValueOnce({
      data: {
        data: [bar],
        watermark: 1700000000000,
        complete: true,
        market_phase: 'regular',
        truncated: false,
        cached: true,
      },
    });

    const res = await fetchStockData('AAPL', '1min', undefined, undefined);

    expect(res.data).toHaveLength(1);
    expect(res.meta).toMatchObject({
      watermark: 1700000000000,
      complete: true,
      marketPhase: 'regular',
      truncated: false,
      cached: true,
    });
  });

  it('reads metadata from the nested cache block (the live wire shape)', async () => {
    apiMock.get.mockResolvedValueOnce({
      data: {
        symbol: 'AAPL',
        data: [bar],
        count: 1,
        cache: {
          cached: true,
          cache_key: 'ohlcv:AAPL.XNAS:ohlcv-1d',
          watermark: 1700000000000,
          complete: true,
          market_phase: 'closed',
          next_change_at: 1700050000000,
          truncated: false,
        },
      },
    });

    const res = await fetchStockData('AAPL', '1day', undefined, undefined);

    expect(res.meta).toMatchObject({
      watermark: 1700000000000,
      marketPhase: 'closed',
      nextChangeAt: 1700050000000,
      truncated: false,
      cached: true,
    });
  });

  it('defaults complete=true and marketPhase=null when the envelope omits them', async () => {
    apiMock.get.mockResolvedValueOnce({ data: { data: [bar] } });
    const res = await fetchStockData('AAPL', '1min', undefined, undefined);
    expect(res.meta?.watermark).toBeNull();
    expect(res.meta?.complete).toBe(true);
    expect(res.meta?.marketPhase).toBeNull();
  });

  it('coerces a string watermark and reads protocol currency fields', async () => {
    apiMock.get.mockResolvedValueOnce({
      data: { data: [bar], watermark: '1700000000001', price_currency: 'GBP', display_decimals: 4 },
    });
    const res = await fetchStockData('VOD.L', '1min', undefined, undefined);
    expect(res.meta?.watermark).toBe(1700000000001);
    expect(res.meta?.currency).toBe('GBP');
    expect(res.meta?.displayDecimals).toBe(4);
  });

  it('omits meta on the error path (no data)', async () => {
    apiMock.get.mockResolvedValueOnce({ data: { data: [] } });
    const res = await fetchStockData('AAPL', '1min', undefined, undefined);
    expect(res.error).toBeTruthy();
    expect(res.meta).toBeUndefined();
  });
});
