// @vitest-environment node
import { beforeEach, describe, expect, it, vi } from 'vitest';

// Mock the API client + supabase (imported at api.ts module scope).
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

  it('reads the body-level currency the CMDP boundary stamps next to the bars', async () => {
    // `currency` rides the body, not the cache block; a caret index like ^HSI
    // has no suffix, so this is the only way the chart learns it is HKD.
    apiMock.get.mockResolvedValueOnce({
      data: {
        symbol: 'HSI',
        data: [bar],
        count: 1,
        currency: 'HKD',
        timezone: 'Asia/Hong_Kong',
        cache: { cached: true, watermark: 1700000000000, complete: true, market_phase: 'closed' },
      },
    });

    const res = await fetchStockData('^HSI', '1day', undefined, undefined);
    expect(res.meta?.currency).toBe('HKD');
  });

  it('reads the body-level adjustment basis, so a live chart can tell a rebuilt series', async () => {
    apiMock.get.mockResolvedValueOnce({
      data: {
        symbol: 'AAPL',
        data: [bar],
        price_treatment: 'split_adjusted',
        cache: { cached: true, revision: 3 },
      },
    });
    const res = await fetchStockData('AAPL', '1min', undefined, undefined);
    expect(res.meta?.priceTreatment).toBe('split_adjusted');
    expect(res.meta?.revision).toBe(3);
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

  it('answers no bars with an empty series and no meta, not an error', async () => {
    apiMock.get.mockResolvedValueOnce({ data: { data: [] } });
    const res = await fetchStockData('AAPL', '1min', undefined, undefined);
    expect(res).toEqual({ data: [] });
  });

  it('keeps a body with no bars array a failure, so it is never cached as an empty series', async () => {
    apiMock.get.mockResolvedValueOnce({ data: '<!doctype html><html></html>' });
    const page = await fetchStockData('AAPL', '1min', undefined, undefined);
    expect(page.data).toEqual([]);
    expect(page.error).toBeTruthy();

    apiMock.get.mockResolvedValueOnce({ data: { detail: 'upstream' } });
    const noBars = await fetchStockData('AAPL', '1day', undefined, undefined);
    expect(noBars.error).toBeTruthy();
  });
});
