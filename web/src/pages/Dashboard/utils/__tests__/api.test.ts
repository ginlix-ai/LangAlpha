import { beforeEach, describe, expect, it, vi } from 'vitest';

const apiMock = vi.hoisted(() => ({
  get: vi.fn(),
  post: vi.fn(),
  put: vi.fn(),
  delete: vi.fn(),
}));

vi.mock('@/api/client', () => ({ api: apiMock }));

import { getIndex, getIndices, getStockPrices } from '../api';

describe('getStockPrices', () => {
  beforeEach(() => {
    apiMock.get.mockReset();
  });

  it('marks symbols missing from the snapshot response as unavailable quotes', async () => {
    apiMock.get.mockResolvedValueOnce({
      data: {
        snapshots: [
          {
            symbol: 'AAPL',
            price: 190.12,
            change: 1.23,
            change_percent: 0.65,
          },
        ],
      },
    });

    const rows = await getStockPrices(['AAPL', '301189.SZ']);

    expect(rows[0]).toMatchObject({
      symbol: 'AAPL',
      price: 190.12,
      quoteAvailable: true,
    });
    expect(rows[1]).toMatchObject({
      symbol: '301189.SZ',
      price: 0,
      change: 0,
      changePercent: 0,
      quoteAvailable: false,
    });
  });

  it('marks all requested symbols unavailable when the snapshot request fails', async () => {
    apiMock.get.mockRejectedValueOnce(new Error('network'));

    const rows = await getStockPrices(['301189.SZ']);

    expect(rows).toEqual([
      {
        symbol: '301189.SZ',
        price: 0,
        change: 0,
        changePercent: 0,
        isPositive: true,
        quoteAvailable: false,
      },
    ]);
  });
});

// ET times expressed as UTC ms (Jan = EST = UTC-5).
const ET = (y: number, mo: number, d: number, h: number, mi: number) =>
  Date.UTC(y, mo, d, h + 5, mi);

describe('getIndex', () => {
  beforeEach(() => {
    apiMock.get.mockReset();
  });

  it('builds the sparkline from the most recent date that has regular-hours bars', async () => {
    // VIX shape: a complete prior session (D1) plus a later date (D2) carrying
    // only pre-market/overnight bars. The old code keyed off D2 (the
    // chronologically last bar) and filtered to regular hours -> empty.
    apiMock.get.mockResolvedValueOnce({
      data: {
        data: [
          // D1 (2025-01-13) — regular hours
          { time: ET(2025, 0, 13, 9, 30), open: 100, close: 100 },
          { time: ET(2025, 0, 13, 12, 0), open: 100, close: 102 },
          { time: ET(2025, 0, 13, 16, 0), open: 100, close: 101 },
          // D2 (2025-01-14) — pre-market only, before 09:30 ET
          { time: ET(2025, 0, 14, 4, 0), open: 99, close: 99 },
          { time: ET(2025, 0, 14, 8, 0), open: 99, close: 98 },
        ],
      },
    });

    const result = await getIndex('VIX');

    expect(result.sparklineData.map((p) => p.val)).toEqual([100, 102, 101]);
    expect(result.sparklineData[0].time).toBe('09:30');
    expect(result.sparklineData[result.sparklineData.length - 1].time).toBe('16:00');
    // price/change derive from the D1 regular-hours session, not the D2 bars.
    expect(result.price).toBe(101);
    // date label reflects the session the data belongs to, not "today".
    expect(result.asOfDate).toBe('2025-01-13');
    // a positive close is a real quote.
    expect(result.quoteAvailable).toBe(true);
  });

  it('marks the quote unavailable when the latest regular-hours close is non-positive', async () => {
    // The session's last regular-hours bar has a 0 close (a partial/bad bar);
    // price derives from it, so getIndex must flag the quote as unavailable for
    // direct consumers instead of returning a real-looking 0.00.
    apiMock.get.mockResolvedValueOnce({
      data: {
        data: [
          { time: ET(2025, 0, 13, 9, 30), open: 100, close: 100 },
          { time: ET(2025, 0, 13, 16, 0), open: 100, close: 0 },
        ],
      },
    });

    const result = await getIndex('VIX');

    expect(result.quoteAvailable).toBe(false);
    expect(result.price).toBe(0);
  });

  it('falls back to the chronologically-last bar when no regular-hours bars exist', async () => {
    // Every bar is pre-market/overnight (before 09:30 ET) across two dates, so
    // the regular-hours slice is empty and the selector falls back to the last
    // bar's date rather than blanking the card.
    apiMock.get.mockResolvedValueOnce({
      data: {
        data: [
          { time: ET(2025, 0, 13, 6, 0), open: 100, close: 100 },
          { time: ET(2025, 0, 14, 7, 0), open: 100, close: 105 },
          { time: ET(2025, 0, 14, 8, 0), open: 100, close: 110 },
        ],
      },
    });

    const result = await getIndex('VIX');

    expect(result.asOfDate).toBe('2025-01-14');
    expect(result.sparklineData.map((p) => p.val)).toEqual([105, 110]);
    expect(result.price).toBe(110);
  });
});

describe('getIndices', () => {
  beforeEach(() => {
    apiMock.get.mockReset();
  });

  it('flags quoteAvailable true when a snapshot is present and false when missing', async () => {
    apiMock.get.mockImplementation((url: string) => {
      if (url.includes('/snapshots/indexes')) {
        return Promise.resolve({
          data: {
            snapshots: [
              {
                symbol: 'GSPC',
                price: 5000.12,
                change: 10.5,
                change_percent: 0.21,
                previous_close: 4989.62,
              },
            ],
          },
        });
      }
      // intraday — no data; getIndex throws and getIndices falls back to []
      return Promise.resolve({ data: { data: [] } });
    });

    const { indices, failedCount } = await getIndices(['GSPC', 'VIX']);
    const gspc = indices.find((i) => i.symbol === 'GSPC')!;
    const vix = indices.find((i) => i.symbol === 'VIX')!;

    expect(gspc.quoteAvailable).toBe(true);
    expect(gspc.price).toBe(5000.12);
    expect(vix.quoteAvailable).toBe(false);
    expect(vix.price).toBe(0);
    expect(failedCount).toBe(1);
  });

  it('treats a zero-price index snapshot as no quote (N/A, not a fake 0.00)', async () => {
    // Index prices are never legitimately 0; a partial provider snapshot
    // (missing lastPrice coerced to 0) must not render as a real quote.
    apiMock.get.mockImplementation((url: string) => {
      if (url.includes('/snapshots/indexes')) {
        return Promise.resolve({
          data: {
            snapshots: [
              { symbol: 'GSPC', price: 0, change: 0, change_percent: 0, previous_close: 0 },
            ],
          },
        });
      }
      return Promise.resolve({ data: { data: [] } });
    });

    const { indices, failedCount } = await getIndices(['GSPC']);
    const gspc = indices.find((i) => i.symbol === 'GSPC')!;

    expect(gspc.quoteAvailable).toBe(false);
    expect(failedCount).toBe(1);
  });

  it('threads the intraday session asOfDate onto the index when the snapshot is present', async () => {
    apiMock.get.mockImplementation((url: string) => {
      if (url.includes('/snapshots/indexes')) {
        return Promise.resolve({
          data: {
            snapshots: [
              { symbol: 'GSPC', price: 5000, change: 1, change_percent: 0.1, previous_close: 4999 },
            ],
          },
        });
      }
      // intraday — a complete regular-hours session on 2025-01-13
      return Promise.resolve({
        data: {
          data: [
            { time: ET(2025, 0, 13, 9, 30), open: 100, close: 100 },
            { time: ET(2025, 0, 13, 16, 0), open: 100, close: 101 },
          ],
        },
      });
    });

    const { indices } = await getIndices(['GSPC']);
    const gspc = indices.find((i) => i.symbol === 'GSPC')!;

    expect(gspc.quoteAvailable).toBe(true);
    expect(gspc.asOfDate).toBe('2025-01-13');
  });
});
