import { beforeEach, describe, expect, it, vi } from 'vitest';

const apiMock = vi.hoisted(() => ({
  get: vi.fn(),
  post: vi.fn(),
  put: vi.fn(),
  delete: vi.fn(),
}));

vi.mock('@/api/client', () => ({ api: apiMock }));

import { getStockPrices } from '../api';

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
