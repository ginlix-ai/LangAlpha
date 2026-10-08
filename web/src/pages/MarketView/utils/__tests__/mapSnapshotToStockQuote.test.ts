import { describe, it, expect } from 'vitest';
import { mapSnapshotToStockQuote } from '../api';

describe('mapSnapshotToStockQuote precision', () => {
  it('keeps a CN fund at its 3-decimal tick', () => {
    const { realTimePrice } = mapSnapshotToStockQuote('510300.SH', {
      symbol: '510300.SH', price: 3.912, open: 3.9, high: 3.925, low: 3.891,
      previous_close: 3.898, change: 0.014, asset_class: 'fund', currency: 'CNY',
    });
    expect(realTimePrice).toMatchObject({
      price: 3.912, high: 3.925, low: 3.891, previousClose: 3.898, change: 0.014,
    });
  });

  it('still rounds a stock to cents', () => {
    const { realTimePrice } = mapSnapshotToStockQuote('AAPL', {
      symbol: 'AAPL', price: 189.456, asset_class: 'equity', currency: 'USD',
    });
    expect(realTimePrice?.price).toBe(189.46);
  });
});
