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

  it('keeps a sub-dollar stock at the places it was quoted, not cents', () => {
    const { realTimePrice } = mapSnapshotToStockQuote('XXII', {
      symbol: 'XXII', price: 0.0483, previous_close: 0.0741, change: -0.0258, asset_class: 'equity', currency: 'USD',
    });
    expect(realTimePrice).toMatchObject({ price: 0.0483, previousClose: 0.0741, change: -0.0258 });
  });
});
