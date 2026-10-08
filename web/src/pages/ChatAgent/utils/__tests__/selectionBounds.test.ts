// @vitest-environment node
import { describe, expect, it } from 'vitest';

import { selectionPriceBounds } from '../selectionBounds';

const region = { selectionType: 'region' as const, priceLow: 180, priceHigh: 195 };

describe('selectionPriceBounds', () => {
  it('joins a region with a plain hyphen, never an en or em dash', () => {
    const bounds = selectionPriceBounds({ ...region, symbol: 'NVDA' }, 'en-US');
    expect(bounds).toBe('$180.00 - $195.00');
    expect(bounds).not.toMatch(/[–—]/);
  });

  it('prints a price level once', () => {
    expect(selectionPriceBounds({ selectionType: 'price_level', symbol: 'NVDA', priceLow: 200, priceHigh: 200 }, 'en-US'))
      .toBe('$200.00');
  });

  it('prices a listing in its venue currency and an index bare', () => {
    expect(selectionPriceBounds({ ...region, symbol: '0700.HK' }, 'en-US')).toBe('HK$180.00 - HK$195.00');
    expect(selectionPriceBounds({ ...region, symbol: '^GSPC', priceLow: 5000, priceHigh: 5100 }, 'en-US'))
      .toBe('5,000.00 - 5,100.00');
    expect(selectionPriceBounds({ ...region, symbol: '000300.SH', priceLow: 4000, priceHigh: 4100 }, 'en-US'))
      .toBe('4,000.00 - 4,100.00');
  });
});
