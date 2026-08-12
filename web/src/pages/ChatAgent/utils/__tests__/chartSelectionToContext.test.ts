import { describe, expect, it } from 'vitest';

import { chartSelectionToContext, describeSelectionImage } from '../fileUpload';
import type { ChartSelection } from '@/pages/MarketView/stores/chartSelectionStore';

function region(overrides: Partial<ChartSelection> = {}): ChartSelection {
  return {
    id: 'sel-1',
    symbol: 'NVDA',
    timeframe: '1day',
    selectionType: 'region',
    timeStart: '2024-01-03T00:00:00.000Z',
    timeEnd: '2024-02-15T00:00:00.000Z',
    priceLow: 180,
    priceHigh: 195,
    bars: [
      { time: '2024-01-03T00:00:00.000Z', open: 181, high: 186, low: 180, close: 185, volume: 1000 },
    ],
    barsTruncated: false,
    comment: '',
    status: 'confirmed',
    ...overrides,
  };
}

describe('chartSelectionToContext', () => {
  it('emits a single structured region item with time bounds + bars', () => {
    const out = chartSelectionToContext(region());
    expect(out).toHaveLength(1);
    const item = out[0] as unknown as Record<string, unknown>;
    expect(item.type).toBe('chart_selection');
    expect(item.selection_type).toBe('region');
    expect(item.time_start).toBe('2024-01-03T00:00:00.000Z');
    expect(item.time_end).toBe('2024-02-15T00:00:00.000Z');
    expect(item.price_low).toBe(180);
    expect(item.price_high).toBe(195);
    expect((item.bars as unknown[]).length).toBe(1);
    expect(item.bars_truncated).toBe(false);
  });

  it('omits the image item when the region carries no cropped screenshot', () => {
    const out = chartSelectionToContext(region({ comment: 'note' }));
    expect(out).toHaveLength(1);
    expect((out[0] as unknown as Record<string, unknown>).type).toBe('chart_selection');
    expect(out.every((i) => (i as unknown as Record<string, unknown>).type !== 'image')).toBe(true);
  });

  it('emits a sibling image item when the region carries a cropped screenshot', () => {
    const out = chartSelectionToContext(region({ croppedImage: 'data:image/jpeg;base64,AAAA' }));
    expect(out).toHaveLength(2);
    expect((out[0] as unknown as Record<string, unknown>).type).toBe('chart_selection');
    const img = out[1] as unknown as Record<string, unknown>;
    expect(img.type).toBe('image');
    expect(img.data).toBe('data:image/jpeg;base64,AAAA');
    expect(typeof img.description).toBe('string');
    expect(img.description as string).toContain('NVDA');
  });

  it('carries a trimmed comment as the item label', () => {
    const out = chartSelectionToContext(region({ comment: '  resistance retest  ' }));
    const item = out[0] as unknown as Record<string, unknown>;
    expect(item.label).toBe('resistance retest');
  });

  it('omits the label when the comment is blank', () => {
    const item = chartSelectionToContext(region({ comment: '   ' }))[0] as unknown as Record<string, unknown>;
    expect(item.label).toBeUndefined();
  });

  it('omits time bounds for a price level and carries no bars', () => {
    const sel = region({
      selectionType: 'price_level',
      timeStart: undefined,
      timeEnd: undefined,
      priceLow: 188.5,
      priceHigh: 188.5,
      bars: [],
    });
    const out = chartSelectionToContext(sel);
    expect(out).toHaveLength(1);
    const item = out[0] as unknown as Record<string, unknown>;
    expect(item.selection_type).toBe('price_level');
    expect(item.time_start).toBeUndefined();
    expect(item.time_end).toBeUndefined();
    expect(item.price_low).toBe(188.5);
  });

  it('drops a stale selection when the live chart no longer matches', () => {
    expect(chartSelectionToContext(region(), { symbol: 'AAPL', timeframe: '1day' })).toEqual([]);
    expect(chartSelectionToContext(region(), { symbol: 'NVDA', timeframe: '1hour' })).toEqual([]);
  });

  it('passes through when the live chart still matches (case-insensitive symbol)', () => {
    const out = chartSelectionToContext(region(), { symbol: 'nvda', timeframe: '1day' });
    expect(out).toHaveLength(1);
  });
});

describe('describeSelectionImage', () => {
  // This caption is the single source for both the image context `description`
  // and the display-attachment `name`, so its exact shape is worth locking.
  it('renders the full caption with price range and time span for a region', () => {
    expect(describeSelectionImage(region())).toBe(
      'Cropped chart image of NVDA 1day (price $180–$195, ' +
        '2024-01-03T00:00:00.000Z → 2024-02-15T00:00:00.000Z)',
    );
  });

  it('omits the time span for a price level (no time bounds)', () => {
    const cap = describeSelectionImage(
      region({ selectionType: 'price_level', timeStart: undefined, timeEnd: undefined, priceLow: 200, priceHigh: 200 }),
    );
    expect(cap).toBe('Cropped chart image of NVDA 1day (price $200–$200)');
    expect(cap).not.toContain('→');
  });
});
