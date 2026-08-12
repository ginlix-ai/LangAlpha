import { describe, expect, it } from 'vitest';

import { createUserMessage } from '../messageHelpers';
import type { ChartSelectionSnapshot } from '@/pages/MarketView/stores/chartSelectionStore';

function snapshot(overrides: Partial<ChartSelectionSnapshot> = {}): ChartSelectionSnapshot {
  return {
    selectionType: 'region',
    symbol: 'NVDA',
    timeframe: '1day',
    priceLow: 180,
    priceHigh: 195,
    bars: [],
    barsTruncated: false,
    ...overrides,
  };
}

describe('createUserMessage — chartSelections', () => {
  it('attaches a non-empty chartSelections array', () => {
    const sels = [snapshot()];
    const msg = createUserMessage('hi', null, null, sels);
    expect(msg.chartSelections).toBe(sels);
    expect(msg.chartSelections).toHaveLength(1);
  });

  it('leaves chartSelections undefined for an empty array', () => {
    const msg = createUserMessage('hi', null, null, []);
    expect(msg.chartSelections).toBeUndefined();
  });

  it('leaves chartSelections undefined for null', () => {
    const msg = createUserMessage('hi', null, null, null);
    expect(msg.chartSelections).toBeUndefined();
  });

  it('leaves chartSelections undefined when the param is omitted', () => {
    const msg = createUserMessage('hi');
    expect(msg.chartSelections).toBeUndefined();
  });

  it('builds a well-formed user message regardless of selections', () => {
    const msg = createUserMessage('analyze this', null, null, [snapshot()]);
    expect(msg.role).toBe('user');
    expect(msg.content).toBe('analyze this');
    expect(msg.contentType).toBe('text');
  });
});
