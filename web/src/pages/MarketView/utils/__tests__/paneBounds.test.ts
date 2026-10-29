import { describe, expect, it } from 'vitest';

import { clampToPricePane, isOnPricePane, pricePaneElement, pricePaneHeight } from '../paneBounds';

const chartWith = (heights: number[]) => ({
  panes: () => heights.map((h) => ({ getHeight: () => h })),
});

describe('pricePaneHeight', () => {
  it('reads pane 0, not the whole container', () => {
    expect(pricePaneHeight(chartWith([320, 80]) as never)).toBe(320);
  });

  it('reports an unknown height as 0 rather than guessing from the host', () => {
    expect(pricePaneHeight(null)).toBe(0);
    expect(pricePaneHeight(chartWith([]) as never)).toBe(0);
    expect(pricePaneHeight(chartWith([0]) as never)).toBe(0);
  });

  it('reports 0 when the chart is disposed', () => {
    const disposed = { panes: () => { throw new Error('disposed'); } };
    expect(pricePaneHeight(disposed as never)).toBe(0);
  });
});

describe('pricePaneElement', () => {
  it('prefers the pane canvas over its table row', () => {
    const row = document.createElement('tr');
    const canvas = document.createElement('canvas');
    row.appendChild(document.createElement('td')).appendChild(canvas);
    const chart = { panes: () => [{ getHTMLElement: () => row }] };
    expect(pricePaneElement(chart as never)).toBe(canvas);
  });

  it('is null without a laid-out pane or once disposed', () => {
    expect(pricePaneElement(null)).toBeNull();
    expect(pricePaneElement({ panes: () => [{ getHTMLElement: () => null }] } as never)).toBeNull();
    expect(pricePaneElement({ panes: () => { throw new Error('disposed'); } } as never)).toBeNull();
  });
});

describe('clampToPricePane / isOnPricePane', () => {
  it('keeps a y inside the price pane', () => {
    expect(clampToPricePane(150, 320)).toBe(150);
    expect(clampToPricePane(-10, 320)).toBe(0);
    expect(clampToPricePane(390, 320)).toBe(320);
  });

  it('treats an unknown height as off the pane', () => {
    expect(isOnPricePane(390, 0)).toBe(false);
    expect(isOnPricePane(10, 0)).toBe(false);
  });

  it('flags a y that starts on the RSI pane', () => {
    expect(isOnPricePane(320, 320)).toBe(true);
    expect(isOnPricePane(321, 320)).toBe(false);
    expect(isOnPricePane(-1, 320)).toBe(false);
  });
});
