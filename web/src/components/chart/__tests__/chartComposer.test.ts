// @vitest-environment node
import { describe, expect, it, vi } from 'vitest';

import { createChartComposer } from '../chartComposer';
import type { ChartView } from '@/pages/MarketView/types/chartView';

const REQ = { symbol: 'NVDA', timeframe: '1day', layerId: 'a', layerTitle: 'Channel' };

describe('createChartComposer', () => {
  it('refuses an ask with no handler, and hands one to the handler registered', () => {
    const composer = createChartComposer();
    expect(composer.ask(REQ)).toBe(false);
    const handler = vi.fn();
    const off = composer.setAskHandler(handler);
    expect(composer.ask(REQ)).toBe(true);
    expect(handler).toHaveBeenCalledWith(REQ);
    off();
    expect(composer.ask(REQ)).toBe(false);
  });

  it("does not let an earlier handler's unregister drop the one that replaced it", () => {
    const composer = createChartComposer();
    const offFirst = composer.setAskHandler(vi.fn());
    const second = vi.fn();
    composer.setAskHandler(second);
    offFirst();
    expect(composer.ask(REQ)).toBe(true);
    expect(second).toHaveBeenCalledOnce();
  });

  it('collects the views of the charts registered, skipping one with nothing to report or mid-teardown', () => {
    const composer = createChartComposer();
    const view = { symbol: 'NVDA' } as unknown as ChartView;
    composer.registerView(() => view);
    composer.registerView(() => null);
    composer.registerView(() => {
      throw new Error('disposed');
    });
    const off = composer.registerView(() => view);
    off();
    expect(composer.collectViews()).toEqual([view]);
  });
});
