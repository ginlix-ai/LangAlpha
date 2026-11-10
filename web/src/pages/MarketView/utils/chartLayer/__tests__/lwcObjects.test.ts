import { describe, expect, it, vi } from 'vitest';
import type { IChartApi } from 'lightweight-charts';

import {
  beginPass,
  emptyLayerObjects,
  reconcileFills,
  reconcileFuture,
  reconcilePriceLines,
  reconcileRegimes,
  reconcileSeries,
  removeLayerObjects,
} from '../lwcObjects';
import { BUILTIN_RSI, type LayerChartPlan } from '../chartPlan';
import type { SceneRegime } from '../scene';

/** The slice of lightweight-charts the reconcilers touch: panes that record their primitives. */
function fakeChart() {
  const pane = (index: number) => {
    const attached = new Set<unknown>();
    let stretch = 1;
    return {
      index,
      attached,
      attachPrimitive: vi.fn((p: unknown) => attached.add(p)),
      detachPrimitive: vi.fn((p: unknown) => attached.delete(p)),
      paneIndex: () => index,
      getStretchFactor: () => stretch,
      setStretchFactor: vi.fn((f: number) => {
        stretch = f;
      }),
      setPreserveEmptyPane: vi.fn(),
      getSeries: () => [],
    };
  };
  const panes = [pane(0), pane(1)];
  const series: { api: Record<string, unknown>; pane: number }[] = [];
  const chart = {
    panes: () => panes,
    chartElement: () => document.createElement('div'),
    addSeries: vi.fn((_def: unknown, _opts: unknown, paneIndex = 0) => {
      while (panes.length <= paneIndex) panes.push(pane(panes.length));
      const api = {
        setData: vi.fn(),
        applyOptions: vi.fn(),
        priceScale: () => ({ applyOptions: vi.fn() }),
        attachPrimitive: vi.fn(),
        detachPrimitive: vi.fn(),
        createPriceLine: vi.fn(),
        removePriceLine: vi.fn(),
      };
      series.push({ api, pane: paneIndex });
      return api;
    }),
    removeSeries: vi.fn(),
    removePane: vi.fn(),
  };
  return { chart: chart as unknown as IChartApi, raw: chart, panes, series };
}

const regime: SceneRegime = {
  key: 'lyr|regime|0',
  id: { layerKey: 'lyr', kind: 'regime', idx: 0 },
  color: '#26a69a',
  i0: 0,
  i1: 5,
  fill: 'rgba(38,166,154,0.08)',
};

describe('removeLayerObjects', () => {
  const plan: LayerChartPlan = {
    series: new Map([
      ['lyr|series|rsi', { s: { id: 'rsi', title: 'RSI', pane: 'rs', type: 'line', data: [] }, paneKey: BUILTIN_RSI, elKey: 'lyr|series|rsi', data: [] }],
      ['lyr|series|osc', { s: { id: 'osc', title: 'Osc', pane: 'o', type: 'line', data: [] }, paneKey: 'lyr|pane|o', elKey: 'lyr|series|osc', data: [] }],
    ]),
    panes: new Map([['lyr|pane|o', { title: 'Osc', height: 0.2, entries: [] }]]),
    axisLines: [],
    fills: [],
    notes: [],
  };

  it('takes back everything a pass attached, regimes on the RSI pane and pane captions included', () => {
    const { chart, raw, panes, series } = fakeChart();
    const live = emptyLayerObjects();
    beginPass(live, chart);
    reconcileSeries(chart, live, plan, { neutral: '#44f', muted: '#888' });
    reconcileRegimes(chart, live, [regime], null);
    reconcileFuture(chart, live, [1, 2, 3]);
    reconcileFills(live, []);
    reconcilePriceLines(chart, live, { createPriceLine: vi.fn(), removePriceLine: vi.fn() }, []);

    // The RSI pane and the layer's own pane each carry a regime shading; the layer pane its caption too.
    expect(panes.flatMap((p) => [...p.attached])).toHaveLength(3);
    expect(live.regimes.size).toBe(panes.length - 1);

    removeLayerObjects(live);

    expect(panes.flatMap((p) => [...p.attached])).toHaveLength(0);
    expect(live.regimes.size).toBe(0);
    expect(live.series.size).toBe(0);
    expect(live.panes.size).toBe(0);
    expect(live.future).toBeNull();
    expect(raw.removeSeries).toHaveBeenCalledTimes(series.length);
    expect(raw.removePane).toHaveBeenCalledWith(2);
  });

  it('tolerates a chart that is already gone', () => {
    const { chart, raw } = fakeChart();
    const live = emptyLayerObjects();
    beginPass(live, chart);
    reconcileSeries(chart, live, plan, { neutral: '#44f', muted: '#888' });
    reconcileRegimes(chart, live, [regime], null);
    raw.removeSeries.mockImplementation(() => {
      throw new Error('Object is disposed');
    });
    raw.removePane.mockImplementation(() => {
      throw new Error('Object is disposed');
    });
    expect(() => removeLayerObjects(live)).not.toThrow();
    expect(live.regimes.size).toBe(0);
  });

  it('does nothing before a chart was ever reconciled', () => {
    expect(() => removeLayerObjects(emptyLayerObjects())).not.toThrow();
  });
});

describe('reconcileSeries panes', () => {
  const planWithHeight = (height: number): LayerChartPlan => ({
    series: new Map([
      ['lyr|series|osc', { s: { id: 'osc', title: 'Osc', pane: 'o', type: 'line', data: [] }, paneKey: 'lyr|pane|o', elKey: 'lyr|series|osc', data: [] }],
    ]),
    panes: new Map([['lyr|pane|o', { title: 'Osc', height, entries: [] }]]),
    axisLines: [],
    fills: [],
    notes: [],
  });
  const colors = { neutral: '#44f', muted: '#888' };

  it('re-sizes a kept pane only when the layer changes its height', () => {
    const { chart, panes } = fakeChart();
    const live = emptyLayerObjects();
    beginPass(live, chart);
    reconcileSeries(chart, live, planWithHeight(0.2), colors);
    const pane = panes[2];
    expect(pane.setStretchFactor).toHaveBeenCalledTimes(1);

    // An update that leaves the height alone keeps whatever size the user dragged it to.
    reconcileSeries(chart, live, planWithHeight(0.2), colors);
    expect(pane.setStretchFactor).toHaveBeenCalledTimes(1);

    reconcileSeries(chart, live, planWithHeight(0.4), colors);
    expect(pane.setStretchFactor).toHaveBeenCalledTimes(2);
    // 0.4 of the chart against the two other panes' stretch of 1 each.
    expect(pane.setStretchFactor).toHaveBeenLastCalledWith((0.4 / 0.6) * 2);
  });

  it('keeps a layer pane while its only series is replaced by one of another type', () => {
    // lightweight-charts drops an empty pane unless told to keep it.
    const { chart, panes } = fakeChart();
    const live = emptyLayerObjects();
    beginPass(live, chart);
    reconcileSeries(chart, live, planWithHeight(0.2), colors);
    expect(panes[2].setPreserveEmptyPane).toHaveBeenCalledWith(true);
  });

  it('keeps every layer pane at its own share as panes open and close', () => {
    const two = (b: boolean): LayerChartPlan => {
      const plan = planWithHeight(0.2);
      if (!b) return plan;
      return {
        ...plan,
        series: new Map([
          ...plan.series,
          ['lyr|series|vol', { s: { id: 'vol', title: 'Vol', pane: 'v', type: 'line', data: [] }, paneKey: 'lyr|pane|v', elKey: 'lyr|series|vol', data: [] }],
        ]),
        panes: new Map([...plan.panes, ['lyr|pane|v', { title: 'Vol', height: 0.3, entries: [] }]]),
      };
    };
    const { chart, panes } = fakeChart();
    const live = emptyLayerObjects();
    const shares = () => {
      const total = panes.reduce((sum, p) => sum + p.getStretchFactor(), 0);
      return panes.slice(2).map((p) => p.getStretchFactor() / total);
    };
    beginPass(live, chart);
    reconcileSeries(chart, live, two(false), colors);
    expect(shares()[0]).toBeCloseTo(0.2, 9);
    // A second pane opening leaves the first at its 20%.
    reconcileSeries(chart, live, two(true), colors);
    const [first, second] = shares();
    expect(first).toBeCloseTo(0.2, 9);
    expect(second).toBeCloseTo(0.3, 9);
  });
});
