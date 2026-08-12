import { act, renderHook } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';

import {
  applyAnnotationArtifact,
  chartAnnotationStore,
  DEFAULT_TIMEFRAME,
  makeChartId,
  normalizeTimeframe,
  subscribeLiveAnnotationAdd,
  useAnnotationsForView,
  useDisplayCleared,
  VALID_TIMEFRAMES,
  type ChartInstance,
  type FibRetracementAnnotation,
  type LiveAnnotationAdd,
  type PriceLineAnnotation,
  type RectangleAnnotation,
  type StoredAnnotation,
  type TextAnnotation,
  type TrendlineAnnotation,
  type VerticalLineAnnotation,
} from '../chartAnnotationStore';

const WS = '11111111-1111-1111-1111-111111111111';
const WS2 = '22222222-2222-2222-2222-222222222222';

const storeKey = (ws: string, chartId: string): string => `${ws}||${chartId}`;

function makePriceLine(
  id: string,
  symbol: string,
  price: number,
  timeframe = '1day',
): PriceLineAnnotation {
  return {
    annotation_id: id,
    symbol: symbol.toUpperCase(),
    timeframe,
    chart_id: makeChartId(symbol, timeframe),
    type: 'price_line',
    price,
  };
}

function makeTrendline(id: string, symbol: string): TrendlineAnnotation {
  return {
    annotation_id: id,
    symbol: symbol.toUpperCase(),
    type: 'trendline',
    point1: { time: '2024-10-01T00:00:00Z', price: 100 },
    point2: { time: '2024-12-01T00:00:00Z', price: 120 },
  };
}

describe('makeChartId / normalizeTimeframe', () => {
  it('makeChartId uppercases the ticker and joins with the timeframe', () => {
    expect(makeChartId('nvda', '1day')).toBe('NVDA:1day');
    expect(makeChartId(' aapl ', '1hour')).toBe('AAPL:1hour');
  });

  it('normalizeTimeframe passes valid intervals through', () => {
    for (const tf of VALID_TIMEFRAMES) {
      expect(normalizeTimeframe(tf)).toBe(tf);
    }
  });

  it('normalizeTimeframe falls back to the default for unknown intervals', () => {
    expect(normalizeTimeframe('1s')).toBe(DEFAULT_TIMEFRAME);
    expect(normalizeTimeframe('2hour')).toBe(DEFAULT_TIMEFRAME);
    expect(normalizeTimeframe('')).toBe(DEFAULT_TIMEFRAME);
    expect(normalizeTimeframe(null)).toBe(DEFAULT_TIMEFRAME);
    expect(normalizeTimeframe(undefined)).toBe(DEFAULT_TIMEFRAME);
  });
});

describe('chartAnnotationStore', () => {
  afterEach(() => {
    chartAnnotationStore._resetForTesting();
  });

  it('add() upserts by annotation_id within one instance (idempotent)', () => {
    const a = makePriceLine('ann_1', 'NVDA', 200);
    chartAnnotationStore.add(WS, 'NVDA:1day', a);
    chartAnnotationStore.add(WS, 'NVDA:1day', { ...a, price: 205 }); // same id → upsert

    const bucket = chartAnnotationStore.getState().byChart[storeKey(WS, 'NVDA:1day')];
    expect(bucket).toBeDefined();
    expect(Object.keys(bucket!)).toHaveLength(1);
    expect((bucket!.ann_1 as PriceLineAnnotation).price).toBe(205);
  });

  it('scopes by timeframe — same ticker, different timeframe is a separate instance', () => {
    chartAnnotationStore.add(WS, 'NVDA:1day', makePriceLine('ann_d', 'NVDA', 200, '1day'));
    chartAnnotationStore.add(WS, 'NVDA:1hour', makePriceLine('ann_h', 'NVDA', 210, '1hour'));

    const state = chartAnnotationStore.getState();
    expect(Object.keys(state.byChart[storeKey(WS, 'NVDA:1day')]!)).toEqual(['ann_d']);
    expect(Object.keys(state.byChart[storeKey(WS, 'NVDA:1hour')]!)).toEqual(['ann_h']);
  });

  it('scopes by workspace — same chart_id in another workspace is a separate instance', () => {
    chartAnnotationStore.add(WS, 'NVDA:1day', makePriceLine('ann_a', 'NVDA', 200));
    chartAnnotationStore.add(WS2, 'NVDA:1day', makePriceLine('ann_b', 'NVDA', 300));

    const state = chartAnnotationStore.getState();
    expect(Object.keys(state.byChart[storeKey(WS, 'NVDA:1day')]!)).toEqual(['ann_a']);
    expect(Object.keys(state.byChart[storeKey(WS2, 'NVDA:1day')]!)).toEqual(['ann_b']);
  });

  it('remove() deletes specific ids only', () => {
    chartAnnotationStore.add(WS, 'NVDA:1day', makePriceLine('ann_1', 'NVDA', 200));
    chartAnnotationStore.add(WS, 'NVDA:1day', makeTrendline('ann_2', 'NVDA'));
    chartAnnotationStore.remove(WS, 'NVDA:1day', ['ann_1']);

    const bucket = chartAnnotationStore.getState().byChart[storeKey(WS, 'NVDA:1day')];
    expect(bucket).toBeDefined();
    expect(Object.keys(bucket!)).toEqual(['ann_2']);
  });

  it('clear() drops one instance but leaves others alone', () => {
    chartAnnotationStore.add(WS, 'NVDA:1day', makePriceLine('ann_nvda', 'NVDA', 200));
    chartAnnotationStore.add(WS, 'AAPL:1day', makePriceLine('ann_aapl', 'AAPL', 180));

    chartAnnotationStore.clear(WS, 'NVDA:1day');

    const state = chartAnnotationStore.getState();
    expect(state.byChart[storeKey(WS, 'NVDA:1day')]).toBeUndefined();
    expect(state.byChart[storeKey(WS, 'AAPL:1day')]).toBeDefined();
    expect(Object.keys(state.byChart[storeKey(WS, 'AAPL:1day')]!)).toEqual(['ann_aapl']);
  });

  it('stores and retrieves every annotation variant by id', () => {
    const vline: VerticalLineAnnotation = {
      annotation_id: 'ann_v',
      symbol: 'NVDA',
      type: 'vertical_line',
      time: '2024-11-14T00:00:00Z',
      label: 'Earnings',
      style: 'dashed',
    };
    const rect: RectangleAnnotation = {
      annotation_id: 'ann_r',
      symbol: 'NVDA',
      type: 'rectangle',
      point1: { time: '2024-10-16T00:00:00Z', price: 150 },
      point2: { time: '2024-11-20T00:00:00Z', price: 140 },
    };
    const text: TextAnnotation = {
      annotation_id: 'ann_t',
      symbol: 'NVDA',
      type: 'text',
      time: '2024-11-14T00:00:00Z',
      price: 205,
      text: 'Breakout',
    };
    const fib: FibRetracementAnnotation = {
      annotation_id: 'ann_f',
      symbol: 'NVDA',
      type: 'fib_retracement',
      point1: { time: '2024-10-16T00:00:00Z', price: 100 },
      point2: { time: '2024-12-20T00:00:00Z', price: 200 },
    };
    for (const a of [vline, rect, text, fib] as StoredAnnotation[]) {
      chartAnnotationStore.add(WS, 'NVDA:1day', a);
    }

    const bucket = chartAnnotationStore.getState().byChart[storeKey(WS, 'NVDA:1day')];
    expect(bucket).toBeDefined();
    expect(Object.keys(bucket!).sort()).toEqual(['ann_f', 'ann_r', 'ann_t', 'ann_v']);
    expect((bucket!.ann_t as TextAnnotation).text).toBe('Breakout');
    expect((bucket!.ann_r as RectangleAnnotation).point2.price).toBe(140);
  });

  it('setAll() replaces only the requested instance', () => {
    chartAnnotationStore.add(WS, 'NVDA:1day', makePriceLine('ann_nvda_1', 'NVDA', 200));
    chartAnnotationStore.add(WS, 'AAPL:1day', makePriceLine('ann_aapl_1', 'AAPL', 180));

    chartAnnotationStore.setAll(WS, 'NVDA:1day', [makePriceLine('ann_nvda_new', 'NVDA', 210)]);

    const state = chartAnnotationStore.getState();
    expect(Object.keys(state.byChart[storeKey(WS, 'NVDA:1day')]!)).toEqual(['ann_nvda_new']);
    // AAPL must be untouched
    expect(Object.keys(state.byChart[storeKey(WS, 'AAPL:1day')]!)).toEqual(['ann_aapl_1']);
  });

  it('setChartsForSymbol() reconciles all timeframes and drops removed instances', () => {
    // Seed two NVDA instances + one AAPL + one in another workspace.
    chartAnnotationStore.add(WS, 'NVDA:1day', makePriceLine('ann_d', 'NVDA', 200, '1day'));
    chartAnnotationStore.add(WS, 'NVDA:1hour', makePriceLine('ann_h', 'NVDA', 210, '1hour'));
    chartAnnotationStore.add(WS, 'AAPL:1day', makePriceLine('ann_aapl', 'AAPL', 180));
    chartAnnotationStore.add(WS2, 'NVDA:1day', makePriceLine('ann_other', 'NVDA', 999));

    // Server now reports only NVDA:1day (with a fresh annotation). The 1hour
    // instance was cleared elsewhere and must disappear locally.
    const charts: ChartInstance[] = [
      {
        chart_id: 'NVDA:1day',
        symbol: 'NVDA',
        timeframe: '1day',
        annotations: [makePriceLine('ann_d_new', 'NVDA', 201, '1day')],
      },
    ];
    chartAnnotationStore.setChartsForSymbol(WS, 'NVDA', charts);

    const state = chartAnnotationStore.getState();
    expect(Object.keys(state.byChart[storeKey(WS, 'NVDA:1day')]!)).toEqual(['ann_d_new']);
    expect(state.byChart[storeKey(WS, 'NVDA:1hour')]).toBeUndefined(); // dropped
    // Other symbol + other workspace untouched.
    expect(Object.keys(state.byChart[storeKey(WS, 'AAPL:1day')]!)).toEqual(['ann_aapl']);
    expect(Object.keys(state.byChart[storeKey(WS2, 'NVDA:1day')]!)).toEqual(['ann_other']);
  });

  it('setChartsForSymbol() with an empty list clears that symbol locally', () => {
    chartAnnotationStore.add(WS, 'NVDA:1day', makePriceLine('ann_d', 'NVDA', 200));
    chartAnnotationStore.setChartsForSymbol(WS, 'NVDA', []);
    expect(chartAnnotationStore.getState().byChart[storeKey(WS, 'NVDA:1day')]).toBeUndefined();
  });

  it('setChartsForSymbol(sinceSeq) does not resurrect an instance cleared mid-fetch', () => {
    // A sync starts and captures the seq it will pass.
    chartAnnotationStore.add(WS, 'NVDA:1day', makePriceLine('ann_d', 'NVDA', 200));
    const seqAtStart = chartAnnotationStore.getMutationSeq();

    // A live clear lands while the (now stale) list request is in flight.
    chartAnnotationStore.clear(WS, 'NVDA:1day');
    expect(chartAnnotationStore.getState().byChart[storeKey(WS, 'NVDA:1day')]).toBeUndefined();

    // The stale response still carries the instance — but it must NOT come back.
    chartAnnotationStore.setChartsForSymbol(
      WS,
      'NVDA',
      [
        {
          chart_id: 'NVDA:1day',
          symbol: 'NVDA',
          timeframe: '1day',
          annotations: [makePriceLine('ann_d', 'NVDA', 200)],
        },
      ],
      seqAtStart,
    );
    expect(chartAnnotationStore.getState().byChart[storeKey(WS, 'NVDA:1day')]).toBeUndefined();
  });

  it('setChartsForSymbol(sinceSeq) keeps an instance added mid-fetch over a stale snapshot', () => {
    const seqAtStart = chartAnnotationStore.getMutationSeq();
    // Live add during the in-flight list (a timeframe the server hasn't returned).
    chartAnnotationStore.add(WS, 'NVDA:1hour', makePriceLine('ann_live', 'NVDA', 210, '1hour'));

    chartAnnotationStore.setChartsForSymbol(
      WS,
      'NVDA',
      [
        {
          chart_id: 'NVDA:1day',
          symbol: 'NVDA',
          timeframe: '1day',
          annotations: [makePriceLine('ann_d', 'NVDA', 200)],
        },
      ],
      seqAtStart,
    );
    // Server's 1day installed AND the live 1hour preserved (not dropped).
    expect(Object.keys(chartAnnotationStore.getState().byChart[storeKey(WS, 'NVDA:1day')]!)).toEqual(['ann_d']);
    expect(Object.keys(chartAnnotationStore.getState().byChart[storeKey(WS, 'NVDA:1hour')]!)).toEqual(['ann_live']);
  });

  it('clearDisplay() / restoreDisplay() flag display only, leaving data intact', () => {
    chartAnnotationStore.add(WS, 'NVDA:1day', makePriceLine('ann_1', 'NVDA', 200));
    expect(chartAnnotationStore.isDisplayCleared(WS, 'NVDA:1day')).toBe(false);

    chartAnnotationStore.clearDisplay(WS, 'NVDA:1day');
    expect(chartAnnotationStore.isDisplayCleared(WS, 'NVDA:1day')).toBe(true);
    // Data must survive a display clear.
    expect(
      Object.keys(chartAnnotationStore.getState().byChart[storeKey(WS, 'NVDA:1day')]!),
    ).toEqual(['ann_1']);

    chartAnnotationStore.restoreDisplay(WS, 'NVDA:1day');
    expect(chartAnnotationStore.isDisplayCleared(WS, 'NVDA:1day')).toBe(false);
  });

  it('clearDisplay() is scoped to one instance', () => {
    chartAnnotationStore.clearDisplay(WS, 'NVDA:1day');
    expect(chartAnnotationStore.isDisplayCleared(WS, 'NVDA:1day')).toBe(true);
    expect(chartAnnotationStore.isDisplayCleared(WS, 'NVDA:1hour')).toBe(false);
    expect(chartAnnotationStore.isDisplayCleared(WS2, 'NVDA:1day')).toBe(false);
  });

  it('clearDisplay() caps the cleared set, evicting the oldest entry', () => {
    // Clear far more distinct instances than the cap (200) so a long session
    // can't grow the set without bound.
    for (let i = 0; i < 250; i += 1) {
      chartAnnotationStore.clearDisplay(WS, `SYM${i}:1day`);
    }
    // The 50 oldest were evicted (re-show, data untouched); the newest remain.
    expect(chartAnnotationStore.isDisplayCleared(WS, 'SYM0:1day')).toBe(false);
    expect(chartAnnotationStore.isDisplayCleared(WS, 'SYM49:1day')).toBe(false);
    expect(chartAnnotationStore.isDisplayCleared(WS, 'SYM50:1day')).toBe(true);
    expect(chartAnnotationStore.isDisplayCleared(WS, 'SYM249:1day')).toBe(true);
  });
});

describe('applyAnnotationArtifact', () => {
  afterEach(() => {
    chartAnnotationStore._resetForTesting();
  });

  it('add op upserts a valid annotation under (workspace, chart_id)', () => {
    applyAnnotationArtifact('chart_annotation', {
      op: 'add',
      workspace_id: WS,
      chart_id: 'NVDA:1day',
      annotation: {
        annotation_id: 'ann_1',
        symbol: 'NVDA',
        type: 'price_line',
        price: 205,
      },
    });
    const bucket = chartAnnotationStore.getState().byChart[storeKey(WS, 'NVDA:1day')];
    expect(bucket).toBeDefined();
    expect((bucket!.ann_1 as PriceLineAnnotation).price).toBe(205);
  });

  it('derives chart_id from symbol + timeframe when chart_id is absent', () => {
    applyAnnotationArtifact('chart_annotation', {
      op: 'add',
      workspace_id: WS,
      symbol: 'NVDA',
      timeframe: '1hour',
      annotation: {
        annotation_id: 'ann_1',
        symbol: 'NVDA',
        type: 'price_line',
        price: 205,
      },
    });
    expect(
      chartAnnotationStore.getState().byChart[storeKey(WS, 'NVDA:1hour')],
    ).toBeDefined();
  });

  it('skips when workspace_id is missing (cannot key the instance)', () => {
    applyAnnotationArtifact('chart_annotation', {
      op: 'add',
      chart_id: 'NVDA:1day',
      annotation: { annotation_id: 'ann_1', symbol: 'NVDA', type: 'price_line', price: 1 },
    });
    expect(chartAnnotationStore.getState().byChart).toEqual({});
  });

  it('rejects a malformed annotation (unknown type / missing id)', () => {
    applyAnnotationArtifact('chart_annotation', {
      op: 'add',
      workspace_id: WS,
      chart_id: 'NVDA:1day',
      annotation: { annotation_id: 'ann_x', symbol: 'NVDA', type: 'bogus' },
    });
    applyAnnotationArtifact('chart_annotation', {
      op: 'add',
      workspace_id: WS,
      chart_id: 'NVDA:1day',
      annotation: { symbol: 'NVDA', type: 'price_line', price: 1 }, // no id
    });
    expect(chartAnnotationStore.getState().byChart[storeKey(WS, 'NVDA:1day')]).toBeUndefined();
  });

  it('ignores non chart_annotation artifact types', () => {
    applyAnnotationArtifact('html_widget', {
      op: 'add',
      workspace_id: WS,
      chart_id: 'NVDA:1day',
      annotation: { annotation_id: 'a', symbol: 'NVDA', type: 'price_line', price: 1 },
    });
    expect(chartAnnotationStore.getState().byChart).toEqual({});
  });

  it('add op restores a cleared display so a fresh draw is visible again', () => {
    chartAnnotationStore.clearDisplay(WS, 'NVDA:1day');
    expect(chartAnnotationStore.isDisplayCleared(WS, 'NVDA:1day')).toBe(true);

    applyAnnotationArtifact('chart_annotation', {
      op: 'add',
      workspace_id: WS,
      chart_id: 'NVDA:1day',
      annotation: { annotation_id: 'ann_new', symbol: 'NVDA', type: 'price_line', price: 207 },
    });

    expect(chartAnnotationStore.isDisplayCleared(WS, 'NVDA:1day')).toBe(false);
  });

  it('remove op deletes ids; clear op drops the instance', () => {
    const add = (id: string, price: number) =>
      applyAnnotationArtifact('chart_annotation', {
        op: 'add',
        workspace_id: WS,
        chart_id: 'NVDA:1day',
        annotation: { annotation_id: id, symbol: 'NVDA', type: 'price_line', price },
      });
    add('ann_1', 205);
    add('ann_2', 210);

    applyAnnotationArtifact('chart_annotation', {
      op: 'remove',
      workspace_id: WS,
      chart_id: 'NVDA:1day',
      ids: ['ann_1'],
    });
    expect(
      Object.keys(chartAnnotationStore.getState().byChart[storeKey(WS, 'NVDA:1day')]!),
    ).toEqual(['ann_2']);

    applyAnnotationArtifact('chart_annotation', {
      op: 'clear',
      workspace_id: WS,
      chart_id: 'NVDA:1day',
    });
    expect(chartAnnotationStore.getState().byChart[storeKey(WS, 'NVDA:1day')]).toBeUndefined();
  });

  it('rejects a marker with no valid shape (would blank the shared marker layer)', () => {
    // A marker with no shape makes lightweight-charts' setMarkers throw, which
    // would wipe earnings + grade markers too — reject it at the store boundary.
    applyAnnotationArtifact('chart_annotation', {
      op: 'add',
      workspace_id: WS,
      chart_id: 'NVDA:1day',
      annotation: {
        annotation_id: 'm_bad',
        symbol: 'NVDA',
        type: 'marker',
        time: '2024-11-14T00:00:00Z',
      },
    });
    expect(chartAnnotationStore.getState().byChart[storeKey(WS, 'NVDA:1day')]).toBeUndefined();

    // A marker with a valid shape is accepted.
    applyAnnotationArtifact('chart_annotation', {
      op: 'add',
      workspace_id: WS,
      chart_id: 'NVDA:1day',
      annotation: {
        annotation_id: 'm_ok',
        symbol: 'NVDA',
        type: 'marker',
        time: '2024-11-14T00:00:00Z',
        shape: 'circle',
      },
    });
    expect(
      Object.keys(chartAnnotationStore.getState().byChart[storeKey(WS, 'NVDA:1day')]!),
    ).toEqual(['m_ok']);
  });
});

describe('subscribeLiveAnnotationAdd', () => {
  afterEach(() => {
    chartAnnotationStore._resetForTesting();
  });

  it('fires on a fresh add op with the resolved instance identity', () => {
    const seen: LiveAnnotationAdd[] = [];
    const unsub = subscribeLiveAnnotationAdd((a) => seen.push(a));

    applyAnnotationArtifact('chart_annotation', {
      op: 'add',
      workspace_id: WS,
      chart_id: 'NVDA:1day',
      annotation: { annotation_id: 'ann_1', symbol: 'NVDA', type: 'price_line', price: 205 },
    });

    expect(seen).toEqual([
      { workspaceId: WS, chartId: 'NVDA:1day', symbol: 'NVDA', timeframe: '1day' },
    ]);
    unsub();
  });

  it('derives symbol + timeframe from a chart_id assembled from symbol+timeframe', () => {
    const seen: LiveAnnotationAdd[] = [];
    subscribeLiveAnnotationAdd((a) => seen.push(a));

    applyAnnotationArtifact('chart_annotation', {
      op: 'add',
      workspace_id: WS,
      symbol: 'AAPL',
      timeframe: '1hour',
      annotation: { annotation_id: 'ann_1', symbol: 'AAPL', type: 'price_line', price: 1 },
    });

    expect(seen).toEqual([
      { workspaceId: WS, chartId: 'AAPL:1hour', symbol: 'AAPL', timeframe: '1hour' },
    ]);
  });

  it('does not fire on remove / clear ops or rejected annotations', () => {
    chartAnnotationStore.add(WS, 'NVDA:1day', makePriceLine('ann_1', 'NVDA', 200));
    const seen: LiveAnnotationAdd[] = [];
    subscribeLiveAnnotationAdd((a) => seen.push(a));

    applyAnnotationArtifact('chart_annotation', {
      op: 'remove',
      workspace_id: WS,
      chart_id: 'NVDA:1day',
      ids: ['ann_1'],
    });
    applyAnnotationArtifact('chart_annotation', {
      op: 'clear',
      workspace_id: WS,
      chart_id: 'NVDA:1day',
    });
    // Malformed annotation is rejected before any emit.
    applyAnnotationArtifact('chart_annotation', {
      op: 'add',
      workspace_id: WS,
      chart_id: 'NVDA:1day',
      annotation: { annotation_id: 'ann_x', symbol: 'NVDA', type: 'bogus' },
    });

    expect(seen).toEqual([]);
  });

  it('does not fire on bare store writes (only live SSE adds)', () => {
    const seen: LiveAnnotationAdd[] = [];
    subscribeLiveAnnotationAdd((a) => seen.push(a));

    chartAnnotationStore.add(WS, 'NVDA:1day', makePriceLine('ann_1', 'NVDA', 200));
    chartAnnotationStore.setChartsForSymbol(WS, 'NVDA', [
      { chart_id: 'NVDA:1day', symbol: 'NVDA', timeframe: '1day', annotations: [] },
    ]);

    expect(seen).toEqual([]);
  });

  it('stops delivering after unsubscribe', () => {
    const seen: LiveAnnotationAdd[] = [];
    const unsub = subscribeLiveAnnotationAdd((a) => seen.push(a));
    unsub();

    applyAnnotationArtifact('chart_annotation', {
      op: 'add',
      workspace_id: WS,
      chart_id: 'NVDA:1day',
      annotation: { annotation_id: 'ann_1', symbol: 'NVDA', type: 'price_line', price: 1 },
    });

    expect(seen).toEqual([]);
  });
});

describe('useAnnotationsForView', () => {
  afterEach(() => {
    act(() => {
      chartAnnotationStore._resetForTesting();
    });
  });

  it('returns only annotations for the requested instance', () => {
    act(() => {
      chartAnnotationStore.add(WS, 'NVDA:1day', makePriceLine('ann_d', 'NVDA', 200, '1day'));
      chartAnnotationStore.add(WS, 'NVDA:1hour', makePriceLine('ann_h', 'NVDA', 210, '1hour'));
    });

    const { result } = renderHook(() => useAnnotationsForView(WS, 'NVDA', '1day'));
    expect(result.current.map((a) => a.annotation_id)).toEqual(['ann_d']);
  });

  it('re-renders when the matching instance mutates', () => {
    const { result } = renderHook(() => useAnnotationsForView(WS, 'NVDA', '1day'));
    expect(result.current).toHaveLength(0);

    act(() => {
      chartAnnotationStore.add(WS, 'NVDA:1day', makePriceLine('ann_1', 'NVDA', 200));
    });
    expect(result.current).toHaveLength(1);

    act(() => {
      chartAnnotationStore.remove(WS, 'NVDA:1day', ['ann_1']);
    });
    expect(result.current).toHaveLength(0);
  });

  it('does not re-render when an unrelated instance mutates', () => {
    let renderCount = 0;
    const { result } = renderHook(() => {
      renderCount += 1;
      return useAnnotationsForView(WS, 'NVDA', '1day');
    });

    const initialRenders = renderCount;

    act(() => {
      // Different timeframe, different workspace — both unrelated.
      chartAnnotationStore.add(WS, 'NVDA:1hour', makePriceLine('ann_h', 'NVDA', 210, '1hour'));
      chartAnnotationStore.add(WS2, 'NVDA:1day', makePriceLine('ann_o', 'NVDA', 999));
    });

    expect(renderCount).toBe(initialRenders);
    expect(result.current).toHaveLength(0);
  });

  it('returns an empty array when workspace / symbol / timeframe is missing', () => {
    const { result: noWs } = renderHook(() => useAnnotationsForView(null, 'NVDA', '1day'));
    const { result: noSym } = renderHook(() => useAnnotationsForView(WS, null, '1day'));
    const { result: noTf } = renderHook(() => useAnnotationsForView(WS, 'NVDA', null));
    expect(noWs.current).toEqual([]);
    expect(noSym.current).toEqual([]);
    expect(noTf.current).toEqual([]);
  });
});

describe('useDisplayCleared', () => {
  afterEach(() => {
    act(() => {
      chartAnnotationStore._resetForTesting();
    });
  });

  it('tracks the cleared/restored state for one instance', () => {
    const { result } = renderHook(() => useDisplayCleared(WS, 'NVDA', '1day'));
    expect(result.current).toBe(false);

    act(() => {
      chartAnnotationStore.clearDisplay(WS, 'NVDA:1day');
    });
    expect(result.current).toBe(true);

    act(() => {
      chartAnnotationStore.restoreDisplay(WS, 'NVDA:1day');
    });
    expect(result.current).toBe(false);
  });

  it('is false when workspace / symbol / timeframe is missing', () => {
    const { result: noWs } = renderHook(() => useDisplayCleared(null, 'NVDA', '1day'));
    const { result: noSym } = renderHook(() => useDisplayCleared(WS, null, '1day'));
    const { result: noTf } = renderHook(() => useDisplayCleared(WS, 'NVDA', null));
    expect(noWs.current).toBe(false);
    expect(noSym.current).toBe(false);
    expect(noTf.current).toBe(false);
  });
});
