import { describe, expect, it } from 'vitest';
import type { IChartApiBase, Logical, Time } from 'lightweight-charts';
import type { CanvasRenderingTarget2D } from 'fancy-canvas';

import i18n from '@/i18n';
import { formatCalendarDayRange } from '@/lib/format';

import type { ChartLayer, LayerDrawing, StoredChartLayer } from '../../../types/chartLayer';
import { ChartLayerPrimitive } from '../ChartLayerPrimitive';
import { compileLayers } from '../compileLayers';
import type { LayerFrameStats, ReportContext } from '../layerReport';
import { buildLayerText } from '../layerText';
import { LAYER_FALLBACK } from '../layerTheme';
import { EMPTY_PAINT_CONTEXT } from '../painter';
import { buildTimeGrid, layerTimeToSeconds, type TimeGrid } from '../timeGrid';

const DAY = 86400;
const BAR_PX = 10;

/** The layers compiled on `grid`, as the primitive's default (dark) theme paints them. */
function compile(layers: StoredChartLayer[], grid: TimeGrid) {
  return compileLayers(layers, grid, { rsiPeriod: null, maPeriods: [], volume: false }, LAYER_FALLBACK.dark);
}

interface FilledText {
  text: string;
  x: number;
  y: number;
}

/** A canvas context that records only the text it fills and measures every glyph 6px wide. */
function fakeContext(texts: FilledText[] = []): CanvasRenderingContext2D {
  const target = {
    measureText: (t: string) => ({ width: t.length * 6 }),
    createLinearGradient: () => ({ addColorStop() {} }),
    fillText: (text: string, x: number, y: number) => {
      texts.push({ text, x, y });
    },
  };
  return new Proxy(target, {
    get: (obj, prop) => (prop in obj ? obj[prop as keyof typeof obj] : () => {}),
    set: () => true,
  }) as unknown as CanvasRenderingContext2D;
}

function weekdayBars(start: string, n: number): number[] {
  const out: number[] = [];
  let t = layerTimeToSeconds(start)!;
  while (out.length < n) {
    const d = new Date(t * 1000).getUTCDay();
    if (d !== 0 && d !== 6) out.push(t);
    t += DAY;
  }
  return out;
}

function stored(partial: Partial<ChartLayer>): StoredChartLayer {
  return {
    workspace_id: 'ws',
    layer_id: 'setup',
    symbol: 'AMD',
    timeframe: '1day',
    chart_id: 'AMD:1day',
    layer: {
      version: 1,
      id: 'setup',
      title: 'Forward setup',
      symbol: 'AMD',
      timeframe: '1day',
      source: { kind: 'python' },
      panes: [],
      series: [],
      fills: [],
      levels: [],
      markers: [],
      regimes: [],
      drawings: [],
      alerts: [],
      ...partial,
    },
  };
}

describe('render report', () => {
  it('names the view, the labels the layout hid and the elements out of view', () => {
    const bars = weekdayBars('2026-10-01', 20);
    const grid = buildTimeGrid(bars, DAY, layerTimeToSeconds('2026-11-06'));
    const fib = { type: 'fib' as const, p1: { time: '2026-10-01', price: 100 }, p2: { time: '2026-10-10', price: 200 } };
    const layers = [
      stored({
        drawings: [
          fib,
          // The same swing twice: its level tags land on the first one's and are hidden.
          { ...fib, id: 'again' },
          { type: 'box', id: 'far-zone', label: 'Far zone', p1: { time: '2026-10-05', price: 9000 }, p2: { time: '2026-10-09', price: 9100 } },
        ],
      }),
    ];
    const { scene, elements } = compile(layers, grid);

    const timeScale = {
      timeToCoordinate: (t: number) => bars.indexOf(t) * BAR_PX,
      coordinateToLogical: (x: number) => x / BAR_PX,
      logicalToCoordinate: (l: number) => l * BAR_PX,
      getVisibleLogicalRange: () => ({ from: 0, to: 24 }),
      options: () => ({ barSpacing: BAR_PX }),
    };
    const chart = { timeScale: () => timeScale } as unknown as IChartApiBase<Time>;
    const prim = new ChartLayerPrimitive();
    prim.attached({ chart, series: { priceToCoordinate: (p: number) => 3 * (230 - p) }, requestUpdate: () => {} });
    prim.setScene(scene, { lastBarTime: bars[bars.length - 1], lastIndex: bars.length - 1 });
    const ctx: ReportContext = {
      grid,
      names: elements.names,
      layers: [{ id: 'setup', title: 'Forward setup' }],
      panes: ['Price', 'RSI (14)'],
      notes: ["setup: layer pane 'RSI 14' merged into built-in RSI (14)"],
      axisLines: [],
      paneTags: () => [],
      paneChips: () => [],
    };
    const reports: (LayerFrameStats | null)[] = [];
    prim.setReporter((stats) => reports.push(stats), ctx);

    const target = {
      useMediaCoordinateSpace: (fn: (s: { context: CanvasRenderingContext2D; mediaSize: { width: number; height: number } }) => void) =>
        fn({ context: fakeContext(), mediaSize: { width: 400, height: 400 } }),
    } as unknown as CanvasRenderingTarget2D;
    const over = prim.paneViews()[1].renderer()!;
    over.draw(target);

    expect(reports).toHaveLength(1);
    const report = reports[0]!;
    expect(report.view).toEqual({
      from: '2026-10-01',
      to: '2026-11-04',
      futureSlots: 5,
      width: 400,
      height: 400,
      panes: ['Price', 'RSI (14)'],
    });
    expect(report.layers).toEqual([{ id: 'setup', title: 'Forward setup' }]);
    expect(report.droppedLabels.length).toBeGreaterThan(0);
    expect(report.droppedLabels.every((l) => l.layer === 'setup' && l.element === 'again')).toBe(true);
    expect(report.droppedLabels.map((l) => l.text)).toContain('0.5 (150.00)');
    expect(report.offscreen).toEqual([{ layer: 'setup', element: 'far-zone', kind: 'box' }]);
    expect(report.cutText).toEqual([]);
    expect(report.coveredLabels).toEqual([]);
    expect(report.markersOverCandles).toEqual([]);
    expect(report.forecasts).toEqual([]);
    expect(report.notes).toEqual(ctx.notes);

    over.draw(target);
    expect(reports).toHaveLength(2);
  });

  it('names text cut short, labels under the chart text, captions over candles and forecast verdicts', () => {
    const bars = weekdayBars('2026-10-01', 20); // 2026-10-01 .. 2026-10-28
    const grid = buildTimeGrid(bars, DAY, layerTimeToSeconds('2026-11-06'));
    const layers = [
      stored({
        levels: [{ price: 150, pane: 'price', title: 'Prior swing high resistance' }],
        drawings: [
          // Its chip sits at the pane's top, where the legend's title chip is.
          { type: 'vline', id: 'fomc', time: '2026-10-06', label: 'FOMC' },
          // Runs right into the "Close" tag at the pane's edge.
          { type: 'label', id: 'note', at: { time: '2026-11-02', price: 200 }, text: 'Breakout retest zone', position: 'right' },
          { type: 'forecast', id: 'up', label: 'Retest high', origin: { time: '2026-10-05', price: 100 }, target: { time: '2026-10-20', price: 120 } },
          { type: 'forecast', id: 'moon', origin: { time: '2026-10-05', price: 100 }, target: { time: '2026-10-12', price: 300 } },
          { type: 'forecast', id: 'later', label: 'Run', origin: { time: '2026-10-26', price: 100 }, target: { time: '2026-11-05', price: 999 } },
        ],
      }),
    ];
    const { scene, plan, elements } = compile(layers, grid);
    const highs = new Float64Array(20).fill(105);
    highs[8] = 125; // 2026-10-13: reaches the first forecast's 120, and stands up into the marker's caption
    const lows = new Float64Array(20).fill(95);

    const timeScale = {
      timeToCoordinate: (t: number) => bars.indexOf(t) * BAR_PX,
      coordinateToLogical: (x: number) => x / BAR_PX,
      logicalToCoordinate: (l: number) => l * BAR_PX,
      getVisibleLogicalRange: () => ({ from: 0, to: 24 }),
      options: () => ({ barSpacing: BAR_PX }),
    };
    const chart = { timeScale: () => timeScale } as unknown as IChartApiBase<Time>;
    const prim = new ChartLayerPrimitive();
    prim.attached({ chart, series: { priceToCoordinate: (p: number) => 3 * (230 - p) }, requestUpdate: () => {} });
    prim.setScene(scene, { lastBarTime: bars[bars.length - 1], lastIndex: bars.length - 1 });
    prim.setPaintContext({
      highs,
      lows,
      markers: [{ i: 10, position: 'aboveBar', text: 'Q3 earnings beat and raise' }, { i: 12, position: 'aboveBar', text: '' }],
      axisLines: [],
    });
    const title = 'Forward setup for the November earnings run';
    const reports: (LayerFrameStats | null)[] = [];
    prim.setReporter((stats) => reports.push(stats), {
      grid,
      names: elements.names,
      layers: [{ id: 'setup', title }],
      panes: ['Price'],
      notes: [],
      axisLines: plan.axisLines,
      paneTags: () => [{ price: 200, title: 'Close' }],
      paneChips: () => [{ left: 8, top: 8, width: 200, height: 24, layer: 'setup', title, shown: 'Forward setup for the Nov…' }],
    });

    const target = {
      useMediaCoordinateSpace: (fn: (s: { context: CanvasRenderingContext2D; mediaSize: { width: number; height: number } }) => void) =>
        fn({ context: fakeContext(), mediaSize: { width: 400, height: 400 } }),
    } as unknown as CanvasRenderingTarget2D;
    prim.paneViews()[1].renderer()!.draw(target);
    const report = reports[0]!;

    expect(report.cutText).toEqual([
      { layer: 'setup', element: 'Prior swing high resistance', where: 'axis', text: 'Prior swing high resistance', shown: 'Prior swing hig…' },
      { layer: 'setup', element: 'title', where: 'title', text: title, shown: 'Forward setup for the Nov…' },
    ]);
    expect(report.coveredLabels).toEqual([
      { layer: 'setup', element: 'note', kind: 'label', by: 'tag', byText: 'Close' },
      { layer: 'setup', element: 'fomc', kind: 'vline', by: 'chip', byText: title },
    ]);
    expect(report.markersOverCandles).toEqual([{ text: 'Q3 earnings beat and raise' }]);
    expect(report.forecasts).toEqual([
      { layer: 'setup', element: 'up', label: 'Retest high', state: 'hit', at: '2026-10-13' },
      { layer: 'setup', element: 'moon', label: null, state: 'missed', at: '2026-10-12' },
      { layer: 'setup', element: 'later', label: 'Run', state: 'pending', at: '2026-11-05' },
    ]);
  });

  it('reports null once no layer is on the chart', () => {
    const prim = new ChartLayerPrimitive();
    const timeScale = { getVisibleLogicalRange: () => ({ from: 0, to: 10 }) };
    const chart = { timeScale: () => timeScale } as unknown as IChartApiBase<Time>;
    prim.attached({ chart, series: { priceToCoordinate: () => 0 }, requestUpdate: () => {} });
    const target = {
      useMediaCoordinateSpace: (fn: (s: { context: CanvasRenderingContext2D; mediaSize: { width: number; height: number } }) => void) =>
        fn({ context: fakeContext(), mediaSize: { width: 400, height: 400 } }),
    } as unknown as CanvasRenderingTarget2D;
    const reports: (LayerFrameStats | null)[] = [];
    prim.setReporter((stats) => reports.push(stats), null);
    prim.paneViews()[1].renderer()!.draw(target);
    expect(reports).toEqual([null]);
  });
});

/** A chart of `bars` at BAR_PX a bar, scrolled so logical `from` sits at x 0, showing `from`..`to`. */
function chartOn(bars: number[], from: number, to: number): IChartApiBase<Time> {
  const timeScale = {
    timeToCoordinate: (t: number) => (bars.indexOf(t) - from) * BAR_PX,
    coordinateToLogical: (x: number) => x / BAR_PX + from,
    logicalToCoordinate: (l: number) => (l - from) * BAR_PX,
    getVisibleLogicalRange: () => ({ from, to }),
    options: () => ({ barSpacing: BAR_PX }),
  };
  return { timeScale: () => timeScale } as unknown as IChartApiBase<Time>;
}

function paintOver(prim: ChartLayerPrimitive, height = 400): FilledText[] {
  const texts: FilledText[] = [];
  const target = {
    useMediaCoordinateSpace: (fn: (s: { context: CanvasRenderingContext2D; mediaSize: { width: number; height: number } }) => void) =>
      fn({ context: fakeContext(texts), mediaSize: { width: 400, height } }),
  } as unknown as CanvasRenderingTarget2D;
  prim.paneViews()[1].renderer()!.draw(target);
  return texts;
}

describe('price axis', () => {
  const bars = weekdayBars('2026-10-01', 20);
  const last = bars.length - 1;
  const grid = buildTimeGrid(bars, DAY, layerTimeToSeconds('2026-11-13'));
  const lastDay = new Date(bars[last] * 1000).toISOString().slice(0, 10);
  const zone: LayerDrawing = { type: 'box', label: 'Old zone', p1: { time: '2026-10-05', price: 9000 }, p2: { time: '2026-10-09', price: 9100 } };
  const fib = { type: 'fib', p1: { time: '2026-10-01', price: 50 }, p2: { time: '2026-10-08', price: 80 }, extend: 'right' } as LayerDrawing;
  const forecast: LayerDrawing = { type: 'forecast', origin: { time: lastDay, price: 200 }, target: { time: '2026-11-06', price: 260 } };

  function axisFor(drawings: LayerDrawing[]) {
    const prim = new ChartLayerPrimitive();
    prim.attached({ chart: chartOn(bars, 0, 30), series: { priceToCoordinate: () => 0 }, requestUpdate: () => {} });
    prim.setScene(compile([stored({ drawings })], grid).scene, { lastBarTime: bars[last], lastIndex: last });
    return prim.autoscaleInfo(0 as Logical, 30 as Logical);
  }

  it('fits only drawings that reach past the last bar, with room for a forecast box', () => {
    expect(axisFor([zone, fib, forecast])).toEqual({
      priceRange: { minValue: 200, maxValue: 260 },
      margins: { above: 34, below: 34 },
    });
  });

  it('leaves the axis to the candles when every drawing sits on the history', () => {
    expect(axisFor([zone, fib])).toBeNull();
  });
});

describe('canvas text', () => {
  const bars = weekdayBars('2026-10-01', 20);
  const last = bars.length - 1;
  const grid = buildTimeGrid(bars, DAY, layerTimeToSeconds('2026-11-13'));
  const day = (k: number) => new Date(bars[k] * 1000).toISOString().slice(0, 10);
  const replay = (k0: number, k1: number): LayerDrawing => ({
    type: 'analog',
    origin: { time: day(last), price: 100 },
    source_start: day(k0),
    source_end: day(k1),
    bars: [{ time: '2026-11-02', open: 100, high: 103, low: 99, close: 102 }],
  });

  it("words the renderer's own labels in the reader's language and the agent's as given", () => {
    const layers = [
      stored({
        drawings: [
          // Reached on bar 4: a hit.
          { type: 'forecast', label: 'Retest', origin: { time: day(2), price: 100 }, target: { time: day(8), price: 110 } },
          replay(3, 6),
        ],
      }),
    ];
    const prim = new ChartLayerPrimitive();
    prim.attached({ chart: chartOn(bars, 0, 24), series: { priceToCoordinate: (p: number) => 3 * (130 - p) }, requestUpdate: () => {} });
    prim.setScene(compile(layers, grid).scene, { lastBarTime: bars[last], lastIndex: last });
    prim.setPaintContext({ ...EMPTY_PAINT_CONTEXT, highs: bars.map((_, k) => (k === 4 ? 111 : 101)), lows: bars.map(() => 95) });
    prim.setText(buildLayerText(i18n.getFixedT('zh-CN'), 'zh-CN'));

    const height = 400;
    const texts = paintOver(prim, height);
    const drawn = texts.map((t) => t.text);
    expect(drawn).toContain('命中');
    expect(drawn).toContain('重演');
    expect(drawn.some((t) => t.startsWith('Retest · '))).toBe(true);
    // The source stretch is named on its floor, in the reader's dates.
    const source = texts.find((t) => t.text.startsWith('重演自 '));
    expect(source?.text).toBe(`重演自 ${formatCalendarDayRange(day(3), day(6), 'zh-CN')}`);
    expect(source!.y).toBeGreaterThan(height - 20);
  });

  it('names a replay source only while its stretch is on screen', () => {
    const scene = compile([stored({ drawings: [replay(1, 3)] })], grid).scene;
    const prim = new ChartLayerPrimitive();
    // Scrolled to the last bars: the source (bars 1-3) is off to the left.
    prim.attached({ chart: chartOn(bars, 10, 30), series: { priceToCoordinate: (p: number) => 3 * (130 - p) }, requestUpdate: () => {} });
    prim.setScene(scene, { lastBarTime: bars[last], lastIndex: last });
    const drawn = paintOver(prim).map((t) => t.text);
    expect(drawn).toContain('Replay');
    expect(drawn.some((t) => t.startsWith('Replayed'))).toBe(false);
  });
});
