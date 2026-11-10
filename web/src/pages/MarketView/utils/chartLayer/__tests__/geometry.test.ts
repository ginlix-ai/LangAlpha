// @vitest-environment node
import { describe, expect, it } from 'vitest';

import type { ChartLayer, LayerDrawing, StoredChartLayer } from '../../../types/chartLayer';
import { withAlpha } from '../../canvasKit';
import { mergeAxisLines, type AxisLineSpec } from '../axisLines';
import { planBuiltinDuplicates, rsiPeriodOf, smaPeriodOf, type BuiltinIndicators } from '../builtinDuplicates';
import { extendSegment } from '../canvasGeom';
import { fillOf, hasAlpha } from '../color';
import { BUILTIN_RSI } from '../chartPlan';
import { compileLayers } from '../compileLayers';
import { cleanAnchors } from '../elements';
import type { SceneDrawing } from '../kinds';
import { channel, channelOffset, type ChannelScene } from '../kinds/channel';
import { orderConeBands } from '../kinds/cone';
import { fibLevels } from '../kinds/fib';
import { fmtMove } from '../format';
import { forecastBox, forecastVerdict } from '../kinds/forecast';
import { measureReadout, measureStats } from '../kinds/measure';
import { pathLabelSides } from '../kinds/path';
import { pitchforkMid } from '../kinds/pitchfork';
import { DEFAULT_POSITION_BARS, positionStats } from '../kinds/position';
import { profile, profileRows } from '../kinds/profile';
import type { SceneBase } from '../scene';
import { maxLayerTime } from '../layerTimes';
import { LAYER_FALLBACK } from '../layerTheme';
import { Painter } from '../painter';
import type { XMapper } from '../xMapper';
import { fillBetween, splitRuns } from '../seriesData';
import { buildTimeGrid, indexToSeconds, layerTimeToSeconds, resolveIndex, snapToGrid, timeToIndex, type TimeGrid } from '../timeGrid';

const DAY = 86400;
const sec = (t: string) => layerTimeToSeconds(t)!;

/** Weekday daily bars from `start` for `n` sessions. */
function weekdayBars(start: string, n: number): number[] {
  const out: number[] = [];
  let t = sec(start);
  while (out.length < n) {
    const d = new Date(t * 1000).getUTCDay();
    if (d !== 0 && d !== 6) out.push(t);
    t += DAY;
  }
  return out;
}

const TH = LAYER_FALLBACK.light;
const NO_BUILTINS: BuiltinIndicators = { rsiPeriod: null, maPeriods: [], volume: false };

/** Everything the layers compile to, on `grid`. */
function compile(layers: StoredChartLayer[], grid: TimeGrid, builtins: BuiltinIndicators = NO_BUILTINS) {
  return compileLayers(layers, grid, builtins, TH);
}

function layer(partial: Partial<ChartLayer>): StoredChartLayer {
  return {
    workspace_id: 'ws',
    layer_id: 'test',
    symbol: 'NVDA',
    timeframe: '1day',
    chart_id: 'NVDA:1day',
    layer: {
      version: 1,
      id: 'test',
      title: 'Test layer',
      symbol: 'NVDA',
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

function drawing<K extends SceneDrawing['kind']>(scene: { drawings: SceneDrawing[] }, kind: K) {
  const d = scene.drawings.find((x) => x.kind === kind);
  expect(d).toBeDefined();
  return d as Extract<SceneDrawing, { kind: K }>;
}

describe('layerTimeToSeconds', () => {
  it('reads zone-less wall clock as UTC chart seconds', () => {
    expect(layerTimeToSeconds('2026-10-28')).toBe(Date.UTC(2026, 9, 28) / 1000);
    expect(layerTimeToSeconds('2026-10-28T09:30')).toBe(Date.UTC(2026, 9, 28, 9, 30) / 1000);
    expect(layerTimeToSeconds('2026-10-28T09:30:15')).toBe(Date.UTC(2026, 9, 28, 9, 30, 15) / 1000);
  });

  it('rejects garbage', () => {
    expect(layerTimeToSeconds('soon')).toBeNull();
    expect(layerTimeToSeconds('')).toBeNull();
    expect(layerTimeToSeconds(null)).toBeNull();
  });
});

describe('time grid', () => {
  // Mon 2026-10-19 .. Wed 2026-10-28: 8 sessions.
  const bars = weekdayBars('2026-10-19', 8);

  it('extends daily bars past the last one on weekdays only', () => {
    const grid = buildTimeGrid(bars, DAY, sec('2026-11-03'));
    // Thu 29, Fri 30, Mon 2, Tue 3, then the pad.
    expect(grid.future.slice(0, 4)).toEqual([sec('2026-10-29'), sec('2026-10-30'), sec('2026-11-02'), sec('2026-11-03')]);
    expect(grid.future.every((t) => ![0, 6].includes(new Date(t * 1000).getUTCDay()))).toBe(true);
  });

  it('reaches a future anchor as far out as a layer series may run', () => {
    // About 5,000 weekday sessions past the last bar.
    const until = sec('2046-01-01');
    const grid = buildTimeGrid(bars, DAY, until);
    expect(grid.future.length).toBeGreaterThan(4000);
    expect(grid.future[grid.future.length - 1]).toBeGreaterThanOrEqual(until);
  });

  it('places a time past the slot cap on the session, not across nights and weekends', () => {
    // 1min bars 04:00-19:59 on Thu Oct 29 and Fri Oct 30; Mon Feb 1 is far past 20,000 slots.
    const minute = weekdayBars('2026-10-29', 2).flatMap((d) => Array.from({ length: 960 }, (_, k) => d + 4 * 3600 + k * 60));
    const target = sec('2027-02-01T04:00');
    const grid = buildTimeGrid(minute, 60, target);
    expect(grid.future.length).toBe(20_000);
    let want = minute.length - 1;
    for (let d = sec('2026-10-31'); d < sec('2027-02-01'); d += DAY) {
      if (![0, 6].includes(new Date(d * 1000).getUTCDay())) want += 960;
    }
    want += 1; // Feb 1's first bar
    expect(resolveIndex(grid, target)).toBe(want);
    expect(indexToSeconds(grid, want, 'slot')).toBe(target);
    expect(indexToSeconds(grid, want + 0.5, 'interpolate')).toBe(target + 30);
    // Between the close and the next open, a time sits between those two slots.
    expect(resolveIndex(grid, sec('2027-01-29T22:00'))).toBeGreaterThan(want - 1);
    expect(resolveIndex(grid, sec('2027-01-29T22:00'))).toBeLessThan(want);
  });

  it('builds no future when nothing reaches past the last bar', () => {
    expect(buildTimeGrid(bars, DAY, sec('2026-10-20')).future).toEqual([]);
    expect(buildTimeGrid(bars, DAY, null).future).toEqual([]);
  });

  it('keeps trading weekends for a symbol that trades them', () => {
    const crypto = Array.from({ length: 10 }, (_, k) => sec('2026-10-19') + k * DAY);
    const grid = buildTimeGrid(crypto, DAY, sec('2026-11-01'));
    expect(grid.future).toContain(sec('2026-10-31')); // a Saturday
  });

  it('repeats the intraday session template forward', () => {
    // 1-hour bars 09:30..15:30 on weekdays.
    const days = weekdayBars('2026-10-26', 3);
    const hourly = days.flatMap((d) => Array.from({ length: 7 }, (_, h) => d + 9.5 * 3600 + h * 3600));
    const grid = buildTimeGrid(hourly, 3600, sec('2026-10-29T10:30'));
    expect(grid.future[0]).toBe(sec('2026-10-29T09:30'));
    expect(grid.future[1]).toBe(sec('2026-10-29T10:30'));
    // No overnight slots.
    expect(grid.future).not.toContain(sec('2026-10-28T20:30'));
  });

  it('resolves exact bars, between bars, the future and the past', () => {
    const grid = buildTimeGrid(bars, DAY, sec('2026-11-03'));
    expect(resolveIndex(grid, sec('2026-10-19'))).toBe(0);
    expect(resolveIndex(grid, sec('2026-10-28'))).toBe(7);
    // Saturday sits a third of the way from Friday (4) to Monday (5).
    expect(resolveIndex(grid, sec('2026-10-24'))).toBeCloseTo(4 + 1 / 3);
    // Future slots continue the index: Thu 29 is 8, Mon 2 is 10.
    expect(timeToIndex(grid, '2026-10-29')).toBe(8);
    expect(timeToIndex(grid, '2026-11-02')).toBe(10);
    // Before the first bar extrapolates by the interval.
    expect(resolveIndex(grid, sec('2026-10-17'))).toBe(-2);
  });

  it('extrapolates past the generated slots', () => {
    const grid = buildTimeGrid(bars, DAY, sec('2026-10-29'));
    const last = grid.future[grid.future.length - 1];
    expect(resolveIndex(grid, last + 2 * DAY)).toBe(7 + grid.future.length + 2);
  });

  it('snaps series times onto bars and slots, dropping what lies outside', () => {
    const grid = buildTimeGrid(bars, DAY, sec('2026-10-30'));
    expect(snapToGrid(grid, sec('2026-10-20') + 3600)).toBe(sec('2026-10-20'));
    expect(snapToGrid(grid, sec('2026-10-29'))).toBe(sec('2026-10-29'));
    expect(snapToGrid(grid, sec('2026-10-10'))).toBeNull();
  });

  it('splits a gapped series into runs so the line breaks', () => {
    const grid = buildTimeGrid(bars, DAY, null);
    const pts = [0, 1, 2, 5, 6].map((k) => ({ time: bars[k], value: k }));
    expect(splitRuns(grid, pts).map((r) => r.map((p) => p.value))).toEqual([[0, 1, 2], [5, 6]]);
  });

  it('keeps a series thinned to every other bar whole, breaking only past its own spacing', () => {
    const grid = buildTimeGrid(bars, DAY, null);
    const pts = [0, 2, 4, 6].map((k) => ({ time: bars[k], value: k }));
    expect(splitRuns(grid, pts)).toHaveLength(1);
  });

  it('breaks a fill where its series gap', () => {
    const grid = buildTimeGrid(bars, DAY, null);
    const a = [0, 1, 4, 5].map((k) => ({ time: bars[k], value: 10 + k }));
    const b = [0, 1, 4, 5].map((k) => ({ time: bars[k], value: k }));
    const pts = fillBetween(splitRuns(grid, a), splitRuns(grid, b));
    expect(pts.map((p) => !!p.breakBefore)).toEqual([false, false, true, false]);
    expect(pts[2]).toMatchObject({ a: 14, b: 4 });
  });
});

describe('maxLayerTime', () => {
  it('reaches into drawings, positions, profiles and series', () => {
    const l = layer({
      series: [{ id: 's', title: 'S', pane: 'price', type: 'line', data: [{ time: '2026-12-01', value: 1 }] }],
      drawings: [{ type: 'position', side: 'long', entry: { time: '2026-10-01', price: 1 }, stop: 0.5, targets: [2], until: '2027-01-05' }],
    });
    expect(maxLayerTime([l.layer])).toBe(sec('2027-01-05'));
  });

  it('reaches a forecast target and the last bar of an analog', () => {
    const l = layer({
      drawings: [
        { type: 'forecast', origin: { time: '2026-10-01', price: 100 }, target: { time: '2026-12-01', price: 110 } },
        {
          type: 'analog',
          origin: { time: '2026-10-01', price: 100 },
          source_start: '2026-05-01',
          source_end: '2026-05-05',
          bars: [
            { time: '2026-10-02', open: 100, high: 102, low: 99, close: 101 },
            { time: '2027-02-01', open: 101, high: 103, low: 100, close: 102 },
          ],
        },
      ],
    });
    expect(maxLayerTime([l.layer])).toBe(sec('2027-02-01'));
  });
});

describe('planLayerChart', () => {
  const bars = weekdayBars('2026-10-01', 10);
  const grid = buildTimeGrid(bars, DAY, null);
  const t = (k: number) => new Date(bars[k] * 1000).toISOString().slice(0, 10);
  const line = (id: string, pane: string, values: number[]) => ({
    id,
    title: id.toUpperCase(),
    pane,
    type: 'line' as const,
    data: values.map((value, k) => ({ time: t(k), value })),
  });

  it('opens a pane per declared layer pane, labels it, and fills on the data the series draw', () => {
    const l = layer({
      panes: [{ id: 'osc', title: 'Oscillator', height: 0.3 }],
      series: [line('a', 'price', [10, 11, 12]), line('b', 'price', [8, 9, 10]), line('o', 'osc', [1, 2, 3]), line('lost', 'nowhere', [1, 2])],
      fills: [{ a: 'a', b: 'b', color: '#336699' }],
    });
    const { plan } = compile([l], grid);
    expect([...plan.series.values()].map((p) => [p.s.id, p.paneKey])).toEqual([
      ['a', 'price'],
      ['b', 'price'],
      ['o', 'test|pane|osc'],
    ]);
    const pane = plan.panes.get('test|pane|osc')!;
    expect(pane).toMatchObject({ title: 'Oscillator', height: 0.3 });
    expect(pane.entries.map((e) => e.text)).toEqual(['O 3.00']);
    expect(plan.fills).toHaveLength(1);
    expect(plan.fills[0].points.map((p) => [p.a, p.b])).toEqual([[10, 8], [11, 9], [12, 10]]);
  });

  it('names a pane value in its last point\'s colour, and in the series\' own when that point is transparent', () => {
    const hist = (color: string) => layer({
      panes: [{ id: 'osc', title: 'Oscillator' }],
      series: [
        { id: 'h', title: 'H', pane: 'osc', type: 'histogram', color: '#336699', data: [{ time: t(0), value: 1 }, { time: t(1), value: -1, color }] },
        line('o', 'osc', [1, 2]),
      ],
    });
    const colorOf = (color: string) => compile([hist(color)], grid).plan.panes.get('test|pane|osc')!.entries[0].color;
    expect(colorOf('rgba(239,83,80,0.4)')).toBe('rgba(239,83,80,1)');
    expect(colorOf('rgba(239,83,80,0)')).toBe('#336699');
    expect(colorOf('#ef535000')).toBe('#336699');
  });

  it('fills between series thinned to every few bars, breaking only where a line does', () => {
    const wide = weekdayBars('2026-10-01', 40);
    const g = buildTimeGrid(wide, DAY, null);
    const at = (k: number) => new Date(wide[k] * 1000).toISOString().slice(0, 10);
    // Weekly values on a daily chart, with the upper line missing a week (a hole in the data).
    const weekly = (id: string, base: number, skip: number[] = []) => ({
      id,
      title: id.toUpperCase(),
      pane: 'price',
      type: 'line' as const,
      data: [0, 5, 10, 15, 20, 25, 30, 35].filter((k) => !skip.includes(k)).map((k) => ({ time: at(k), value: base + k })),
    });
    const l = layer({
      series: [weekly('up', 110, [20]), weekly('lo', 90)],
      fills: [{ a: 'up', b: 'lo', color: '#336699' }],
    });
    const { plan } = compile([l], g);
    expect(plan.fills).toHaveLength(1);
    const pts = plan.fills[0].points;
    // Every shared week is a fill point, and the only break is the week the upper line skipped.
    expect(pts.map((p) => p.time)).toEqual([0, 5, 10, 15, 25, 30, 35].map((k) => wide[k]));
    expect(pts.filter((p) => p.breakBefore).map((p) => p.time)).toEqual([wide[25]]);
  });

  it("draws into the built-in RSI pane, skips the built-in's copy and says so", () => {
    const l = layer({
      panes: [{ id: 'rsi', title: 'RSI 14' }],
      series: [{ ...line('r', 'rsi', [40, 50]), title: 'RSI 14' }, { ...line('sma', 'price', [1, 2]), title: 'SMA 20' }, { ...line('r2', 'rsi', [30, 35]), title: 'Signal' }],
    });
    const builtins = { rsiPeriod: 14, maPeriods: [20], volume: false };
    const { plan } = compile([l], grid, builtins);
    expect([...plan.series.values()].map((p) => [p.s.id, p.paneKey])).toEqual([['r2', BUILTIN_RSI]]);
    expect(plan.panes.size).toBe(0);
    expect(plan.notes[0]).toMatch(/^test: layer pane 'RSI 14' merged into built-in RSI \(14\)/);
  });
});

describe('colors', () => {
  it('uses an agent alpha as given and adds ours to an opaque color', () => {
    expect(hasAlpha('rgba(1,2,3,0.2)')).toBe(true);
    expect(hasAlpha('#11223380')).toBe(true);
    expect(hasAlpha('#112233')).toBe(false);
    expect(fillOf('rgba(1,2,3,0.2)', 0.1)).toBe('rgba(1,2,3,0.2)');
    expect(fillOf('#112233', 0.1)).toBe('rgba(17,34,51,0.1)');
    expect(withAlpha('#abc', 1)).toBe('rgba(170,187,204,1)');
    expect(withAlpha('rgb(1, 2, 3)', 0.5)).toBe('rgba(1,2,3,0.5)');
  });
});

describe('drawing math', () => {
  it('fib: 0 at p2, 1 at p1, extensions past p1', () => {
    const lv = fibLevels(100, 200, [0, 0.5, 1, 1.618]);
    expect(lv.map((l) => l.price)).toEqual([200, 150, 100, expect.closeTo(38.2, 5)]);
    expect(fibLevels(100, 200).map((l) => l.ratio)).toEqual([0, 0.236, 0.382, 0.5, 0.618, 0.786, 1]);
  });

  it('channel: the parallel passes through p3', () => {
    const a = { i: 0, p: 100 };
    const b = { i: 10, p: 120 };
    expect(channelOffset(a, b, { i: 5, p: 125 })).toBe(15);
  });

  it('channel: on a log scale the second rail is parallel on screen and runs through p3', () => {
    const ctx = new Proxy({}, { get: () => () => ({ width: 0 }) }) as unknown as CanvasRenderingContext2D;
    const xm = { x: (i: number) => i * 10 } as unknown as XMapper;
    const y = (price: number) => 600 - 200 * Math.log(price);
    const p = new Painter(ctx, 400, 600, xm, { priceToCoordinate: y }, LAYER_FALLBACK.light, null);
    const a = { i: 0, p: 100 };
    const b = { i: 20, p: 200 };
    const c = { i: 10, p: 180 };
    const scene = {
      kind: 'channel', key: 'k', color: '#2962ff', a, b, c, offset: channelOffset(a, b, c), extend: 'none', fill: 'rgba(0,0,0,0.1)',
    } as unknown as ChannelScene;
    channel.paintOver(p, scene);
    const [first, second] = p.hits.filter((h) => h.t === 'seg') as { a: { x: number; y: number }; b: { x: number; y: number } }[];
    const slope = (s: typeof first) => (s.b.y - s.a.y) / (s.b.x - s.a.x);
    expect(slope(second)).toBeCloseTo(slope(first), 9);
    const atC = second.a.y + slope(second) * (100 - second.a.x);
    expect(atC).toBeCloseTo(y(180), 9);
  });

  it('pitchfork: median aims at the midpoint of p2 and p3', () => {
    expect(pitchforkMid({ i: 10, p: 200 }, { i: 20, p: 160 })).toEqual({ i: 15, p: 180 });
  });

  it('position: R multiples per target, long and short', () => {
    const long = positionStats('long', 100, 95, [110, 115]);
    expect(long.targets.map((t) => t.r)).toEqual([2, 3]);
    expect(long.stopPct).toBeCloseTo(-0.05);
    const short = positionStats('short', 100, 104, [92]);
    expect(short.targets[0].r).toBe(2);
    expect(short.targets[0].pct).toBeCloseTo(-0.08);
  });

  it('measure: delta, percent, bars and days', () => {
    const s = measureStats({ i: 3, p: 200 }, { i: 15, p: 180 }, sec('2026-10-01'), sec('2026-10-17'));
    expect(s).toEqual({ delta: -20, pct: -0.1, bars: 12, days: 16 });
    expect(measureReadout(s)).toBe('−20.00 (−10.00%) · 12 bars · 16d');
  });

  it('forecast box: label, odds and move, each only when present', () => {
    expect(forecastBox('Retest high', 0.3, 200, 260.08)).toBe('Retest high · 30% · 260.08  +30.0%');
    expect(forecastBox(undefined, null, 200, 180)).toBe('180.00  −10.0%');
    expect(fmtMove(200, 212.4)).toBe('212.40  +6.2%');
  });

  it('forecast verdict: hit at the first reaching bar, missed once the time passes, else pending', () => {
    const origin = { i: 2, p: 100 };
    const bars = (highs: number[], lows: number[]) => ({ highs, lows });
    const rise = { i: 6, p: 110 };
    expect(forecastVerdict(origin, rise, bars([0, 0, 100, 105, 111, 112], [0, 0, 99, 100, 104, 105]))).toEqual({ state: 'hit', i: 4 });
    expect(forecastVerdict(origin, rise, bars([0, 0, 100, 105, 108], [0, 0, 99, 100, 104]))).toEqual({ state: 'pending' });
    expect(forecastVerdict(origin, rise, bars([0, 0, 100, 105, 108, 108, 109], [0, 0, 99, 100, 104, 104, 104]))).toEqual({ state: 'missed' });
    // A bar before the origin never counts.
    expect(forecastVerdict(origin, rise, bars([120, 120, 100, 105], [0, 0, 99, 100]))).toEqual({ state: 'pending' });
    const fall = { i: 5, p: 90 };
    expect(forecastVerdict(origin, fall, bars([0, 0, 100, 99, 98], [0, 0, 99, 95, 89]))).toEqual({ state: 'hit', i: 4 });
    // The bars start after the origin: the target may have been reached on one the chart does not hold.
    expect(forecastVerdict({ i: -3, p: 100 }, { i: 2, p: 110 }, bars([100, 105, 108], [99, 100, 104]))).toEqual({ state: 'unknown' });
  });

  it('cone: widest band first', () => {
    const narrow = { upper: [{ i: 1, p: 105 }], lower: [{ i: 1, p: 95 }] };
    const wide = { upper: [{ i: 1, p: 120 }], lower: [{ i: 1, p: 80 }] };
    expect(orderConeBands([narrow, wide])).toEqual([wide, narrow]);
  });

  it('path labels sit above highs and below lows', () => {
    const pts = [{ i: 0, p: 10 }, { i: 1, p: 20 }, { i: 2, p: 12 }, { i: 3, p: 25 }, { i: 4, p: 11 }];
    expect(pathLabelSides(pts)).toEqual(['below', 'above', 'below', 'above', 'below']);
  });

  it('a profile over one bar still spans that bar\'s slot', () => {
    const grid = buildTimeGrid(weekdayBars('2026-10-19', 8), DAY, null);
    const one = (from: string, to: string) =>
      profile.resolve(
        { type: 'profile', from, to, rows: [{ low: 10, high: 11, volume: 5 }] },
        { grid, base: {} as SceneBase, P: () => null },
      )!;
    expect([one('2026-10-21', '2026-10-21').i0, one('2026-10-21', '2026-10-21').i1]).toEqual([2, 3]);
    expect([one('2026-10-21', '2026-10-23').i0, one('2026-10-21', '2026-10-23').i1]).toEqual([2, 4]);
  });

  it('profile rows scale to the largest and flag the value area', () => {
    const rows = profileRows(
      [
        { low: 10, high: 11, volume: 50, up_volume: 20 },
        { low: 11, high: 12, volume: 100 },
      ],
      [11, 12],
    );
    expect(rows.map((r) => r.frac)).toEqual([0.5, 1]);
    expect(rows[0].upShare).toBeCloseTo(0.4);
    expect(rows[1].upShare).toBeNull();
    expect(rows.map((r) => r.inValueArea)).toEqual([false, true]);
  });
});

describe('extendSegment', () => {
  const a = { x: 10, y: 10 };
  const b = { x: 20, y: 20 };
  it('extends rays to the pane edge on the asked side', () => {
    expect(extendSegment(a, b, 'right', 0, 100)).toEqual([a, { x: 100, y: 100 }]);
    expect(extendSegment(a, b, 'left', 0, 100)).toEqual([{ x: 0, y: 0 }, b]);
    expect(extendSegment(b, a, 'right', 0, 100)).toEqual([{ x: 100, y: 100 }, a]);
    expect(extendSegment(a, b, 'none', 0, 100)).toEqual([a, b]);
  });
});

describe('resolveLayerScene', () => {
  const bars = weekdayBars('2026-09-01', 40);
  const lastDay = new Date(bars[39] * 1000).toISOString().slice(0, 10);

  function sceneFor(drawings: LayerDrawing[], extra: Partial<ChartLayer> = {}) {
    const l = layer({ drawings, ...extra });
    const grid = buildTimeGrid(bars, DAY, maxLayerTime([l.layer]));
    return { scene: compile([l], grid).scene, grid };
  }

  it('places future anchors past the last bar', () => {
    const { scene } = sceneFor([{ type: 'vline', time: '2026-11-04', label: 'Earnings' }]);
    expect(drawing(scene, 'vline').i).toBeGreaterThan(39);
  });

  it('leaves a forecast and a measure their direction for the painter, and keeps the drawing\'s own color', () => {
    const { scene } = sceneFor([
      { type: 'forecast', origin: { time: lastDay, price: 100 }, target: { time: '2026-11-10', price: 120 }, probability: 0.3, label: 'Breakout' },
      { type: 'forecast', origin: { time: lastDay, price: 100 }, target: { time: '2026-11-10', price: 90 } },
      { type: 'forecast', origin: { time: lastDay, price: 100 }, target: { time: '2026-11-10', price: 90 }, color: '#123456' },
    ]);
    const [up, down, custom] = scene.drawings.filter((d) => d.kind === 'forecast') as Extract<SceneDrawing, { kind: 'forecast' }>[];
    expect([up, down, custom].map((f) => [f.up, f.given])).toEqual([[true, null], [false, null], [false, '#123456']]);
    expect(up.origin).toEqual({ i: 39, p: 100 });
    expect(up.target.i).toBeGreaterThan(39);
    expect(up.box).toBe('Breakout · 30% · 120.00  +20.0%');
  });

  it('places analog bars on the grid, marks its source and words the end', () => {
    const t = (k: number) => new Date(bars[k] * 1000).toISOString().slice(0, 10);
    const { scene } = sceneFor([
      {
        type: 'analog',
        origin: { time: lastDay, price: 100 },
        source_start: t(5),
        source_end: t(8),
        bars: [
          { time: '2026-11-03', open: 100, high: 103, low: 99, close: 102 },
          { time: '2026-11-04', open: 102, high: 112, low: 101, close: 110 },
        ],
      },
    ]);
    const a = drawing(scene, 'analog');
    expect(a.bars).toHaveLength(2);
    expect(a.bars[0].i).toBeGreaterThan(39);
    expect(a.bars[1].i).toBe(a.bars[0].i + 1);
    expect(a.source).toEqual({ i0: 5, i1: 8 });
    expect(a.end).toBe(fmtMove(100, 110));
    expect(a.end).toBe('110.00  +10.0%');
  });

  it('defaults a position to a fixed span, leaving its side unnamed for the painter to word', () => {
    const { scene } = sceneFor([
      { type: 'position', side: 'long', entry: { time: lastDay, price: 100 }, stop: 95, targets: [110] },
    ]);
    const p = drawing(scene, 'position');
    expect(p.i1 - p.i0).toBe(DEFAULT_POSITION_BARS);
    expect(p.label).toBeUndefined();
    expect(p.side).toBe('long');
    expect(p.targets[0].r).toBe(2);
  });

  it('keeps an agent wash under its cap so the candles stay readable', () => {
    const from = new Date(bars[2] * 1000).toISOString().slice(0, 10);
    const to = new Date(bars[5] * 1000).toISOString().slice(0, 10);
    const { scene } = sceneFor([], { regimes: [{ from, to, color: 'rgba(255,0,0,0.4)' }, { from, to, color: 'rgba(255,0,0,0.05)' }] });
    expect(scene.regimes.map((r) => r.fill)).toEqual(['rgba(255,0,0,0.1)', 'rgba(255,0,0,0.05)']);
  });

  it('shades a regime through its inclusive last bar', () => {
    const from = new Date(bars[2] * 1000).toISOString().slice(0, 10);
    const to = new Date(bars[5] * 1000).toISOString().slice(0, 10);
    const { scene } = sceneFor([], { regimes: [{ from, to, color: '#ff0000', label: 'Risk-off' }] });
    expect(scene.regimes[0]).toMatchObject({ i0: 1.5, i1: 5.5, fill: 'rgba(255,0,0,0.08)' });
  });

  it('skips an unknown drawing type from a newer server', () => {
    const { scene } = sceneFor([{ type: 'hologram' } as unknown as LayerDrawing, { type: 'vline', time: lastDay }]);
    expect(scene.drawings.map((d) => d.kind)).toEqual(['vline']);
  });
});

describe('markers', () => {
  const bars = weekdayBars('2026-10-01', 10);
  it('snaps bar markers and turns a priced future marker into a label', () => {
    const l = layer({
      markers: [
        { time: '2026-10-03', position: 'belowBar', shape: 'arrowUp', text: 'Buy' }, // Saturday → Friday
        { time: '2026-10-06', position: 'atPrice', price: 210, shape: 'circle' },
        { time: '2026-11-20', position: 'aboveBar', price: 230, shape: 'circle', text: 'Target' },
        { time: '2026-11-20', position: 'aboveBar', shape: 'circle' }, // no price: dropped
      ],
    });
    const grid = buildTimeGrid(bars, DAY, maxLayerTime([l.layer]));
    const { markers, scene, markerBoxes } = compile([l], grid);
    const future = scene.drawings.filter((d) => d.kind === 'label');
    expect(markers.map((m) => [m.time, m.position])).toEqual([
      [sec('2026-10-02'), 'belowBar'],
      [sec('2026-10-06'), 'atPriceMiddle'],
    ]);
    expect(markers[1]).toMatchObject({ position: 'atPriceMiddle', price: 210 });
    // The painter keeps labels off each marker's caption: the same two, by grid index.
    expect(markerBoxes).toEqual([
      { i: 1, position: 'belowBar', price: undefined, text: 'Buy' },
      { i: 3, position: 'atPriceMiddle', price: 210, text: '' },
    ]);
    expect(future).toHaveLength(1);
    expect(future[0]).toMatchObject({ kind: 'label', text: 'Target' });
  });
});

describe('elements', () => {
  it('lists explained elements with their anchors for the ask', () => {
    const l = layer({
      levels: [{ price: 200, pane: 'price', title: 'Support', note: 'Prior breakout.' }, { price: 150, pane: 'price' }],
      drawings: [
        { type: 'segment', p1: { time: '2026-10-01', price: 1 }, p2: { time: '2026-10-05', price: 2 }, label: 'Trend' },
        { type: 'segment', p1: { time: '2026-10-01', price: 1 }, p2: { time: '2026-10-05', price: 2 } },
        { type: 'vline', time: '2026-10-07', note: 'Earnings' },
      ],
    });
    const els = compile([l], buildTimeGrid(weekdayBars('2026-10-01', 10), DAY, null)).elements.explain;
    expect([...els.keys()]).toEqual(['test|level|0', 'test|drawing|0', 'test|drawing|2']);
    const trend = els.get('test|drawing|0')!;
    expect(trend.ref).toMatchObject({ layerId: 'test', layerTitle: 'Test layer', kind: 'drawing', type: 'segment', label: 'Trend' });
    expect(trend.ref.anchors).toHaveLength(2);
    expect(els.get('test|drawing|2')!).toMatchObject({ label: null, typeKey: 'marketView.layerExplain.types.vline' });
  });

  it('prices a time-only anchor at the bar close, or drops it', () => {
    const closeAt = (t: string) => (t === '2026-10-07' ? 212.5 : null);
    expect(
      cleanAnchors(
        [
          { time: '2026-10-07', price: NaN },
          { time: '2026-12-01', price: NaN },
          { time: '2026-10-08', price: 99 },
        ],
        closeAt,
      ),
    ).toEqual([
      { time: '2026-10-07', price: 212.5 },
      { time: '2026-10-08', price: 99 },
    ]);
  });
});

describe('axis labels', () => {
  const spec = (p: Partial<AxisLineSpec> & { key: string; price: number }): AxisLineSpec => ({
    id: { layerKey: 'test', kind: 'level', idx: p.key },
    host: 'price',
    color: '#26a69a',
    style: 'dashed',
    title: '',
    alert: null,
    ...p,
  });

  it('merges a level and the alert at its price into one label with a bell', () => {
    const merged = mergeAxisLines([
      spec({ key: 'lv', price: 523.78364, title: '61.8% 524' }),
      spec({ key: 'al', price: 523.78, title: 'AMD reclaim', alert: 'above', color: '#D8B04C', style: 'sparse' }),
      spec({ key: 'low', price: 492.33, title: '20d low 492' }),
    ]);
    expect(merged).toHaveLength(2);
    const at524 = merged.find((m) => m.key === 'lv')!;
    expect(at524.title).toBe('\u{1F514}↑ 61.8% 524');
    expect(at524.color).toBe('#26a69a');
    expect(at524.style).toBe('dashed');
    expect(at524.members.map((m) => m.key)).toEqual(['al', 'lv']);
  });

  it('keeps an alert alone as its own belled line', () => {
    const [m] = mergeAxisLines([spec({ key: 'al', price: 100, title: 'Break', alert: 'below', style: 'sparse' })]);
    expect(m.title).toBe('\u{1F514}↓ Break');
    expect(m.style).toBe('sparse');
  });

  it('merges within a tick or 0.05%, never across scales or wider gaps', () => {
    const merged = mergeAxisLines([
      spec({ key: 'a', price: 70, host: 'rsi' }),
      spec({ key: 'b', price: 70, host: 'price' }),
      spec({ key: 'c', price: 524, title: 'One' }),
      spec({ key: 'd', price: 523.8, title: 'Two' }),
      spec({ key: 'e', price: 520, title: 'Three' }),
    ]);
    const groups = merged.map((m) => m.members.map((x) => x.key).sort().join('+')).sort();
    expect(groups).toEqual(['a', 'b', 'c+d', 'e']);
    expect(merged.find((m) => m.members.length === 2)!.title).toBe('Two · One');
  });
});

describe('built-in duplicates', () => {
  const rsiData = [{ time: '2026-10-01', value: 55 }];
  const base = layer({
    panes: [{ id: 'rsi', title: 'RSI 14', height: 0.2 }],
    series: [
      { id: 'r', title: 'RSI 14', pane: 'rsi', type: 'line', data: rsiData },
      { id: 'sma', title: 'SMA 20', pane: 'price', type: 'line', data: [{ time: '2026-10-01', value: 1 }] },
      { id: 'ema', title: 'EMA 20', pane: 'price', type: 'line', data: [{ time: '2026-10-01', value: 1 }] },
    ],
  }).layer;

  it('reads RSI and SMA periods from their usual names only', () => {
    expect(rsiPeriodOf('RSI 14')).toBe(14);
    expect(rsiPeriodOf('RSI(7)')).toBe(7);
    expect(rsiPeriodOf('RSI')).toBe(14);
    expect(rsiPeriodOf('Stoch RSI 14')).toBeNull();
    expect(smaPeriodOf('MA20')).toBe(20);
    expect(smaPeriodOf('SMA (50)')).toBe(50);
    expect(smaPeriodOf('EMA 20')).toBeNull();
  });

  it("joins an RSI pane of the built-in's period to the built-in pane and skips the copy", () => {
    const plan = planBuiltinDuplicates(base, { rsiPeriod: 14, maPeriods: [20, 50], volume: true });
    expect([...plan.intoRsi]).toEqual(['rsi']);
    expect([...plan.skipSeries].sort()).toEqual(['r', 'sma']);
    expect(plan.notes[0]).toBe("layer pane 'RSI 14' merged into built-in RSI (14); its 'RSI 14' not drawn twice");
  });

  it('keeps an RSI of another period, an EMA, and a series a fill needs', () => {
    const plan = planBuiltinDuplicates(base, { rsiPeriod: 7, maPeriods: [50], volume: true });
    expect(plan.intoRsi.size).toBe(0);
    expect(plan.skipSeries.size).toBe(0);
    const filled = planBuiltinDuplicates({ ...base, fills: [{ a: 'sma', b: 'ema', color: '#888888' }] }, { rsiPeriod: null, maPeriods: [20], volume: true });
    expect(filled.skipSeries.has('sma')).toBe(false);
  });

  it('folds a Volume histogram pane into the chart’s volume, not a Volume line', () => {
    const volumePane = (type: 'histogram' | 'line') => layer({
      panes: [{ id: 'vol', title: 'Volume', height: 0.2 }],
      series: [{ id: 'v', title: 'Volume', pane: 'vol', type, data: [{ time: '2026-10-01', value: 1e6 }] }],
    }).layer;
    const builtins = { rsiPeriod: null, maPeriods: [], volume: true };
    expect([...planBuiltinDuplicates(volumePane('histogram'), builtins).dropPanes]).toEqual(['vol']);
    // A line of volume (smoothed, say) is not what the floor histogram shows.
    const line = planBuiltinDuplicates(volumePane('line'), builtins);
    expect(line.dropPanes.size).toBe(0);
    expect(line.skipSeries.size).toBe(0);
  });

  it('does not pull a pane off its own scale into RSI', () => {
    const macd = {
      ...base,
      series: [
        ...base.series,
        { id: 'm', title: 'MACD', pane: 'rsi', type: 'line' as const, data: [{ time: '2026-10-01', value: -3 }] },
      ],
    };
    expect(planBuiltinDuplicates(macd, { rsiPeriod: 14, maPeriods: [], volume: false }).intoRsi.size).toBe(0);
  });
});
