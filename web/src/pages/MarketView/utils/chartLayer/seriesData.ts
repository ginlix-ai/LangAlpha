/** A layer series as lightweight-charts data: snapped to the grid, split where the agent's line breaks. */

import type { LayerSeries, LayerSeriesPoint } from '../../types/chartLayer';
import { layerTimeToSeconds, resolveIndex, snapToGrid, type TimeGrid } from './timeGrid';

export interface SnappedPoint {
  time: number;
  value: number;
  color?: string;
}

/** Series data snapped to the grid, deduped (last wins), ascending. */
export function snapSeriesData(grid: TimeGrid, data: readonly LayerSeriesPoint[]): SnappedPoint[] {
  const byTime = new Map<number, SnappedPoint>();
  for (const pt of data) {
    if (!Number.isFinite(pt.value)) continue;
    const secs = layerTimeToSeconds(pt.time);
    if (secs == null) continue;
    const t = snapToGrid(grid, secs);
    if (t == null) continue;
    byTime.set(t, pt.color ? { time: t, value: pt.value, color: pt.color } : { time: t, value: pt.value });
  }
  return [...byTime.values()].sort((a, b) => a.time - b.time);
}

/** Past this many runs a series is drawn whole: each run is a chart series of its own. */
const MAX_SERIES_RUNS = 64;

/**
 * Split snapped series data into runs where the agent's line breaks.
 * lightweight-charts bridges a gap in a line series, and chartkit drops NaN
 * points, so a hole in the data is the only break there is (Pine's `linebr`).
 * A hole is a step past the series' own spacing: on every bar any skipped bar
 * breaks, while a series thinned to every few bars (weekly values on a daily
 * chart) breaks only past one and a half of its usual steps. A series that
 * would still split into more than `MAX_SERIES_RUNS` runs is drawn whole.
 */
export function splitRuns<T extends { time: number }>(grid: TimeGrid, data: readonly T[]): T[][] {
  const placed: { pt: T; idx: number }[] = [];
  for (const pt of data) {
    const idx = resolveIndex(grid, pt.time);
    if (idx != null) placed.push({ pt, idx: Math.round(idx) });
  }
  if (!placed.length) return [];
  const steps = placed.slice(1).map((p, i) => p.idx - placed[i].idx).sort((a, b) => a - b);
  const usual = steps.length ? steps[Math.floor(steps.length / 2)] : 1;
  const maxStep = usual <= 1 ? 1 : Math.ceil(usual * 1.5);
  const runs: T[][] = [];
  let run: T[] = [];
  let prev: number | null = null;
  for (const { pt, idx } of placed) {
    if (prev != null && idx - prev > maxStep && run.length) {
      runs.push(run);
      run = [];
    }
    run.push(pt);
    prev = idx;
  }
  if (run.length) runs.push(run);
  return runs.length > MAX_SERIES_RUNS ? [placed.map((p) => p.pt)] : runs;
}

/**
 * A layer series as the LWC series that draw it: one per contiguous run, since
 * a line bridges whitespace and the agent's gaps must break (bars do not
 * bridge, so a histogram stays whole).
 */
export function seriesRuns(s: LayerSeries, pts: readonly SnappedPoint[], grid: TimeGrid): SnappedPoint[][] {
  if (!pts.length) return [];
  return s.type === 'histogram' ? [[...pts]] : splitRuns(grid, pts);
}

export interface FillPoint {
  time: number;
  a: number;
  b: number;
  /** Starts a new run: the area breaks here rather than bridging a gap. */
  breakBefore?: boolean;
}

/**
 * Points where both series of a fill have a value, for the area between them.
 * It takes the runs the series draw, so the area breaks exactly where a line
 * does, whichever of the two breaks (a series thinned to every few bars is one
 * run, not a point per run).
 */
export function fillBetween(a: readonly (readonly SnappedPoint[])[], b: readonly (readonly SnappedPoint[])[]): FillPoint[] {
  const bAt = new Map<number, { value: number; run: number }>();
  b.forEach((run, r) => run.forEach((pt) => bAt.set(pt.time, { value: pt.value, run: r })));
  const out: FillPoint[] = [];
  let prev: { a: number; b: number } | null = null;
  a.forEach((run, ra) => {
    for (const pt of run) {
      const hit = bAt.get(pt.time);
      if (!hit) continue;
      const point: FillPoint = { time: pt.time, a: pt.value, b: hit.value };
      if (prev && (prev.a !== ra || prev.b !== hit.run)) point.breakBefore = true;
      out.push(point);
      prev = { a: ra, b: hit.run };
    }
  });
  return out;
}
