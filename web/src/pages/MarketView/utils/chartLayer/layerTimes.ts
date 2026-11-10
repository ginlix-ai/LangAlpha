/** How far in time a layer reaches: the grid's future room, and the span the framing rule shows. */

import type { ChartLayer, LayerTime } from '../../types/chartLayer';
import { drawingKind } from './kinds';
import { layerTimeToSeconds } from './timeGrid';

/**
 * Every time a layer's drawings, markers and regimes are anchored at; `framed`
 * keeps to the times the framing rule shows (`DrawingKind.frameTimes`).
 */
function eachAnchoredTime(layer: ChartLayer, see: (t: LayerTime | null | undefined) => void, framed = false): void {
  for (const d of layer.drawings) {
    const kind = drawingKind(d.type);
    if (!kind) continue;
    for (const t of (framed && kind.frameTimes ? kind.frameTimes(d) : kind.times(d))) see(t);
  }
  for (const m of layer.markers) see(m.time);
  for (const r of layer.regimes) {
    see(r.from);
    see(r.to);
  }
}

/** Furthest time any element of these layers reaches (series included), in chart seconds. */
export function maxLayerTime(layers: readonly ChartLayer[]): number | null {
  let max: number | null = null;
  const see = (t: LayerTime | null | undefined) => {
    const s = layerTimeToSeconds(t);
    if (s != null && (max == null || s > max)) max = s;
  };
  for (const layer of layers) {
    for (const s of layer.series) for (const d of s.data) see(d.time);
    eachAnchoredTime(layer, see);
  }
  return max;
}

/**
 * Earliest and latest time the layer's elements are anchored at, in chart
 * seconds: the span the framing rule shows. Series, levels and alerts are left
 * out: a series spans whatever history the agent computed it over, and a level
 * or an alert has no time. So is a replay's source stretch (`frameTimes`).
 */
export function layerTimeExtent(layers: ChartLayer | readonly ChartLayer[]): { min: number; max: number } | null {
  let min = Infinity;
  let max = -Infinity;
  const see = (t: LayerTime | null | undefined) => {
    const s = layerTimeToSeconds(t);
    if (s == null) return;
    if (s < min) min = s;
    if (s > max) max = s;
  };
  const list: readonly ChartLayer[] = Array.isArray(layers) ? layers : [layers as ChartLayer];
  for (const layer of list) eachAnchoredTime(layer, see, true);
  return Number.isFinite(min) ? { min, max } : null;
}

/** The drawings whose end is a count of bars past one of their times (`DrawingKind.barsAfter`), the time in chart seconds. */
export function barCountedEnds(layers: ChartLayer | readonly ChartLayer[]): { time: number; bars: number }[] {
  const ends: { time: number; bars: number }[] = [];
  const list: readonly ChartLayer[] = Array.isArray(layers) ? layers : [layers as ChartLayer];
  for (const layer of list) {
    for (const d of layer.drawings) {
      const end = drawingKind(d.type)?.barsAfter?.(d);
      const time = end ? layerTimeToSeconds(end.time) : null;
      if (end && time != null) ends.push({ time, bars: end.bars });
    }
  }
  return ends;
}

/**
 * The furthest a series reaches past `after` (the last bar), in chart seconds,
 * or null: a forward projection (`c.plot` with values past the last candle).
 * Framing shows that tail, though not the history the series also spans.
 */
export function seriesProjectionEnd(layers: ChartLayer | readonly ChartLayer[], after: number): number | null {
  let end: number | null = null;
  const list: readonly ChartLayer[] = Array.isArray(layers) ? layers : [layers as ChartLayer];
  for (const layer of list) {
    for (const series of layer.series) {
      for (const d of series.data) {
        const s = layerTimeToSeconds(d.time);
        if (s != null && s > after && (end == null || s > end)) end = s;
      }
    }
  }
  return end;
}
