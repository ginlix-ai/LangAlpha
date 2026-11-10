/**
 * The one framing rule for a chart layer: the view a layer opens in, used by
 * the live chart (a fresh publish, a card's "Open chart") and by the sandbox's
 * snapshot, so the agent sees the layer the way the user will.
 *
 * Indices are logical bar indices on the chart's time scale, and a time past
 * the last bar maps through the same session-aware future slots the painter
 * uses (`gridFor` / `resolveIndex`), so a forward drawing is framed where it
 * is drawn.
 */

import { defaultBarsView, type DefaultBarsView } from '@/lib/bars/chartDataLoaders';
import type { ChartLayer } from '../types/chartLayer';
import { TARGET_BAR_SPACING } from './chartConstants';
import { barCountedEnds, layerTimeExtent, seriesProjectionEnd } from './chartLayer/layerTimes';
import { gridFor, layerTimeToSeconds, resolveIndex, type TimeGrid } from './chartLayer/timeGrid';

/** Room each side of the layer's extent, as a fraction of it. */
export const FRAME_PAD = 0.08;
/** Bars of room past the last bar when the frame reaches it, as the range presets leave. */
export const FRAME_FUTURE_GUTTER = 2;

/** A marker's text is centred on its bar, so half of it hangs past the bar: its width per character. */
const MARKER_CHAR_PX = 6.5;
/** The most bars of room a marker's text is given past the last bar; longer text is the layer's to shorten. */
const MARKER_TEXT_GUTTER_MAX = 16;

/**
 * Bars of room past the last bar that marker text near it needs to be read
 * whole. "Today's low" marked on the latest bar is the common case, and the
 * plain gutter cuts its label at the plot's edge.
 */
function markerTextGutter(layers: readonly ChartLayer[], grid: TimeGrid, n: number, barPx: number): number {
  let bars = 0;
  for (const layer of layers) {
    for (const m of layer.markers ?? []) {
      if (!m.text) continue;
      const secs = layerTimeToSeconds(m.time);
      const i = secs == null ? null : resolveIndex(grid, secs);
      if (i == null) continue;
      const overhangPx = (m.text.length * MARKER_CHAR_PX) / 2 - (n - 1 - i) * barPx;
      if (overhangPx > 0) bars = Math.max(bars, Math.ceil(overhangPx / barPx) + 1);
    }
  }
  return Math.min(bars, MARKER_TEXT_GUTTER_MAX);
}

export interface LogicalFrame {
  from: number;
  to: number;
}

/**
 * The logical range to frame `layers` in on a chart of `chartWidthPx` holding
 * `barTimes`: the layer's extent padded `FRAME_PAD` each side (plus
 * `FRAME_FUTURE_GUTTER` when it reaches the last bar, more where a marker's
 * text there would be cut), never tighter than the
 * default view's bar count for the interval, never wider than the loaded
 * history plus the layer's own future. A layer with no time extent gets the
 * default view. Null with no bars.
 */
export function layerFrame(
  layers: ChartLayer | readonly ChartLayer[],
  barTimes: readonly number[],
  chartWidthPx: number,
  interval: string,
  opts: { defaultView?: DefaultBarsView } = {},
): LogicalFrame | null {
  const n = barTimes.length;
  if (n === 0) return null;
  const barSpacing = TARGET_BAR_SPACING[interval] || 8;
  const width = chartWidthPx > 0 ? chartWidthPx : 800;
  const lastBar = barTimes[n - 1];
  const projected = seriesProjectionEnd(layers, lastBar);
  const anchored = layerTimeExtent(layers);
  // A series projected past the last bar is framed from that bar to its end.
  const extent = projected == null
    ? anchored
    : { min: Math.min(anchored?.min ?? lastBar, lastBar), max: Math.max(anchored?.max ?? projected, projected) };
  if (!extent) {
    return defaultBarsView({ defaultView: opts.defaultView ?? 'centered', chartWidth: width, barSpacing, dataLen: n });
  }
  const grid = gridFor(barTimes, interval, { until: extent.max });
  const i0 = resolveIndex(grid, extent.min);
  let i1 = resolveIndex(grid, extent.max);
  if (i0 == null || i1 == null) return null;
  for (const end of barCountedEnds(layers)) {
    const at = resolveIndex(grid, end.time);
    if (at != null) i1 = Math.max(i1, at + end.bars);
  }

  const pad = Math.max((i1 - i0) * FRAME_PAD, 1);
  let from = i0 - pad;
  let to = i1 + pad;
  const lo = Math.min(-0.5, from);
  // Sized on the span the frame shows with the plain gutter, which on a short
  // history is the whole history, not the default view's bar count; widening
  // it for the text only narrows the bars a little.
  const shown = Math.min(width / barSpacing, Math.max(n - 1, to) + FRAME_FUTURE_GUTTER - lo);
  const barPx = width / Math.max(to - from + FRAME_FUTURE_GUTTER, shown);
  const list: readonly ChartLayer[] = Array.isArray(layers) ? layers : [layers as ChartLayer];
  const gutter = Math.max(FRAME_FUTURE_GUTTER, markerTextGutter(list, grid, n, barPx));
  if (to >= n - 1) to = Math.max(to + FRAME_FUTURE_GUTTER, n - 1 + gutter);

  // Bounds: the loaded history (with the presets' gutter) and the layer itself.
  const hi = Math.max(n - 1 + FRAME_FUTURE_GUTTER, to);
  const minSpan = Math.min(width / barSpacing, hi - lo);
  if (to - from < minSpan) {
    const grow = (minSpan - (to - from)) / 2;
    from -= grow;
    to += grow;
    if (from < lo) { to += lo - from; from = lo; }
    if (to > hi) { from -= to - hi; to = hi; }
    from = Math.max(from, lo);
  }
  return { from, to };
}
