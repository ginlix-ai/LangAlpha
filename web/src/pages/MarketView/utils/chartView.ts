/**
 * The chart's visible window as times, and back. A chat message carries the
 * window as `chart_views` times (`buildChartView`) and the snapshot page
 * turns them back into a logical range (`logicalRangeForTimes`); both go
 * through the same session-aware time grid the layer painter uses, so a right
 * edge in the empty room past the last bar names the slot that is drawn there
 * and maps back onto it.
 */

import { displaySpelling } from '@/lib/bars/exchanges';
import type { Locale } from '@/lib/locale';
import type { ChartView } from '../types/chartView';
import { normalizeTimeframe } from './chartInstanceKey';
import { gridFor, indexToSeconds, layerTimeToSeconds, resolveIndex, secondsToLayerTime } from './chartLayer/timeGrid';
import type { LogicalFrame } from './layerFrame';

/**
 * The chart view for a chart on screen, or null when there is nothing to
 * describe (no bars, no range, a chart with no size). Edges round to the
 * nearest bar or slot, so a snapshot framed from it is within half a bar.
 */
export function buildChartView({
  symbol,
  interval,
  theme,
  locale,
  barTimes,
  range,
  width,
  height,
}: {
  symbol: string;
  interval: string;
  theme: 'dark' | 'light';
  locale: Locale;
  barTimes: readonly number[];
  range: LogicalFrame | null;
  width: number;
  height: number;
}): ChartView | null {
  if (!symbol || barTimes.length === 0 || !range) return null;
  if (!Number.isFinite(range.from) || !Number.isFinite(range.to)) return null;
  const w = Math.round(width);
  const h = Math.round(height);
  if (w < 1 || h < 1) return null;
  const timeframe = normalizeTimeframe(interval);
  const fromIdx = Math.round(range.from);
  const toIdx = Math.max(Math.round(range.to), fromIdx);
  // Slots skip nights and weekends: the grid holds as many as the right edge reaches past the last bar.
  const grid = gridFor(barTimes, timeframe, { minSlots: toIdx - (barTimes.length - 1) });
  const fromSecs = indexToSeconds(grid, fromIdx, 'interpolate');
  const toSecs = indexToSeconds(grid, toIdx, 'interpolate');
  if (fromSecs == null || toSecs == null) return null;
  const daily = timeframe === '1day';
  return {
    symbol: displaySpelling(symbol),
    timeframe,
    theme,
    locale,
    from: secondsToLayerTime(fromSecs, daily),
    to: secondsToLayerTime(toSecs, daily),
    width: w,
    height: h,
  };
}

/** Times in the chart's convention → the logical range that shows them. Null when unreadable. */
export function logicalRangeForTimes(
  barTimes: readonly number[],
  interval: string,
  from: string,
  to: string,
): LogicalFrame | null {
  if (barTimes.length === 0) return null;
  const a = layerTimeToSeconds(from);
  const b = layerTimeToSeconds(to);
  if (a == null || b == null) return null;
  const lo = Math.min(a, b);
  const hi = Math.max(a, b);
  const grid = gridFor(barTimes, normalizeTimeframe(interval), { until: hi });
  const i0 = resolveIndex(grid, lo);
  const i1 = resolveIndex(grid, hi);
  if (i0 == null || i1 == null) return null;
  // A zero-width window cannot be shown; give it one bar.
  return i1 - i0 < 1 ? { from: i0 - 0.5, to: i0 + 0.5 } : { from: i0, to: i1 };
}
