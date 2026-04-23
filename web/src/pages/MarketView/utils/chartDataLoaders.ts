/**
 * Shared chart data-loading primitives used by the MarketView `MarketChart`
 * and the dashboard `ChartWidget`.
 *
 * These are small, pure helpers rather than a custom hook so neither caller
 * has to give up its own refs, state, or lifecycle. Extracting them here
 * means a fix to e.g. dedupe logic lands in both places at once.
 */

import { BARS_PER_DAY, INITIAL_LOAD_DAYS, STAGE1_LOAD_DAYS } from './chartConstants';

export interface TimedBar {
  time: number;
}

/**
 * Format a Date as YYYY-MM-DD in America/New_York wall-clock. The chart
 * backend interprets date-only bounds in ET, so using UTC (`toISOString`)
 * produces off-by-one errors in the ~4h window between ET evening and UTC
 * midnight — the caller asks for "tomorrow" and gets empty intraday data.
 */
export function etDateStr(d: Date = new Date()): string {
  // 'en-CA' gives ISO-8601 style YYYY-MM-DD.
  return d.toLocaleDateString('en-CA', { timeZone: 'America/New_York' });
}

/**
 * Decide the initial from/to date range for a given bar interval.
 *
 * Uses `STAGE1_LOAD_DAYS[interval]` when defined (fast first render, with a
 * background stage-2 backfill filling the rest of `INITIAL_LOAD_DAYS`);
 * otherwise falls back to `INITIAL_LOAD_DAYS[interval]`. `maxMaPeriod` adds
 * lookback overhead so moving averages can render on the first paint
 * without waiting for the backfill — pass 0 (or omit) when the caller has
 * no MAs to warm up (the dashboard widget's case).
 *
 * Returns `{ fromStr: undefined, toStr: undefined }` when `days === 0`,
 * which the backend treats as "full available history".
 */
export function computeInitialLoadRange(
  interval: string,
  { now = new Date(), maxMaPeriod = 0 }: { now?: Date; maxMaPeriod?: number } = {},
): { fromStr?: string; toStr?: string; days: number } {
  const days = (interval in STAGE1_LOAD_DAYS)
    ? STAGE1_LOAD_DAYS[interval]
    : (INITIAL_LOAD_DAYS[interval] ?? 90);
  if (days <= 0) return { fromStr: undefined, toStr: undefined, days: 0 };

  const overheadDays = maxMaPeriod > 0
    ? Math.ceil((maxMaPeriod / (BARS_PER_DAY[interval] || 1)) * 1.5)
    : 0;

  const toStr = etDateStr(now);
  const from = new Date(now);
  from.setDate(from.getDate() - days - overheadDays);
  return { fromStr: etDateStr(from), toStr, days };
}

/**
 * Compute a logical-range window that centers the latest bar on the chart
 * with roughly half the container width reserved as empty future-space on
 * the right. Used by MarketView's and the dashboard's default view.
 */
export function centerLatestBarView({
  chartWidth,
  barSpacing,
  dataLen,
}: {
  chartWidth: number;
  barSpacing: number;
  dataLen: number;
}): { from: number; to: number } {
  const halfBars = Math.floor(chartWidth / barSpacing / 2);
  return { from: dataLen - halfBars, to: dataLen + halfBars };
}

/**
 * Merge a set of newly-fetched bars into an existing, time-sorted timeline,
 * de-duplicating by `.time`. Returns the merged array plus the number of
 * bars that landed *before* the existing data (the prepend count — callers
 * use this to compensate the visible logical range so the user's current
 * viewport doesn't jump when older history arrives).
 */
export function dedupeMergeByTime<T extends TimedBar>(
  existing: T[],
  incoming: T[],
): { merged: T[]; prependedCount: number } {
  if (!incoming?.length) return { merged: existing, prependedCount: 0 };
  const map = new Map(existing.map((d) => [d.time, d]));
  for (const d of incoming) if (!map.has(d.time)) map.set(d.time, d);
  const merged = Array.from(map.values()).sort((a, b) => a.time - b.time);
  return { merged, prependedCount: merged.length - existing.length };
}

/**
 * Given the unix-seconds timestamp of the current oldest bar, produce the
 * YYYY-MM-DD from/to date strings for a fetch that asks for `days` worth of
 * bars *before* that point. There's a 1-day gap between `toDate` and the
 * oldest bar so the new range doesn't overlap with what we already have.
 */
export function rangeBeforeOldest(
  oldestSec: number,
  days: number,
): { fromStr: string; toStr: string } {
  // Bar timestamps are market-hours moments, so the UTC and ET calendar
  // dates agree. Using UTC arithmetic keeps this function TZ-independent.
  const oldest = new Date(oldestSec * 1000);
  const to = new Date(oldest);
  to.setUTCDate(to.getUTCDate() - 1);
  const from = new Date(to);
  from.setUTCDate(from.getUTCDate() - days);
  const iso = (d: Date) => d.toISOString().split('T')[0];
  return { fromStr: iso(from), toStr: iso(to) };
}

/**
 * Prefetch buffer consumer: returns the buffered bars if the buffer is for
 * the current oldest bar (anchor matches), else null. Callers clear the
 * buffer themselves after consuming so the logic stays explicit.
 */
export function consumePrefetchBuffer<T extends TimedBar>(
  buffer: { data: T[]; anchorOldest: number } | null,
  currentOldest: number | null,
): T[] | null {
  if (!buffer || currentOldest == null) return null;
  if (buffer.anchorOldest !== currentOldest) return null;
  if (!buffer.data?.length) return null;
  return buffer.data;
}
