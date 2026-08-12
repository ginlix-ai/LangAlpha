/**
 * Shared chart data-loading primitives used by the MarketView `MarketChart`,
 * the dashboard `ChartWidget`/`MiniChartGridWidget`, and the `useLiveBars` hook.
 *
 * These are small, pure helpers rather than a custom hook so no caller has to
 * give up its own refs, state, or lifecycle. Extracting them here means a fix to
 * e.g. dedupe logic lands in every place at once. Lives in lib (not under
 * pages/MarketView) so lib/ never imports a page module.
 */

import { dateStrInTz } from '@/lib/utils';
import { BarsNotAvailableError, fetchBarsSeries, headerToMeta, toChartBars } from './barsClient';
import { timezoneForSymbol, US_MARKET_TZ } from './exchanges';
import { intervalToSchema } from './marketProtocol';
import type { ChartBar, LoaderMeta } from './marketProtocol';
import {
  BARS_PER_DAY,
  INITIAL_LOAD_DAYS,
  STAGE1_LOAD_DAYS,
  WS_RECONCILE_POLL_MS,
  WS_STALE_WINDOW_MS,
} from './chartConstants';
import { fetchStockData } from './legacyBars';

export interface TimedBar {
  time: number;
}

/**
 * Format a Date as YYYY-MM-DD in America/New_York wall-clock. Date-only
 * request bounds are venue trading dates, so using UTC (`toISOString`)
 * produces off-by-one errors in the ~4h window between ET evening and UTC
 * midnight — the caller asks for "tomorrow" and gets empty intraday data.
 * For foreign symbols use `dateStrInTz(d, timezoneForSymbol(sym))` instead —
 * an ET "today" is already tomorrow in Asia while those venues trade.
 */
export function etDateStr(d: Date = new Date()): string {
  // 'en-CA' gives ISO-8601 style YYYY-MM-DD.
  return dateStrInTz(d, US_MARKET_TZ);
}

/**
 * Decide the initial from/to date range for a given bar interval.
 *
 * Uses `STAGE1_LOAD_DAYS[interval]` when defined (fast first render, with a
 * background stage-2 backfill filling the rest of `INITIAL_LOAD_DAYS`);
 * otherwise falls back to `INITIAL_LOAD_DAYS[interval]`. `maxMaPeriod` adds
 * lookback overhead so moving averages can render on the first paint
 * without waiting for the backfill — pass 0 (or omit) when the caller has
 * no MAs to warm up (the dashboard widget's case). `tz` is the symbol's
 * venue timezone — "today" must be the venue's trading date (an ET "today"
 * excludes the session an Asian venue is currently trading).
 *
 * Returns `{ fromStr: undefined, toStr: undefined }` when `days === 0`,
 * which the backend treats as "full available history".
 */
export function computeInitialLoadRange(
  interval: string,
  { now = new Date(), maxMaPeriod = 0, tz = US_MARKET_TZ }:
    { now?: Date; maxMaPeriod?: number; tz?: string } = {},
): { fromStr?: string; toStr?: string; days: number } {
  const days = (interval in STAGE1_LOAD_DAYS)
    ? STAGE1_LOAD_DAYS[interval]
    : (INITIAL_LOAD_DAYS[interval] ?? 90);
  if (days <= 0) return { fromStr: undefined, toStr: undefined, days: 0 };

  const overheadDays = maxMaPeriod > 0
    ? Math.ceil((maxMaPeriod / (BARS_PER_DAY[interval] || 1)) * 1.5)
    : 0;

  const toStr = dateStrInTz(now, tz);
  const from = new Date(now);
  from.setDate(from.getDate() - days - overheadDays);
  return { fromStr: dateStrInTz(from, tz), toStr, days };
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
 * de-duplicating by `.time`. On a time collision the INCOMING bar wins — the
 * server (or a fresher delta poll) is authoritative, and the forming head bar
 * is re-served with the SAME timestamp as its value updates, so a merge that
 * kept the existing bar would freeze the last candle. Returns the merged array
 * plus the number of bars that landed *before* the existing data (the prepend
 * count — callers use this to compensate the visible logical range so the
 * user's current viewport doesn't jump when older history arrives).
 */
export function dedupeMergeByTime<T extends TimedBar>(
  existing: T[],
  incoming: T[],
): { merged: T[]; prependedCount: number } {
  if (!incoming?.length) return { merged: existing, prependedCount: 0 };
  const map = new Map(existing.map((d) => [d.time, d]));
  for (const d of incoming) map.set(d.time, d);
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
  // Chart timestamps encode venue wall clock as fake UTC, so reading them
  // with UTC arithmetic yields the venue's calendar date directly.
  const oldest = new Date(oldestSec * 1000);
  const to = new Date(oldest);
  to.setUTCDate(to.getUTCDate() - 1);
  const from = new Date(to);
  from.setUTCDate(from.getUTCDate() - days);
  const iso = (d: Date) => d.toISOString().split('T')[0];
  return { fromStr: iso(from), toStr: iso(to) };
}

/**
 * Advance the delta-poll watermark cursor given a freshly-served header
 * watermark. Forward-only against ordinary jitter (an out-of-order response
 * must not rewind the cursor a few bars) — but an incoming watermark more than
 * one bucket OLDER than the cursor means the server envelope was rebuilt
 * (re-pin, cache eviction) and is adopted: holding the newer cursor would
 * leave `after=` past every server bar, freezing the delta stream permanently.
 * A null incoming watermark (empty/failed poll) never moves the cursor.
 */
export function advanceWatermark(
  current: number | null | undefined,
  incoming: number | null | undefined,
  intervalSec: number,
): number | null {
  if (incoming == null) return current ?? null;
  if (current == null) return incoming;
  if (incoming < current - intervalSec * 1000) return incoming;
  return Math.max(current, incoming);
}

/**
 * Decide whether a delta-poll tick should be skipped because WS is driving
 * the forming bar. Skips only while the WS feed is healthy (a tick within
 * `WS_STALE_WINDOW_MS`) AND the periodic reconcile isn't due — one poll per
 * `WS_RECONCILE_POLL_MS` always gets through as the authoritative correction
 * (fold drift, suspend/resume holes, server-side revisions, MA/RSI refresh).
 */
export function shouldSkipPollWhileWsHealthy(
  lastWsTickAt: number,
  lastReconcileAt: number,
  now: number,
): boolean {
  const wsHealthy = lastWsTickAt > now - WS_STALE_WINDOW_MS;
  const reconcileDue = lastReconcileAt <= now - WS_RECONCILE_POLL_MS;
  return wsHealthy && !reconcileDue;
}

export interface BarsDeltaResult {
  /**
   * `'protocol'` — only records newer than `watermark` (server-filtered via
   * `after=`). `'legacy'` — a full re-fetch of the window from the watermark's
   * venue date to today; callers dedupe-merge by time either way.
   */
  bars: ChartBar[];
  meta: LoaderMeta;
  source: 'protocol' | 'legacy';
}

const EMPTY_META: LoaderMeta = { watermark: null, complete: false, marketPhase: null };

/**
 * Fetch bars newer than `watermark` via the protocol endpoint's `after=` delta
 * mode, falling back to a single legacy full re-fetch when the endpoint isn't
 * available (404 / network). The legacy window starts at the watermark's
 * venue date (delta-sized) when we have one, else the last few days.
 *
 * Callers merge `bars` into their timeline by `.time` (protocol bars are
 * already newer-only; legacy bars are the full window) and persist
 * `meta.watermark` to drive the next delta poll.
 */
export async function fetchBarsDelta(
  instrument: string,
  interval: string,
  watermark: number | null | undefined,
  { assetClass, signal }: { assetClass?: string; signal?: AbortSignal } = {},
): Promise<BarsDeltaResult> {
  const schema = intervalToSchema(interval);
  const tz = timezoneForSymbol(instrument);
  try {
    const res = await fetchBarsSeries(instrument, schema, {
      after: watermark ?? undefined,
      assetClass,
      signal,
    });
    return {
      bars: toChartBars(res.series.records, tz),
      meta: headerToMeta(res.series.header, res.cache),
      source: 'protocol',
    };
  } catch (err) {
    if (!(err instanceof BarsNotAvailableError)) throw err;
    // Legacy fallback — one full re-fetch.
    const toStr = dateStrInTz(new Date(), tz);
    let fromStr: string;
    if (watermark != null) {
      fromStr = dateStrInTz(watermark, tz);
    } else {
      const d = new Date();
      d.setDate(d.getDate() - 3);
      fromStr = dateStrInTz(d, tz);
    }
    const result = await fetchStockData(instrument, interval, fromStr, toStr, { signal });
    return { bars: result.data ?? [], meta: result.meta ?? EMPTY_META, source: 'legacy' };
  }
}
