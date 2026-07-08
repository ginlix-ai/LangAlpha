/**
 * Live forming-bar synthesis helpers.
 *
 * The chart's last bar (the "forming" bar of the current, not-yet-closed
 * bucket) must update lively between REST delta polls. These pure helpers do
 * that folding: `foldMinuteBar` folds a finer-grained live bar into a coarser
 * series' forming bucket; `applyQuoteToDailyBar` folds a live snapshot quote
 * into the head daily bar.
 *
 * Both are drift-tolerant by design — a REST delta poll replaces the forming
 * bar wholesale at least every ~60s, correcting any accumulation or alignment
 * error these introduce. All functions return a NEW array and never mutate.
 */
import type { ChartBar } from './marketProtocol';
import { chartSecToDateStr, dateStrInTz } from '@/lib/utils';

/**
 * The subset of a snapshot quote row {@link applyQuoteToDailyBar} reads. Kept
 * local (rather than importing `QuoteRow` from `@/lib/quotes`) so the bars lib
 * stays decoupled from the quote layer — any structurally-compatible row works.
 */
export interface QuoteLike {
  price?: number | null;
  high?: number | null;
  low?: number | null;
  volume?: number | null;
}

/**
 * Fold one finer-grained bar (`minuteBar`, in chart-time seconds — same units
 * as ChartBar) into the forming bucket of a coarser `bars` series whose bucket
 * size is `intervalSec` seconds.
 *
 * - If `minuteBar.time` falls inside the last bar's bucket
 *   `[lastBar.time, lastBar.time + intervalSec)`, the last bar is updated:
 *   high=max, low=min, close=minuteBar.close, volume accumulates.
 * - If it falls at/after the bucket end, a new forming bar is opened anchored at
 *   `lastBar.time + k*intervalSec` (the aligned bucket start containing
 *   `minuteBar.time`), seeded with the minute bar's OHLCV.
 * - A minute bar strictly before the current bucket (late/out-of-order) is a
 *   no-op — only the head/forming bar is synthesized here.
 *
 * Volume accumulation drift is acceptable: the next REST poll (≤60s) replaces
 * the forming bar wholesale. Session gaps (HK lunch, overnight) can misalign the
 * arithmetic `k*intervalSec` rollover anchor; that too is corrected by the next
 * REST poll.
 *
 * Returns a new array; never mutates `bars`.
 */
export function foldMinuteBar(
  bars: ChartBar[],
  minuteBar: ChartBar,
  intervalSec: number,
): ChartBar[] {
  if (!bars?.length || !minuteBar || intervalSec <= 0) return bars;

  // A bar without a positive close is a zero-filled provider row, not a trade
  // (mirrors applyQuoteToDailyBar's guard) — folding it would wick the candle
  // to zero until the next REST reconcile.
  if (!(minuteBar.close > 0)) return bars;

  const lastBar = bars[bars.length - 1];
  const bucketStart = lastBar.time;
  const bucketEnd = bucketStart + intervalSec;

  // Late / out-of-order bar belonging to an already-closed bucket — ignore.
  if (minuteBar.time < bucketStart) return bars;

  if (minuteBar.time < bucketEnd) {
    // Inside the forming bucket — accumulate into the last bar. Non-positive
    // high/low are ignored for the same zero-fill reason as close above.
    const updated: ChartBar = {
      ...lastBar,
      high: minuteBar.high > 0 ? Math.max(lastBar.high, minuteBar.high) : lastBar.high,
      low: minuteBar.low > 0 ? Math.min(lastBar.low, minuteBar.low) : lastBar.low,
      close: minuteBar.close,
      volume: (lastBar.volume || 0) + (minuteBar.volume || 0),
    };
    return [...bars.slice(0, -1), updated];
  }

  // At/after the bucket end — open a new forming bar at the aligned anchor.
  // Zeroed OHL fields fall back to the (positive) close.
  const k = Math.floor((minuteBar.time - bucketStart) / intervalSec);
  const newBar: ChartBar = {
    time: bucketStart + k * intervalSec,
    open: minuteBar.open > 0 ? minuteBar.open : minuteBar.close,
    high: minuteBar.high > 0 ? minuteBar.high : minuteBar.close,
    low: minuteBar.low > 0 ? minuteBar.low : minuteBar.close,
    close: minuteBar.close,
    volume: minuteBar.volume || 0,
  };
  return [...bars, newBar];
}

/**
 * Fold a live snapshot `quote` into the LAST daily bar of `bars`:
 * close=price, high=max(high, quote.high), low=min(low, quote.low),
 * volume=quote.volume when present.
 *
 * Update-only — never creates a new daily bar (bar creation stays REST-owned).
 * No-op (returns `bars` unchanged) when `bars` is empty or `quote` lacks a
 * usable price. Non-positive high/low/volume are ignored: pre-open snapshot
 * day-aggregates are zeroed, and folding a 0 low (or 0 day volume) into the
 * previous session's settled candle would destroy it. Price 0 marks a
 * quote-unavailable row, never a real trade.
 * Returns a new array; never mutates `bars`.
 */
/**
 * True when the head bar of a daily series is a SETTLED prior-session candle
 * (pre-market, weekends, holidays) rather than the current venue day's live
 * bar. Chart times encode venue wall clock as fake UTC, so the head bar's date
 * decodes via a UTC read and compares against "today" on the venue clock.
 * False for an empty/missing series.
 */
export function isSettledDailyHead(
  bars: readonly { time: number }[] | null | undefined,
  tz: string,
  now: Date = new Date(),
): boolean {
  const head = bars?.[bars.length - 1];
  if (!head) return false;
  return isSettledDailyHeadTime(head.time, tz, now);
}

/** Head-bar-time core of {@link isSettledDailyHead} for callers that hold a bare time. */
export function isSettledDailyHeadTime(
  headTimeSec: number,
  tz: string,
  now: Date = new Date(),
): boolean {
  return chartSecToDateStr(headTimeSec) !== dateStrInTz(now, tz);
}

export function applyQuoteToDailyBar(
  bars: ChartBar[],
  quote: QuoteLike | null | undefined,
): ChartBar[] {
  if (!bars?.length || quote?.price == null || quote.price <= 0) return bars;

  const lastBar = bars[bars.length - 1];
  const updated: ChartBar = {
    ...lastBar,
    close: quote.price,
    high: quote.high != null && quote.high > 0 ? Math.max(lastBar.high, quote.high) : lastBar.high,
    low: quote.low != null && quote.low > 0 ? Math.min(lastBar.low, quote.low) : lastBar.low,
    volume: quote.volume != null && quote.volume > 0 ? quote.volume : lastBar.volume,
  };
  return [...bars.slice(0, -1), updated];
}
