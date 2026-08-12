/**
 * Shared chart data constants — the interval vocabulary and the load/poll/live
 * tables used by the bars pipeline (chartDataLoaders, useLiveBars) and by the
 * cross-page chart consumers (Dashboard widgets, ChatAgent annotation cards).
 *
 * MarketView-page-only presentation constants (theme colors, scroll/UI layout,
 * extended-hours session shading, MA/RSI/overlay config) live in
 * pages/MarketView/utils/chartConstants and re-export these back for
 * page-internal callers.
 */

export interface IntervalConfig {
  key: string;
  label: string;
}

export const INTERVALS: IntervalConfig[] = [
  { key: '1min',  label: '1m'  },
  { key: '5min',  label: '5m'  },
  { key: '15min', label: '15m' },
  { key: '30min', label: '30m' },
  { key: '1hour', label: '1H'  },
  { key: '4hour', label: '4H'  },
  { key: '1day',  label: '1D'  },
];

// Interval key → short display label, derived from INTERVALS. Single source for
// any surface that shows a timeframe badge (chart picker, annotation cards/rows).
// Look up as `INTERVAL_LABEL[key] ?? key` so unknown keys degrade to the raw value.
export const INTERVAL_LABEL: Record<string, string> = Object.fromEntries(
  INTERVALS.map((i) => [i.key, i.label]),
);

// Days of history per interval for initial load (0 = full history)
export const INITIAL_LOAD_DAYS: Record<string, number> = {
  '1min': 7, '5min': 30, '15min': 60, '30min': 120,
  '1hour': 180, '4hour': 365, '1day': 0,
};

// Stage 1 (fast) initial load — days to fetch for immediate render.
// Intervals not listed here skip staged loading entirely.
export const STAGE1_LOAD_DAYS: Record<string, number> = {
  '1min': 2,  // 1min: stage 1 = 2 days (fast render)
};

// Bucket size in seconds per interval. Used to fold a finer-grained live bar
// (WS second/minute aggregate) into the forming bucket of a coarser series.
export const INTERVAL_SECONDS: Record<string, number> = {
  '1min': 60, '5min': 300, '15min': 900, '30min': 1800,
  '1hour': 3600, '4hour': 14400, '1day': 86400,
};

// REST delta-poll cadence per interval (ms). Faster intervals poll more often;
// the forming head bar is re-served on every poll so the last candle stays live.
export const DELTA_POLL_CADENCE_MS: Record<string, number> = {
  '1min': 15000, '5min': 30000, '15min': 30000, '30min': 30000,
  '1hour': 30000, '4hour': 30000, '1day': 60000,
};

// Intervals whose forming bar is kept live by folding the finer WS aggregate
// into the current bucket (via foldMinuteBar) rather than appending natively.
// 1min updates the head bar directly; 1day uses the quote layer instead.
export const WS_FOLD_INTERVALS = new Set(['5min', '15min', '30min', '1hour', '4hour']);

// While WS ticks keep arriving the delta poll is skipped — but one poll is
// let through at this cadence as the authoritative reconcile: it corrects
// fold volume drift, backfills buckets missed during a tab suspend (the WS
// gap-fill only covers 1min), picks up server-side corrections, and
// refreshes MA/RSI past the forming bar.
export const WS_RECONCILE_POLL_MS = 60_000;

// A WS tick within this window counts the feed as healthy, letting the delta
// poll skip (except for the periodic reconcile above).
export const WS_STALE_WINDOW_MS = 5000;

// Approximate trading bars per day per interval (extended hours: 4AM-8PM = 16h)
export const BARS_PER_DAY: Record<string, number> = {
  '1min': 960, '5min': 192, '15min': 64, '30min': 32,
  '1hour': 16, '4hour': 4, '1day': 1,
};

// Ideal visible bar count per interval (legacy, used by scroll-load heuristics)
export const AUTO_FIT_BARS: Record<string, number> = {
  '1min': 390, '5min': 390, '15min': 200,
  '30min': 200, '1hour': 180, '4hour': 180, '1day': 180,
};
