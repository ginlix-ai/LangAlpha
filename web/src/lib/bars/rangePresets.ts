/**
 * TradingView-style viewing-window presets for the chart bottom bar
 * (1D 5D 1M … All). Each preset names the bar interval it charts with and how
 * far back the visible window reaches. Pure data + math — the component owns
 * the interval switch and the timeScale() call.
 */

export interface RangePreset {
  /** Display label AND identity: '1D' | '5D' | '1M' | '3M' | '6M' | 'YTD' | '1Y' | '5Y' | 'All'. */
  key: string;
  /** Bar interval the preset charts with (legacy interval key, e.g. '15min'). */
  interval: string;
  /** Interval to use instead when `interval` is unavailable for the symbol/provider. */
  fallback?: string;
}

export const RANGE_PRESETS: RangePreset[] = [
  { key: '1D', interval: '1min' },
  { key: '5D', interval: '15min' },
  { key: '1M', interval: '1hour' },
  { key: '3M', interval: '4hour', fallback: '1day' },
  { key: '6M', interval: '1day' },
  { key: 'YTD', interval: '1day' },
  { key: '1Y', interval: '1day' },
  { key: '5Y', interval: '1day' },
  { key: 'All', interval: '1day' },
];

const DAY = 86_400;

// Calendar days back per fixed-span preset. 5D uses 7 calendar days so ~5
// trading days stay visible across a weekend.
const SPAN_DAYS: Record<string, number> = {
  '5D': 7, '1M': 30, '3M': 91, '6M': 182, '1Y': 365, '5Y': 1826,
};

/**
 * Left edge (chart seconds) of a preset's visible window, anchored on the LAST
 * bar's chart time — anchoring on wall-clock "now" would show an empty chart
 * on weekends. Returns null for 'All' (callers `fitContent()`).
 *
 * Chart times encode venue wall clock as fake UTC, so "the session's venue
 * midnight" (1D) and "venue Jan 1" (YTD) are plain UTC arithmetic here.
 */
export function rangeStartChartSec(key: string, lastBarChartSec: number): number | null {
  if (key === 'All') return null;
  if (key === '1D') return Math.floor(lastBarChartSec / DAY) * DAY;
  if (key === 'YTD') {
    return Date.UTC(new Date(lastBarChartSec * 1000).getUTCFullYear(), 0, 1) / 1000;
  }
  const days = SPAN_DAYS[key];
  return days ? lastBarChartSec - days * DAY : null;
}
