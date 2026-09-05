/**
 * MarketView page chart constants.
 *
 * The shared interval vocabulary, load/poll/live tables, and symbol
 * classification now live in lib/bars (so lib/ never imports a page); they are
 * re-exported here for page-internal callers. Cross-page consumers should import
 * those from '@/lib/bars' directly. Everything defined below is MarketView-page
 * presentation: theme colors, scroll/UI layout, extended-hours session shading,
 * and MA/RSI/overlay config.
 */
export {
  AUTO_FIT_BARS,
  BARS_PER_DAY,
  DELTA_POLL_CADENCE_MS,
  INITIAL_LOAD_DAYS,
  INTERVAL_LABEL,
  INTERVAL_SECONDS,
  INTERVALS,
  STAGE1_LOAD_DAYS,
  WS_FOLD_INTERVALS,
  WS_RECONCILE_POLL_MS,
  WS_STALE_WINDOW_MS,
} from '@/lib/bars/chartConstants';
export type { IntervalConfig } from '@/lib/bars/chartConstants';
export { FOREIGN_EXCHANGES, isUSEquity } from '@/lib/bars/exchanges';
import { createThemeResolver } from '@/lib/themeTokens';
import { getExtendedHoursType } from '@/lib/bars/marketSession';
import type { ExtendedHoursType } from '@/lib/bars/marketSession';

// --- Chart theme constants ---
export interface ChartThemeColors {
  bg: string;
  text: string;
  grid: string;
  upColor: string;
  downColor: string;
  volumeUp: string;
  volumeDown: string;
  extBgPre: string;
  extBgPost: string;
  extVolumeUp: string;
  extVolumeDown: string;
  watermark: string;
  rsiLine: string;
  rsiTop: string;
  rsiBottom: string;
  baselineUp: string;
  baselineUpFill1: string;
  baselineUpFill2: string;
  baselineDown: string;
  baselineDownFill1: string;
  baselineDownFill2: string;
}

// Canvas config can't read CSS custom properties, so each solid slot names the
// token it comes from and is resolved off <html> at paint time. A non-`--`
// entry is a one-off: the pre/post-market washes have no token, and the
// alpha derivations need opacities no *-soft/-border token carries.
const CHART_SOURCES: Record<'dark' | 'light', ChartThemeColors> = {
  dark: {
    bg: '--color-bg-card',
    text: '--color-text-secondary',
    grid: '--color-border-default',
    upColor: '--color-profit',
    downColor: '--color-loss',
    volumeUp: 'rgba(63,185,80,0.3)',
    volumeDown: 'rgba(248,81,73,0.3)',
    extBgPre: 'rgba(251,191,36,0.08)',       // amber/yellow pre-market
    extBgPost: 'rgba(59,130,246,0.10)',      // dark blue after-hours
    extVolumeUp: 'rgba(63,185,80,0.15)',
    extVolumeDown: 'rgba(248,81,73,0.15)',
    watermark: 'rgba(230,230,228,0.05)',
    rsiLine: '--color-accent-primary',
    rsiTop: 'rgba(233,149,74,0.2)',
    rsiBottom: 'rgba(233,149,74,0.02)',
    baselineUp: '--color-profit',
    baselineUpFill1: 'rgba(63,185,80,0.2)',
    baselineUpFill2: 'rgba(63,185,80,0.02)',
    baselineDown: '--color-loss',
    baselineDownFill1: 'rgba(248,81,73,0.02)',
    baselineDownFill2: 'rgba(248,81,73,0.2)',
  },
  light: {
    bg: '--color-bg-card',
    text: '--color-text-secondary',
    grid: '--color-border-default',
    upColor: '--color-profit',
    downColor: '--color-loss',
    volumeUp: 'rgba(26,127,55,0.25)',
    volumeDown: 'rgba(207,34,46,0.25)',
    extBgPre: 'rgba(217,119,6,0.05)',        // amber/yellow pre-market
    extBgPost: 'rgba(30,64,175,0.06)',       // dark blue after-hours
    extVolumeUp: 'rgba(26,127,55,0.12)',
    extVolumeDown: 'rgba(207,34,46,0.12)',
    watermark: 'rgba(31,31,30,0.04)',
    rsiLine: '--color-accent-primary',
    rsiTop: 'rgba(208,125,51,0.2)',
    rsiBottom: 'rgba(208,125,51,0.02)',
    baselineUp: '--color-profit',
    baselineUpFill1: 'rgba(26,127,55,0.15)',
    baselineUpFill2: 'rgba(26,127,55,0.02)',
    baselineDown: '--color-loss',
    baselineDownFill1: 'rgba(207,34,46,0.02)',
    baselineDownFill2: 'rgba(207,34,46,0.15)',
  },
};

/** Literal mirror of CHART_SOURCES — the jsdom / pre-stamp path. */
export const CHART_THEME: Record<'dark' | 'light', ChartThemeColors> = {
  dark: {
    bg: '#232426',
    text: '#9B9FA6',
    grid: '#2E3033',
    upColor: '#3FB950',
    downColor: '#F85149',
    volumeUp: 'rgba(63,185,80,0.3)',
    volumeDown: 'rgba(248,81,73,0.3)',
    extBgPre: 'rgba(251,191,36,0.08)',       // amber/yellow pre-market
    extBgPost: 'rgba(59,130,246,0.10)',      // dark blue after-hours
    extVolumeUp: 'rgba(63,185,80,0.15)',
    extVolumeDown: 'rgba(248,81,73,0.15)',
    watermark: 'rgba(230,230,228,0.05)',
    rsiLine: '#E9954A',
    rsiTop: 'rgba(233,149,74,0.2)',
    rsiBottom: 'rgba(233,149,74,0.02)',
    baselineUp: '#3FB950',
    baselineUpFill1: 'rgba(63,185,80,0.2)',
    baselineUpFill2: 'rgba(63,185,80,0.02)',
    baselineDown: '#F85149',
    baselineDownFill1: 'rgba(248,81,73,0.02)',
    baselineDownFill2: 'rgba(248,81,73,0.2)',
  },
  light: {
    bg: '#FFFFFF',
    text: '#73726E',
    grid: '#E8E8E6',
    upColor: '#1A7F37',
    downColor: '#CF222E',
    volumeUp: 'rgba(26,127,55,0.25)',
    volumeDown: 'rgba(207,34,46,0.25)',
    extBgPre: 'rgba(217,119,6,0.05)',        // amber/yellow pre-market
    extBgPost: 'rgba(30,64,175,0.06)',       // dark blue after-hours
    extVolumeUp: 'rgba(26,127,55,0.12)',
    extVolumeDown: 'rgba(207,34,46,0.12)',
    watermark: 'rgba(31,31,30,0.04)',
    rsiLine: '#D07D33',
    rsiTop: 'rgba(208,125,51,0.2)',
    rsiBottom: 'rgba(208,125,51,0.02)',
    baselineUp: '#1A7F37',
    baselineUpFill1: 'rgba(26,127,55,0.15)',
    baselineUpFill2: 'rgba(26,127,55,0.02)',
    baselineDown: '#CF222E',
    baselineDownFill1: 'rgba(207,34,46,0.02)',
    baselineDownFill2: 'rgba(207,34,46,0.15)',
  },
};

const resolveChartTheme = createThemeResolver(CHART_SOURCES, CHART_THEME);

export function getChartTheme(theme: 'dark' | 'light'): ChartThemeColors {
  return resolveChartTheme(theme === 'light' ? 'light' : 'dark');
}

// Intervals shown as direct buttons in the toolbar
export const PRIMARY_INTERVAL_KEYS = new Set(['1min', '1day']);

// Days to prepend on scroll-left per interval
export const SCROLL_CHUNK_DAYS: Record<string, number> = {
  '1min': 5, '5min': 20, '15min': 30, '30min': 60,
  '1hour': 120, '4hour': 180, '1day': 365,
};

// Scroll-load: how close to left edge (in bars) before fetching more data
export const SCROLL_LOAD_THRESHOLD = 20;
// Debounce delay for visible range changes (ms)
export const RANGE_CHANGE_DEBOUNCE_MS = 300;

// Stage 2 (background backfill) — additional days to fetch silently after stage 1.
export const STAGE2_BACKFILL_DAYS: Record<string, number> = {
  '1min': 5,  // backfill remaining 5 days (total = 2 + 5 = 7 = INITIAL_LOAD_DAYS)
};

// --- MA / RSI / Volume configuration ---
export interface MAConfig {
  period: number;
  color: string;
  label: string;
}

export const MA_CONFIGS: MAConfig[] = [
  { period: 5,   color: '#22d3ee', label: 'MA5'   },  // cyan
  { period: 10,  color: '#34d399', label: 'MA10'  },  // green
  { period: 20,  color: '#fbbf24', label: 'MA20'  },  // yellow
  { period: 50,  color: '#3b82f6', label: 'MA50'  },  // blue
  { period: 100, color: '#a78bfa', label: 'MA100' },  // purple
  { period: 200, color: '#f59e0b', label: 'MA200' },  // orange
];
export const DEFAULT_ENABLED_MA: number[] = [20, 50];
export const RSI_PERIODS: number[] = [7, 14, 21];

// Target bar spacing (pixels) per interval for readable candlestick charts.
// Container width determines how many bars are visible at this spacing.
export const TARGET_BAR_SPACING: Record<string, number> = {
  '1min': 8,   // Sweet spot for intraday monitoring
  '5min': 8,
  '15min': 9,
  '30min': 9,
  '1hour': 10,
  '4hour': 10,
  '1day': 7,   // Tighter for longer history overview
};

// --- Overlay constants ---
export const OVERLAY_COLORS: Record<string, string> = {
  earnings: '#3FB950',
  grades: '#22d3ee',
  priceTargets: '#a78bfa',
};

export const OVERLAY_LABELS: Record<string, string> = {
  earnings: 'Earn',
  grades: 'Grade',
  priceTargets: 'PT',
};

// --- Extended-hours presentation ---
// Session semantics (window math, ext interval set) live in the session model
// (@/lib/bars/marketSession); this file keeps the page's colors + shading.
export const EXT_COLOR_PRE = '#fbbf24';   // amber — pre-market
export const EXT_COLOR_POST = '#3b82f6';  // blue  — after-hours
// Neutral grey for lines that mark a SETTLED close — must never look like a
// live price (the ext colors above are the UI's "live extended price" hues).
export const CLOSE_LINE_COLOR = 'rgba(139,143,163,0.7)';
export { EXTENDED_HOURS_INTERVALS, getExtendedHoursType } from '@/lib/bars/marketSession';
export type { ExtendedHoursType } from '@/lib/bars/marketSession';

export interface ExtendedHoursRegion {
  start: number;
  end: number;
  type: ExtendedHoursType;
}

export interface ChartDataPoint {
  time: number;
  [key: string]: unknown;
}

/**
 * Max time (seconds) between two consecutive bars before we consider the
 * stream to have "jumped" and forcibly close the currently-open extended-
 * hours region. Pre/post-market windows are at most ~5.5h and ~4h long, and
 * within a session bars on supported intervals (1min..1hour) are always spaced
 * well under this. Any gap larger than this is a day-boundary crossing or a
 * backend data gap — both cases where a region should not be stretched.
 */
const EXT_REGION_MAX_GAP_SEC = 2 * 60 * 60;

/**
 * Compute contiguous extended-hours time regions from chart data.
 * Returns [{start, end, type}] where type is 'pre' or 'post'.
 */
export function computeExtendedHoursRegions(data: ChartDataPoint[]): ExtendedHoursRegion[] {
  if (!data || data.length === 0) return [];
  const regions: ExtendedHoursRegion[] = [];
  let regionStart: number | null = null;
  let regionType: ExtendedHoursType | null = null;
  let prevTime: number | null = null;
  for (const d of data) {
    const ext = getExtendedHoursType(d.time);
    // If the gap since the last bar is large enough that it must span a
    // session boundary (or a backend data gap), close any active region
    // before doing anything else. Otherwise a string of pre-market bars
    // that happens to skip over a missing day gets merged into one
    // continuous region spanning multiple calendar days.
    if (
      regionStart !== null &&
      prevTime !== null &&
      d.time - prevTime > EXT_REGION_MAX_GAP_SEC
    ) {
      regions.push({ start: regionStart, end: prevTime, type: regionType! });
      regionStart = null;
      regionType = null;
    }
    if (ext) {
      if (regionStart === null || ext !== regionType) {
        // Close previous region if type changed (e.g. pre -> post across gap)
        if (regionStart !== null) {
          regions.push({ start: regionStart, end: prevTime!, type: regionType! });
        }
        regionStart = d.time;
        regionType = ext;
      }
    } else {
      if (regionStart !== null) {
        regions.push({ start: regionStart, end: prevTime!, type: regionType! });
        regionStart = null;
        regionType = null;
      }
    }
    prevTime = d.time;
  }
  if (regionStart !== null) {
    regions.push({ start: regionStart, end: prevTime!, type: regionType! });
  }
  return regions;
}
