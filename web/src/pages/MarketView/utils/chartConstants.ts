// --- Chart theme constants ---
/** @deprecated Use getChartTheme(theme).bg instead */
export const CHART_BG = '#000000';
/** @deprecated Use getChartTheme(theme).text instead */
export const CHART_TEXT = '#666666';
/** @deprecated Use getChartTheme(theme).grid instead */
export const CHART_GRID = '#1A1A1A';

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

// Light theme overrides
export const CHART_THEME: Record<'dark' | 'light', ChartThemeColors> = {
  dark: {
    bg: '#000000',
    text: '#666666',
    grid: '#1A1A1A',
    upColor: '#10b981',
    downColor: '#ef4444',
    volumeUp: 'rgba(16,185,129,0.3)',
    volumeDown: 'rgba(239,68,68,0.3)',
    extBgPre: 'rgba(251,191,36,0.08)',       // amber/yellow pre-market
    extBgPost: 'rgba(59,130,246,0.10)',      // dark blue after-hours
    extVolumeUp: 'rgba(16,185,129,0.15)',
    extVolumeDown: 'rgba(239,68,68,0.15)',
    watermark: 'rgba(102,102,102,0.06)',
    rsiLine: '#667eea',
    rsiTop: 'rgba(102,126,234,0.3)',
    rsiBottom: 'rgba(102,126,234,0.02)',
    baselineUp: '#10b981',
    baselineUpFill1: 'rgba(16,185,129,0.2)',
    baselineUpFill2: 'rgba(16,185,129,0.02)',
    baselineDown: '#ef4444',
    baselineDownFill1: 'rgba(239,68,68,0.02)',
    baselineDownFill2: 'rgba(239,68,68,0.2)',
  },
  light: {
    bg: '#FFFCF9',
    text: '#7A756F',
    grid: '#E8E2DB',
    upColor: '#16A34A',
    downColor: '#DC2626',
    volumeUp: 'rgba(22,163,74,0.25)',
    volumeDown: 'rgba(220,38,38,0.25)',
    extBgPre: 'rgba(217,119,6,0.05)',        // amber/yellow pre-market
    extBgPost: 'rgba(30,64,175,0.06)',       // dark blue after-hours
    extVolumeUp: 'rgba(22,163,74,0.12)',
    extVolumeDown: 'rgba(220,38,38,0.12)',
    watermark: 'rgba(45,43,40,0.04)',
    rsiLine: '#37528B',
    rsiTop: 'rgba(55,82,139,0.2)',
    rsiBottom: 'rgba(55,82,139,0.02)',
    baselineUp: '#16A34A',
    baselineUpFill1: 'rgba(22,163,74,0.15)',
    baselineUpFill2: 'rgba(22,163,74,0.02)',
    baselineDown: '#DC2626',
    baselineDownFill1: 'rgba(220,38,38,0.02)',
    baselineDownFill2: 'rgba(220,38,38,0.15)',
  },
};

export function getChartTheme(theme: 'dark' | 'light'): ChartThemeColors {
  return CHART_THEME[theme] || CHART_THEME.dark;
}

export interface IntervalConfig {
  key: string;
  label: string;
}

export const INTERVALS: IntervalConfig[] = [
  { key: '1s',    label: '1s'  },
  { key: '1min',  label: '1m'  },
  { key: '5min',  label: '5m'  },
  { key: '15min', label: '15m' },
  { key: '30min', label: '30m' },
  { key: '1hour', label: '1H'  },
  { key: '4hour', label: '4H'  },
  { key: '1day',  label: '1D'  },
];

// Intervals shown as direct buttons in the toolbar
export const PRIMARY_INTERVAL_KEYS = new Set(['1s', '1min', '1day']);

// Days of history per interval for initial load (0 = full history)
export const INITIAL_LOAD_DAYS: Record<string, number> = {
  '1s': 0, '1min': 7, '5min': 30, '15min': 60, '30min': 120,
  '1hour': 180, '4hour': 365, '1day': 0,
};

// Days to prepend on scroll-left per interval
export const SCROLL_CHUNK_DAYS: Record<string, number> = {
  '1s': 0, '1min': 5, '5min': 20, '15min': 30, '30min': 60,
  '1hour': 120, '4hour': 180, '1day': 365,
};

// Scroll-load: how close to left edge (in bars) before fetching more data
export const SCROLL_LOAD_THRESHOLD = 20;
// Debounce delay for visible range changes (ms)
export const RANGE_CHANGE_DEBOUNCE_MS = 300;

// Stage 1 (fast) initial load — days to fetch for immediate render.
// Intervals not listed here skip staged loading entirely.
export const STAGE1_LOAD_DAYS: Record<string, number> = {
  '1s': 0,    // 1s: stage 1 = no date range (today's data from backend)
  '1min': 2,  // 1min: stage 1 = 2 days (fast render)
};

// Stage 2 (background backfill) — additional days to fetch silently after stage 1.
export const STAGE2_BACKFILL_DAYS: Record<string, number> = {
  '1s': 1,    // backfill 1 prior day
  '1min': 5,  // backfill remaining 5 days (total = 2 + 5 = 7 = INITIAL_LOAD_DAYS)
};

// Background prefetch: intervals that pre-load the next scroll chunk before user reaches the edge
export const PREFETCH_ENABLED_INTERVALS = new Set(['1s']);

// How far from left edge (in bars) to trigger background prefetch (well before SCROLL_LOAD_THRESHOLD=20)
export const PREFETCH_THRESHOLD = 150;

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

// Approximate trading bars per day per interval (extended hours: 4AM-8PM = 16h)
export const BARS_PER_DAY: Record<string, number> = {
  '1s': 57600, '1min': 960, '5min': 192, '15min': 64, '30min': 32,
  '1hour': 16, '4hour': 4, '1day': 1,
};

// Ideal visible bar count per interval (legacy, used by scroll-load heuristics)
export const AUTO_FIT_BARS: Record<string, number> = {
  '1s': 300, '1min': 390, '5min': 390, '15min': 200,
  '30min': 200, '1hour': 180, '4hour': 180, '1day': 180,
};

// Target bar spacing (pixels) per interval for readable candlestick charts.
// Container width determines how many bars are visible at this spacing.
export const TARGET_BAR_SPACING: Record<string, number> = {
  '1s': 5,     // Dense: overview of rapid ticks
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
  earnings: '#10b981',
  grades: '#22d3ee',
  priceTargets: '#a78bfa',
};

export const OVERLAY_LABELS: Record<string, string> = {
  earnings: 'Earn',
  grades: 'Grade',
  priceTargets: 'PT',
};

// --- Extended-hours detection ---
export const EXT_COLOR_PRE = '#fbbf24';   // amber — pre-market
export const EXT_COLOR_POST = '#3b82f6';  // blue  — after-hours
export const EXTENDED_HOURS_INTERVALS = new Set(['1s', '1min', '5min', '15min', '30min', '1hour']);

export type ExtendedHoursType = 'pre' | 'post';

/**
 * Check if a unix timestamp (seconds) falls outside regular market hours.
 * Times are ET wall-clock stored as UTC (the 'Z' trick).
 * Regular session: 9:30 – 16:00 ET.
 * Returns 'pre' (pre-market), 'post' (after-hours), or null (regular).
 */
export function getExtendedHoursType(timeSec: number): ExtendedHoursType | null {
  const d = new Date(timeSec * 1000);
  const mins = d.getUTCHours() * 60 + d.getUTCMinutes();
  if (mins < 570) return 'pre';   // before 9:30
  if (mins >= 960) return 'post'; // 16:00 or later
  return null;
}

/** @deprecated Use getExtendedHoursType(t) !== null */
export function isExtendedHours(timeSec: number): boolean {
  return getExtendedHoursType(timeSec) !== null;
}

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
 * within a session bars on supported intervals (1s..1hour) are always spaced
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

// --- Symbol classification ---
export const FOREIGN_EXCHANGES = new Set(['HK', 'SS', 'SZ', 'L', 'T', 'TO', 'AX', 'DE', 'PA', 'MC']);

/** Returns true for US-listed equities (not indexes, not foreign stocks). */
export function isUSEquity(sym: string | null | undefined): boolean {
  if (!sym) return true;
  if (sym.startsWith('^')) return false;
  const dotIdx = sym.lastIndexOf('.');
  if (dotIdx === -1) return true;
  const suffix = sym.slice(dotIdx + 1).toUpperCase();
  return !FOREIGN_EXCHANGES.has(suffix);
}

/** 1s interval is only supported for US equities. */
export function supports1sInterval(sym: string | null | undefined): boolean {
  return isUSEquity(sym);
}
