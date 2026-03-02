// --- Chart theme constants ---
/** @deprecated Use getChartTheme(theme).bg instead */
export const CHART_BG = '#000000';
/** @deprecated Use getChartTheme(theme).text instead */
export const CHART_TEXT = '#666666';
/** @deprecated Use getChartTheme(theme).grid instead */
export const CHART_GRID = '#1A1A1A';

// Light theme overrides
export const CHART_THEME = {
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
    rsiLine: '#5548D9',
    rsiTop: 'rgba(85,72,217,0.2)',
    rsiBottom: 'rgba(85,72,217,0.02)',
    baselineUp: '#16A34A',
    baselineUpFill1: 'rgba(22,163,74,0.15)',
    baselineUpFill2: 'rgba(22,163,74,0.02)',
    baselineDown: '#DC2626',
    baselineDownFill1: 'rgba(220,38,38,0.02)',
    baselineDownFill2: 'rgba(220,38,38,0.15)',
  },
};

export function getChartTheme(theme) {
  return CHART_THEME[theme] || CHART_THEME.dark;
}

export const INTERVALS = [
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
export const INITIAL_LOAD_DAYS = {
  '1s': 1, '1min': 7, '5min': 30, '15min': 60, '30min': 120,
  '1hour': 180, '4hour': 365, '1day': 0,
};

// Days to prepend on scroll-left per interval
export const SCROLL_CHUNK_DAYS = {
  '1s': 1, '1min': 5, '5min': 20, '15min': 30, '30min': 60,
  '1hour': 120, '4hour': 180, '1day': 365,
};

// Scroll-load: how close to left edge (in bars) before fetching more data
export const SCROLL_LOAD_THRESHOLD = 20;
// Debounce delay for visible range changes (ms)
export const RANGE_CHANGE_DEBOUNCE_MS = 300;

// --- MA / RSI / Volume configuration ---
export const MA_CONFIGS = [
  { period: 5,   color: '#22d3ee', label: 'MA5'   },  // cyan
  { period: 10,  color: '#34d399', label: 'MA10'  },  // green
  { period: 20,  color: '#fbbf24', label: 'MA20'  },  // yellow
  { period: 50,  color: '#3b82f6', label: 'MA50'  },  // blue
  { period: 100, color: '#a78bfa', label: 'MA100' },  // purple
  { period: 200, color: '#f59e0b', label: 'MA200' },  // orange
];
export const DEFAULT_ENABLED_MA = [20, 50];
export const RSI_PERIODS = [7, 14, 21];

// Approximate trading bars per day per interval (extended hours: 4AM-8PM = 16h)
export const BARS_PER_DAY = {
  '1s': 57600, '1min': 960, '5min': 192, '15min': 64, '30min': 32,
  '1hour': 16, '4hour': 4, '1day': 1,
};

// Ideal visible bar count per interval for "auto normalize"
export const AUTO_FIT_BARS = {
  '1s': 300, '1min': 390, '5min': 390, '15min': 200,
  '30min': 200, '1hour': 180, '4hour': 180, '1day': 180,
};

// --- Overlay constants ---
export const OVERLAY_COLORS = {
  earnings: '#10b981',
  grades: '#22d3ee',
  priceTargets: '#a78bfa',
};

export const OVERLAY_LABELS = {
  earnings: 'Earn',
  grades: 'Grade',
  priceTargets: 'PT',
};

// --- Extended-hours detection ---
export const EXTENDED_HOURS_INTERVALS = new Set(['1s', '1min', '5min', '15min', '30min', '1hour']);

/**
 * Check if a unix timestamp (seconds) falls outside regular market hours.
 * Times are ET wall-clock stored as UTC (the 'Z' trick).
 * Regular session: 9:30 – 16:00 ET.
 * Returns 'pre' (pre-market), 'post' (after-hours), or null (regular).
 */
export function getExtendedHoursType(timeSec) {
  const d = new Date(timeSec * 1000);
  const mins = d.getUTCHours() * 60 + d.getUTCMinutes();
  if (mins < 570) return 'pre';   // before 9:30
  if (mins >= 960) return 'post'; // 16:00 or later
  return null;
}

/** @deprecated Use getExtendedHoursType(t) !== null */
export function isExtendedHours(timeSec) {
  return getExtendedHoursType(timeSec) !== null;
}

/**
 * Compute contiguous extended-hours time regions from chart data.
 * Returns [{start, end, type}] where type is 'pre' or 'post'.
 */
export function computeExtendedHoursRegions(data) {
  if (!data || data.length === 0) return [];
  const regions = [];
  let regionStart = null;
  let regionType = null;
  let prevTime = null;
  for (const d of data) {
    const ext = getExtendedHoursType(d.time);
    if (ext) {
      if (regionStart === null || ext !== regionType) {
        // Close previous region if type changed (e.g. pre → post across gap)
        if (regionStart !== null) {
          regions.push({ start: regionStart, end: prevTime, type: regionType });
        }
        regionStart = d.time;
        regionType = ext;
      }
      prevTime = d.time;
    } else {
      if (regionStart !== null) {
        regions.push({ start: regionStart, end: prevTime, type: regionType });
        regionStart = null;
        regionType = null;
      }
    }
  }
  if (regionStart !== null) {
    regions.push({ start: regionStart, end: prevTime, type: regionType });
  }
  return regions;
}
