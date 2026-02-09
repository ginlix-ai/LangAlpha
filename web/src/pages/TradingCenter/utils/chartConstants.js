// --- Chart theme constants ---
export const CHART_BG = '#0f1422';
export const CHART_TEXT = '#8b8fa3';
export const CHART_GRID = '#1a1f35';

export const INTERVALS = [
  { key: '1min',  label: '1m'  },
  { key: '5min',  label: '5m'  },
  { key: '15min', label: '15m' },
  { key: '30min', label: '30m' },
  { key: '1hour', label: '1H'  },
  { key: '4hour', label: '4H'  },
  { key: '1day',  label: '1D'  },
];

// Days of history per interval for initial load
export const INITIAL_LOAD_DAYS = {
  '1min': 7, '5min': 30, '15min': 60, '30min': 120,
  '1hour': 180, '4hour': 365, '1day': 0,  // 0 = full history
};

// Days to prepend on scroll-left per interval
export const SCROLL_CHUNK_DAYS = {
  '1min': 5, '5min': 20, '15min': 30, '30min': 60,
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

// Approximate trading bars per day per interval (6.5h session)
export const BARS_PER_DAY = {
  '1min': 390, '5min': 78, '15min': 26, '30min': 13,
  '1hour': 7, '4hour': 2, '1day': 1,
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
