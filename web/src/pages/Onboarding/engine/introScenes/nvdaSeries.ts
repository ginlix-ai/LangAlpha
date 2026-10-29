/* NVDA's last ten weeks as the market scenes and the thread's chart tab
   draw them: daily bars made up to move on four pieces of news, the averages
   and RSI the chart derives from them, the quote the stock header prints,
   and the scale that lays a series out on a chart of a given size. Data and
   arithmetic only; the chart that draws it is candles.tsx. */

/** One daily bar. */
interface Ohlc {
  o: number;
  h: number;
  l: number;
  c: number;
  v: number;
}

/** A symbol's visible bars with what the chart derives from them. */
export interface Series {
  sym: string;
  bars: Ohlc[];
  /** MA20 then MA50, one value per visible bar, in the app's MA colors. */
  ma: { color: string; values: number[] }[];
  /** RSI (14), one value per visible bar. */
  rsi: number[];
}

/* ── the series ────────────────────────────────────────────────────────── */

/** A repeatable 0 to 1 jitter for bar `i`. */
function jitter(i: number, salt: number): number {
  const s = Math.sin((i + 1) * 12.9898 + salt * 78.233) * 43758.5453;
  return s - Math.floor(s);
}

/** Daily bars from closes: each opens near the last close and wicks a little
    past its body, and volume swells with the size of the move. `fixed` pins
    the bars the story turns on. */
function toBars(
  closes: number[],
  first: number,
  volume: number,
  salt: number,
  fixed: Record<number, Partial<Ohlc>>
): Ohlc[] {
  let prev = first;
  return closes.map((c, i) => {
    const o = prev + (jitter(i, salt) - 0.5) * 0.9;
    const reach = 0.45 + Math.abs(c - o) * 0.2;
    const bar = {
      o,
      c,
      h: Math.max(o, c) + 0.12 + jitter(i, salt + 1) * reach,
      l: Math.min(o, c) - 0.12 - jitter(i, salt + 2) * reach,
      v: volume * (0.7 + jitter(i, salt + 3) * 0.5) * (1 + (Math.abs(c - prev) / prev) * 18),
    };
    prev = c;
    return { ...bar, ...fixed[i] };
  });
}

/** The simple moving average of `closes`, from index `from` on. */
function sma(closes: number[], period: number, from: number): number[] {
  return closes.slice(from).map((_, k) => {
    const end = from + k + 1;
    const win = closes.slice(Math.max(0, end - period), end);
    return win.reduce((a, b) => a + b, 0) / win.length;
  });
}

/** Wilder's RSI of `closes`, from index `from` on. */
function rsiOf(closes: number[], period: number, from: number): number[] {
  let gain = 0;
  let loss = 0;
  const out: number[] = [];
  for (let i = 1; i < closes.length; i++) {
    const d = closes[i] - closes[i - 1];
    // the first `period` moves average plainly, then each folds in at 1/period
    const keep = i <= period ? 1 : (period - 1) / period;
    gain = gain * keep + Math.max(d, 0) / period;
    loss = loss * keep + Math.max(-d, 0) / period;
    if (i >= from) out.push(loss === 0 ? 100 : 100 - 100 / (1 + gain / loss));
  }
  return out;
}

/** A series from its history (for the averages and RSI) and its visible closes. */
function seriesOf(
  sym: string,
  prior: number[],
  closes: number[],
  volume: number,
  salt: number,
  fixed: Record<number, Partial<Ohlc>> = {}
): Series {
  const all = [...prior, ...closes];
  const from = prior.length;
  return {
    sym,
    bars: toBars(closes, prior[from - 1], volume, salt, fixed),
    ma: [
      { color: '#fbbf24', values: sma(all, 20, from) },
      { color: '#3b82f6', values: sma(all, 50, from) },
    ],
    rsi: rsiOf(all, 14, from),
  };
}

/** Sixty closes before the visible stretch, for the averages and RSI. */
function priorCloses(base: number, rise: number): number[] {
  return Array.from(
    { length: 60 },
    (_, k) => base + (rise * k) / 59 + 2.6 * Math.sin(k * 0.45) + 1.2 * Math.sin(k * 1.3)
  );
}

/** The time axis's marks, as lightweight-charts spaces them: a month where
    one starts, a day of the month between. The 49 sessions run Aug 13 to
    Oct 21, 2026. */
export const TIME_MARKS = [
  { i: 2, label: '17' },
  { i: 13, label: 'Sep' },
  { i: 21, label: '14' },
  { i: 34, label: 'Oct' },
  { i: 42, label: '13' },
];

/** NVDA's prior close and today's last, as every scene quotes them. */
export const NVDA_PREV = 179.53;
export const NVDA_LAST = 182.42;

/** NVDA's closes from Aug 13, short of yesterday's and today's (`NVDA_PREV`, `NVDA_LAST`). */
const NVDA_CLOSES = [
  170.2, 171.4, 170.8, 172.1, 173.5, 172.9, 174.2, 175.0, 174.4, 176.1, 184.5, 185.3, 184.0, 182.6,
  181.9, 180.6, 180.2, 174.8, 173.4, 171.9, 171.3, 172.0, 171.6, 175.1, 176.3, 177.9, 179.4, 178.8,
  181.2, 183.0, 184.6, 183.2, 181.1, 179.3, 177.6, 176.0, 174.9, 174.0, 178.1, 178.6, 177.9, 179.2,
  180.1, 179.4, 180.7, 181.2, 180.4,
];

/**
 * NVDA, daily, each move on news. It gapped up on earnings (Aug 27) into
 * 186.90 and faded, fell 3% in a day on a new export rule (Sep 8) and slid
 * to 170.60, turned on the Fed's cut (Sep 16) and rallied back to 186.30,
 * pulled back to 173.10, and turned again on an upgrade (Oct 7) to 182.42
 * today.
 */
export const NVDA = seriesOf(
  'NVDA',
  priorCloses(160, 9),
  [...NVDA_CLOSES, NVDA_PREV, NVDA_LAST],
  180e6,
  1,
  {
    10: { o: 182.6, h: 186.8, l: 181.9, v: 412e6 },
    11: { h: 186.9 },
    17: { o: 179.6, h: 180.1, l: 174.3, v: 390e6 },
    20: { l: 170.6 },
    22: { l: 170.9 },
    23: { o: 171.9, h: 175.6, l: 171.4, v: 330e6 },
    30: { h: 186.3 },
    37: { l: 173.1 },
    38: { o: 174.3, h: 178.6, l: 173.9, v: 310e6 },
    48: { o: 180.4, h: 183.05, l: 180.15 },
  }
);

/** One `event` annotation: the bar it is pinned to, the price its badge
    anchors at, the headline on the badge and the story behind it. */
export interface ChartEvent {
  i: number;
  price: number;
  title: string;
  detail?: string;
}

/** The news the agent pins to NVDA's moves, each at the bar it hit. */
export const NVDA_EVENTS = {
  earnings: { i: 10, price: 184.5, title: 'Earnings beat' },
  exportRule: {
    i: 17,
    price: 180.1,
    title: 'New export rule',
    detail: 'Licenses tightened on AI chips sold abroad. NVDA fell 3% in a day.',
  },
  fedCut: { i: 23, price: 171.4, title: 'Fed cuts rates' },
  upgrade: { i: 38, price: 173.9, title: 'Analyst upgrade' },
} satisfies Record<string, ChartEvent>;

/** What the stock header prints for NVDA. */
export const NVDA_QUOTE = {
  sym: 'NVDA',
  name: 'NVIDIA Corp',
  prev: NVDA_PREV,
  open: 180.4,
  low: 180.15,
  high: 183.05,
  hi52: 195.62,
  lo52: 86.62,
  avgVol: '181.52M',
  vol: '143.08M',
};

/* ── the scale ─────────────────────────────────────────────────────────── */

/** The price axis's width, right of the panes, and the time axis's height
    under them. */
export const AXIS_W = 24;
export const TIME_H = 10;
/** The empty room right of the last bar, where the next one will print. */
const RIGHT_GAP = 5;

/** Where things sit on a chart `width` × `height` px: the price pane and its
    volume, the RSI pane under it, the price axis right of both. */
export interface ChartScale {
  width: number;
  height: number;
  paneW: number;
  priceH: number;
  rsiTop: number;
  rsiH: number;
  step: number;
  /** A bar's center. */
  x: (i: number) => number;
  /** A price's height. */
  y: (p: number) => number;
  /** An RSI value's height. */
  ry: (v: number) => number;
  /** Labelled levels on the price axis. */
  ticks: number[];
}

/** Price steps an axis picks among, smallest first. */
const STEPS = [1, 2, 2.5, 5, 10, 20, 25, 50];

/** The chart's layout for `s` at `width` × `height`. The price scale fits the
    bars and the averages, inside the app's margins (10% top, 20% bottom, the
    volume's fifth), as the chart's autoscale does. */
export function chartScale(s: Series, width: number, height: number): ChartScale {
  const paneW = width - AXIS_W;
  const plotH = height - TIME_H;
  const priceH = Math.round(plotH * 0.8);
  const rsiTop = priceH + 1;
  const rsiH = plotH - rsiTop;
  const step = (paneW - RIGHT_GAP) / s.bars.length;
  const prices = [...s.bars.flatMap((b) => [b.h, b.l]), ...s.ma.flatMap((m) => m.values)];
  const lo = Math.min(...prices);
  const hi = Math.max(...prices);
  const top = priceH * 0.1;
  const span = priceH * 0.7;
  const y = (p: number) => top + ((hi - p) / (hi - lo)) * span;
  const rlo = Math.min(...s.rsi);
  const rhi = Math.max(...s.rsi);
  const tick = STEPS.find((d) => (hi - lo) / d <= 5) ?? 100;
  const ticks: number[] = [];
  for (let p = Math.ceil((lo - 4 * tick) / tick) * tick; p <= hi + 4 * tick; p += tick) {
    if (y(p) > 6 && y(p) < priceH - 5) ticks.push(p);
  }
  return {
    width,
    height,
    paneW,
    priceH,
    rsiTop,
    rsiH,
    step,
    x: (i) => (i + 0.5) * step,
    y,
    ry: (v) => rsiTop + rsiH * 0.18 + ((rhi - v) / (rhi - rlo)) * rsiH * 0.64,
    ticks,
  };
}
