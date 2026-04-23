import { useCallback, useEffect, useRef, useState } from 'react';
import { ExternalLink, Loader2, Maximize2, RotateCcw, ZoomIn, ZoomOut } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import {
  createChart,
  ColorType,
  CrosshairMode,
  LineStyle,
  LineType,
  type IChartApi,
  type ISeriesApi,
  type MouseEventParams,
  type UTCTimestamp,
} from 'lightweight-charts';
import { fetchStockData } from '@/pages/MarketView/utils/api';
import { searchStocks } from '@/lib/marketUtils';
import {
  getChartTheme,
  STAGE2_BACKFILL_DAYS,
  SCROLL_CHUNK_DAYS,
  SCROLL_LOAD_THRESHOLD,
  RANGE_CHANGE_DEBOUNCE_MS,
  EXTENDED_HOURS_INTERVALS,
  INTERVALS as MARKET_INTERVALS,
  TARGET_BAR_SPACING,
  computeExtendedHoursRegions,
  getExtendedHoursType,
  isUSEquity,
} from '@/pages/MarketView/utils/chartConstants';
import { ExtendedHoursBgPrimitive } from '@/pages/MarketView/utils/extendedHoursBg';
import {
  centerLatestBarView,
  computeInitialLoadRange,
  dedupeMergeByTime,
  etDateStr,
  rangeBeforeOldest,
} from '@/pages/MarketView/utils/chartDataLoaders';
import { useMarketDataWSContext } from '@/pages/MarketView/contexts/MarketDataWSContext';
import { useTheme } from '@/contexts/ThemeContext';
import type { WidgetRenderProps, WidgetSettingsProps } from '../types';

// Widget intervals mirror MarketView's bar-interval keys (not the old
// visible-range labels). The interval IS the backend interval; we share
// INITIAL_LOAD_DAYS / SCROLL_CHUNK_DAYS / etc. with MarketView so history
// loading behaves identically.
//   • 1s excluded — requires US equities + live WS and doesn't suit a small widget.
//   • 4hour excluded — not supported on the yfinance tier, and we don't plumb
//     provider-tier gating into the widget today.
type ChartInterval = '1min' | '5min' | '15min' | '30min' | '1hour' | '1day';
type ChartType = 'candle' | 'area' | 'line';

type ChartConfig = {
  symbol: string;
  interval: ChartInterval;
  chartType: ChartType;
};

const EXCLUDED_INTERVALS = new Set<string>(['1s', '4hour']);

// Ordered for the toolbar; labels mirror MarketView's INTERVALS table.
const WIDGET_INTERVALS: { key: ChartInterval; label: string }[] = MARKET_INTERVALS
  .filter(({ key }) => !EXCLUDED_INTERVALS.has(key))
  .map(({ key, label }) => ({ key: key as ChartInterval, label }));

const VALID_INTERVALS: ReadonlySet<ChartInterval> = new Set(
  WIDGET_INTERVALS.map((i) => i.key),
);

const DEFAULT_CONFIG: ChartConfig = { symbol: 'NVDA', interval: '1day', chartType: 'candle' };

// Live-tick aggregation: MarketView only applies WS ticks on 1s/1min so the
// bar's `open` is anchored to the true start of the bucket. We mirror that:
// for every interval other than 1min the REST fetch (initial + stage-2) is
// the source of truth and WS is ignored. Keeps OHLC consistent with MV.
const LIVE_BUCKET_SEC = 60;
// REST polling cadence for the 1min interval — same as MV's 1min fallback.
const DELTA_POLL_MS = 15000;
// Skip a REST poll if a WS tick arrived within this window — the WS path
// already has fresher data in place.
const STALE_WS_WINDOW_MS = 5000;

type Bar = {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
};

// Hover timestamps are stored as ET-wall-clock-as-UTC (same 'Z' trick the rest
// of the chart uses), so read with UTC getters to display ET-local values.
const HOVER_MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
function formatHoverTime(timeSec: number, daily: boolean): string {
  const d = new Date(timeSec * 1000);
  const mon = HOVER_MONTHS[d.getUTCMonth()];
  const day = d.getUTCDate();
  if (daily) return `${mon} ${day}, ${d.getUTCFullYear()}`;
  const hh = String(d.getUTCHours()).padStart(2, '0');
  const mm = String(d.getUTCMinutes()).padStart(2, '0');
  return `${mon} ${day}, ${hh}:${mm}`;
}

function formatHoverVolume(v: number): string {
  if (v >= 1e9) return `${(v / 1e9).toFixed(2)}B`;
  if (v >= 1e6) return `${(v / 1e6).toFixed(2)}M`;
  if (v >= 1e3) return `${(v / 1e3).toFixed(1)}K`;
  return String(Math.round(v));
}

// Return the ET calendar date (YYYY-MM-DD) of a bar. Bar timestamps are stored
// as ET-wall-clock-as-UTC (the 'Z' trick used across the chart), so UTC getters
// read back the ET-local values. Used to group bars by trading day for the
// header's intraday-change summary.
function etDateOfBar(timeSec: number): string {
  const d = new Date(timeSec * 1000);
  const y = d.getUTCFullYear();
  const m = d.getUTCMonth() + 1;
  const dd = d.getUTCDate();
  return `${y}-${String(m).padStart(2, '0')}-${String(dd).padStart(2, '0')}`;
}

function ChartWidget({ instance, updateConfig }: WidgetRenderProps<ChartConfig>) {
  // Merge stored config onto the default, then sanitize the interval: if a
  // persisted dashboard pref has a legacy visible-range key (e.g. '3M') from
  // before the bar-interval refactor, silently fall back to the default so
  // downstream backend calls never see an unrecognized key.
  const merged = { ...DEFAULT_CONFIG, ...instance.config };
  const config: ChartConfig = {
    ...merged,
    interval: VALID_INTERVALS.has(merged.interval as ChartInterval)
      ? (merged.interval as ChartInterval)
      : DEFAULT_CONFIG.interval,
  };
  const navigate = useNavigate();
  const { theme } = useTheme();
  const ct = getChartTheme(theme);
  const ctRef = useRef(ct);
  ctRef.current = ct;

  // Shared WS provided at DashboardCustom level. When ginlix-data is disabled
  // the hook returns an empty `prices` map and `subscribe` is a no-op.
  const { prices, subscribe, unsubscribe, ginlixDataEnabled } = useMarketDataWSContext();

  // --- Refs mirroring MarketChart's data-loading machinery ---
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<ISeriesApi<'Candlestick'> | ISeriesApi<'Area'> | ISeriesApi<'Line'> | null>(null);
  const volumeRef = useRef<ISeriesApi<'Histogram'> | null>(null);
  const seriesTypeRef = useRef<ChartType>(config.chartType);

  const allDataRef = useRef<Bar[]>([]);
  const oldestDateRef = useRef<number | null>(null); // unix seconds (lightweight-charts time)
  const fetchingRef = useRef(false);
  const rangeChangeTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const extHoursBgRef = useRef<ExtendedHoursBgPrimitive | null>(null);
  // Track which series the primitive is currently attached to so detach on
  // series swap works regardless of series type.
  const extHoursAttachedToRef = useRef<ISeriesApi<'Candlestick' | 'Area' | 'Line'> | null>(null);
  const symbolRef = useRef(config.symbol);
  const intervalRef = useRef<ChartInterval>(config.interval);
  symbolRef.current = config.symbol;
  intervalRef.current = config.interval;

  // --- AbortControllers (one per fetch path) ---
  const initAbortRef = useRef<AbortController | null>(null);
  const stage2AbortRef = useRef<AbortController | null>(null);

  // --- WS live-tick bookkeeping ---
  const lastLiveTickTimeRef = useRef(0); // wall-clock ms of the most recent applied WS tick
  const gapFillDoneRef = useRef(false);
  const gapFillInProgressRef = useRef(false);
  const gapFillRetryRef = useRef(0);
  // WS prices are keyed by uppercased symbol
  const sym = config.symbol.toUpperCase();
  const liveTick = prices.get(sym) ?? null;

  // Live header price (from the latest applied WS tick or REST poll result).
  const [liveLast, setLiveLast] = useState<number | null>(null);
  // `loading` is true during the initial fetch for a (symbol, interval) combo.
  // Used to render the chart-body spinner overlay; cleared once data lands.
  const [loading, setLoading] = useState(true);

  // --- Summary state (header) ---
  const [summary, setSummary] = useState<{ first: number; last: number } | null>(null);
  // --- Hover state: populated from crosshair move, shown as a small overlay
  // in the top-left corner of the chart body (TradingView-style).
  const [hover, setHover] = useState<
    | { time: number; open?: number; high?: number; low?: number; close: number; volume?: number }
    | null
  >(null);
  // Live last overrides the bar-based summary.last whenever a fresher tick is
  // available — lets the header update between bar closes.
  const headerLast = liveLast ?? summary?.last ?? 0;
  const change = summary ? headerLast - summary.first : 0;
  const pct = summary && summary.first ? (change / summary.first) * 100 : 0;
  const positive = change >= 0;
  const changeColor = positive ? ct.upColor : ct.downColor;

  // --- Push the full ref-held dataset to both price and volume series ---
  // Reads theme via `ctRef.current` (synced each render above) so this callback
  // stays referentially stable across renders. Listing `ct.volumeUp` etc. in
  // the deps would re-memoize on every theme toggle, which would then cascade
  // through `mergePrependedData` → the initial-load effect and abort every
  // in-flight fetch. MV uses the same pattern.
  const updateSeriesData = useCallback((bars: Bar[]) => {
    const series = seriesRef.current;
    if (!series) return;
    const theme = ctRef.current;
    const type = seriesTypeRef.current;

    if (type === 'candle') {
      (series as ISeriesApi<'Candlestick'>).setData(
        bars.map((r) => ({
          time: r.time as UTCTimestamp,
          open: r.open,
          high: r.high,
          low: r.low,
          close: r.close,
        })),
      );
    } else {
      (series as ISeriesApi<'Area'> | ISeriesApi<'Line'>).setData(
        bars.map((r) => ({ time: r.time as UTCTimestamp, value: r.close })),
      );
    }

    if (volumeRef.current) {
      volumeRef.current.setData(
        bars.map((r) => ({
          time: r.time as UTCTimestamp,
          value: r.volume,
          color: r.close >= r.open ? theme.volumeUp : theme.volumeDown,
        })),
      );
      // Freeze the volume scale across all loaded bars so zooming/scrolling
      // doesn't rescale each time (lightweight-charts default is auto-scale
      // per visible range, which crushes normal bars when one outlier is in
      // view). We clamp to the 95th percentile so a single opening-auction
      // spike doesn't dominate either — extreme bars simply clip.
      const vols: number[] = [];
      for (const r of bars) if (r.volume > 0) vols.push(r.volume);
      if (vols.length > 0) {
        vols.sort((a, b) => a - b);
        const p95 = vols[Math.min(vols.length - 1, Math.floor(vols.length * 0.95))];
        const maxValue = p95 * 1.1;
        volumeRef.current.applyOptions({
          autoscaleInfoProvider: () => ({
            priceRange: { minValue: 0, maxValue },
          }),
        });
      }
    }

    // Extended-hours shading: only meaningful for US equities on intraday
    // intervals (same gate MarketView uses). Note: the underlying data
    // provider typically only returns pre/post bars for the *current* trading
    // day — historical days come back RTH-only, so those sessions can't be
    // shaded (there are no logical bars in those windows to anchor a region
    // to). This matches MarketView's behavior.
    if (extHoursBgRef.current) {
      const ivOk = EXTENDED_HOURS_INTERVALS.has(intervalRef.current);
      const usEquity = isUSEquity(symbolRef.current);
      extHoursBgRef.current.setColors({ pre: theme.extBgPre, post: theme.extBgPost });
      if (ivOk && usEquity) {
        extHoursBgRef.current.setRegions(computeExtendedHoursRegions(bars));
      } else {
        extHoursBgRef.current.setRegions([]);
      }
    }

    if (bars.length === 0) {
      setSummary(null);
      return;
    }
    // Header change is always the current-session move: the open of the
    // first bar of the latest trading day, vs. the latest close. This stays
    // consistent across intervals (1min and 1day both show "today's move"),
    // and — critically — doesn't drift into "last N days" territory when
    // scroll-load pulls more history into view.
    const lastBar = bars[bars.length - 1];
    const sessionKey = etDateOfBar(lastBar.time);
    let sessionStartIdx = bars.length - 1;
    for (let i = bars.length - 1; i >= 0; i--) {
      if (etDateOfBar(bars[i].time) === sessionKey) sessionStartIdx = i;
      else break;
    }
    const first = bars[sessionStartIdx].open;
    const last = lastBar.close;
    setSummary({ first, last });
  }, []);

  // --- Default view: mirror MarketView's convention — latest bar centered
  // at `TARGET_BAR_SPACING[interval]` pixels per bar. Keeps the candlestick
  // ratio and scroll feel consistent between the widget and the full chart
  // page. Half the chart width is reserved as empty future-space on the
  // right (same as `centerLatestBarView`).
  const applyDefaultView = useCallback(() => {
    const chart = chartRef.current;
    if (!chart) return;
    const bars = allDataRef.current;
    const ts = chart.timeScale();
    if (bars.length === 0) {
      ts.scrollToRealTime();
      return;
    }
    const barSpacing = TARGET_BAR_SPACING[intervalRef.current] || 8;
    ts.applyOptions({ barSpacing });
    const chartWidth =
      (chart.options() as { width?: number }).width ||
      containerRef.current?.clientWidth ||
      800;
    ts.setVisibleLogicalRange(
      centerLatestBarView({ chartWidth, barSpacing, dataLen: bars.length }),
    );
  }, []);

  // --- Merge newly-fetched older bars and preserve user's visible window ---
  const mergePrependedData = useCallback((newData: Bar[]) => {
    const { merged, prependedCount } = dedupeMergeByTime(allDataRef.current, newData);
    if (prependedCount === 0 && merged === allDataRef.current) return;
    allDataRef.current = merged;
    oldestDateRef.current = merged[0]?.time ?? null;

    const ts = chartRef.current?.timeScale();
    const saved = ts?.getVisibleLogicalRange();
    updateSeriesData(merged);
    if (ts && saved && prependedCount > 0) {
      ts.setVisibleLogicalRange({
        from: saved.from + prependedCount,
        to: saved.to + prependedCount,
      });
    }
  }, [updateSeriesData]);

  // --- Fetch older bars before current oldest and merge (scroll-load path) ---
  // Mirrors MarketView's `fetchAndPrepend`: no AbortController — staleness is
  // detected via the symbol/interval ref guard after the response lands.
  const fetchAndPrepend = useCallback(async (days: number) => {
    if (!oldestDateRef.current || days <= 0) return;
    const sym = symbolRef.current;
    const iv = intervalRef.current;
    const { fromStr, toStr } = rangeBeforeOldest(oldestDateRef.current, days);
    try {
      const res = await fetchStockData(sym, iv, fromStr, toStr);
      if (symbolRef.current !== sym || intervalRef.current !== iv) return; // stale
      const bars = (res?.data ?? []) as Bar[];
      if (bars.length > 0) mergePrependedData(bars);
    } catch (err) {
      const e = err as { name?: string };
      if (e?.name === 'AbortError' || e?.name === 'CanceledError') return;
      console.warn('[chart-widget] scroll-load fetch failed:', err);
    }
  }, [mergePrependedData]);

  const handleScrollLoadMore = useCallback(async () => {
    if (fetchingRef.current) return;
    fetchingRef.current = true;
    try {
      await fetchAndPrepend(SCROLL_CHUNK_DAYS[intervalRef.current] || 0);
    } finally {
      fetchingRef.current = false;
    }
  }, [fetchAndPrepend]);

  // ============================================================
  // Effect 1: Create chart once on mount
  // ============================================================
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;

    const chart = createChart(el, {
      autoSize: true,
      layout: {
        background: { type: ColorType.Solid, color: 'transparent' },
        textColor: ct.text,
        fontSize: 10,
      },
      grid: {
        vertLines: { visible: false },
        horzLines: { color: ct.grid, style: LineStyle.Solid, visible: true },
      },
      crosshair: {
        mode: CrosshairMode.Magnet,
        vertLine: { color: ct.text, width: 1, style: LineStyle.Dashed, labelVisible: false },
        horzLine: { color: ct.text, width: 1, style: LineStyle.Dashed, labelVisible: true },
      },
      rightPriceScale: {
        borderVisible: false,
        scaleMargins: { top: 0.1, bottom: 0.2 },
      },
      timeScale: {
        borderVisible: false,
        timeVisible: true,
        secondsVisible: false,
        rightOffset: 4,
      },
      handleScroll: {
        mouseWheel: true,
        pressedMouseMove: true,
        horzTouchDrag: true,
        vertTouchDrag: false,
      },
      handleScale: {
        // Drag the time axis to scale horizontally, drag the price axis to
        // scale vertically — TradingView-style.
        axisPressedMouseMove: { time: true, price: true },
        mouseWheel: true,
        pinch: true,
      },
    });
    chartRef.current = chart;

    const volume = chart.addHistogramSeries({
      priceFormat: { type: 'volume' },
      priceScaleId: 'volume',
      color: 'rgba(128,128,128,0.3)',
    });
    volume.priceScale().applyOptions({
      scaleMargins: { top: 0.8, bottom: 0 },
    });
    volumeRef.current = volume;

    // Extended-hours background primitive — attached to the price series
    // in Effect 3 whenever a series is created or swapped.
    extHoursBgRef.current = new ExtendedHoursBgPrimitive();

    // Scroll-load subscription (debounced). Only fires once initial data
    // has been loaded + default view applied (oldestDateRef is set then).
    const rangeHandler = (range: { from: number; to: number } | null) => {
      if (!range || !oldestDateRef.current) return;
      if (rangeChangeTimerRef.current) clearTimeout(rangeChangeTimerRef.current);
      rangeChangeTimerRef.current = setTimeout(() => {
        // Scroll-load: merge when near the left edge.
        if (range.from <= SCROLL_LOAD_THRESHOLD) {
          handleScrollLoadMore();
        }
      }, RANGE_CHANGE_DEBOUNCE_MS);
    };
    chart.timeScale().subscribeVisibleLogicalRangeChange(rangeHandler);

    // Crosshair → hover overlay. We read seriesRef.current inside the callback
    // so swapping chart types (candle ↔ area ↔ line) keeps working without
    // re-subscribing.
    const crosshairHandler = (param: MouseEventParams) => {
      const series = seriesRef.current;
      if (!series || param.time == null || !param.point) {
        setHover((prev) => (prev ? null : prev));
        return;
      }
      const bar = param.seriesData.get(series) as
        | { open?: number; high?: number; low?: number; close?: number; value?: number }
        | undefined;
      if (!bar) {
        setHover((prev) => (prev ? null : prev));
        return;
      }
      const volBar = volumeRef.current
        ? (param.seriesData.get(volumeRef.current) as { value?: number } | undefined)
        : undefined;
      const time = param.time as unknown as number;
      if (bar.close != null && bar.open != null) {
        setHover({
          time,
          open: bar.open,
          high: bar.high,
          low: bar.low,
          close: bar.close,
          volume: volBar?.value,
        });
      } else if (bar.value != null) {
        setHover({ time, close: bar.value, volume: volBar?.value });
      } else {
        setHover((prev) => (prev ? null : prev));
      }
    };
    chart.subscribeCrosshairMove(crosshairHandler);

    return () => {
      if (rangeChangeTimerRef.current) clearTimeout(rangeChangeTimerRef.current);
      chart.timeScale().unsubscribeVisibleLogicalRangeChange(rangeHandler);
      chart.unsubscribeCrosshairMove(crosshairHandler);
      chart.remove();
      chartRef.current = null;
      seriesRef.current = null;
      volumeRef.current = null;
      extHoursAttachedToRef.current = null;
      extHoursBgRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // ============================================================
  // Effect 2: Apply theme changes without recreating the chart
  // ============================================================
  useEffect(() => {
    const chart = chartRef.current;
    if (!chart) return;
    chart.applyOptions({
      layout: {
        background: { type: ColorType.Solid, color: 'transparent' },
        textColor: ct.text,
      },
      grid: {
        horzLines: { color: ct.grid, style: LineStyle.Solid, visible: true },
        vertLines: { visible: false },
      },
      crosshair: {
        vertLine: { color: ct.text },
        horzLine: { color: ct.text },
      },
    });
  }, [ct.text, ct.grid]);

  // ============================================================
  // Effect 3: Create / swap the price series on chart-type change
  // ============================================================
  useEffect(() => {
    const chart = chartRef.current;
    if (!chart) return;

    // Already correct type — just reapply colors (positive may have flipped).
    if (seriesRef.current && seriesTypeRef.current === config.chartType) {
      const s = seriesRef.current;
      if (config.chartType === 'candle') {
        (s as ISeriesApi<'Candlestick'>).applyOptions({
          upColor: ct.upColor,
          downColor: ct.downColor,
          borderVisible: false,
          wickUpColor: ct.upColor,
          wickDownColor: ct.downColor,
        });
      } else if (config.chartType === 'area') {
        (s as ISeriesApi<'Area'>).applyOptions({
          lineColor: changeColor,
          topColor: positive ? ct.baselineUpFill1 : ct.baselineDownFill2,
          bottomColor: positive ? ct.baselineUpFill2 : ct.baselineDownFill1,
        });
      } else {
        (s as ISeriesApi<'Line'>).applyOptions({ color: changeColor });
      }
      return;
    }

    if (seriesRef.current) {
      chart.removeSeries(seriesRef.current);
      seriesRef.current = null;
      // removeSeries auto-detaches primitives, but clear our bookkeeping too.
      extHoursAttachedToRef.current = null;
    }

    let next: ISeriesApi<'Candlestick'> | ISeriesApi<'Area'> | ISeriesApi<'Line'>;
    if (config.chartType === 'candle') {
      next = chart.addCandlestickSeries({
        upColor: ct.upColor,
        downColor: ct.downColor,
        borderVisible: false,
        wickUpColor: ct.upColor,
        wickDownColor: ct.downColor,
      });
    } else if (config.chartType === 'area') {
      next = chart.addAreaSeries({
        lineColor: changeColor,
        topColor: positive ? ct.baselineUpFill1 : ct.baselineDownFill2,
        bottomColor: positive ? ct.baselineUpFill2 : ct.baselineDownFill1,
        lineWidth: 2,
        lineType: LineType.Simple,
        priceLineVisible: false,
      });
    } else {
      next = chart.addLineSeries({
        color: changeColor,
        lineWidth: 2,
        priceLineVisible: false,
      });
    }
    seriesRef.current = next;
    seriesTypeRef.current = config.chartType;

    // Attach the extended-hours primitive to the new series so the
    // pre/post-market bands stay rendered after a chart-type swap.
    if (extHoursBgRef.current) {
      (next as ISeriesApi<'Candlestick' | 'Area' | 'Line'>).attachPrimitive(extHoursBgRef.current);
      extHoursAttachedToRef.current = next as ISeriesApi<'Candlestick' | 'Area' | 'Line'>;
    }

    // If we already have data loaded (e.g., user toggled chart type), push it.
    if (allDataRef.current.length > 0) updateSeriesData(allDataRef.current);
  }, [config.chartType, changeColor, positive, ct.upColor, ct.downColor, ct.baselineUpFill1, ct.baselineUpFill2, ct.baselineDownFill1, ct.baselineDownFill2, updateSeriesData]);

  // ============================================================
  // Effect 4: Interval label format + initial data load
  // ============================================================
  useEffect(() => {
    const chart = chartRef.current;
    if (!chart) return;
    const isIntraday = config.interval !== '1day';
    const showSeconds = config.interval === '1min';
    chart.timeScale().applyOptions({ timeVisible: isIntraday, secondsVisible: showSeconds });
  }, [config.interval]);

  // Initial (symbol, interval) fetch — resets data, applies default view, and
  // kicks off a silent Stage-2 backfill for more history.
  useEffect(() => {
    const chart = chartRef.current;
    if (!chart) return;

    const sym = config.symbol;
    const iv = config.interval;

    // Abort any in-flight fetches from a prior (symbol, interval) combo so
    // stale responses can't overwrite fresh data.
    initAbortRef.current?.abort();
    stage2AbortRef.current?.abort();
    const ac = new AbortController();
    initAbortRef.current = ac;

    // Reset refs so scroll-load doesn't trigger off stale oldestDateRef during
    // the fetch window; it also prevents a stale merge of old bars.
    allDataRef.current = [];
    oldestDateRef.current = null;
    fetchingRef.current = false;
    gapFillDoneRef.current = false;
    gapFillInProgressRef.current = false;
    gapFillRetryRef.current = 0;
    lastLiveTickTimeRef.current = 0;
    setLiveLast(null);
    setLoading(true);
    // Clear the currently-drawn series so the user sees a clean loading state
    // rather than stale bars from the previous (symbol, interval) combo.
    updateSeriesData([]);

    // Shared helper: STAGE1_LOAD_DAYS if defined (fast-render path for 1min),
    // else INITIAL_LOAD_DAYS; 0 days → undefined bounds (full history).
    // ET-anchored so the ET/UTC boundary doesn't drop today's intraday bars.
    const { fromStr, toStr } = computeInitialLoadRange(iv);

    (async () => {
      try {
        const res = await fetchStockData(sym, iv, fromStr, toStr, { signal: ac.signal });
        if (ac.signal.aborted || symbolRef.current !== sym || intervalRef.current !== iv) return;
        const bars = (res?.data ?? []) as Bar[];
        if (bars.length === 0) {
          updateSeriesData([]);
          setLoading(false);
          return;
        }
        allDataRef.current = bars;
        oldestDateRef.current = bars[0].time;
        updateSeriesData(bars);
        // Apply the default view immediately so the visible range is set
        // before the chart first paints (prevents a flash of "all history"
        // when setVisibleRange is called later than the chart's initial
        // layout). We then re-apply in an rAF *and* a short setTimeout to
        // cover the autoSize race: when the widget container is inside a
        // just-laid-out grid, the first ResizeObserver tick may not have
        // fired yet on the sync call, so we reassert once layout is stable.
        applyDefaultView();
        requestAnimationFrame(() => {
          if (ac.signal.aborted) return;
          if (symbolRef.current !== sym || intervalRef.current !== iv) return;
          applyDefaultView();
          setLoading(false);
        });
        setTimeout(() => {
          if (ac.signal.aborted) return;
          if (symbolRef.current !== sym || intervalRef.current !== iv) return;
          applyDefaultView();
        }, 150);

        // --- Stage 2: silent background backfill of more prior history ---
        const backfillDays = STAGE2_BACKFILL_DAYS[iv] || 0;
        if (backfillDays > 0 && oldestDateRef.current) {
          const stage2Ac = new AbortController();
          stage2AbortRef.current = stage2Ac;
          const capturedSym = sym;
          const capturedIv = iv;
          // Yield to the first paint before starting the background fetch.
          setTimeout(async () => {
            if (stage2Ac.signal.aborted) return;
            if (symbolRef.current !== capturedSym || intervalRef.current !== capturedIv) return;
            if (!oldestDateRef.current) return;
            const { fromStr: bfFrom, toStr: bfTo } = rangeBeforeOldest(
              oldestDateRef.current,
              backfillDays,
            );
            try {
              const backfill = await fetchStockData(
                capturedSym,
                capturedIv,
                bfFrom,
                bfTo,
                { signal: stage2Ac.signal },
              );
              if (stage2Ac.signal.aborted) return;
              if (symbolRef.current !== capturedSym || intervalRef.current !== capturedIv) return;
              const extra = (backfill?.data ?? []) as Bar[];
              if (extra.length > 0) mergePrependedData(extra);
            } catch (err) {
              const e = err as { name?: string };
              if (e?.name === 'AbortError' || e?.name === 'CanceledError') return;
              console.warn('[chart-widget] stage-2 backfill failed:', err);
            }
          }, 50);
        }
      } catch (err) {
        const e = err as { name?: string };
        if (e?.name === 'AbortError' || e?.name === 'CanceledError') return;
        console.warn('[chart-widget] initial fetch failed:', err);
        setLoading(false);
      }
    })();

    return () => {
      ac.abort();
      stage2AbortRef.current?.abort();
    };
  }, [config.symbol, config.interval, applyDefaultView, updateSeriesData, mergePrependedData]);

  // ============================================================
  // Effect 5: WS subscription — one ref-count per symbol across all
  // ChartWidgets sharing the same MarketDataWSProvider. Only subscribe on
  // 1min; other intervals don't consume ticks so subscribing there is waste.
  // ============================================================
  useEffect(() => {
    if (!ginlixDataEnabled) return;
    if (config.interval !== '1min') return;
    const upper = config.symbol.toUpperCase();
    if (!upper) return;
    subscribe([upper]);
    return () => {
      unsubscribe([upper]);
    };
  }, [config.symbol, config.interval, ginlixDataEnabled, subscribe, unsubscribe]);

  // ============================================================
  // Effect 6: WS live-tick apply — aggregate 1s ticks into the widget's
  // bucket (1min only — matches MarketView's WS aggregation scope). Longer
  // intervals (5min+) rely on the REST initial + stage-2 fetches to keep OHLC
  // canonical; aggregating 1s ticks into e.g. a 5min bucket would record a
  // wrong `open` whenever we start observing mid-bucket.
  // Also handles a REST gap fill if the first WS tick lands > 2 buckets past
  // the last REST bar.
  // ============================================================
  useEffect(() => {
    if (!liveTick) return;
    if (intervalRef.current !== '1min') return;
    const series = seriesRef.current;
    const vol = volumeRef.current;
    if (!series || !vol) return;

    const b = liveTick.barData;
    if (!b || b.close == null) return;

    // Track freshness so the REST poll can skip when WS is alive.
    lastLiveTickTimeRef.current = Date.now();
    setLiveLast(liveTick.price);

    const data = allDataRef.current;
    const bucketTime = Math.floor(b.time / LIVE_BUCKET_SEC) * LIVE_BUCKET_SEC;

    // Gap fill: if the first WS tick lands well past our last REST bar, do a
    // one-shot REST fetch to bridge the gap. Retries up to 3 times.
    if (!gapFillDoneRef.current && !gapFillInProgressRef.current && data.length > 0) {
      const lastDataTime = data[data.length - 1].time;
      const gap = bucketTime - lastDataTime;
      const threshold = LIVE_BUCKET_SEC * 2;
      if (gap > threshold) {
        gapFillRetryRef.current += 1;
        if (gapFillRetryRef.current > 3) {
          gapFillDoneRef.current = true;
        } else {
          gapFillInProgressRef.current = true;
          const sym = symbolRef.current;
          const iv = intervalRef.current;
          // ET-anchored date strings — a `new Date().toISOString()` here would
          // straddle UTC midnight late in the trading day and ask for tomorrow.
          const fromStr = etDateStr(new Date(lastDataTime * 1000));
          const toStr = etDateStr();
          (async () => {
            try {
              const res = await fetchStockData(sym, iv, fromStr, toStr);
              if (symbolRef.current !== sym || intervalRef.current !== iv) return;
              const fill = (res?.data ?? []) as Bar[];
              if (fill.length > 0) {
                const existing = new Set(allDataRef.current.map((x) => x.time));
                const merged = [...allDataRef.current, ...fill.filter((x) => !existing.has(x.time))]
                  .sort((a, z) => a.time - z.time);
                allDataRef.current = merged;
                updateSeriesData(merged);
                const lastFilled = merged[merged.length - 1]?.time ?? 0;
                if (lastFilled >= bucketTime - threshold) gapFillDoneRef.current = true;
              }
            } catch (err) {
              console.debug('[chart-widget] gap fill failed:', err);
            } finally {
              gapFillInProgressRef.current = false;
            }
          })();
        }
      } else {
        gapFillDoneRef.current = true;
      }
    }

    // Out-of-order guard — lightweight-charts' series.update requires time
    // monotonically non-decreasing.
    if (data.length > 0 && bucketTime < data[data.length - 1].time) return;

    let bar: Bar;
    const last = data[data.length - 1];
    if (last && last.time === bucketTime) {
      // Merge into the currently-drawn bar.
      last.high = Math.max(last.high, b.high);
      last.low = Math.min(last.low, b.low);
      last.close = b.close;
      last.volume += b.volume ?? 0;
      bar = last;
    } else {
      // Start a new bucket.
      bar = {
        time: bucketTime,
        open: b.open,
        high: b.high,
        low: b.low,
        close: b.close,
        volume: b.volume ?? 0,
      };
      data.push(bar);
    }

    const up = bar.close >= bar.open;
    const theme = ctRef.current;
    const ext = isUSEquity(symbolRef.current) &&
      EXTENDED_HOURS_INTERVALS.has(intervalRef.current) &&
      getExtendedHoursType(bar.time);
    if (seriesTypeRef.current === 'candle') {
      (series as ISeriesApi<'Candlestick'>).update({
        time: bar.time as UTCTimestamp,
        open: bar.open,
        high: bar.high,
        low: bar.low,
        close: bar.close,
      });
    } else {
      (series as ISeriesApi<'Area'> | ISeriesApi<'Line'>).update({
        time: bar.time as UTCTimestamp,
        value: bar.close,
      });
    }
    vol.update({
      time: bar.time as UTCTimestamp,
      value: bar.volume,
      color: ext
        ? up ? theme.extVolumeUp : theme.extVolumeDown
        : up ? theme.volumeUp : theme.volumeDown,
    });
  }, [liveTick, updateSeriesData]);

  // ============================================================
  // Effect 7: REST delta-poll (every 15s) — WS fallback for 1min only.
  // MarketView polls 1s every 5s and 1min every 15s; longer intervals refresh
  // through stage-2 + scroll, not a fixed poll. The widget excludes 1s, so
  // only the 1min path runs here. Skips a tick if WS delivered a fresh tick
  // in the last 5s, and re-uses `updateSeriesData` for the in-place merge.
  // ============================================================
  useEffect(() => {
    if (config.interval !== '1min') return;
    let aborted = false;
    const poll = async () => {
      if (aborted) return;
      if (lastLiveTickTimeRef.current > Date.now() - STALE_WS_WINDOW_MS) return;

      const lastBar = allDataRef.current[allDataRef.current.length - 1];
      if (!lastBar) return;

      const sym = symbolRef.current;
      const iv = intervalRef.current;
      const fromStr = etDateStr(new Date(lastBar.time * 1000));
      const toStr = etDateStr();

      try {
        const res = await fetchStockData(sym, iv, fromStr, toStr);
        if (aborted) return;
        if (symbolRef.current !== sym || intervalRef.current !== iv) return;
        const bars = (res?.data ?? []) as Bar[];
        if (bars.length === 0) return;
        const lastKnown = allDataRef.current[allDataRef.current.length - 1]?.time ?? 0;
        const newer = bars.filter((b) => b.time >= lastKnown);
        if (newer.length === 0) return;

        // Drop the stale last-bar and append the refreshed span so its close /
        // high / low reflects the latest intraday data.
        const trimmed = allDataRef.current.slice(0, -1);
        const merged = [...trimmed, ...newer].sort((a, b) => a.time - b.time);
        allDataRef.current = merged;
        updateSeriesData(merged);
        const latest = merged[merged.length - 1];
        if (latest) setLiveLast(latest.close);
      } catch (err) {
        const e = err as { name?: string };
        if (e?.name === 'AbortError' || e?.name === 'CanceledError') return;
        console.debug('[chart-widget] delta poll failed:', err);
      }
    };
    const timer = setInterval(poll, DELTA_POLL_MS);
    return () => {
      aborted = true;
      clearInterval(timer);
    };
  }, [config.interval, updateSeriesData]);

  // --- Toolbar handlers ---
  const handleZoomIn = useCallback(() => {
    const ts = chartRef.current?.timeScale();
    if (!ts) return;
    const current = ts.options().barSpacing ?? 8;
    ts.applyOptions({ barSpacing: Math.min(current * 1.5, 50) });
  }, []);

  const handleZoomOut = useCallback(() => {
    const ts = chartRef.current?.timeScale();
    if (!ts) return;
    const current = ts.options().barSpacing ?? 8;
    ts.applyOptions({ barSpacing: Math.max(current / 1.5, 1) });
  }, []);

  const handleFitAll = useCallback(() => {
    chartRef.current?.timeScale().fitContent();
  }, []);

  const handleResetView = useCallback(() => {
    applyDefaultView();
  }, [applyDefaultView]);

  const hasData = allDataRef.current.length > 0 || summary !== null;

  // Inline symbol search — click the symbol text to edit in place. The same
  // debounced search the DashboardHeader uses powers a dropdown of matches
  // so users can ticker-hunt without leaving the widget. Enter picks the
  // first match (or the raw uppercase query if no matches); Escape or
  // outside-click cancels.
  type SymbolHit = { symbol: string; name?: string };
  const [editingSymbol, setEditingSymbol] = useState(false);
  const [symbolDraft, setSymbolDraft] = useState(config.symbol);
  const [searchHits, setSearchHits] = useState<SymbolHit[]>([]);
  const [searchLoading, setSearchLoading] = useState(false);
  const symbolEditorRef = useRef<HTMLDivElement | null>(null);
  const symbolInputRef = useRef<HTMLInputElement | null>(null);

  const selectSymbol = useCallback((sym: string) => {
    const next = sym.trim().toUpperCase();
    if (next && next !== config.symbol) updateConfig({ symbol: next });
    setEditingSymbol(false);
    setSearchHits([]);
  }, [config.symbol, updateConfig]);

  const startEditSymbol = useCallback(() => {
    setSymbolDraft(config.symbol);
    setSearchHits([]);
    setEditingSymbol(true);
    requestAnimationFrame(() => {
      symbolInputRef.current?.focus();
      symbolInputRef.current?.select();
    });
  }, [config.symbol]);

  // Debounced search while editing. Kept in the component (not extracted) —
  // this is the only caller and the state is small.
  useEffect(() => {
    if (!editingSymbol) return;
    const q = symbolDraft.trim();
    if (!q) {
      setSearchHits([]);
      setSearchLoading(false);
      return;
    }
    setSearchLoading(true);
    const timer = setTimeout(async () => {
      try {
        const res = await searchStocks(q, 8);
        setSearchHits(((res.results || []) as SymbolHit[]).slice(0, 8));
      } catch {
        setSearchHits([]);
      } finally {
        setSearchLoading(false);
      }
    }, 250);
    return () => clearTimeout(timer);
  }, [symbolDraft, editingSymbol]);

  // Outside-click dismiss for the editor + dropdown.
  useEffect(() => {
    if (!editingSymbol) return;
    const onMouseDown = (e: MouseEvent) => {
      if (symbolEditorRef.current && !symbolEditorRef.current.contains(e.target as Node)) {
        setEditingSymbol(false);
        setSearchHits([]);
      }
    };
    document.addEventListener('mousedown', onMouseDown);
    return () => document.removeEventListener('mousedown', onMouseDown);
  }, [editingSymbol]);

  return (
    <div
      className="flex flex-col h-full rounded-xl px-4 pt-4 pb-3"
      style={{
        backgroundColor: 'var(--color-bg-card)',
        border: '1px solid var(--color-border-muted)',
      }}
    >
      <div className="flex items-center justify-between mb-2">
        <div ref={symbolEditorRef} className="relative text-left min-w-0">
          {editingSymbol ? (
            <input
              ref={symbolInputRef}
              type="text"
              value={symbolDraft}
              onChange={(e) => setSymbolDraft(e.target.value.toUpperCase())}
              onKeyDown={(e) => {
                if (e.key === 'Enter') {
                  // Prefer the top search hit so users can type "nvid" and
                  // land on NVDA. Fall back to the raw draft for exact
                  // symbols the search doesn't know (e.g. freshly listed).
                  const pick = searchHits[0]?.symbol || symbolDraft;
                  selectSymbol(pick);
                } else if (e.key === 'Escape') {
                  setEditingSymbol(false);
                  setSymbolDraft(config.symbol);
                  setSearchHits([]);
                }
              }}
              className="text-sm font-semibold tabular-nums bg-transparent outline-none border-b px-0 py-0 w-24"
              style={{
                color: 'var(--color-text-primary)',
                borderColor: 'var(--color-accent-primary)',
              }}
              maxLength={12}
              autoComplete="off"
              spellCheck={false}
              placeholder="Search…"
            />
          ) : (
            <div className="flex items-baseline gap-2">
              <button
                type="button"
                onClick={startEditSymbol}
                title="Click to change symbol"
                className="text-sm font-semibold tabular-nums hover:opacity-70 transition-opacity"
                style={{ color: 'var(--color-text-primary)' }}
              >
                {config.symbol}
              </button>
              {headerLast > 0 && (
                <span
                  className="text-sm tabular-nums"
                  style={{ color: 'var(--color-text-primary)' }}
                >
                  {headerLast.toFixed(2)}
                </span>
              )}
            </div>
          )}
          <div className="text-[11px] tabular-nums" style={{ color: changeColor }}>
            {positive ? '+' : ''}{change.toFixed(2)} ({positive ? '+' : ''}{pct.toFixed(2)}%)
          </div>
          {editingSymbol && symbolDraft.trim() && (
            <div
              className="absolute left-0 top-full mt-1 z-20 min-w-[200px] max-w-[280px] rounded-md shadow-lg overflow-hidden"
              style={{
                backgroundColor: 'var(--color-bg-elevated)',
                border: '1px solid var(--color-border-default)',
              }}
            >
              {searchLoading ? (
                <div
                  className="px-2.5 py-1.5 text-[11px]"
                  style={{ color: 'var(--color-text-tertiary)' }}
                >
                  Searching…
                </div>
              ) : searchHits.length === 0 ? (
                <div
                  className="px-2.5 py-1.5 text-[11px]"
                  style={{ color: 'var(--color-text-tertiary)' }}
                >
                  No matches. Press Enter to use &quot;{symbolDraft.trim().toUpperCase()}&quot;.
                </div>
              ) : (
                searchHits.map((hit) => (
                  <button
                    key={hit.symbol}
                    type="button"
                    onMouseDown={(e) => {
                      // Use onMouseDown so the click fires before the input
                      // blur handler can tear down the dropdown.
                      e.preventDefault();
                      selectSymbol(hit.symbol);
                    }}
                    className="w-full text-left px-2.5 py-1.5 flex items-baseline gap-2 transition-colors"
                    style={{ color: 'var(--color-text-primary)' }}
                    onMouseEnter={(e) => {
                      e.currentTarget.style.backgroundColor = 'var(--color-bg-hover)';
                    }}
                    onMouseLeave={(e) => {
                      e.currentTarget.style.backgroundColor = 'transparent';
                    }}
                  >
                    <span className="text-xs font-semibold tabular-nums">{hit.symbol}</span>
                    {hit.name && (
                      <span
                        className="text-[11px] truncate"
                        style={{ color: 'var(--color-text-tertiary)' }}
                      >
                        {hit.name}
                      </span>
                    )}
                  </button>
                ))
              )}
            </div>
          )}
        </div>
        <div className="flex items-center gap-2">
          <div className="flex items-center gap-0.5">
            {WIDGET_INTERVALS.map(({ key, label }) => (
              <button
                key={key}
                type="button"
                onClick={() => updateConfig({ interval: key })}
                className="px-1.5 py-0.5 text-[10px] rounded transition-colors"
                style={{
                  backgroundColor: config.interval === key ? 'var(--color-bg-elevated)' : 'transparent',
                  color: config.interval === key ? 'var(--color-text-primary)' : 'var(--color-text-tertiary)',
                  fontWeight: config.interval === key ? 600 : 400,
                }}
              >
                {label}
              </button>
            ))}
          </div>
          <div
            className="flex items-center gap-0.5 pl-2"
            style={{ borderLeft: '1px solid var(--color-border-muted)' }}
          >
            <ToolbarIconBtn title="Zoom in" onClick={handleZoomIn}>
              <ZoomIn size={12} />
            </ToolbarIconBtn>
            <ToolbarIconBtn title="Zoom out" onClick={handleZoomOut}>
              <ZoomOut size={12} />
            </ToolbarIconBtn>
            <ToolbarIconBtn title="Fit all" onClick={handleFitAll}>
              <Maximize2 size={12} />
            </ToolbarIconBtn>
            <ToolbarIconBtn title="Reset view" onClick={handleResetView}>
              <RotateCcw size={12} />
            </ToolbarIconBtn>
            <ToolbarIconBtn
              title="Open in Market view"
              onClick={() => navigate(`/market?symbol=${encodeURIComponent(config.symbol)}`)}
            >
              <ExternalLink size={12} />
            </ToolbarIconBtn>
          </div>
        </div>
      </div>
      <div className="flex-1 min-h-0 relative overflow-hidden">
        <div ref={containerRef} className="absolute inset-0 overflow-hidden" />
        {hover && (
          <div
            className="absolute top-1 left-1 pointer-events-none text-[10px] tabular-nums flex flex-wrap items-center gap-x-2 gap-y-0.5"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            <span style={{ color: 'var(--color-text-secondary)' }}>
              {formatHoverTime(hover.time, config.interval === '1day')}
            </span>
            {hover.open != null && hover.high != null && hover.low != null && (
              <>
                <span>
                  O <span style={{ color: 'var(--color-text-primary)' }}>{hover.open.toFixed(2)}</span>
                </span>
                <span>
                  H <span style={{ color: 'var(--color-text-primary)' }}>{hover.high.toFixed(2)}</span>
                </span>
                <span>
                  L <span style={{ color: 'var(--color-text-primary)' }}>{hover.low.toFixed(2)}</span>
                </span>
              </>
            )}
            <span>
              C <span style={{ color: 'var(--color-text-primary)' }}>{hover.close.toFixed(2)}</span>
            </span>
            {hover.volume != null && hover.volume > 0 && (
              <span>
                V <span style={{ color: 'var(--color-text-primary)' }}>{formatHoverVolume(hover.volume)}</span>
              </span>
            )}
          </div>
        )}
        {loading && !hasData && (
          // Initial load — full-body shimmer so empty axes don't flash.
          <div
            className="absolute inset-0 rounded animate-pulse flex items-center justify-center"
            style={{ backgroundColor: 'var(--color-bg-subtle)' }}
          >
            <Loader2 size={18} className="animate-spin" style={{ color: 'var(--color-text-tertiary)' }} />
          </div>
        )}
        {loading && hasData && (
          // Refetch (symbol/interval swap) — subtle corner spinner only; the
          // chart body is already cleared at fetch-start so there's no stale
          // data to hide behind an overlay.
          <div className="absolute top-2 right-2 pointer-events-none">
            <Loader2 size={14} className="animate-spin" style={{ color: 'var(--color-text-tertiary)' }} />
          </div>
        )}
      </div>
    </div>
  );
}

function ToolbarIconBtn({
  title,
  onClick,
  children,
}: {
  title: string;
  onClick: () => void;
  children: React.ReactNode;
}) {
  return (
    <button
      type="button"
      title={title}
      aria-label={title}
      onClick={onClick}
      className="w-5 h-5 flex items-center justify-center rounded transition-colors"
      style={{ color: 'var(--color-text-tertiary)' }}
      onMouseEnter={(e) => {
        e.currentTarget.style.color = 'var(--color-text-primary)';
        e.currentTarget.style.backgroundColor = 'var(--color-bg-hover)';
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.color = 'var(--color-text-tertiary)';
        e.currentTarget.style.backgroundColor = 'transparent';
      }}
    >
      {children}
    </button>
  );
}

export function ChartSettings({ config, onChange, onClose }: WidgetSettingsProps<ChartConfig>) {
  const [draft, setDraft] = useState<ChartConfig>({ ...DEFAULT_CONFIG, ...config });
  useEffect(() => setDraft({ ...DEFAULT_CONFIG, ...config }), [config]);

  const save = () => {
    onChange(draft);
    onClose();
  };

  const types: { key: ChartType; label: string }[] = [
    { key: 'candle', label: 'Candle' },
    { key: 'area', label: 'Area' },
    { key: 'line', label: 'Line' },
  ];

  return (
    <div className="space-y-3">
      <div>
        <label className="text-xs block mb-1" style={{ color: 'var(--color-text-secondary)' }}>
          Symbol
        </label>
        <input
          type="text"
          className="w-full px-2 py-1.5 text-sm rounded border"
          style={{
            backgroundColor: 'var(--color-bg-input)',
            borderColor: 'var(--color-border-default)',
            color: 'var(--color-text-primary)',
          }}
          value={draft.symbol}
          onChange={(e) => setDraft((p) => ({ ...p, symbol: e.target.value.toUpperCase() }))}
          onKeyDown={(e) => e.key === 'Enter' && save()}
        />
      </div>
      <div>
        <label className="text-xs block mb-1" style={{ color: 'var(--color-text-secondary)' }}>
          Chart type
        </label>
        <div className="flex gap-1">
          {types.map((t) => (
            <button
              key={t.key}
              type="button"
              onClick={() => setDraft((p) => ({ ...p, chartType: t.key }))}
              className="flex-1 px-2 py-1 text-xs rounded border transition-colors"
              style={{
                backgroundColor: draft.chartType === t.key ? 'var(--color-accent-primary)' : 'transparent',
                color: draft.chartType === t.key ? 'var(--color-text-on-accent)' : 'var(--color-text-secondary)',
                borderColor: 'var(--color-border-default)',
              }}
            >
              {t.label}
            </button>
          ))}
        </div>
      </div>
      <div className="flex justify-end gap-2 pt-1">
        <button
          type="button"
          onClick={onClose}
          className="px-3 py-1 text-xs rounded border"
          style={{ color: 'var(--color-text-primary)', borderColor: 'var(--color-border-default)' }}
        >
          Cancel
        </button>
        <button
          type="button"
          onClick={save}
          className="px-3 py-1 text-xs rounded font-medium"
          style={{ backgroundColor: 'var(--color-accent-primary)', color: 'var(--color-text-on-accent)' }}
        >
          Save
        </button>
      </div>
    </div>
  );
}

export default ChartWidget;
