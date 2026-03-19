import React, { useEffect, useRef, useState, useImperativeHandle, forwardRef, useCallback } from 'react';
import { createChart, ColorType, CrosshairMode, PriceScaleMode, LineType, LineStyle } from 'lightweight-charts';
import type { IChartApi, LogicalRange, MouseEventParams } from 'lightweight-charts';
import html2canvas from 'html2canvas';
import './MarketChart.css';
import { fetchStockData } from '../utils/api';
import { calculateMA, calculateRSI, updateRSIIncremental } from '../utils/chartHelpers';
import type { RSIState, OHLCDataPoint } from '../utils/chartHelpers';
import {
  getChartTheme,
  INTERVALS, PRIMARY_INTERVAL_KEYS, INITIAL_LOAD_DAYS, SCROLL_CHUNK_DAYS,
  SCROLL_LOAD_THRESHOLD, RANGE_CHANGE_DEBOUNCE_MS,
  STAGE1_LOAD_DAYS, STAGE2_BACKFILL_DAYS, PREFETCH_ENABLED_INTERVALS, PREFETCH_THRESHOLD,
  MA_CONFIGS, DEFAULT_ENABLED_MA, RSI_PERIODS, BARS_PER_DAY, TARGET_BAR_SPACING,
  OVERLAY_COLORS, OVERLAY_LABELS,
  EXTENDED_HOURS_INTERVALS, getExtendedHoursType, computeExtendedHoursRegions,
  EXT_COLOR_PRE, EXT_COLOR_POST,
  isUSEquity, supports1sInterval,
} from '../utils/chartConstants';
import type { ChartDataPoint as ChartConstDataPoint } from '../utils/chartConstants';
import { ExtendedHoursBgPrimitive } from '../utils/extendedHoursBg';
import { useTheme } from '@/contexts/ThemeContext';
import CrosshairTooltip from './CrosshairTooltip';
import TradingViewWidget from './TradingViewWidget';
import { useChartAnnotations } from '../hooks/useChartAnnotations';
import { useChartOverlays } from '../hooks/useChartOverlays';
import { SlidersHorizontal, Settings2, Maximize2, Minimize2, ChevronDown, Plus, Minus, RotateCcw, Menu } from 'lucide-react';

import { loadPref, savePref } from '../utils/prefs';
import type { SnapshotData } from '@/types/market';
import type { BarData } from '../hooks/useMarketDataWS';
import { useOnClickOutside } from '@/hooks/useOnClickOutside';

interface ChartDataBar {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

interface TooltipState {
  visible: boolean;
  x: number;
  y: number;
  data: {
    time: number;
    open: number;
    high: number;
    low: number;
    close: number;
    volume?: number;
    maValues: Record<number, number>;
    rsiValue: number | null;
  } | null;
}

interface OverlayVisibility {
  earnings: boolean;
  grades: boolean;
  priceTargets: boolean;
  [key: string]: boolean;
}

interface MarketChartProps {
  symbol: string;
  interval?: string;
  onIntervalChange?: (interval: string) => void;
  onCapture?: () => void;
  onStockMeta?: (meta: unknown) => void;
  onLatestBar?: (bar: ChartDataBar) => void;
  quoteData: Record<string, unknown> | null;
  earningsData: unknown;
  overlayData: Record<string, unknown> | null;
  stockMeta: Record<string, unknown> | null;
  liveTick: BarData | null;
  wsStatus: string;
  ginlixDataEnabled?: boolean;
  marketStatus?: Record<string, unknown> | null;
  snapshot: SnapshotData | null;
}

export interface MarketChartHandle {
  captureChart: () => Promise<Blob | null>;
  captureChartAsDataUrl: () => Promise<string | null>;
  getChartMetadata: () => Record<string, unknown> | null;
}


const MarketChart = React.memo(forwardRef<MarketChartHandle, MarketChartProps>(({
  symbol,
  interval = '1day',
  onIntervalChange,
  onCapture: _onCapture,
  onStockMeta,
  onLatestBar,
  quoteData,
  earningsData,
  overlayData,
  stockMeta,
  liveTick,
  wsStatus: _wsStatus,
  ginlixDataEnabled = true,
  marketStatus,
  snapshot,
}, ref) => {
  const { theme } = useTheme();
  const ct = getChartTheme(theme as 'dark' | 'light');
  const providers = Array.isArray(marketStatus?.providers) ? marketStatus.providers as string[] : [];
  const supports4hInterval = marketStatus == null || providers.some(p => p !== 'yfinance');
  const rootRef = useRef<HTMLDivElement>(null);
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const rsiChartContainerRef = useRef<HTMLDivElement>(null);
  const lightWrapperRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const rsiChartRef = useRef<IChartApi | null>(null);
  // TODO: type properly — lightweight-charts series types are complex generics
  const candlestickSeriesRef = useRef<any>(null);
  const rsiSeriesRef = useRef<any>(null);
  const volumeSeriesRef = useRef<any>(null);
  const maSeriesRefs = useRef<Record<number, any>>({});
  const baselineSeriesRef = useRef<any>(null);
  const extHoursBgRef = useRef<ExtendedHoursBgPrimitive | null>(null);
  const extCloseLineRef = useRef<any>(null);
  const currentExtTypeRef = useRef<string | null>(null);
  const quoteDataRef = useRef(quoteData);
  const snapshotRef = useRef(snapshot);

  const [loading, setLoading] = useState<boolean>(true);
  const [scrollLoading, setScrollLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [_lastUpdateTime, setLastUpdateTime] = useState<Date | null>(null);
  const [rsiValue, setRsiValue] = useState<string | null>(null);

  // MA / RSI config state (persisted)
  const [enabledMaPeriods, setEnabledMaPeriods] = useState<number[]>(() => loadPref('maPeriods', DEFAULT_ENABLED_MA));
  const [rsiPeriod, setRsiPeriod] = useState<number>(() => loadPref('rsiPeriod', 14));
  const [maValues, setMaValues] = useState<Record<number, string>>({});

  // Chart mode: 'custom' (our lightweight-charts) or 'tradingview' (full TV widget) (persisted)
  const [chartMode, setChartMode] = useState<string>(() => loadPref('chartMode', 'custom'));

  // Chart feature toggles (persisted)
  const [priceScaleMode, setPriceScaleMode] = useState<number>(() => loadPref('priceScaleMode', PriceScaleMode.Normal));
  const [magnetMode, setMagnetMode] = useState<boolean>(() => loadPref('magnetMode', false));
  const [showBaseline, setShowBaseline] = useState<boolean>(false);
  const [annotationsVisible, setAnnotationsVisible] = useState<boolean>(() => loadPref('annotationsVisible', false));
  const [overlayVisibility, setOverlayVisibility] = useState<OverlayVisibility>(
    () => loadPref('overlayVisibility', { earnings: false, grades: false, priceTargets: false }),
  );

  // Responsive compact mode — based on actual chart container width, not viewport
  const [isCompact, setIsCompact] = useState<boolean>(false);
  useEffect(() => {
    const el = rootRef.current;
    if (!el) return;
    const ro = new ResizeObserver(([entry]) => {
      setIsCompact(entry.contentRect.width < 925);
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  // Toolbar dropdown state
  const [indicatorsOpen, setIndicatorsOpen] = useState<boolean>(false);
  const [toolsOpen, setToolsOpen] = useState<boolean>(false);
  const [intervalsOpen, setIntervalsOpen] = useState<boolean>(false);
  const [viewOpen, setViewOpen] = useState<boolean>(false);
  const [disabledTooltip, setDisabledTooltip] = useState<string | null>(null);
  const disabledTooltipTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const indicatorsDropdownRef = useRef<HTMLDivElement>(null);
  const toolsDropdownRef = useRef<HTMLDivElement>(null);
  const intervalsDropdownRef = useRef<HTMLDivElement>(null);
  const viewDropdownRef = useRef<HTMLDivElement>(null);
  useOnClickOutside(indicatorsDropdownRef, () => setIndicatorsOpen(false), indicatorsOpen);
  useOnClickOutside(toolsDropdownRef, () => setToolsOpen(false), toolsOpen);
  useOnClickOutside(intervalsDropdownRef, () => setIntervalsOpen(false), intervalsOpen);
  useOnClickOutside(viewDropdownRef, () => setViewOpen(false), viewOpen);

  // Crosshair tooltip state
  const [tooltipState, setTooltipState] = useState<TooltipState>({ visible: false, x: 0, y: 0, data: null });

  // Refs for stable callbacks (avoid stale closures)
  const enabledMaPeriodsRef = useRef(DEFAULT_ENABLED_MA);
  const rsiPeriodRef = useRef(14);

  // Track current interval for use inside stable callbacks (avoids stale closures)
  const intervalRef = useRef(interval);

  // Keep refs synced with state
  useEffect(() => { enabledMaPeriodsRef.current = enabledMaPeriods; }, [enabledMaPeriods]);
  useEffect(() => { rsiPeriodRef.current = rsiPeriod; }, [rsiPeriod]);
  useEffect(() => { intervalRef.current = interval; }, [interval]);
  useEffect(() => { quoteDataRef.current = quoteData; }, [quoteData]);
  useEffect(() => { snapshotRef.current = snapshot; }, [snapshot]);
  const symbolRef = useRef(symbol);
  useEffect(() => { symbolRef.current = symbol; }, [symbol]);

  // Persist user preferences to localStorage
  useEffect(() => { savePref('maPeriods', enabledMaPeriods); }, [enabledMaPeriods]);
  useEffect(() => { savePref('rsiPeriod', rsiPeriod); }, [rsiPeriod]);
  useEffect(() => { savePref('chartMode', chartMode); }, [chartMode]);
  useEffect(() => { savePref('overlayVisibility', overlayVisibility); }, [overlayVisibility]);
  useEffect(() => { savePref('priceScaleMode', priceScaleMode); }, [priceScaleMode]);
  useEffect(() => { savePref('magnetMode', magnetMode); }, [magnetMode]);
  useEffect(() => { savePref('annotationsVisible', annotationsVisible); }, [annotationsVisible]);

  // Keep chart theme ref synced for stable callbacks
  const ctRef = useRef(ct);
  useEffect(() => { ctRef.current = ct; }, [ct]);

  // RSI incremental-update refs
  const rsiSmoothingRef = useRef<RSIState | null>(null);          // Wilder state { avgGain, avgLoss, lastClose, period }
  const prevBarSmoothingRef = useRef<RSIState | null>(null);       // State *before* current bar (for same-bar re-updates)
  const pendingRsiDataRef = useRef<Array<{ time: number; value: number }> | null>(null);         // Buffered rsiData when series isn't ready
  const rsiDataMapRef = useRef<Map<number, number>>(new Map());        // time->rsiValue for O(1) crosshair lookup

  // Track when the last WS live tick was applied (for REST polling fallback)
  const lastLiveTickTimeRef = useRef<number>(0);
  const gapFillDoneRef = useRef<boolean>(false);  // gap fill between REST data and first WS tick
  const gapFillRetryRef = useRef<number>(0);    // retry count for gap fill attempts
  const gapFillInProgressRef = useRef<boolean>(false);  // prevent concurrent gap fill fetches
  // Aggregation buffer for building 1m candles from 1s WS ticks
  const minuteAggRef = useRef<ChartDataBar>({ time: 0, open: 0, high: 0, low: 0, close: 0, volume: 0 });

  // Refs for scroll-based loading
  const allDataRef = useRef<ChartDataBar[]>([]);
  const oldestDateRef = useRef<number | null>(null);
  const fetchingRef = useRef<boolean>(false);
  const rangeChangeTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const rangeUnsubRef = useRef<(() => void) | null>(null);

  // Refs for staged loading and background prefetch
  const stage2AbortRef = useRef<AbortController | null>(null);
  const prefetchAbortRef = useRef<AbortController | null>(null);
  const prefetchedDataRef = useRef<{ data: ChartDataBar[]; anchorOldest: number } | null>(null);
  const prefetchingRef = useRef<boolean>(false);

  // Chart data state for hooks
  const [chartDataForHooks, setChartDataForHooks] = useState<ChartDataBar[]>([]);

  // --- Price lines via hook ---
  const priceTargetsForAnnotations = overlayVisibility.priceTargets ? (overlayData?.priceTargets as any) : null;
  useChartAnnotations(candlestickSeriesRef, stockMeta, quoteData, priceTargetsForAnnotations, annotationsVisible, symbol);

  // --- Series markers via hook ---
  useChartOverlays(candlestickSeriesRef, chartDataForHooks as any, earningsData as any, overlayData as any, overlayVisibility as any, symbol);

  // --- Live tick updates from WS (1s and 1min intervals, custom/Light mode only) ---
  useEffect(() => {
    if (!liveTick || !candlestickSeriesRef.current) return;
    // Only apply live updates for 1s/1min interval in custom (Light) mode
    if ((interval !== '1s' && interval !== '1min') || chartMode !== 'custom') return;

    const { time, open, high, low, close, volume } = liveTick;
    if (!time || close == null) return;

    const data = allDataRef.current;

    // Track when WS last delivered a usable tick (used by REST polling fallback)
    lastLiveTickTimeRef.current = Date.now();

    // Gap fill: if there's a gap between REST data and first WS tick,
    // fetch REST data to fill it.  Retries up to 3 times with concurrency guard.
    // Threshold: 5 seconds for 1s, 120s for 1min.
    if (!gapFillDoneRef.current && !gapFillInProgressRef.current && data.length > 0) {
      const lastDataTime = data[data.length - 1].time;
      const gapSec = time - lastDataTime;
      const gapThreshold = interval === '1s' ? 5 : 120;
      if (gapSec > gapThreshold) {
        gapFillRetryRef.current += 1;
        if (gapFillRetryRef.current > 3) {
          gapFillDoneRef.current = true;  // give up after 3 attempts
        } else {
          gapFillInProgressRef.current = true;
          (async () => {
            try {
              const fromDate = new Date(lastDataTime * 1000).toISOString().split('T')[0];
              const toDate = new Date().toLocaleDateString('en-CA', { timeZone: 'America/New_York' });
              const sym = symbol;
              const result = await fetchStockData(sym, interval, fromDate, toDate);
              if (symbolRef.current !== sym) return; // symbol changed, discard stale data
              const fillData = result?.data;
              if (Array.isArray(fillData) && fillData.length > 0) {
                // Merge: insert bars that fill the gap (between old last bar and current last bar)
                const existingTimes = new Set(allDataRef.current.map(b => b.time));
                const newBars = fillData.filter(b => !existingTimes.has(b.time));
                if (newBars.length > 0) {
                  const merged = [...allDataRef.current, ...newBars].sort((a, b) => a.time - b.time);
                  // Deduplicate by time (keep last occurrence)
                  const deduped = [];
                  const seen = new Set();
                  for (let i = merged.length - 1; i >= 0; i--) {
                    if (!seen.has(merged[i].time)) {
                      seen.add(merged[i].time);
                      deduped.push(merged[i]);
                    }
                  }
                  deduped.reverse();
                  allDataRef.current = deduped;
                  updateSeriesData(deduped);
                  // Only mark done if we actually bridged the gap
                  const lastFilled = deduped[deduped.length - 1]?.time || 0;
                  if (lastFilled >= time - gapThreshold) {
                    gapFillDoneRef.current = true;
                  }
                  // else: leave false → retry on next tick
                }
              }
            } catch (err) {
              console.debug('Gap fill attempt', gapFillRetryRef.current, 'failed:', err);
            } finally {
              gapFillInProgressRef.current = false;
            }
          })();
        }
      } else {
        gapFillDoneRef.current = true;  // no gap, mark as done
      }
    }

    // Clear the "waiting for live data" hint once first tick arrives
    if (interval === '1s' && error) setError(null);

    // Aggregate 1s ticks into 1m candle when on 1min interval
    let barTime = time, barOpen = open, barHigh = high, barLow = low, barClose = close, barVolume = volume;
    if (interval === '1min') {
      const minuteTime = Math.floor(time / 60) * 60;
      const agg = minuteAggRef.current;
      if (minuteTime === agg.time) {
        agg.high = Math.max(agg.high, high);
        agg.low = Math.min(agg.low, low);
        agg.close = close;
        agg.volume += volume;
      } else {
        agg.time = minuteTime;
        agg.open = open;
        agg.high = high;
        agg.low = low;
        agg.close = close;
        agg.volume = volume;
      }
      barTime = agg.time;
      barOpen = agg.open;
      barHigh = agg.high;
      barLow = agg.low;
      barClose = agg.close;
      barVolume = agg.volume;
    }

    // Skip out-of-order ticks — series.update() only accepts time >= last bar.
    // Guard uses barTime (post-aggregation) rather than raw time to prevent crashes
    // when switching intervals (e.g. 1s→1min where minute-flooring produces older times).
    if (data.length > 0 && barTime < data[data.length - 1].time) return;

    // Update candlestick series in-place (same time = update, newer = append)
    candlestickSeriesRef.current.update({ time: barTime, open: barOpen, high: barHigh, low: barLow, close: barClose });

    const ext = isUSEquity(symbolRef.current) && EXTENDED_HOURS_INTERVALS.has(interval) && getExtendedHoursType(barTime);
    const up = barClose >= barOpen;
    const ct = ctRef.current;
    if (volumeSeriesRef.current) {
      volumeSeriesRef.current.update({
        time: barTime,
        value: barVolume,
        color: ext
          ? (up ? ct.extVolumeUp : ct.extVolumeDown)
          : (up ? ct.upColor : ct.downColor),
      });
    }

    // Keep allDataRef in sync (data already declared above for the time guard)
    const isSameBar = data.length > 0 && data[data.length - 1].time === barTime;
    if (isSameBar) {
      data[data.length - 1] = { time: barTime, open: barOpen, high: barHigh, low: barLow, close: barClose, volume: barVolume };
    } else if (!data.length || barTime > data[data.length - 1].time) {
      data.push({ time: barTime, open: barOpen, high: barHigh, low: barLow, close: barClose, volume: barVolume });
    }

    // Keep extended-hours background in sync with live bars
    if (ext && extHoursBgRef.current) {
      extHoursBgRef.current.setRegions(computeExtendedHoursRegions(data as unknown as ChartConstDataPoint[]));
    }

    // Keep extended-hours price lines in sync with live bars
    syncExtendedHoursLines(barTime);

    // Incremental RSI update
    if (rsiSmoothingRef.current && rsiSeriesRef.current) {
      if (isSameBar) {
        // Same bar updated — recalculate from state *before* this bar was first applied
        if (prevBarSmoothingRef.current) {
          const { value, state } = updateRSIIncremental(prevBarSmoothingRef.current, barClose);
          rsiSmoothingRef.current = state;
          rsiSeriesRef.current.update({ time: barTime, value });
          rsiDataMapRef.current.set(barTime, value);
          setRsiValue(value.toFixed(0));
        }
      } else {
        // New bar — advance smoothing state
        prevBarSmoothingRef.current = rsiSmoothingRef.current;
        const { value, state } = updateRSIIncremental(rsiSmoothingRef.current, barClose);
        rsiSmoothingRef.current = state;
        rsiSeriesRef.current.update({ time: barTime, value });
        rsiDataMapRef.current.set(barTime, value);
        setRsiValue(value.toFixed(0));
      }
    }
  }, [liveTick, interval, chartMode]);

  // Temporarily reveal the hidden Light chart for capture, then restore.
  // Since it's behind the TV widget (z-index: -1), no visual flash occurs.
  const revealForCapture = useCallback(async <T,>(fn: () => Promise<T>): Promise<T> => {
    const wrapper = lightWrapperRef.current;
    const needsReveal = wrapper && wrapper.classList.contains('light-chart-hidden');
    if (needsReveal) wrapper.style.visibility = 'visible';
    try {
      return await fn();
    } finally {
      if (needsReveal) wrapper.style.visibility = '';
    }
  }, []);

  useImperativeHandle(ref, () => ({
    captureChart: async () => {
      // Use native takeScreenshot for main chart download
      if (chartRef.current) {
        try {
          const canvas = chartRef.current.takeScreenshot();
          if (canvas) {
            return new Promise((resolve) => {
              canvas.toBlob((blob) => resolve(blob), 'image/png');
            });
          }
        } catch (err) {
          console.warn('Native takeScreenshot failed, falling back to html2canvas:', err);
        }
      }
      // Fallback to html2canvas (temporarily reveal if hidden)
      if (!chartContainerRef.current) return null;
      return revealForCapture(async () => {
        try {
          const canvas = await html2canvas(chartContainerRef.current!, {
            backgroundColor: ct.bg,
            scale: 2,
            logging: false,
          });
          return new Promise((resolve) => {
            canvas.toBlob((blob) => resolve(blob), 'image/png');
          });
        } catch (err) {
          console.error('Chart capture failed:', err);
          return null;
        }
      });
    },
    captureChartAsDataUrl: async () => {
      // Capture main chart (+ RSI if visible) using native takeScreenshot.
      // html2canvas can't read lightweight-charts canvas pixels, so we
      // stitch the native screenshots together on an offscreen canvas.
      try {
        const mainCanvas = chartRef.current?.takeScreenshot();
        const rsiCanvas = rsiChartRef.current?.takeScreenshot();
        if (!mainCanvas) return null;

        const mainW = mainCanvas.width, mainH = mainCanvas.height;
        const rsiW = rsiCanvas?.width || 0, rsiH = rsiCanvas?.height || 0;
        const totalH = mainH + (rsiCanvas ? rsiH : 0);

        const offscreen = document.createElement('canvas');
        offscreen.width = Math.max(mainW, rsiW);
        offscreen.height = totalH;
        const ctx = offscreen.getContext('2d')!;
        ctx.fillStyle = ct.bg;
        ctx.fillRect(0, 0, offscreen.width, offscreen.height);
        ctx.drawImage(mainCanvas, 0, 0);
        if (rsiCanvas) ctx.drawImage(rsiCanvas, 0, mainH);

        return offscreen.toDataURL('image/jpeg', 0.85);
      } catch (err) {
        console.error('Chart capture failed:', err);
        return null;
      }
    },
    getChartMetadata: () => {
      const data = allDataRef.current;
      if (!data || data.length === 0) return null;

      const firstTime = data[0].time;
      const lastTime = data[data.length - 1].time;
      // Chart timestamps are already ET-shifted, so UTC interpretation gives the ET date
      const formatDate = (ts: number) => new Date(ts * 1000).toISOString().split('T')[0];

      const enabledMAs = enabledMaPeriodsRef.current;
      const maInfo = enabledMAs
        .filter((p) => maValues[p] != null)
        .map((p) => `MA${p}: ${maValues[p]}`);

      const lastCandle = data[data.length - 1];

      return {
        chartMode: chartMode === 'tradingview' ? 'Advanced (TradingView)' : 'Light',
        dateRange: { from: formatDate(firstTime), to: formatDate(lastTime) },
        dataPoints: data.length,
        enabledMAs,
        maValues: Object.fromEntries(
          enabledMAs.filter((p) => maValues[p] != null).map((p) => [p, maValues[p]])
        ),
        maDescription: maInfo.length > 0 ? maInfo.join(', ') : null,
        rsiPeriod: rsiPeriodRef.current,
        rsiValue: rsiValue,
        lastCandle: {
          open: lastCandle.open,
          high: lastCandle.high,
          low: lastCandle.low,
          close: lastCandle.close,
          volume: lastCandle.volume,
        },
        annotationsVisible,
        overlayVisibility,
        priceScaleMode,
      };
    },
  }));

  // --- Extended-hours price lines (close line + colored current-price line) ---
  const syncExtendedHoursLines = useCallback((lastBarTime: number) => {
    const series = candlestickSeriesRef.current;
    if (!series) return;

    const isExtInterval = isUSEquity(symbolRef.current) && EXTENDED_HOURS_INTERVALS.has(intervalRef.current);
    const extType = isExtInterval ? getExtendedHoursType(lastBarTime) : null;
    const prevType = currentExtTypeRef.current;

    if (extType === prevType) return;
    currentExtTypeRef.current = extType;

    if (extType) {
      const color = extType === 'pre' ? EXT_COLOR_PRE : EXT_COLOR_POST;
      const label = extType === 'pre' ? 'Pre' : 'After';
      // Color the last-value price line amber/blue and label it "Pre"/"After"
      series.applyOptions({ priceLineColor: color, title: label });

      // After-hours: show today's regular session close (prev_close + regular_trading_change).
      // Pre-market: skip — the "Prev Close" annotation already shows the same value.
      if (extCloseLineRef.current) {
        try { series.removePriceLine(extCloseLineRef.current); } catch (_) { /* ok */ }
        extCloseLineRef.current = null;
      }
      if (extType === 'post') {
        const snap = snapshotRef.current;
        // Derive today's 4 PM close: previous_close (yesterday) + regular_trading_change
        const prevClose = snap?.previous_close;
        const regChange = snap?.regular_trading_change as number | undefined;
        const closePrice = (prevClose != null && regChange != null)
          ? (prevClose as number) + regChange
          : null;
        if (closePrice != null) {
          extCloseLineRef.current = series.createPriceLine({
            price: closePrice,
            title: 'Close',
            color: 'rgba(139,143,163,0.7)',
            lineWidth: 1,
            lineStyle: LineStyle.Dashed,
            axisLabelVisible: true,
          });
        }
      }
    } else {
      // Regular hours — reset to defaults
      series.applyOptions({ priceLineColor: undefined, title: '' });
      if (extCloseLineRef.current) {
        try { series.removePriceLine(extCloseLineRef.current); } catch (_) { /* ok */ }
        extCloseLineRef.current = null;
      }
    }
  }, []);

  // Re-sync extended-hours close line when quoteData or snapshot changes
  useEffect(() => {
    if (currentExtTypeRef.current) {
      // Force re-sync by temporarily clearing the cached type
      currentExtTypeRef.current = null;
      const data = allDataRef.current;
      if (data.length > 0) syncExtendedHoursLines(data[data.length - 1].time);
    }
  }, [snapshot?.previous_close, snapshot?.regular_trading_change, syncExtendedHoursLines]);

  // --- Update series data helper (used by both initial load and scroll load) ---
  const updateSeriesData = useCallback((data: ChartDataBar[]) => {
    const ct = ctRef.current;
    const applyExt = isUSEquity(symbolRef.current) && EXTENDED_HOURS_INTERVALS.has(intervalRef.current);

    // Candlestick
    if (candlestickSeriesRef.current) {
      candlestickSeriesRef.current.setData(data);
    }

    // Volume histogram — dim extended-hours bars
    if (volumeSeriesRef.current) {
      volumeSeriesRef.current.setData(data.map((d, i) => {
        const up = i > 0 && d.close >= data[i - 1].close;
        const ext = applyExt && getExtendedHoursType(d.time);
        return {
          time: d.time,
          value: d.volume || 0,
          color: ext
            ? (up ? ct.extVolumeUp : ct.extVolumeDown)
            : (up ? ct.volumeUp : ct.volumeDown),
        };
      }));
    }

    // Extended-hours background shading
    if (extHoursBgRef.current) {
      if (applyExt) {
        extHoursBgRef.current.setRegions(computeExtendedHoursRegions(data as unknown as ChartConstDataPoint[]));
        extHoursBgRef.current.setColors({ pre: ct.extBgPre, post: ct.extBgPost });
      } else {
        extHoursBgRef.current.setRegions([]);
      }
    }

    // Extended-hours price lines (close + colored current price)
    if (data.length > 0) {
      syncExtendedHoursLines(data[data.length - 1].time);
    }

    // All MAs — compute all enabled, clear disabled
    const enabled = enabledMaPeriodsRef.current;
    const newMaValues: Record<number, string> = {};
    MA_CONFIGS.forEach(({ period }) => {
      const series = maSeriesRefs.current[period];
      if (!series) return;
      if (enabled.includes(period)) {
        const maData = calculateMA(data as unknown as OHLCDataPoint[], period);
        series.setData(maData);
        const last = maData[maData.length - 1]?.value;
        if (last != null) newMaValues[period] = last.toFixed(2);
      } else {
        series.setData([]);
      }
    });
    setMaValues(newMaValues);

    // RSI — compute and store smoothing state for incremental updates
    const currentRsiPeriod = rsiPeriodRef.current;
    const { data: rsiData, state: rsiState } = calculateRSI(data as unknown as OHLCDataPoint[], currentRsiPeriod);

    // Always update smoothing state and lookup map regardless of series readiness
    rsiSmoothingRef.current = rsiState;
    prevBarSmoothingRef.current = rsiState; // reset: full recalc, no "previous bar" distinction
    const map = new Map();
    for (const pt of rsiData) map.set(pt.time, pt.value);
    rsiDataMapRef.current = map;

    if (rsiData.length > 0) {
      const lastRsi = rsiData[rsiData.length - 1]?.value;
      if (lastRsi != null) setRsiValue(lastRsi.toFixed(0));

      if (rsiSeriesRef.current) {
        // Series ready — apply immediately
        rsiSeriesRef.current.setData(rsiData);
        pendingRsiDataRef.current = null;
      } else {
        // Series not ready yet (mount race) — stash for flush after creation
        pendingRsiDataRef.current = rsiData;
      }
    }

    // Update chart data state for overlay hooks
    setChartDataForHooks(data);
  }, []);

  // --- Merge prepended data helper (shared by scroll-load & MA backfill) ---
  const mergePrependedData = (newData: ChartDataBar[] | null | undefined) => {
    if (!newData?.length) return;
    const prevLen = allDataRef.current.length;
    const existingMap = new Map(allDataRef.current.map(d => [d.time, d]));
    newData.forEach(d => { if (!existingMap.has(d.time)) existingMap.set(d.time, d); });
    const merged = Array.from(existingMap.values()).sort((a, b) => a.time - b.time);
    const prependedCount = merged.length - prevLen;
    allDataRef.current = merged;
    oldestDateRef.current = merged[0].time;
    const ts = chartRef.current?.timeScale();
    const savedRange = ts?.getVisibleLogicalRange();
    updateSeriesData(merged);
    if (ts && savedRange && prependedCount > 0) {
      ts.setVisibleLogicalRange({ from: savedRange.from + prependedCount, to: savedRange.to + prependedCount });
    }
  };

  // --- Fetch older bars before current oldest and merge into series ---
  const fetchAndPrepend = async (days: number) => {
    if (!oldestDateRef.current) return;
    const sym = symbol;
    const oldest = new Date(oldestDateRef.current * 1000);
    const toDate = new Date(oldest);
    toDate.setDate(toDate.getDate() - 1);
    const fromDate = new Date(toDate);
    fromDate.setDate(fromDate.getDate() - days);

    const result = await fetchStockData(sym, interval,
      fromDate.toISOString().split('T')[0], toDate.toISOString().split('T')[0]);
    if (symbolRef.current !== sym) return; // symbol changed, discard stale data
    const newData = result?.data;
    if (newData && Array.isArray(newData) && newData.length > 0) {
      mergePrependedData(newData);
    }
  };

  // --- Background prefetch for 1s (pre-load next scroll chunk before user reaches edge) ---
  const triggerPrefetch = () => {
    if (!PREFETCH_ENABLED_INTERVALS.has(intervalRef.current)) return;
    if (prefetchingRef.current || prefetchedDataRef.current) return;
    if (!oldestDateRef.current) return;

    prefetchingRef.current = true;
    const capturedOldest = oldestDateRef.current;
    const capturedSym = symbolRef.current;
    const capturedInterval = intervalRef.current;
    const days = SCROLL_CHUNK_DAYS[capturedInterval] || 0;

    prefetchAbortRef.current?.abort();
    const ac = new AbortController();
    prefetchAbortRef.current = ac;

    const oldest = new Date(capturedOldest * 1000);
    const toDate = new Date(oldest);
    toDate.setDate(toDate.getDate() - 1);
    const fromDate = new Date(toDate);
    fromDate.setDate(fromDate.getDate() - days);

    fetchStockData(capturedSym, capturedInterval,
      fromDate.toISOString().split('T')[0],
      toDate.toISOString().split('T')[0],
      { signal: ac.signal }
    ).then(result => {
      if (ac.signal.aborted || symbolRef.current !== capturedSym) return;
      if (result?.data?.length > 0) {
        prefetchedDataRef.current = { data: result.data, anchorOldest: capturedOldest };
      }
    }).catch(err => {
      if (err?.name !== 'AbortError' && err?.name !== 'CanceledError') {
        console.warn('Prefetch failed:', err);
      }
    }).finally(() => {
      prefetchingRef.current = false;
    });
  };

  // --- Scroll-based lazy loading ---
  const handleScrollLoadMore = useCallback(async () => {
    if (fetchingRef.current || !oldestDateRef.current) return;
    fetchingRef.current = true;
    setScrollLoading(true);
    try {
      const buf = prefetchedDataRef.current;
      if (buf && buf.anchorOldest === oldestDateRef.current && buf.data?.length > 0) {
        // Instant merge from prefetch buffer — no network wait
        mergePrependedData(buf.data);
        prefetchedDataRef.current = null;
        triggerPrefetch();
      } else {
        // Buffer miss — fall back to synchronous fetch
        prefetchedDataRef.current = null;
        await fetchAndPrepend(SCROLL_CHUNK_DAYS[interval] || 0);
        if (PREFETCH_ENABLED_INTERVALS.has(interval)) {
          triggerPrefetch();
        }
      }
    } catch (err) {
      console.warn('Scroll-load fetch failed:', err);
    } finally {
      fetchingRef.current = false;
      setScrollLoading(false);
    }
  }, [symbol, interval, updateSeriesData]);

  // --- Backfill older data when a newly-enabled MA needs more bars ---
  const backfillForMaPeriod = useCallback(async (period: number) => {
    const currentLen = allDataRef.current.length;
    if (currentLen >= period || fetchingRef.current || !oldestDateRef.current) return;
    fetchingRef.current = true;
    try {
      const deficit = period - currentLen;
      const extraDays = Math.ceil((deficit / (BARS_PER_DAY[interval] || 1)) * 1.5);
      await fetchAndPrepend(extraDays);
    } catch (err) {
      console.warn('MA backfill fetch failed:', err);
    } finally {
      fetchingRef.current = false;
    }
  }, [symbol, interval, updateSeriesData]);

  // --- Toggle handlers ---
  const handleToggleMa = useCallback((period: number) => {
    const isCurrentlyEnabled = enabledMaPeriodsRef.current.includes(period);
    if (!isCurrentlyEnabled && allDataRef.current.length < period) {
      backfillForMaPeriod(period);
    }
    setEnabledMaPeriods(prev =>
      prev.includes(period) ? prev.filter(p => p !== period) : [...prev, period]
    );
  }, [backfillForMaPeriod]);

  const handleChangeRsiPeriod = useCallback((period: number) => {
    setRsiPeriod(period);
  }, []);

  // --- Effect 1: Chart creation (mount only) ---
  useEffect(() => {
    if (!chartContainerRef.current) return;

    const t0 = getChartTheme(theme);
    const chart = createChart(chartContainerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: t0.bg },
        textColor: t0.text,
      },
      autoSize: true,
      grid: {
        vertLines: { color: t0.grid },
        horzLines: { color: t0.grid },
      },
      watermark: {
        visible: true,
        text: symbol,
        fontSize: 48,
        color: t0.watermark,
        horzAlign: 'center',
        vertAlign: 'center',
      },
      crosshair: { mode: CrosshairMode.Normal },
      rightPriceScale: {
        borderColor: t0.grid,
        scaleMargins: { top: 0.1, bottom: 0.2 },
      },
      timeScale: {
        borderColor: t0.grid,
        timeVisible: true,
        secondsVisible: false,
      },
      handleScroll: {
        mouseWheel: true,
        pressedMouseMove: true,
        horzTouchDrag: true,
        vertTouchDrag: false,
      },
    } as any);
    chartRef.current = chart;

    candlestickSeriesRef.current = chart.addCandlestickSeries({
      upColor: t0.upColor,
      downColor: t0.downColor,
      borderVisible: false,
      wickUpColor: t0.upColor,
      wickDownColor: t0.downColor,
    });

    // Extended-hours background shading primitive
    extHoursBgRef.current = new ExtendedHoursBgPrimitive();
    candlestickSeriesRef.current.attachPrimitive(extHoursBgRef.current);

    // Volume histogram series
    volumeSeriesRef.current = chart.addHistogramSeries({
      priceFormat: { type: 'volume' },
      priceScaleId: 'volume',
    });
    chart.priceScale('volume').applyOptions({
      scaleMargins: { top: 0.8, bottom: 0 },
    });

    // All MA line series (curved)
    MA_CONFIGS.forEach(({ period, color }) => {
      maSeriesRefs.current[period] = chart.addLineSeries({
        color,
        lineWidth: 1.5 as any,
        lineType: LineType.Curved,
        title: '',
        lastValueVisible: false,
        priceLineVisible: false,
      });
    });

    // Subscribe to crosshair move for tooltip
    chart.subscribeCrosshairMove((param: MouseEventParams) => {
      if (!param.time || !param.point) {
        setTooltipState((prev) => prev.visible ? { visible: false, x: 0, y: 0, data: null } : prev);
        return;
      }
      const candleData = param.seriesData.get(candlestickSeriesRef.current) as any;
      if (!candleData) {
        setTooltipState((prev) => prev.visible ? { visible: false, x: 0, y: 0, data: null } : prev);
        return;
      }

      // Gather MA values from crosshair
      const maVals: Record<number, number> = {};
      const enabled = enabledMaPeriodsRef.current;
      MA_CONFIGS.forEach(({ period }) => {
        if (!enabled.includes(period)) return;
        const s = maSeriesRefs.current[period];
        if (!s) return;
        const val = param.seriesData.get(s) as any;
        if (val && val.value != null) maVals[period] = val.value;
      });

      // Gather RSI value via lookup map (Bug 3 fix — rsiSeries is on a separate chart instance)
      const candleTime = (candleData.time ?? param.time) as number;
      const rsiVal = rsiDataMapRef.current.get(candleTime) ?? null;

      setTooltipState({
        visible: true,
        x: param.point.x,
        y: param.point.y,
        data: {
          time: candleTime,
          open: candleData.open,
          high: candleData.high,
          low: candleData.low,
          close: candleData.close,
          volume: candleData.volume,
          maValues: maVals,
          rsiValue: rsiVal,
        },
      });
    });

    // RSI chart (deferred so DOM is ready)
    const rsiTimeout = setTimeout(() => {
      if (!rsiChartContainerRef.current || rsiChartRef.current) return;
      const t0 = getChartTheme(theme);
      const rsiChart = createChart(rsiChartContainerRef.current, {
        layout: {
          background: { type: ColorType.Solid, color: t0.bg },
          textColor: t0.text,
        },
        autoSize: true,
        grid: {
          vertLines: { color: t0.grid },
          horzLines: { color: t0.grid },
        },
        rightPriceScale: {
          borderColor: t0.grid,
          visible: true,
          scaleMargins: { top: 0.1, bottom: 0.1 },
        },
        timeScale: {
          borderColor: t0.grid,
          timeVisible: true,
          secondsVisible: false,
        },
        handleScroll: {
          mouseWheel: false,
          pressedMouseMove: false,
          horzTouchDrag: false,
          vertTouchDrag: false,
        },
        handleScale: {
          mouseWheel: false,
          pinch: false,
          axisPressedMouseMove: false,
          axisDoubleClickReset: false,
        },
      } as any);
      rsiChartRef.current = rsiChart;
      // RSI as area series with gradient
      rsiSeriesRef.current = rsiChart.addAreaSeries({
        lineColor: t0.rsiLine,
        topColor: t0.rsiTop,
        bottomColor: t0.rsiBottom,
        lineWidth: 2,
        priceFormat: { type: 'price', precision: 0, minMove: 1 },
      });

      // Flush any RSI data that was computed before the series was ready (Bug 1 fix)
      if (pendingRsiDataRef.current) {
        rsiSeriesRef.current.setData(pendingRsiDataRef.current);
        pendingRsiDataRef.current = null;
        rsiChart.timeScale().fitContent();
      }

      // One-directional logical-range sync: main chart drives RSI chart.
      // RSI data starts `period` bars later than main data, so logical
      // index 0 on RSI = index `period` on main. Subtract the offset
      // when forwarding the range.
      // RSI chart has all scroll/scale interactions disabled.
      const mainTs = chart.timeScale();
      const rsiTs = rsiChart.timeScale();
      mainTs.subscribeVisibleLogicalRangeChange((range) => {
        if (!range) return;
        const offset = rsiPeriodRef.current;
        try {
          rsiTs.setVisibleLogicalRange({ from: range.from - offset, to: range.to - offset });
        } catch { /* RSI data may not cover the range yet */ }
      });
    }, 100);

    return () => {
      clearTimeout(rsiTimeout);

      // Unsubscribe scroll-load listener
      if (rangeUnsubRef.current) {
        rangeUnsubRef.current();
        rangeUnsubRef.current = null;
      }
      if (rangeChangeTimerRef.current) clearTimeout(rangeChangeTimerRef.current);

      extHoursBgRef.current = null;
      extCloseLineRef.current = null;
      currentExtTypeRef.current = null;
      candlestickSeriesRef.current = null;
      volumeSeriesRef.current = null;
      baselineSeriesRef.current = null;
      Object.keys(maSeriesRefs.current).forEach(k => { maSeriesRefs.current[Number(k)] = null; });
      rsiSeriesRef.current = null;

      if (chartRef.current) {
        chartRef.current.remove();
        chartRef.current = null;
      }
      if (rsiChartRef.current) {
        rsiChartRef.current.remove();
        rsiChartRef.current = null;
      }
    };
  }, []); // Mount only

  // --- Effect: Update watermark when symbol changes ---
  useEffect(() => {
    if (chartRef.current) {
      chartRef.current.applyOptions({
        watermark: {
          visible: true,
          text: symbol,
          fontSize: 48,
          color: ct.watermark,
          horzAlign: 'center',
          vertAlign: 'center',
        },
      });
    }
  }, [symbol, ct.watermark]);

  // --- Effect: Re-apply theme colors when theme changes ---
  useEffect(() => {
    const chart = chartRef.current;
    const rsiChart = rsiChartRef.current;
    if (chart) {
      chart.applyOptions({
        layout: { background: { type: ColorType.Solid, color: ct.bg }, textColor: ct.text },
        grid: { vertLines: { color: ct.grid }, horzLines: { color: ct.grid } },
        rightPriceScale: { borderColor: ct.grid },
        timeScale: { borderColor: ct.grid },
        watermark: { color: ct.watermark },
      });
      if (candlestickSeriesRef.current) {
        candlestickSeriesRef.current.applyOptions({
          upColor: ct.upColor, downColor: ct.downColor,
          wickUpColor: ct.upColor, wickDownColor: ct.downColor,
        });
      }
      if (baselineSeriesRef.current) {
        baselineSeriesRef.current.applyOptions({
          topLineColor: ct.baselineUp, topFillColor1: ct.baselineUpFill1, topFillColor2: ct.baselineUpFill2,
          bottomLineColor: ct.baselineDown, bottomFillColor1: ct.baselineDownFill1, bottomFillColor2: ct.baselineDownFill2,
        });
      }
      // Re-color volume bars (extended-hours aware)
      if (volumeSeriesRef.current && allDataRef.current.length > 0) {
        const data = allDataRef.current;
        const applyExt = isUSEquity(symbolRef.current) && EXTENDED_HOURS_INTERVALS.has(intervalRef.current);
        volumeSeriesRef.current.setData(data.map((d, i) => {
          const up = i > 0 && d.close >= data[i - 1].close;
          const ext = applyExt && getExtendedHoursType(d.time);
          return {
            time: d.time, value: d.volume || 0,
            color: ext
              ? (up ? ct.extVolumeUp : ct.extVolumeDown)
              : (up ? ct.volumeUp : ct.volumeDown),
          };
        }));
      }
      // Update extended-hours background color on theme change
      if (extHoursBgRef.current) {
        extHoursBgRef.current.setColors({ pre: ct.extBgPre, post: ct.extBgPost });
      }
    }
    if (rsiChart) {
      rsiChart.applyOptions({
        layout: { background: { type: ColorType.Solid, color: ct.bg }, textColor: ct.text },
        grid: { vertLines: { color: ct.grid }, horzLines: { color: ct.grid } },
        rightPriceScale: { borderColor: ct.grid },
        timeScale: { borderColor: ct.grid },
      });
      if (rsiSeriesRef.current) {
        rsiSeriesRef.current.applyOptions({
          lineColor: ct.rsiLine, topColor: ct.rsiTop, bottomColor: ct.rsiBottom,
        });
      }
    }
  }, [ct]);

  // --- Effect: Price scale mode ---
  useEffect(() => {
    if (chartRef.current) {
      chartRef.current.priceScale('right').applyOptions({ mode: priceScaleMode });
    }
  }, [priceScaleMode]);

  // --- Effect: Crosshair magnet mode ---
  useEffect(() => {
    if (chartRef.current) {
      chartRef.current.applyOptions({
        crosshair: { mode: magnetMode ? CrosshairMode.Magnet : CrosshairMode.Normal },
      });
    }
  }, [magnetMode]);

  // --- Effect: Baseline series toggle ---
  useEffect(() => {
    const chart = chartRef.current;
    if (!chart) return;

    if (showBaseline) {
      // Hide candlestick + volume, show baseline
      if (candlestickSeriesRef.current) {
        candlestickSeriesRef.current.applyOptions({ visible: false });
      }
      if (volumeSeriesRef.current) {
        volumeSeriesRef.current.applyOptions({ visible: false });
      }
      // Hide MAs too
      MA_CONFIGS.forEach(({ period }) => {
        const s = maSeriesRefs.current[period];
        if (s) s.applyOptions({ visible: false });
      });

      const prevClose = (quoteData?.previousClose || quoteData?.open) as number | undefined;
      const basePrice: number = prevClose || (allDataRef.current.length > 0 ? allDataRef.current[0].open : 0);

      if (!baselineSeriesRef.current) {
        baselineSeriesRef.current = chart.addBaselineSeries({
          baseValue: { type: 'price', price: basePrice },
          topLineColor: ct.baselineUp,
          topFillColor1: ct.baselineUpFill1,
          topFillColor2: ct.baselineUpFill2,
          bottomLineColor: ct.baselineDown,
          bottomFillColor1: ct.baselineDownFill1,
          bottomFillColor2: ct.baselineDownFill2,
          lineWidth: 2,
        });
      } else {
        baselineSeriesRef.current.applyOptions({
          baseValue: { type: 'price', price: basePrice },
        });
      }

      // Set close-only data
      const data = allDataRef.current;
      if (data.length > 0) {
        baselineSeriesRef.current.setData(data.map((d) => ({ time: d.time, value: d.close })));
      }
    } else {
      // Show candlestick + volume + MAs, remove baseline
      if (candlestickSeriesRef.current) {
        candlestickSeriesRef.current.applyOptions({ visible: true });
      }
      if (volumeSeriesRef.current) {
        volumeSeriesRef.current.applyOptions({ visible: true });
      }
      MA_CONFIGS.forEach(({ period }) => {
        const s = maSeriesRefs.current[period];
        if (s) s.applyOptions({ visible: true });
      });

      if (baselineSeriesRef.current) {
        try { chart.removeSeries(baselineSeriesRef.current); } catch (_) { /* ok */ }
        baselineSeriesRef.current = null;
      }
    }
  }, [showBaseline, quoteData]);

  // --- Effect 2: Data loading (on symbol or interval change) ---
  useEffect(() => {
    const abortController = new AbortController();

    // Reset scroll-load state
    allDataRef.current = [];
    oldestDateRef.current = null;
    fetchingRef.current = false;
    gapFillDoneRef.current = false;
    gapFillRetryRef.current = 0;
    gapFillInProgressRef.current = false;
    minuteAggRef.current = { time: 0, open: 0, high: 0, low: 0, close: 0, volume: 0 };

    // Cancel any in-flight stage 2 / prefetch from previous symbol/interval
    stage2AbortRef.current?.abort();
    prefetchAbortRef.current?.abort();
    prefetchedDataRef.current = null;
    prefetchingRef.current = false;

    // Unsubscribe previous scroll listener
    if (rangeUnsubRef.current) {
      rangeUnsubRef.current();
      rangeUnsubRef.current = null;
    }

    // Reset baseline on symbol/interval change
    if (showBaseline) setShowBaseline(false);

    // Reset extended-hours price lines on symbol/interval change
    if (extCloseLineRef.current && candlestickSeriesRef.current) {
      try { candlestickSeriesRef.current.removePriceLine(extCloseLineRef.current); } catch (_) { /* ok */ }
    }
    extCloseLineRef.current = null;
    currentExtTypeRef.current = null;
    if (candlestickSeriesRef.current) {
      candlestickSeriesRef.current.applyOptions({ priceLineColor: undefined });
    }

    // Clear stale chart data so previous interval/symbol doesn't linger under an error
    const clearChartSeries = () => {
      if (candlestickSeriesRef.current) candlestickSeriesRef.current.setData([]);
      if (volumeSeriesRef.current) volumeSeriesRef.current.setData([]);
      if (rsiSeriesRef.current) rsiSeriesRef.current.setData([]);
      MA_CONFIGS.forEach(({ period }) => {
        const s = maSeriesRefs.current[period];
        if (s) s.setData([]);
      });
      setChartDataForHooks([]);
      // Reset RSI incremental state on symbol/interval change
      rsiSmoothingRef.current = null;
      prevBarSmoothingRef.current = null;
      pendingRsiDataRef.current = null;
      rsiDataMapRef.current = new Map();
      setRsiValue(null);
    };

    // Immediately clear stale data so the price scale resets for the new symbol
    clearChartSeries();
    if (chartRef.current) {
      chartRef.current.applyOptions({ watermark: { text: symbol } });
    }

    const loadData = async () => {
      setLoading(true);
      setError(null);

      try {
        const loadDays = (interval in STAGE1_LOAD_DAYS) ? STAGE1_LOAD_DAYS[interval] : INITIAL_LOAD_DAYS[interval];

        let fromDate, toDate;
        if (loadDays > 0) {
          const now = new Date();
          toDate = now.toLocaleDateString('en-CA', { timeZone: 'America/New_York' });
          {
            const maxMaPeriod = Math.max(...enabledMaPeriodsRef.current, 0);
            const overheadDays = Math.ceil((maxMaPeriod / (BARS_PER_DAY[interval] || 1)) * 1.5);
            const from = new Date(now);
            from.setDate(from.getDate() - loadDays - overheadDays);
            fromDate = from.toLocaleDateString('en-CA', { timeZone: 'America/New_York' });
          }
        }

        const result = await fetchStockData(symbol, interval, fromDate, toDate, { signal: abortController.signal });

        if (abortController.signal.aborted) return;

        const data = result?.data || [];

        if (Array.isArray(data) && data.length > 0) {
          allDataRef.current = data;
          oldestDateRef.current = data[0].time;

          updateSeriesData(data);

          // Apply default view: auto-fit barSpacing + latest bar centered
          applyDefaultView();
          if (chartRef.current) {
            chartRef.current.priceScale('right').applyOptions({ autoScale: true });
          }
          setLastUpdateTime(new Date());
          setError(null);

          // Report latest bar to parent so header can show fresh price
          if (typeof onLatestBar === 'function') {
            onLatestBar(data[data.length - 1]);
          }

          // Subscribe to visible range changes for scroll-based loading (debounced)
          if (chartRef.current) {
            const unsubscribe = chartRef.current.timeScale().subscribeVisibleLogicalRangeChange((range: LogicalRange | null) => {
              if (rangeChangeTimerRef.current) clearTimeout(rangeChangeTimerRef.current);
              rangeChangeTimerRef.current = setTimeout(() => {
                if (!range) return;
                // Prefetch: start loading next chunk early (150 bars from left)
                if (range.from <= PREFETCH_THRESHOLD && PREFETCH_ENABLED_INTERVALS.has(intervalRef.current)) {
                  triggerPrefetch();
                }
                // Scroll-load: merge when near edge (20 bars from left)
                if (range.from <= SCROLL_LOAD_THRESHOLD) {
                  handleScrollLoadMore();
                }
              }, RANGE_CHANGE_DEBOUNCE_MS);
            }) as unknown as (() => void);
            rangeUnsubRef.current = unsubscribe;
          }

          // --- Stage 2: background backfill ---
          const backfillDays = STAGE2_BACKFILL_DAYS[interval];
          if (backfillDays > 0 && oldestDateRef.current) {
            const stage2Abort = new AbortController();
            stage2AbortRef.current = stage2Abort;
            const capturedSym = symbol;

            // Yield to rendering before starting background fetch
            setTimeout(async () => {
              if (stage2Abort.signal.aborted || symbolRef.current !== capturedSym) return;

              // --- Today gap fill for 1s: fill missing morning bars ---
              if (interval === '1s' && oldestDateRef.current) {
                const firstBarDate = new Date(oldestDateRef.current * 1000);
                const firstBarMins = firstBarDate.getUTCHours() * 60 + firstBarDate.getUTCMinutes();
                const todayStr = new Date().toLocaleDateString('en-CA', { timeZone: 'America/New_York' });
                const firstBarDateStr = firstBarDate.toISOString().split('T')[0];

                // Fill if first bar is from today and starts after 4:30 AM ET (270 mins)
                if (firstBarDateStr === todayStr && firstBarMins > 270) {
                  try {
                    const gapResult = await fetchStockData(capturedSym, '1s', todayStr, todayStr,
                      { signal: stage2Abort.signal });
                    if (stage2Abort.signal.aborted || symbolRef.current !== capturedSym) return;
                    if (gapResult?.data?.length > 0) {
                      const gapBars = gapResult.data.filter(b => b.time < oldestDateRef.current!);
                      if (gapBars.length > 0) {
                        mergePrependedData(gapBars);
                      }
                    }
                  } catch (err: unknown) {
                    if (err instanceof Error && err.name !== 'AbortError' && err.name !== 'CanceledError') {
                      console.warn('Today gap fill failed:', err);
                    }
                  }
                }
              }

              if (stage2Abort.signal.aborted || symbolRef.current !== capturedSym) return;

              // --- Backwards backfill: fetch prior days ---
              const oldest = new Date(oldestDateRef.current! * 1000);
              const to = new Date(oldest);
              to.setDate(to.getDate() - 1);
              const from = new Date(to);
              const maxMaPeriod = Math.max(...enabledMaPeriodsRef.current, 0);
              const maOverhead = Math.ceil((maxMaPeriod / (BARS_PER_DAY[interval] || 1)) * 1.5);
              from.setDate(from.getDate() - backfillDays - maOverhead);

              try {
                const result = await fetchStockData(capturedSym, interval,
                  from.toISOString().split('T')[0],
                  to.toISOString().split('T')[0],
                  { signal: stage2Abort.signal });
                if (stage2Abort.signal.aborted || symbolRef.current !== capturedSym) return;
                if (result?.data?.length > 0) {
                  mergePrependedData(result.data);
                }
              } catch (err: unknown) {
                if (err instanceof Error && err.name !== 'AbortError' && err.name !== 'CanceledError') {
                  console.warn('Stage 2 backfill failed:', err);
                }
              }

              // Start prefetching the next scroll chunk for prefetch-enabled intervals
              if (PREFETCH_ENABLED_INTERVALS.has(interval) && symbolRef.current === capturedSym) {
                triggerPrefetch();
              }
            }, 50);
          }

        } else {
          // Silently downgrade 1s → 1min when ginlix-data unavailable or symbol ineligible
          if (interval === '1s' && (!ginlixDataEnabled || !supports1sInterval(symbol))) {
            onIntervalChange?.('1min');
            return;
          }
          // Silently downgrade 4H → 1H when provider doesn't support it
          if (interval === '4hour' && !supports4hInterval) {
            onIntervalChange?.('1hour');
            return;
          }
          clearChartSeries();
          let fallbackMsg;
          if (interval === '1s') {
            fallbackMsg = 'No 1s data yet — waiting for pre-market to open (4:00 AM ET).';
          } else if (interval !== '1day') {
            fallbackMsg = 'Intraday data not available — market may be closed. Try the 1D interval.';
          } else {
            fallbackMsg = 'Stock data not found';
          }
          setError(result?.error || fallbackMsg);
          if (typeof onStockMeta === 'function') onStockMeta(null);
        }
      } catch (err: unknown) {
        if (abortController.signal.aborted) return;
        // Silently downgrade 1s → 1min when ginlix-data unavailable or symbol ineligible
        if (interval === '1s' && (!ginlixDataEnabled || !supports1sInterval(symbol))) {
          onIntervalChange?.('1min');
          return;
        }
        // Silently downgrade 4H → 1H when provider doesn't support it
        if (interval === '4hour' && !supports4hInterval) {
          onIntervalChange?.('1hour');
          return;
        }
        console.error('Failed to load stock data:', err);
        clearChartSeries();
        setError(err instanceof Error ? err.message : 'Failed to load data');
      } finally {
        if (!abortController.signal.aborted) {
          setLoading(false);
        }
      }
    };

    loadData();

    return () => {
      abortController.abort();
      stage2AbortRef.current?.abort();
      prefetchAbortRef.current?.abort();
    };
  }, [symbol, interval, onStockMeta, updateSeriesData, handleScrollLoadMore]);

  // --- REST polling fallback (1s and 1min safety net when WS not delivering) ---
  useEffect(() => {
    const is1sPolling = interval === '1s';
    const is1mPolling = interval === '1min';
    if ((!is1sPolling && !is1mPolling) || chartMode !== 'custom') return;

    let timer = null;
    let aborted = false;

    const poll = async () => {
      if (aborted) return;
      // Skip if WS delivered a live tick recently
      if (lastLiveTickTimeRef.current > Date.now() - 5000) return;
      try {
        const now = new Date();
        const toDate = now.toLocaleDateString('en-CA', { timeZone: 'America/New_York' });

        // Delta-based: fetch only from last known bar's time onward
        const lastBar = allDataRef.current?.[allDataRef.current.length - 1];
        const fromDate = lastBar
          ? new Date(lastBar.time * 1000).toISOString().split('T')[0]
          : (() => { const d = new Date(now); d.setDate(d.getDate() - 3); return d.toLocaleDateString('en-CA', { timeZone: 'America/New_York' }); })();

        const result = await fetchStockData(symbol, interval, fromDate, toDate);
        if (aborted) return;

        const data = result?.data;
        if (Array.isArray(data) && data.length > 0) {
          if (lastBar) {
            // Merge: append only genuinely new bars (compare by unix time)
            const lastTime = allDataRef.current[allDataRef.current.length - 1].time;
            const newBars = data.filter(b => b.time > lastTime);
            if (newBars.length > 0) {
              const merged = [...allDataRef.current, ...newBars];
              allDataRef.current = merged;
              updateSeriesData(merged);
            }
          } else {
            allDataRef.current = data;
            updateSeriesData(data);
          }

          // Don't re-zoom on poll updates — preserve user's manual scroll/zoom position
          setLastUpdateTime(new Date());
          setError(null);

          // Report latest bar to parent so header stays in sync
          const latest = allDataRef.current[allDataRef.current.length - 1];
          if (latest && typeof onLatestBar === 'function') {
            onLatestBar(latest);
          }
        }
      } catch (err) {
        if (!aborted) console.debug('REST poll failed:', err);
      }
    };

    // 1s: poll every 5s as WS fallback; native 1m: poll every 15s for canonical bars
    const pollMs = is1sPolling ? 5000 : 15000;
    timer = setInterval(poll, pollMs);

    return () => {
      aborted = true;
      if (timer) clearInterval(timer);
    };
  }, [interval, symbol, chartMode, updateSeriesData]);

  // --- Effect 3: TimeScale options per interval ---
  useEffect(() => {
    const isIntraday = interval !== '1day';
    const showSeconds = interval === '1s' || interval === '1min';
    const opts = { timeVisible: isIntraday, secondsVisible: showSeconds };
    if (chartRef.current) chartRef.current.applyOptions({ timeScale: opts });
    if (rsiChartRef.current) rsiChartRef.current.applyOptions({ timeScale: opts });
  }, [interval]);

  // --- Effect 4: Re-run updateSeriesData when MA/RSI config changes ---
  useEffect(() => {
    if (allDataRef.current.length > 0) {
      updateSeriesData(allDataRef.current);
    }
  }, [enabledMaPeriods, rsiPeriod, updateSeriesData]);

  // --- Default view: auto-fit barSpacing + latest bar centered ---
  const applyDefaultView = useCallback(() => {
    if (!chartRef.current) return;
    const ts = chartRef.current.timeScale();
    const target = TARGET_BAR_SPACING[intervalRef.current] || 8;
    ts.applyOptions({ barSpacing: target });
    const dataLen = allDataRef.current.length;
    if (dataLen === 0) { ts.scrollToRealTime(); return; }
    const chartWidth = chartRef.current.options().width || chartContainerRef.current?.clientWidth || 800;
    const halfBars = Math.floor(chartWidth / target / 2);
    ts.setVisibleLogicalRange({
      from: dataLen - halfBars,
      to: dataLen + halfBars,
    });
  }, []);

  // --- Tool button handlers ---
  const handleZoomIn = useCallback(() => {
    if (!chartRef.current) return;
    const ts = chartRef.current.timeScale();
    const current = ts.options().barSpacing;
    ts.applyOptions({ barSpacing: Math.min(current * 1.5, 50) });
  }, []);

  const handleZoomOut = useCallback(() => {
    if (!chartRef.current) return;
    const ts = chartRef.current.timeScale();
    const current = ts.options().barSpacing;
    ts.applyOptions({ barSpacing: Math.max(current / 1.5, 1) });
  }, []);

  const handleScrollToRealTime = useCallback(() => applyDefaultView(), [applyDefaultView]);

  const handleAutoNormalize = useCallback(() => {
    if (!chartRef.current) return;
    const ts = chartRef.current.timeScale();
    // Reset zoom to comfortable bar size, keeping current scroll position
    ts.applyOptions({ barSpacing: TARGET_BAR_SPACING[intervalRef.current] || 8 });
  }, []);

  const handleFitAll = useCallback(() => {
    if (chartRef.current) chartRef.current.timeScale().fitContent();
  }, []);

  const handleToggleAnnotations = useCallback(() => {
    setAnnotationsVisible((prev) => !prev);
  }, []);

  const handleToggleOverlay = useCallback((key: string) => {
    setOverlayVisibility((prev) => ({ ...prev, [key]: !prev[key] }));
  }, []);

  const handleTogglePriceScale = useCallback((mode: number) => {
    setPriceScaleMode((prev) => prev === mode ? PriceScaleMode.Normal : mode);
  }, []);

  const isTV = chartMode === 'tradingview';

  // --- Toolbar render helpers (shared between wide & compact layouts) ---

  const renderIndicatorsContent = () => (
    <>
      <div className="dropdown-section">
        <span className="indicator-toggles-label">MA</span>
        <div className="indicator-toggles">
          {MA_CONFIGS.map(({ period, color }) => (
            <button
              key={period}
              type="button"
              className={`indicator-toggle-btn${enabledMaPeriods.includes(period) ? ' indicator-toggle-active' : ''}`}
              style={enabledMaPeriods.includes(period) ? { color, borderColor: color } : undefined}
              onClick={() => handleToggleMa(period)}
            >
              {period}
            </button>
          ))}
        </div>
      </div>
      <div className="dropdown-section">
        <span className="indicator-toggles-label">RSI</span>
        <div className="indicator-toggles">
          {RSI_PERIODS.map((p) => (
            <button
              key={p}
              type="button"
              className={`indicator-toggle-btn${rsiPeriod === p ? ' indicator-toggle-active' : ''}`}
              style={rsiPeriod === p ? { color: 'var(--color-accent-primary)', borderColor: 'var(--color-accent-primary)' } : undefined}
              onClick={() => handleChangeRsiPeriod(p)}
            >
              {p}
            </button>
          ))}
        </div>
      </div>
      <div className="dropdown-section">
        <span className="indicator-toggles-label">Overlay</span>
        <div className="indicator-toggles">
          {Object.entries(OVERLAY_LABELS).map(([key, label]) => (
            <button
              key={key}
              type="button"
              className={`indicator-toggle-btn${overlayVisibility[key] ? ' indicator-toggle-active' : ''}`}
              style={overlayVisibility[key] ? { color: OVERLAY_COLORS[key], borderColor: OVERLAY_COLORS[key] } : undefined}
              onClick={() => handleToggleOverlay(key)}
            >
              {label}
            </button>
          ))}
        </div>
      </div>
    </>
  );

  const renderToolsContent = () => (
    <>
      <button
        type="button"
        className={`chart-tool-btn${priceScaleMode === PriceScaleMode.Percentage ? ' chart-tool-btn-active' : ''}`}
        onClick={() => handleTogglePriceScale(PriceScaleMode.Percentage)}
        title="Percentage Scale"
      >
        %
      </button>
      <button
        type="button"
        className={`chart-tool-btn${magnetMode ? ' chart-tool-btn-active' : ''}`}
        onClick={() => setMagnetMode((v) => !v)}
        title="Magnet Mode"
      >
        M
      </button>
      <button
        type="button"
        className={`chart-tool-btn${showBaseline ? ' chart-tool-btn-active' : ''}`}
        onClick={() => setShowBaseline((v) => !v)}
        title="Baseline vs Previous Close"
      >
        B
      </button>
      <button
        type="button"
        className={`chart-tool-btn${annotationsVisible ? ' chart-tool-btn-active' : ''}`}
        onClick={handleToggleAnnotations}
        title="Toggle Annotations"
      >
        T
      </button>
    </>
  );

  const renderViewButtons = () => (
    <>
      <button
        type="button"
        className={`chart-tool-btn${priceScaleMode === PriceScaleMode.Logarithmic ? ' chart-tool-btn-active' : ''}`}
        onClick={() => handleTogglePriceScale(PriceScaleMode.Logarithmic)}
        title="Log Scale"
      >
        Log
      </button>
      <button type="button" className="chart-tool-btn" onClick={handleZoomIn} title="Zoom In"><Plus size={14} /></button>
      <button type="button" className="chart-tool-btn" onClick={handleZoomOut} title="Zoom Out"><Minus size={14} /></button>
      <button type="button" className="chart-tool-btn" onClick={handleAutoNormalize} title="Auto Fit"><Maximize2 size={14} /></button>
      <button type="button" className="chart-tool-btn" onClick={handleFitAll} title="Fit All Data"><Minimize2 size={14} /></button>
      <button type="button" className="chart-tool-btn" onClick={handleScrollToRealTime} title="Scroll to Latest"><RotateCcw size={14} /></button>
    </>
  );

  return (
    <div className={`market-chart-container${isCompact ? ' chart--compact' : ''}`} ref={rootRef}>
      {/* ---- Toolbar: intervals, indicator dropdown, values, tools dropdown, mode switcher ---- */}
      <div className="chart-tools">
        <div className="chart-tools-left">
          <div className="interval-selector">
            {INTERVALS.filter(({ key }) => PRIMARY_INTERVAL_KEYS.has(key)).map(({ key, label }) => {
              const is1sDisabled = key === '1s' && (!ginlixDataEnabled || !supports1sInterval(symbol));
              const is4hDisabled = key === '4hour' && !supports4hInterval;
              const isDisabled = is1sDisabled || is4hDisabled;
              return (
              <div key={key} style={{ position: 'relative', display: 'inline-flex' }}>
                <button
                  type="button"
                  className={`interval-btn${interval === key ? ' interval-btn-active' : ''}${isDisabled ? ' interval-btn-disabled' : ''}`}
                  onClick={() => {
                    if (isDisabled) {
                      const msg = is1sDisabled
                        ? (!ginlixDataEnabled ? '1s data is not available' : '1s interval is only available for US stocks')
                        : '4H data requires FMP or Ginlix Data provider';
                      setDisabledTooltip(msg);
                      if (disabledTooltipTimer.current) clearTimeout(disabledTooltipTimer.current);
                      disabledTooltipTimer.current = setTimeout(() => setDisabledTooltip(null), 2000);
                      return;
                    }
                    onIntervalChange?.(key); setIntervalsOpen(false); setIndicatorsOpen(false); setToolsOpen(false); setViewOpen(false);
                  }}
                >
                  {label}
                </button>
                {isDisabled && disabledTooltip && (
                  <div className="interval-disabled-tooltip">{disabledTooltip}</div>
                )}
              </div>
              );
            })}
            {/* "More" dropdown for secondary intervals */}
            <div className="toolbar-dropdown" ref={intervalsDropdownRef} style={{ display: 'inline-flex' }}>
              <button
                type="button"
                className={`interval-btn${(!PRIMARY_INTERVAL_KEYS.has(interval) || intervalsOpen) ? ' interval-btn-active' : ''}`}
                onClick={() => { setIntervalsOpen((v) => !v); setIndicatorsOpen(false); setToolsOpen(false); setViewOpen(false); }}
              >
                {!PRIMARY_INTERVAL_KEYS.has(interval)
                  ? INTERVALS.find(({ key }) => key === interval)?.label
                  : 'More'}
                <ChevronDown size={10} style={{ marginLeft: 2, opacity: 0.6 }} />
              </button>
              {intervalsOpen && (
                <div className="toolbar-dropdown-panel interval-dropdown-panel">
                  {INTERVALS.filter(({ key }) => !PRIMARY_INTERVAL_KEYS.has(key)).map(({ key, label }) => (
                    <button
                      key={key}
                      type="button"
                      className={`interval-dropdown-item${interval === key ? ' interval-dropdown-item-active' : ''}`}
                      onClick={() => { onIntervalChange?.(key); setIntervalsOpen(false); setIndicatorsOpen(false); setToolsOpen(false); }}
                    >
                      {label}
                    </button>
                  ))}
                </div>
              )}
            </div>
          </div>
          {!isTV && (
            <>
              {/* Indicators dropdown — wide only */}
              <div className="toolbar-dropdown toolbar--wide-only" ref={indicatorsDropdownRef}>
                <button
                  type="button"
                  className={`chart-tool-btn${indicatorsOpen ? ' chart-tool-btn-active' : ''}`}
                  onClick={() => { setIndicatorsOpen((v) => !v); setToolsOpen(false); setViewOpen(false); }}
                  title="Indicators"
                >
                  <SlidersHorizontal size={14} />
                </button>
                {indicatorsOpen && (
                  <div className="toolbar-dropdown-panel">
                    {renderIndicatorsContent()}
                  </div>
                )}
              </div>
              {/* Indicator values — wide only */}
              <div className="chart-indicators toolbar--wide-only">
                {MA_CONFIGS.filter(({ period }) => enabledMaPeriods.includes(period)).map(({ period, color, label }) => (
                  <span className="indicator-item" key={period}>
                    <span className="indicator-color" style={{ backgroundColor: color }} />
                    {label}: {maValues[period] ?? '\u2014'}
                  </span>
                ))}
                <span className="indicator-item">
                  <span className="indicator-color" style={{ backgroundColor: 'var(--color-accent-primary)' }} />
                  RSI ({rsiPeriod}): {rsiValue ?? '\u2014'}
                </span>
              </div>
            </>
          )}
        </div>
        <div className="chart-tools-right">
          {!isTV && (
            <>
              {/* Tools dropdown — wide only */}
              <div className="toolbar-dropdown toolbar--wide-only" ref={toolsDropdownRef}>
                <button
                  type="button"
                  className={`chart-tool-btn${toolsOpen ? ' chart-tool-btn-active' : ''}`}
                  onClick={() => { setToolsOpen((v) => !v); setIndicatorsOpen(false); setViewOpen(false); }}
                  title="Chart Tools"
                >
                  <Settings2 size={14} />
                </button>
                {toolsOpen && (
                  <div className="toolbar-dropdown-panel toolbar-dropdown-panel--right">
                    <div className="dropdown-tool-grid">
                      {renderToolsContent()}
                    </div>
                  </div>
                )}
              </div>
              {/* Zoom, fit, navigation — inline, wide only */}
              <div className="chart-tool-buttons toolbar--wide-only">
                {renderViewButtons()}
              </div>
              {/* Combined menu — compact only (merges indicators + tools + view) */}
              <div className="toolbar-dropdown toolbar--compact-only" ref={viewDropdownRef}>
                <button
                  type="button"
                  className={`chart-tool-btn${viewOpen ? ' chart-tool-btn-active' : ''}`}
                  onClick={() => { setViewOpen((v) => !v); setIndicatorsOpen(false); setToolsOpen(false); }}
                  title="Chart Settings"
                >
                  <Menu size={14} />
                </button>
                {viewOpen && (
                  <div className="toolbar-dropdown-panel toolbar-dropdown-panel--right compact-menu-panel">
                    {/* Indicators section */}
                    <div className="compact-menu-section-label">Indicators</div>
                    {renderIndicatorsContent()}
                    {/* Tools section */}
                    <div className="compact-menu-divider" />
                    <div className="compact-menu-section-label">Tools</div>
                    <div className="dropdown-tool-grid">
                      {renderToolsContent()}
                    </div>
                    {/* View section */}
                    <div className="compact-menu-divider" />
                    <div className="compact-menu-section-label">View</div>
                    <div className="dropdown-tool-grid compact-menu-view-grid">
                      {renderViewButtons()}
                    </div>
                  </div>
                )}
              </div>
            </>
          )}
          <div className="chart-mode-switcher">
            <div className="interval-selector">
              <button
                type="button"
                className={`interval-btn${!isTV ? ' interval-btn-active' : ''}`}
                onClick={() => { setChartMode('custom'); setIndicatorsOpen(false); setToolsOpen(false); setViewOpen(false); }}
              >
                Light
              </button>
              <button
                type="button"
                className={`interval-btn${isTV ? ' interval-btn-active' : ''}`}
                onClick={() => { setChartMode('tradingview'); setIndicatorsOpen(false); setToolsOpen(false); setViewOpen(false); }}
              >
                Advanced
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* ---- Charts area: shared flex container for both modes ---- */}
      <div style={{ flex: 1, position: 'relative', minHeight: 0, display: 'flex', flexDirection: 'column' }}>
        {/* Light chart: always in DOM with layout preserved for screenshot capture.
            When Advanced is active, positioned absolutely behind TV widget (invisible). */}
        <div
          ref={lightWrapperRef}
          className={isTV ? 'light-chart-hidden' : 'light-chart-visible'}
        >
          <div
            className="charts-container chart-wheel-capture"
            onWheel={(e) => e.stopPropagation()}
            role="region"
            aria-label="K-line chart"
          >
            <div
              ref={chartContainerRef}
              className="chart-wrapper"
            >
              <CrosshairTooltip
                visible={tooltipState.visible}
                x={tooltipState.x}
                y={tooltipState.y}
                data={tooltipState.data}
                enabledMaPeriods={enabledMaPeriods}
                containerWidth={chartContainerRef.current?.clientWidth}
                containerHeight={chartContainerRef.current?.clientHeight}
              />
              {scrollLoading && (
                <div className="chart-scroll-loading">
                  <div className="chart-scroll-loading-spinner" />
                </div>
              )}
            </div>
            <div className="rsi-container">
              <div className="rsi-label">RSI ({rsiPeriod}): {rsiValue ?? '\u2014'}</div>
              <div className="rsi-chart-wrapper" ref={rsiChartContainerRef}></div>
            </div>
          </div>
          {loading && (
            <div className="chart-loading">
              <div className="chart-loading-shimmer">Fetching real-time market data…</div>
            </div>
          )}
          {error && (
            <div className="chart-error">
              <div className="chart-error-title">Data Loading Failed</div>
              <div>{error}</div>
            </div>
          )}
        </div>

        {/* TradingView Advanced Chart (only mounted when active) */}
        {isTV && (
          <div className="charts-container" style={{ flex: 1, minHeight: 0 }}>
            <TradingViewWidget symbol={symbol} interval={interval} />
          </div>
        )}
      </div>
    </div>
  );
}));

MarketChart.displayName = 'MarketChart';

export default MarketChart;
