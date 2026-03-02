import React, { useEffect, useRef, useState, useImperativeHandle, forwardRef, useCallback } from 'react';
import { createChart, ColorType, CrosshairMode, PriceScaleMode, LineType } from 'lightweight-charts';
import html2canvas from 'html2canvas';
import './MarketChart.css';
import { fetchStockData } from '../utils/api';
import { calculateMA, calculateRSI, updateRSIIncremental } from '../utils/chartHelpers';
import {
  CHART_BG, CHART_TEXT, CHART_GRID, getChartTheme,
  INTERVALS, PRIMARY_INTERVAL_KEYS, INITIAL_LOAD_DAYS, SCROLL_CHUNK_DAYS,
  SCROLL_LOAD_THRESHOLD, RANGE_CHANGE_DEBOUNCE_MS,
  MA_CONFIGS, DEFAULT_ENABLED_MA, RSI_PERIODS, BARS_PER_DAY, AUTO_FIT_BARS,
  OVERLAY_COLORS, OVERLAY_LABELS,
  EXTENDED_HOURS_INTERVALS, isExtendedHours, computeExtendedHoursRegions,
} from '../utils/chartConstants';
import { ExtendedHoursBgPrimitive } from '../utils/extendedHoursBg';
import { useTheme } from '@/contexts/ThemeContext';
import CrosshairTooltip from './CrosshairTooltip';
import TradingViewWidget from './TradingViewWidget';
import { useChartAnnotations } from '../hooks/useChartAnnotations';
import { useChartOverlays } from '../hooks/useChartOverlays';
import { SlidersHorizontal, Settings2, Maximize2, Minimize2, ChevronDown, Plus, Minus, RotateCcw } from 'lucide-react';

// --- localStorage persistence helpers ---
const STORAGE_PREFIX = 'market-chart:';

function loadPref(key, fallback) {
  try {
    const raw = localStorage.getItem(STORAGE_PREFIX + key);
    return raw !== null ? JSON.parse(raw) : fallback;
  } catch { return fallback; }
}

function savePref(key, value) {
  try { localStorage.setItem(STORAGE_PREFIX + key, JSON.stringify(value)); } catch { /* noop */ }
}

function useClickOutside(ref, onClose) {
  useEffect(() => {
    if (!onClose) return;
    const handler = (e) => {
      if (ref.current && !ref.current.contains(e.target)) onClose();
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [ref, onClose]);
}

const MarketChart = React.memo(forwardRef(({
  symbol,
  interval = '1day',
  onIntervalChange,
  onCapture,
  onStockMeta,
  quoteData,
  earningsData,
  overlayData,
  stockMeta,
  liveTick,
  wsStatus,
}, ref) => {
  const { theme } = useTheme();
  const ct = getChartTheme(theme);
  const chartContainerRef = useRef();
  const rsiChartContainerRef = useRef();
  const lightWrapperRef = useRef();
  const chartRef = useRef();
  const rsiChartRef = useRef();
  const candlestickSeriesRef = useRef();
  const rsiSeriesRef = useRef();
  const volumeSeriesRef = useRef(null);
  const maSeriesRefs = useRef({});
  const baselineSeriesRef = useRef(null);
  const extHoursBgRef = useRef(null);

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [lastUpdateTime, setLastUpdateTime] = useState(null);
  const [rsiValue, setRsiValue] = useState(null);

  // MA / RSI config state (persisted)
  const [enabledMaPeriods, setEnabledMaPeriods] = useState(() => loadPref('maPeriods', DEFAULT_ENABLED_MA));
  const [rsiPeriod, setRsiPeriod] = useState(() => loadPref('rsiPeriod', 14));
  const [maValues, setMaValues] = useState({});

  // Chart mode: 'custom' (our lightweight-charts) or 'tradingview' (full TV widget) (persisted)
  const [chartMode, setChartMode] = useState(() => loadPref('chartMode', 'custom'));

  // Chart feature toggles (persisted)
  const [priceScaleMode, setPriceScaleMode] = useState(() => loadPref('priceScaleMode', PriceScaleMode.Normal));
  const [magnetMode, setMagnetMode] = useState(() => loadPref('magnetMode', false));
  const [showBaseline, setShowBaseline] = useState(false);
  const [annotationsVisible, setAnnotationsVisible] = useState(() => loadPref('annotationsVisible', false));
  const [overlayVisibility, setOverlayVisibility] = useState(
    () => loadPref('overlayVisibility', { earnings: false, grades: false, priceTargets: false }),
  );

  // Toolbar dropdown state
  const [indicatorsOpen, setIndicatorsOpen] = useState(false);
  const [toolsOpen, setToolsOpen] = useState(false);
  const [intervalsOpen, setIntervalsOpen] = useState(false);
  const indicatorsDropdownRef = useRef(null);
  const toolsDropdownRef = useRef(null);
  const intervalsDropdownRef = useRef(null);
  useClickOutside(indicatorsDropdownRef, indicatorsOpen ? () => setIndicatorsOpen(false) : null);
  useClickOutside(toolsDropdownRef, toolsOpen ? () => setToolsOpen(false) : null);
  useClickOutside(intervalsDropdownRef, intervalsOpen ? () => setIntervalsOpen(false) : null);

  // Crosshair tooltip state
  const [tooltipState, setTooltipState] = useState({ visible: false, x: 0, y: 0, data: null });

  // Refs for stable callbacks (avoid stale closures)
  const enabledMaPeriodsRef = useRef(DEFAULT_ENABLED_MA);
  const rsiPeriodRef = useRef(14);

  // Track current interval for use inside stable callbacks (avoids stale closures)
  const intervalRef = useRef(interval);

  // Keep refs synced with state
  useEffect(() => { enabledMaPeriodsRef.current = enabledMaPeriods; }, [enabledMaPeriods]);
  useEffect(() => { rsiPeriodRef.current = rsiPeriod; }, [rsiPeriod]);
  useEffect(() => { intervalRef.current = interval; }, [interval]);

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
  const rsiSmoothingRef = useRef(null);          // Wilder state { avgGain, avgLoss, lastClose, period }
  const prevBarSmoothingRef = useRef(null);       // State *before* current bar (for same-bar re-updates)
  const pendingRsiDataRef = useRef(null);         // Buffered { rsiData, smoothingState } when series isn't ready
  const rsiDataMapRef = useRef(new Map());        // time→rsiValue for O(1) crosshair lookup
  const isSyncingTimeScaleRef = useRef(false);    // Guard for bidirectional time-scale sync

  // Refs for scroll-based loading
  const allDataRef = useRef([]);
  const oldestDateRef = useRef(null);
  const fetchingRef = useRef(false);
  const rangeChangeTimerRef = useRef(null);
  const rangeUnsubRef = useRef(null);

  // Chart data state for hooks
  const [chartDataForHooks, setChartDataForHooks] = useState([]);

  // --- Price lines via hook ---
  const priceTargetsForAnnotations = overlayVisibility.priceTargets ? overlayData?.priceTargets : null;
  useChartAnnotations(candlestickSeriesRef, stockMeta, quoteData, priceTargetsForAnnotations, annotationsVisible, symbol);

  // --- Series markers via hook ---
  useChartOverlays(candlestickSeriesRef, chartDataForHooks, earningsData, overlayData, overlayVisibility, symbol);

  // --- Live tick updates from WS (1s and 1min intervals, custom/Light mode only) ---
  useEffect(() => {
    if (!liveTick || !candlestickSeriesRef.current) return;
    // Only apply live updates for 1s/1min interval in custom (Light) mode
    if ((interval !== '1s' && interval !== '1min') || chartMode !== 'custom') return;

    const { time, open, high, low, close, volume } = liveTick;
    if (!time || close == null) return;

    // Clear the "waiting for live data" hint once first tick arrives
    if (interval === '1s' && error) setError(null);

    // Update candlestick series in-place (same time = update, newer = append)
    candlestickSeriesRef.current.update({ time, open, high, low, close });

    const ext = EXTENDED_HOURS_INTERVALS.has(interval) && isExtendedHours(time);
    const up = close >= open;
    const ct = ctRef.current;
    if (volumeSeriesRef.current) {
      volumeSeriesRef.current.update({
        time,
        value: volume,
        color: ext
          ? (up ? ct.extVolumeUp : ct.extVolumeDown)
          : (up ? ct.upColor : ct.downColor),
      });
    }

    // Keep allDataRef in sync
    const data = allDataRef.current;
    const isSameBar = data.length > 0 && data[data.length - 1].time === time;
    if (isSameBar) {
      data[data.length - 1] = { time, open, high, low, close, volume };
    } else if (!data.length || time > data[data.length - 1].time) {
      data.push({ time, open, high, low, close, volume });
    }

    // Incremental RSI update (Bug 2 fix)
    if (rsiSmoothingRef.current && rsiSeriesRef.current) {
      if (isSameBar) {
        // Same bar updated — recalculate from state *before* this bar was first applied
        if (prevBarSmoothingRef.current) {
          const { value, state } = updateRSIIncremental(prevBarSmoothingRef.current, close);
          rsiSmoothingRef.current = state;
          rsiSeriesRef.current.update({ time, value });
          rsiDataMapRef.current.set(time, value);
          setRsiValue(value.toFixed(0));
        }
      } else {
        // New bar — advance smoothing state
        prevBarSmoothingRef.current = rsiSmoothingRef.current;
        const { value, state } = updateRSIIncremental(rsiSmoothingRef.current, close);
        rsiSmoothingRef.current = state;
        rsiSeriesRef.current.update({ time, value });
        rsiDataMapRef.current.set(time, value);
        setRsiValue(value.toFixed(0));
      }
    }
  }, [liveTick, interval, chartMode]);

  // Temporarily reveal the hidden Light chart for capture, then restore.
  // Since it's behind the TV widget (z-index: -1), no visual flash occurs.
  const revealForCapture = useCallback(async (fn) => {
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
          const canvas = await html2canvas(chartContainerRef.current, {
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
      // Capture the Light chart (main + RSI) for LLM context.
      // Temporarily reveal the hidden wrapper so html2canvas can render it.
      const container = chartContainerRef.current?.parentElement; // .charts-container
      if (!container) return null;
      return revealForCapture(async () => {
        try {
          const canvas = await html2canvas(container, {
            backgroundColor: ct.bg,
            scale: 1,
            logging: false,
          });
          return canvas.toDataURL('image/jpeg', 0.85);
        } catch (err) {
          console.error('Chart capture failed:', err);
          return null;
        }
      });
    },
    getChartMetadata: () => {
      const data = allDataRef.current;
      if (!data || data.length === 0) return null;

      const firstTime = data[0].time;
      const lastTime = data[data.length - 1].time;
      const formatDate = (ts) => new Date(ts * 1000).toISOString().split('T')[0];

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

  // --- Update series data helper (used by both initial load and scroll load) ---
  const updateSeriesData = useCallback((data) => {
    const ct = ctRef.current;
    const applyExt = EXTENDED_HOURS_INTERVALS.has(intervalRef.current);

    // Candlestick
    if (candlestickSeriesRef.current) {
      candlestickSeriesRef.current.setData(data);
    }

    // Volume histogram — dim extended-hours bars
    if (volumeSeriesRef.current) {
      volumeSeriesRef.current.setData(data.map((d, i) => {
        const up = i > 0 && d.close >= data[i - 1].close;
        const ext = applyExt && isExtendedHours(d.time);
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
        extHoursBgRef.current.setRegions(computeExtendedHoursRegions(data));
        extHoursBgRef.current.setColors({ pre: ct.extBgPre, post: ct.extBgPost });
      } else {
        extHoursBgRef.current.setRegions([]);
      }
    }

    // All MAs — compute all enabled, clear disabled
    const enabled = enabledMaPeriodsRef.current;
    const newMaValues = {};
    MA_CONFIGS.forEach(({ period }) => {
      const series = maSeriesRefs.current[period];
      if (!series) return;
      if (enabled.includes(period)) {
        const maData = calculateMA(data, period);
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
    const { data: rsiData, state: rsiState } = calculateRSI(data, currentRsiPeriod);

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

  // --- Scroll-based lazy loading ---
  const handleScrollLoadMore = useCallback(async () => {
    if (fetchingRef.current || !oldestDateRef.current) return;
    fetchingRef.current = true;

    try {
      const oldest = new Date(oldestDateRef.current * 1000);
      const toDate = new Date(oldest);
      toDate.setDate(toDate.getDate() - 1);
      const fromDate = new Date(toDate);
      fromDate.setDate(fromDate.getDate() - SCROLL_CHUNK_DAYS[interval]);

      const fromStr = fromDate.toISOString().split('T')[0];
      const toStr = toDate.toISOString().split('T')[0];

      const result = await fetchStockData(symbol, interval, fromStr, toStr);
      const newData = result?.data;

      if (newData && Array.isArray(newData) && newData.length > 0) {
        const existingMap = new Map(allDataRef.current.map((d) => [d.time, d]));
        newData.forEach((d) => {
          if (!existingMap.has(d.time)) existingMap.set(d.time, d);
        });
        const merged = Array.from(existingMap.values()).sort((a, b) => a.time - b.time);
        allDataRef.current = merged;
        oldestDateRef.current = merged[0].time;
        updateSeriesData(merged);
      }
    } catch (err) {
      console.warn('Scroll-load fetch failed:', err);
    } finally {
      fetchingRef.current = false;
    }
  }, [symbol, interval, updateSeriesData]);

  // --- Backfill older data when a newly-enabled MA needs more bars ---
  const backfillForMaPeriod = useCallback(async (period) => {
    const currentLen = allDataRef.current.length;
    if (currentLen >= period || fetchingRef.current || !oldestDateRef.current) return;

    fetchingRef.current = true;
    try {
      const deficit = period - currentLen;
      const extraDays = Math.ceil((deficit / (BARS_PER_DAY[interval] || 1)) * 1.5);

      const oldest = new Date(oldestDateRef.current * 1000);
      const toDate = new Date(oldest);
      toDate.setDate(toDate.getDate() - 1);
      const fromDate = new Date(toDate);
      fromDate.setDate(fromDate.getDate() - extraDays);

      const fromStr = fromDate.toISOString().split('T')[0];
      const toStr = toDate.toISOString().split('T')[0];

      const result = await fetchStockData(symbol, interval, fromStr, toStr);
      const newData = result?.data;

      if (newData && Array.isArray(newData) && newData.length > 0) {
        const existingMap = new Map(allDataRef.current.map((d) => [d.time, d]));
        newData.forEach((d) => {
          if (!existingMap.has(d.time)) existingMap.set(d.time, d);
        });
        const merged = Array.from(existingMap.values()).sort((a, b) => a.time - b.time);
        allDataRef.current = merged;
        oldestDateRef.current = merged[0].time;
        updateSeriesData(merged);
      }
    } catch (err) {
      console.warn('MA backfill fetch failed:', err);
    } finally {
      fetchingRef.current = false;
    }
  }, [symbol, interval, updateSeriesData]);

  // --- Toggle handlers ---
  const handleToggleMa = useCallback((period) => {
    const isCurrentlyEnabled = enabledMaPeriodsRef.current.includes(period);
    if (!isCurrentlyEnabled && allDataRef.current.length < period) {
      backfillForMaPeriod(period);
    }
    setEnabledMaPeriods(prev =>
      prev.includes(period) ? prev.filter(p => p !== period) : [...prev, period]
    );
  }, [backfillForMaPeriod]);

  const handleChangeRsiPeriod = useCallback((period) => {
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
        handleScroll: {
          mouseWheel: true,
          pressedMouseMove: true,
          horzTouchDrag: true,
          vertTouchDrag: false,
        },
      },
    });
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
        lineWidth: 1.5,
        lineType: LineType.Curved,
        title: '',
        lastValueVisible: false,
        priceLineVisible: false,
      });
    });

    // Subscribe to crosshair move for tooltip
    chart.subscribeCrosshairMove((param) => {
      if (!param.time || !param.point) {
        setTooltipState((prev) => prev.visible ? { visible: false, x: 0, y: 0, data: null } : prev);
        return;
      }
      const candleData = param.seriesData.get(candlestickSeriesRef.current);
      if (!candleData) {
        setTooltipState((prev) => prev.visible ? { visible: false, x: 0, y: 0, data: null } : prev);
        return;
      }

      // Gather MA values from crosshair
      const maVals = {};
      const enabled = enabledMaPeriodsRef.current;
      MA_CONFIGS.forEach(({ period }) => {
        if (!enabled.includes(period)) return;
        const s = maSeriesRefs.current[period];
        if (!s) return;
        const val = param.seriesData.get(s);
        if (val && val.value != null) maVals[period] = val.value;
      });

      // Gather RSI value via lookup map (Bug 3 fix — rsiSeries is on a separate chart instance)
      const candleTime = candleData.time ?? param.time;
      let rsiVal = rsiDataMapRef.current.get(candleTime) ?? null;

      setTooltipState({
        visible: true,
        x: param.point.x,
        y: param.point.y,
        data: {
          time: candleData.time ?? param.time,
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
      });
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

      // Bidirectional time-scale sync between main chart and RSI chart (Bug 4 fix)
      const mainTs = chart.timeScale();
      const rsiTs = rsiChart.timeScale();
      mainTs.subscribeVisibleLogicalRangeChange((range) => {
        if (isSyncingTimeScaleRef.current || !range) return;
        isSyncingTimeScaleRef.current = true;
        rsiTs.setVisibleLogicalRange(range);
        isSyncingTimeScaleRef.current = false;
      });
      rsiTs.subscribeVisibleLogicalRangeChange((range) => {
        if (isSyncingTimeScaleRef.current || !range) return;
        isSyncingTimeScaleRef.current = true;
        mainTs.setVisibleLogicalRange(range);
        isSyncingTimeScaleRef.current = false;
      });
    }, 100);

    return () => {
      clearTimeout(rsiTimeout);

      // Unsubscribe scroll-load listener
      if (rangeUnsubRef.current) {
        rangeUnsubRef.current();
        rangeUnsubRef.current = null;
      }
      clearTimeout(rangeChangeTimerRef.current);

      extHoursBgRef.current = null;
      candlestickSeriesRef.current = null;
      volumeSeriesRef.current = null;
      baselineSeriesRef.current = null;
      Object.keys(maSeriesRefs.current).forEach(k => { maSeriesRefs.current[k] = null; });
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
        const applyExt = EXTENDED_HOURS_INTERVALS.has(intervalRef.current);
        volumeSeriesRef.current.setData(data.map((d, i) => {
          const up = i > 0 && d.close >= data[i - 1].close;
          const ext = applyExt && isExtendedHours(d.time);
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

      const prevClose = quoteData?.previousClose || quoteData?.open;
      const basePrice = prevClose || (allDataRef.current.length > 0 ? allDataRef.current[0].open : 0);

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

    // Unsubscribe previous scroll listener
    if (rangeUnsubRef.current) {
      rangeUnsubRef.current();
      rangeUnsubRef.current = null;
    }

    // Reset baseline on symbol/interval change
    if (showBaseline) setShowBaseline(false);

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

    const loadData = async () => {
      setLoading(true);
      setError(null);

      try {
        const loadDays = INITIAL_LOAD_DAYS[interval];

        let fromDate, toDate;
        if (loadDays > 0) {
          const now = new Date();
          toDate = now.toISOString().split('T')[0];
          if (interval === '1s') {
            // For 1s: load only today — avoids fetching 50k+ bars from
            // previous sessions. Users can scroll back for more history.
            fromDate = toDate;
          } else {
            const maxMaPeriod = Math.max(...enabledMaPeriodsRef.current, 0);
            const overheadDays = Math.ceil((maxMaPeriod / (BARS_PER_DAY[interval] || 1)) * 1.5);
            const from = new Date(now);
            from.setDate(from.getDate() - loadDays - overheadDays);
            fromDate = from.toISOString().split('T')[0];
          }
        }

        const result = await fetchStockData(symbol, interval, fromDate, toDate, { signal: abortController.signal });

        if (abortController.signal.aborted) return;

        const data = result?.data || [];

        if (Array.isArray(data) && data.length > 0) {
          allDataRef.current = data;
          oldestDateRef.current = data[0].time;

          updateSeriesData(data);

          if (chartRef.current) {
            const ts = chartRef.current.timeScale();
            const idealBars = AUTO_FIT_BARS[interval];
            if (idealBars && interval !== '1day' && data.length > idealBars) {
              // For intraday intervals, show only the most recent N bars so that
              // overnight/weekend gaps don't dominate the view at wider zoom.
              ts.setVisibleLogicalRange({ from: data.length - idealBars, to: data.length });
            } else {
              ts.fitContent();
            }
          }
          setLastUpdateTime(new Date());
          setError(null);


          // Subscribe to visible range changes for scroll-based loading (debounced)
          if (chartRef.current) {
            const unsubscribe = chartRef.current.timeScale().subscribeVisibleLogicalRangeChange((range) => {
              clearTimeout(rangeChangeTimerRef.current);
              rangeChangeTimerRef.current = setTimeout(() => {
                if (range && range.from <= SCROLL_LOAD_THRESHOLD) {
                  handleScrollLoadMore();
                }
              }, RANGE_CHANGE_DEBOUNCE_MS);
            });
            rangeUnsubRef.current = unsubscribe;
          }

          // For 1s: prefetch previous session so scroll-left is seamless
          if (interval === '1s') {
            handleScrollLoadMore();
          }
        } else {
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
      } catch (err) {
        if (abortController.signal.aborted) return;
        console.error('Failed to load stock data:', err);
        clearChartSeries();
        setError(err?.message || 'Failed to load data');
      } finally {
        if (!abortController.signal.aborted) {
          setLoading(false);
        }
      }
    };

    loadData();

    return () => {
      abortController.abort();
    };
  }, [symbol, interval, onStockMeta, updateSeriesData, handleScrollLoadMore]);

  // --- REST polling fallback for 1s interval when WS is unavailable ---
  useEffect(() => {
    if (interval !== '1s' || chartMode !== 'custom') return;
    if (wsStatus === 'connected') return; // WS is live — no polling needed

    let timer = null;
    let aborted = false;

    const poll = async () => {
      if (aborted) return;
      try {
        const now = new Date();
        const toDate = now.toISOString().split('T')[0];

        // Delta-based: fetch only from last known bar's time onward
        const lastBar = allDataRef.current?.[allDataRef.current.length - 1];
        const fromDate = lastBar
          ? new Date(lastBar.time * 1000).toISOString().split('T')[0]
          : (() => { const d = new Date(now); d.setDate(d.getDate() - 3); return d.toISOString().split('T')[0]; })();

        const result = await fetchStockData(symbol, '1s', fromDate, toDate);
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

          if (chartRef.current) {
            const idealBars = AUTO_FIT_BARS['1s'];
            if (idealBars && allDataRef.current.length > idealBars) {
              chartRef.current.timeScale().setVisibleLogicalRange({
                from: allDataRef.current.length - idealBars,
                to: allDataRef.current.length,
              });
            }
          }
          setLastUpdateTime(new Date());
          setError(null);
        }
      } catch (err) {
        if (!aborted) console.debug('1s REST poll failed:', err);
      }
    };

    // Start polling every 5 seconds
    timer = setInterval(poll, 5000);

    return () => {
      aborted = true;
      if (timer) clearInterval(timer);
    };
  }, [interval, wsStatus, symbol, chartMode, updateSeriesData]);

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

  // --- Tool button handlers ---
  const handleZoomIn = useCallback(() => {
    if (!chartRef.current) return;
    const ts = chartRef.current.timeScale();
    const range = ts.getVisibleLogicalRange();
    if (!range) return;
    const center = (range.from + range.to) / 2;
    const halfSpan = (range.to - range.from) / 4; // halve the range
    ts.setVisibleLogicalRange({ from: center - halfSpan, to: center + halfSpan });
  }, []);

  const handleZoomOut = useCallback(() => {
    if (!chartRef.current) return;
    const ts = chartRef.current.timeScale();
    const range = ts.getVisibleLogicalRange();
    if (!range) return;
    const center = (range.from + range.to) / 2;
    const halfSpan = (range.to - range.from); // double the range
    ts.setVisibleLogicalRange({ from: center - halfSpan, to: center + halfSpan });
  }, []);

  const handleScrollToRealTime = useCallback(() => {
    if (!chartRef.current) return;
    const ts = chartRef.current.timeScale();
    const dataLen = allDataRef.current.length;
    if (dataLen === 0) { ts.scrollToRealTime(); return; }
    // Show ideal bar count anchored to the latest bar
    const idealBars = AUTO_FIT_BARS[intervalRef.current] || 180;
    const barsToShow = Math.min(idealBars, dataLen);
    ts.setVisibleLogicalRange({ from: dataLen - barsToShow, to: dataLen });
  }, []);

  const handleAutoNormalize = useCallback(() => {
    if (!chartRef.current) return;
    const ts = chartRef.current.timeScale();
    const dataLen = allDataRef.current.length;
    if (dataLen === 0) return;
    const idealBars = AUTO_FIT_BARS[intervalRef.current] || 180;
    const half = Math.min(idealBars, dataLen) / 2;
    // Center on the midpoint of the currently visible range
    const range = ts.getVisibleLogicalRange();
    const center = range ? (range.from + range.to) / 2 : dataLen - half;
    // Clamp so we don't scroll past data boundaries
    const from = Math.max(0, center - half);
    const to = Math.min(dataLen, from + half * 2);
    ts.setVisibleLogicalRange({ from, to });
  }, []);

  const handleFitAll = useCallback(() => {
    if (chartRef.current) chartRef.current.timeScale().fitContent();
  }, []);

  const handleToggleAnnotations = useCallback(() => {
    setAnnotationsVisible((prev) => !prev);
  }, []);

  const handleToggleOverlay = useCallback((key) => {
    setOverlayVisibility((prev) => ({ ...prev, [key]: !prev[key] }));
  }, []);

  const handleTogglePriceScale = useCallback((mode) => {
    setPriceScaleMode((prev) => prev === mode ? PriceScaleMode.Normal : mode);
  }, []);

  const isTV = chartMode === 'tradingview';

  return (
    <div className="market-chart-container">
      {/* ---- Toolbar: intervals, indicator dropdown, values, tools dropdown, mode switcher ---- */}
      <div className="chart-tools">
        <div className="chart-tools-left">
          <div className="interval-selector">
            {INTERVALS.filter(({ key }) => PRIMARY_INTERVAL_KEYS.has(key)).map(({ key, label }) => {
              const wsUnavailable = key === '1s' && wsStatus === 'disabled';
              return (
              <button
                key={key}
                type="button"
                className={`interval-btn${interval === key ? ' interval-btn-active' : ''}${wsUnavailable ? ' interval-btn-disabled' : ''}`}
                onClick={() => { if (wsUnavailable) return; onIntervalChange?.(key); setIntervalsOpen(false); setIndicatorsOpen(false); setToolsOpen(false); }}
                title={wsUnavailable ? '1s interval is not supported by the current data source' : undefined}
              >
                {label}
              </button>
              );
            })}
            {/* "More" dropdown for secondary intervals */}
            <div className="toolbar-dropdown" ref={intervalsDropdownRef} style={{ display: 'inline-flex' }}>
              <button
                type="button"
                className={`interval-btn${(!PRIMARY_INTERVAL_KEYS.has(interval) || intervalsOpen) ? ' interval-btn-active' : ''}`}
                onClick={() => { setIntervalsOpen((v) => !v); setIndicatorsOpen(false); setToolsOpen(false); }}
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
              {/* Indicators dropdown: MA, RSI, Overlay toggles */}
              <div className="toolbar-dropdown" ref={indicatorsDropdownRef}>
                <button
                  type="button"
                  className={`chart-tool-btn${indicatorsOpen ? ' chart-tool-btn-active' : ''}`}
                  onClick={() => { setIndicatorsOpen((v) => !v); setToolsOpen(false); }}
                  title="Indicators"
                >
                  <SlidersHorizontal size={14} />
                </button>
                {indicatorsOpen && (
                  <div className="toolbar-dropdown-panel">
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
                  </div>
                )}
              </div>
              {/* Indicator values — always visible */}
              <div className="chart-indicators">
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
              {/* Tools dropdown: scale, magnet, baseline, annotations, scroll-to-latest */}
              <div className="toolbar-dropdown" ref={toolsDropdownRef}>
                <button
                  type="button"
                  className={`chart-tool-btn${toolsOpen ? ' chart-tool-btn-active' : ''}`}
                  onClick={() => { setToolsOpen((v) => !v); setIndicatorsOpen(false); }}
                  title="Chart Tools"
                >
                  <Settings2 size={14} />
                </button>
                {toolsOpen && (
                  <div className="toolbar-dropdown-panel toolbar-dropdown-panel--right">
                    <div className="dropdown-tool-grid">
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
                    </div>
                  </div>
                )}
              </div>
              {/* Zoom, fit, and navigation — always visible */}
              <div className="chart-tool-buttons">
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
              </div>
            </>
          )}
          <div className="chart-mode-switcher">
            <div className="interval-selector">
              <button
                type="button"
                className={`interval-btn${!isTV ? ' interval-btn-active' : ''}`}
                onClick={() => { setChartMode('custom'); setIndicatorsOpen(false); setToolsOpen(false); }}
              >
                Light
              </button>
              <button
                type="button"
                className={`interval-btn${isTV ? ' interval-btn-active' : ''}`}
                onClick={() => { setChartMode('tradingview'); setIndicatorsOpen(false); setToolsOpen(false); }}
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
