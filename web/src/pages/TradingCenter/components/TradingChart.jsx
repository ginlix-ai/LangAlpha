import React, { useEffect, useRef, useState, useImperativeHandle, forwardRef, useCallback } from 'react';
import { createChart, ColorType, CrosshairMode, PriceScaleMode, LineType } from 'lightweight-charts';
import html2canvas from 'html2canvas';
import './TradingChart.css';
import { fetchStockData } from '../utils/api';
import { calculateMA, calculateRSI } from '../utils/chartHelpers';
import {
  CHART_BG, CHART_TEXT, CHART_GRID,
  INTERVALS, INITIAL_LOAD_DAYS, SCROLL_CHUNK_DAYS,
  SCROLL_LOAD_THRESHOLD, RANGE_CHANGE_DEBOUNCE_MS,
  MA_CONFIGS, DEFAULT_ENABLED_MA, RSI_PERIODS, BARS_PER_DAY,
  OVERLAY_COLORS, OVERLAY_LABELS,
} from '../utils/chartConstants';
import CrosshairTooltip from './CrosshairTooltip';
import { useChartAnnotations } from '../hooks/useChartAnnotations';
import { useChartOverlays } from '../hooks/useChartOverlays';

const TradingChart = React.memo(forwardRef(({
  symbol,
  interval = '1day',
  onIntervalChange,
  onCapture,
  onStockMeta,
  quoteData,
  earningsData,
  overlayData,
  stockMeta,
}, ref) => {
  const chartContainerRef = useRef();
  const rsiChartContainerRef = useRef();
  const chartRef = useRef();
  const rsiChartRef = useRef();
  const candlestickSeriesRef = useRef();
  const rsiSeriesRef = useRef();
  const volumeSeriesRef = useRef(null);
  const maSeriesRefs = useRef({});
  const baselineSeriesRef = useRef(null);

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [lastUpdateTime, setLastUpdateTime] = useState(null);
  const [rsiValue, setRsiValue] = useState(null);

  // MA / RSI config state
  const [enabledMaPeriods, setEnabledMaPeriods] = useState(DEFAULT_ENABLED_MA);
  const [rsiPeriod, setRsiPeriod] = useState(14);
  const [maValues, setMaValues] = useState({});

  // Chart feature toggles
  const [priceScaleMode, setPriceScaleMode] = useState(PriceScaleMode.Normal);
  const [magnetMode, setMagnetMode] = useState(false);
  const [showBaseline, setShowBaseline] = useState(false);
  const [annotationsVisible, setAnnotationsVisible] = useState(false);
  const [overlayVisibility, setOverlayVisibility] = useState({
    earnings: false,
    grades: false,
    priceTargets: false,
  });

  // Crosshair tooltip state
  const [tooltipState, setTooltipState] = useState({ visible: false, x: 0, y: 0, data: null });

  // Refs for stable callbacks (avoid stale closures)
  const enabledMaPeriodsRef = useRef(DEFAULT_ENABLED_MA);
  const rsiPeriodRef = useRef(14);

  // Keep refs synced with state
  useEffect(() => { enabledMaPeriodsRef.current = enabledMaPeriods; }, [enabledMaPeriods]);
  useEffect(() => { rsiPeriodRef.current = rsiPeriod; }, [rsiPeriod]);

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
      // Fallback to html2canvas
      if (!chartContainerRef.current) return null;
      try {
        const canvas = await html2canvas(chartContainerRef.current, {
          backgroundColor: CHART_BG,
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
    },
    captureChartAsDataUrl: async () => {
      // Keep html2canvas for combined main+RSI for LLM context
      const container = chartContainerRef.current?.parentElement; // .charts-container
      if (!container) return null;
      try {
        const canvas = await html2canvas(container, {
          backgroundColor: CHART_BG,
          scale: 1,
          logging: false,
        });
        return canvas.toDataURL('image/jpeg', 0.85);
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
      const formatDate = (ts) => new Date(ts * 1000).toISOString().split('T')[0];

      const enabledMAs = enabledMaPeriodsRef.current;
      const maInfo = enabledMAs
        .filter((p) => maValues[p] != null)
        .map((p) => `MA${p}: ${maValues[p]}`);

      const lastCandle = data[data.length - 1];

      return {
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
    // Candlestick
    if (candlestickSeriesRef.current) {
      candlestickSeriesRef.current.setData(data);
    }

    // Volume histogram
    if (volumeSeriesRef.current) {
      volumeSeriesRef.current.setData(data.map((d, i) => ({
        time: d.time,
        value: d.volume || 0,
        color: i > 0 && d.close >= data[i - 1].close
          ? 'rgba(16,185,129,0.3)' : 'rgba(239,68,68,0.3)',
      })));
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

    // RSI
    const currentRsiPeriod = rsiPeriodRef.current;
    const rsiData = calculateRSI(data, currentRsiPeriod);
    if (rsiData.length > 0 && rsiSeriesRef.current) {
      rsiSeriesRef.current.setData(rsiData);
      const lastRsi = rsiData[rsiData.length - 1]?.value;
      if (lastRsi != null) setRsiValue(lastRsi.toFixed(0));
      if (rsiChartRef.current) rsiChartRef.current.timeScale().fitContent();
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

    const chart = createChart(chartContainerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: CHART_BG },
        textColor: CHART_TEXT,
      },
      autoSize: true,
      grid: {
        vertLines: { color: CHART_GRID },
        horzLines: { color: CHART_GRID },
      },
      watermark: {
        visible: true,
        text: symbol,
        fontSize: 48,
        color: 'rgba(139,143,163,0.08)',
        horzAlign: 'center',
        vertAlign: 'center',
      },
      crosshair: { mode: CrosshairMode.Normal },
      rightPriceScale: {
        borderColor: CHART_GRID,
        scaleMargins: { top: 0.1, bottom: 0.2 },
      },
      timeScale: {
        borderColor: CHART_GRID,
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
      upColor: '#10b981',
      downColor: '#ef4444',
      borderVisible: false,
      wickUpColor: '#10b981',
      wickDownColor: '#ef4444',
    });

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

      // Gather RSI value
      let rsiVal = null;
      if (rsiSeriesRef.current) {
        const rsiData = param.seriesData.get(rsiSeriesRef.current);
        if (rsiData && rsiData.value != null) rsiVal = rsiData.value;
      }

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
      const rsiChart = createChart(rsiChartContainerRef.current, {
        layout: {
          background: { type: ColorType.Solid, color: CHART_BG },
          textColor: CHART_TEXT,
        },
        autoSize: true,
        grid: {
          vertLines: { color: CHART_GRID },
          horzLines: { color: CHART_GRID },
        },
        rightPriceScale: {
          borderColor: CHART_GRID,
          visible: true,
          scaleMargins: { top: 0.1, bottom: 0.1 },
        },
        timeScale: {
          borderColor: CHART_GRID,
          timeVisible: true,
          secondsVisible: false,
        },
      });
      rsiChartRef.current = rsiChart;
      // RSI as area series with gradient
      rsiSeriesRef.current = rsiChart.addAreaSeries({
        lineColor: '#667eea',
        topColor: 'rgba(102,126,234,0.3)',
        bottomColor: 'rgba(102,126,234,0.02)',
        lineWidth: 2,
        priceFormat: { type: 'price', precision: 0, minMove: 1 },
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
          color: 'rgba(139,143,163,0.08)',
          horzAlign: 'center',
          vertAlign: 'center',
        },
      });
    }
  }, [symbol]);

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
          topLineColor: '#10b981',
          topFillColor1: 'rgba(16,185,129,0.2)',
          topFillColor2: 'rgba(16,185,129,0.02)',
          bottomLineColor: '#ef4444',
          bottomFillColor1: 'rgba(239,68,68,0.02)',
          bottomFillColor2: 'rgba(239,68,68,0.2)',
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
    };

    const loadData = async () => {
      setLoading(true);
      setError(null);

      try {
        const loadDays = INITIAL_LOAD_DAYS[interval];
        let fromDate, toDate;
        if (loadDays > 0) {
          const maxMaPeriod = Math.max(...enabledMaPeriodsRef.current, 0);
          const overheadDays = Math.ceil((maxMaPeriod / (BARS_PER_DAY[interval] || 1)) * 1.5);
          const now = new Date();
          toDate = now.toISOString().split('T')[0];
          const from = new Date(now);
          from.setDate(from.getDate() - loadDays - overheadDays);
          fromDate = from.toISOString().split('T')[0];
        }

        const result = await fetchStockData(symbol, interval, fromDate, toDate, { signal: abortController.signal });

        if (abortController.signal.aborted) return;

        const data = result?.data || [];

        if (Array.isArray(data) && data.length > 0) {
          allDataRef.current = data;
          oldestDateRef.current = data[0].time;

          updateSeriesData(data);

          if (chartRef.current) {
            chartRef.current.timeScale().fitContent();
          }
          setLastUpdateTime(new Date());
          setError(null);

          if (typeof onStockMeta === 'function' && result?.fiftyTwoWeekHigh != null && result?.fiftyTwoWeekLow != null) {
            onStockMeta({ fiftyTwoWeekHigh: result.fiftyTwoWeekHigh, fiftyTwoWeekLow: result.fiftyTwoWeekLow });
          }

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
        } else {
          clearChartSeries();
          const isIntraday = interval !== '1day';
          const fallbackMsg = isIntraday
            ? 'Intraday data not available — market may be closed. Try the 1D interval.'
            : 'Stock data not found';
          setError(result?.error || fallbackMsg);
          if (typeof onStockMeta === 'function') onStockMeta(null);
        }
      } catch (err) {
        if (abortController.signal.aborted) return;
        console.error('Failed to load stock data:', err);
        clearChartSeries();
        setError('Failed to load data. Please check FMP API configuration.');
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

  // --- Effect 3: TimeScale options per interval ---
  useEffect(() => {
    const isIntraday = interval !== '1day';
    const showSeconds = interval === '1min';
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
    if (chartRef.current) {
      chartRef.current.timeScale().scrollToRealTime();
    }
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

  return (
    <div className="trading-chart-container">
      <div className="chart-header">
        <div className="chart-info">
          <span className="chart-label">{interval === '1day' ? 'Daily' : INTERVALS.find(i => i.key === interval)?.label} Summary & Indicators</span>
          {lastUpdateTime && (
            <span className="update-time">
              Last update: {lastUpdateTime.toLocaleTimeString()}
            </span>
          )}
        </div>
        <div className="chart-indicators">
          {MA_CONFIGS.filter(({ period }) => enabledMaPeriods.includes(period)).map(({ period, color, label }) => (
            <span className="indicator-item" key={period}>
              <span className="indicator-color" style={{ backgroundColor: color }} />
              {label}: {maValues[period] ?? '\u2014'}
            </span>
          ))}
          <span className="indicator-item">
            <span className="indicator-color" style={{ backgroundColor: '#667eea' }} />
            RSI ({rsiPeriod}): {rsiValue ?? '\u2014'}
          </span>
        </div>
      </div>
      <div className="chart-tools">
        <div className="chart-tools-left">
          <div className="interval-selector">
            {INTERVALS.map(({ key, label }) => (
              <button
                key={key}
                type="button"
                className={`interval-btn${interval === key ? ' interval-btn-active' : ''}`}
                onClick={() => onIntervalChange?.(key)}
              >
                {label}
              </button>
            ))}
          </div>
          <div className="indicator-toggles">
            <span className="indicator-toggles-label">MA</span>
            {MA_CONFIGS.map(({ period, color, label }) => (
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
          <div className="indicator-toggles">
            <span className="indicator-toggles-label">RSI</span>
            {RSI_PERIODS.map((p) => (
              <button
                key={p}
                type="button"
                className={`indicator-toggle-btn${rsiPeriod === p ? ' indicator-toggle-active' : ''}`}
                style={rsiPeriod === p ? { color: '#667eea', borderColor: '#667eea' } : undefined}
                onClick={() => handleChangeRsiPeriod(p)}
              >
                {p}
              </button>
            ))}
          </div>
          <div className="indicator-toggles">
            <span className="indicator-toggles-label">Overlay</span>
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
        <div className="chart-tool-buttons">
          <button
            type="button"
            className={`chart-tool-btn${priceScaleMode === PriceScaleMode.Logarithmic ? ' chart-tool-btn-active' : ''}`}
            onClick={() => handleTogglePriceScale(PriceScaleMode.Logarithmic)}
            title="Log Scale"
          >
            Log
          </button>
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
          <button type="button" className="chart-tool-btn" onClick={handleZoomIn} title="Zoom In">+</button>
          <button type="button" className="chart-tool-btn" onClick={handleZoomOut} title="Zoom Out">&minus;</button>
          <button
            type="button"
            className={`chart-tool-btn${annotationsVisible ? ' chart-tool-btn-active' : ''}`}
            onClick={handleToggleAnnotations}
            title="Toggle Annotations"
          >
            T
          </button>
          <button type="button" className="chart-tool-btn" onClick={handleScrollToRealTime} title="Scroll to Latest">&#8635;</button>
        </div>
      </div>
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
          <div>Loading...</div>
          <div className="chart-loading-hint">
            If there is no response for a long time, it may be due to API rate limiting. Please wait 1 second and refresh.
          </div>
        </div>
      )}
      {error && (
        <div className="chart-error">
          <div className="chart-error-title">Data Loading Failed</div>
          <div>{error}</div>
        </div>
      )}
    </div>
  );
}));

TradingChart.displayName = 'TradingChart';

export default TradingChart;
