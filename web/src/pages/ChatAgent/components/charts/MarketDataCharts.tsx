import React, { useEffect, useRef, useMemo, useCallback, useState, memo } from 'react';
import { createChart, ColorType } from 'lightweight-charts';
import type { IChartApi, ISeriesApi, Time } from 'lightweight-charts';
import { useNavigate, useParams } from 'react-router-dom';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  PieChart, Pie, Cell, Legend, LabelList,
  LineChart, Line, ReferenceLine,
} from 'recharts';
import { fetchStockData } from '../../../MarketView/utils/api';
import { utcMsToChartSec } from '@/lib/utils';
import { Sunrise, Sunset } from 'lucide-react';
import { useTheme } from '../../../../contexts/ThemeContext';
import { useTranslation } from 'react-i18next';

// ─── Shared Constants ───────────────────────────────────────────────

// CSS-variable colors for recharts (SVG) and DOM elements
const GRID_COLOR = 'var(--color-border-default)';
const TEXT_COLOR = 'var(--color-text-secondary)';
// Resolved hex colors for canvas charts (lightweight-charts cannot resolve CSS variables)
interface CanvasTheme {
  bg: string;
  grid: string;
  text: string;
  up: string;
  down: string;
  upA: string;
  downA: string;
}
const CANVAS_THEMES: Record<string, CanvasTheme> = {
  dark:  { bg: '#000000', grid: '#1A1A1A', text: '#666666', up: '#0FEDBE', down: '#FF383C', upA: 'rgba(15, 237, 190, 0.3)', downA: 'rgba(255, 56, 60, 0.3)' },
  light: { bg: '#FFFCF9', grid: '#DDD7D0', text: '#7A756F', up: '#16A34A', down: '#DC2626', upA: 'rgba(22, 163, 74, 0.3)', downA: 'rgba(220, 38, 38, 0.3)' },
};
const GREEN = 'var(--color-profit)';
const RED = 'var(--color-loss)';
const MA_BLUE = '#3b82f6';
const MA_ORANGE = '#f59e0b';

const PIE_COLORS = ['var(--color-accent-primary)', 'var(--color-profit)', '#f59e0b', 'var(--color-loss)', '#3b82f6', '#ec4899', '#8b5cf6', '#14b8a6'];
const ANALYST_COLORS: Record<string, string> = {
  'Strong Buy': 'var(--color-profit)',
  'Buy': '#34d399',
  'Hold': '#f59e0b',
  'Sell': '#f87171',
  'Strong Sell': 'var(--color-loss)',
};

const formatNumber = (num: number | null | undefined): string => {
  if (num == null) return 'N/A';
  if (Math.abs(num) >= 1e12) return `$${(num / 1e12).toFixed(2)}T`;
  if (Math.abs(num) >= 1e9) return `$${(num / 1e9).toFixed(2)}B`;
  if (Math.abs(num) >= 1e6) return `$${(num / 1e6).toFixed(2)}M`;
  if (Math.abs(num) >= 1e3) return `$${(num / 1e3).toFixed(1)}K`;
  return typeof num === 'number' ? `$${num.toFixed(2)}` : String(num);
};

const formatPct = (val: number | null | undefined): string => {
  if (val == null) return 'N/A';
  const sign = val >= 0 ? '+' : '';
  return `${sign}${val.toFixed(2)}%`;
};

/**
 * Convert date string to lightweight-charts time value.
 * Daily dates ("2024-01-15") -> kept as string (business day format).
 * Intraday datetimes ("2024-01-15 09:30:00") -> UNIX timestamp (seconds).
 */
const toChartTime = (val: unknown): Time => {
  if (val == null) return val as unknown as Time;
  if (typeof val === 'number') return utcMsToChartSec(val) as unknown as Time; // Unix ms -> ET chart seconds
  // Intraday datetime string "YYYY-MM-DD HH:MM:SS" -> Unix seconds
  if (typeof val === 'string' && (val.includes(' ') || val.includes('T'))) {
    return Math.floor(new Date(val + 'Z').getTime() / 1000) as unknown as Time;
  }
  return val as unknown as Time; // daily date string, lightweight-charts handles it natively
};

// ─── Scroll-load config (mirrors MarketView/MarketChart.jsx) ────

const SCROLL_LOAD_THRESHOLD = 20;
const RANGE_CHANGE_DEBOUNCE_MS = 300;
const SCROLL_CHUNK_DAYS: Record<string, number> = {
  '1min': 5, '5min': 20, '15min': 30, '30min': 60,
  '1hour': 120, '4hour': 180, '1day': 365, daily: 365,
};

/** Map chart_interval values to API interval params */
const INTERVAL_TO_API: Record<string, string> = {
  '5min': '5min', '15min': '15min', '30min': '30min',
  '1hour': '1hour', '4hour': '4hour', daily: '1day',
};

// ─── Open in Market View link ────────────────────────────────────

interface OpenInMarketLinkProps {
  symbol: string | undefined;
}

function OpenInMarketLink({ symbol }: OpenInMarketLinkProps): React.ReactElement | null {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const params = useParams();
  if (!symbol) return null;

  const handleClick = (e: React.MouseEvent) => {
    e.stopPropagation();
    const qs = new URLSearchParams({ symbol });
    // Encode current chat route so MarketView can offer a "Return to Chat" button
    if (params.threadId) {
      qs.set('returnTo', `/chat/t/${params.threadId}`);
    }
    navigate(`/market?${qs.toString()}`);
  };

  return (
    <button
      onClick={handleClick}
      style={{
        marginLeft: 'auto',
        fontSize: 11,
        color: 'var(--color-accent-primary)',
        background: 'none',
        border: 'none',
        cursor: 'pointer',
        padding: '2px 0',
        whiteSpace: 'nowrap',
        opacity: 0.85,
      }}
      onMouseEnter={(e) => (e.currentTarget.style.opacity = '1')}
      onMouseLeave={(e) => (e.currentTarget.style.opacity = '0.85')}
    >
      {t('toolArtifact.openInMarketView')}
    </button>
  );
}

// Custom tooltip for dark theme
interface DarkTooltipProps {
  active?: boolean;
  payload?: Array<{ color?: string; value: number }>;
  label?: string;
  formatter?: (value: number) => string;
}

const DarkTooltip = ({ active, payload, label, formatter }: DarkTooltipProps): React.ReactElement | null => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{ background: 'var(--color-bg-card)', border: '1px solid var(--color-border-muted)', borderRadius: 6, padding: '8px 12px' }}>
      <p style={{ color: TEXT_COLOR, fontSize: 12, margin: 0 }}>{label}</p>
      {payload.map((entry, i) => (
        <p key={i} style={{ color: entry.color || 'var(--color-text-primary)', fontSize: 12, margin: '2px 0 0' }}>
          {formatter ? formatter(entry.value) : entry.value}
        </p>
      ))}
    </div>
  );
};

// ─── ChartData type for internal use ────────────────────────────────

interface ChartDataPoint {
  time: Time;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

// ─── StockPriceChart ────────────────────────────────────────────────

interface DataProps {
  data: Record<string, unknown>;
}

export function StockPriceChart({ data }: DataProps): React.ReactElement {
  const { t } = useTranslation();
  const { theme } = useTheme();
  const ct = CANVAS_THEMES[theme] || CANVAS_THEMES.dark;
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleSeriesRef = useRef<ISeriesApi<'Candlestick'> | null>(null);
  const volumeSeriesRef = useRef<ISeriesApi<'Histogram'> | null>(null);
  const maSeriesRefs = useRef<Record<number, ISeriesApi<'Line'>>>({});

  // Prefer chart_ohlcv (intraday) when available, fall back to daily ohlcv
  const chartOhlcv = data?.chart_ohlcv as Record<string, unknown>[] | undefined;
  const dailyOhlcv = data?.ohlcv as Record<string, unknown>[] | undefined;
  const initialOhlcv = chartOhlcv && chartOhlcv.length > 0 ? chartOhlcv : dailyOhlcv;
  const chartInterval = (data?.chart_interval as string) || 'daily';
  const symbol = data?.symbol as string | undefined;

  // Scroll-load state (refs for stable closures)
  const allDataRef = useRef<ChartDataPoint[]>([]);
  const oldestTimeRef = useRef<Time | null>(null);
  const fetchingRef = useRef(false);
  const rangeTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const rangeUnsubRef = useRef<(() => void) | null>(null);

  // Helper: set data on all series
  const updateAllSeries = useCallback((chartData: ChartDataPoint[]) => {
    if (candleSeriesRef.current) {
      candleSeriesRef.current.setData(chartData.map((d) => ({
        time: d.time, open: d.open, high: d.high, low: d.low, close: d.close,
      })));
    }
    if (volumeSeriesRef.current) {
      volumeSeriesRef.current.setData(chartData.map((d, i) => ({
        time: d.time,
        value: d.volume || 0,
        color: i > 0 && d.close >= chartData[i - 1].close
          ? ct.upA : ct.downA,
      })));
    }
    // Update MAs
    [{ period: 20, color: MA_BLUE }, { period: 50, color: MA_ORANGE }].forEach(({ period }) => {
      const series = maSeriesRefs.current[period];
      if (!series) return;
      if (chartData.length < period) { series.setData([]); return; }
      const maData: Array<{ time: Time; value: number }> = [];
      let sum = 0;
      for (let i = 0; i < period; i++) sum += chartData[i].close;
      maData.push({ time: chartData[period - 1].time, value: sum / period });
      for (let i = period; i < chartData.length; i++) {
        sum += chartData[i].close - chartData[i - period].close;
        maData.push({ time: chartData[i].time, value: sum / period });
      }
      series.setData(maData);
    });
  }, [ct]);

  // Scroll-load handler
  const handleScrollLoadMore = useCallback(async () => {
    if (fetchingRef.current || !oldestTimeRef.current || !symbol) return;
    const apiInterval = INTERVAL_TO_API[chartInterval];
    if (!apiInterval) return;

    fetchingRef.current = true;
    try {
      const oldest = new Date((oldestTimeRef.current as unknown as number) * 1000);
      const toDate = new Date(oldest);
      toDate.setDate(toDate.getDate() - 1);
      const fromDate = new Date(toDate);
      fromDate.setDate(fromDate.getDate() - (SCROLL_CHUNK_DAYS[chartInterval] || 90));

      const result = await fetchStockData(
        symbol, apiInterval,
        fromDate.toISOString().split('T')[0],
        toDate.toISOString().split('T')[0],
      );
      const newData = (result as unknown as Record<string, unknown>)?.data;

      if (newData && Array.isArray(newData) && newData.length > 0) {
        const existingMap = new Map(allDataRef.current.map((d) => [d.time, d]));
        (newData as ChartDataPoint[]).forEach((d) => { if (!existingMap.has(d.time)) existingMap.set(d.time, d); });
        const merged = Array.from(existingMap.values()).sort((a, b) => (a.time as unknown as number) - (b.time as unknown as number));
        allDataRef.current = merged;
        oldestTimeRef.current = merged[0].time;
        updateAllSeries(merged);
      }
    } catch (err) {
      console.warn('Detail chart scroll-load failed:', err);
    } finally {
      fetchingRef.current = false;
    }
  }, [symbol, chartInterval, updateAllSeries]);

  useEffect(() => {
    if (!containerRef.current || !initialOhlcv?.length) return;

    // Clean up previous chart
    if (chartRef.current) {
      chartRef.current.remove();
      chartRef.current = null;
    }
    if (rangeUnsubRef.current) { rangeUnsubRef.current(); rangeUnsubRef.current = null; }
    candleSeriesRef.current = null;
    volumeSeriesRef.current = null;
    maSeriesRefs.current = {};

    const chart = createChart(containerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: ct.bg },
        textColor: ct.text,
      },
      width: containerRef.current.clientWidth,
      height: 360,
      grid: {
        vertLines: { color: ct.grid },
        horzLines: { color: ct.grid },
      },
      crosshair: { mode: 1 },
      handleScroll: { mouseWheel: true, pressedMouseMove: true, horzTouchDrag: true },
      rightPriceScale: { borderColor: ct.grid },
      timeScale: {
        borderColor: ct.grid,
        timeVisible: chartInterval !== 'daily',
      },
    });
    chartRef.current = chart;

    // Candlestick series
    candleSeriesRef.current = chart.addCandlestickSeries({
      upColor: ct.up, downColor: ct.down,
      borderDownColor: ct.down, borderUpColor: ct.up,
      wickDownColor: ct.down, wickUpColor: ct.up,
    });

    // Volume histogram series (bottom 20%)
    volumeSeriesRef.current = chart.addHistogramSeries({
      priceFormat: { type: 'volume' },
      priceScaleId: 'volume',
    });
    chart.priceScale('volume').applyOptions({
      scaleMargins: { top: 0.8, bottom: 0 },
    });

    // MA line series (daily only)
    if (chartInterval === 'daily') {
      [{ period: 20, color: MA_BLUE }, { period: 50, color: MA_ORANGE }].forEach(({ period, color }) => {
        maSeriesRefs.current[period] = chart.addLineSeries({
          color, lineWidth: 1, priceLineVisible: false, lastValueVisible: false,
        });
      });
    }

    // Convert initial OHLCV to lightweight-charts format
    const chartData: ChartDataPoint[] = initialOhlcv.map((d) => ({
      time: toChartTime((d as Record<string, unknown>).time ?? (d as Record<string, unknown>).date),
      open: (d as Record<string, unknown>).open as number,
      high: (d as Record<string, unknown>).high as number,
      low: (d as Record<string, unknown>).low as number,
      close: (d as Record<string, unknown>).close as number,
      volume: ((d as Record<string, unknown>).volume as number) || 0,
    })).sort((a, b) => (a.time as unknown as number) - (b.time as unknown as number))
      .filter((item, i, arr) => i === 0 || item.time !== arr[i - 1].time);

    allDataRef.current = chartData;
    oldestTimeRef.current = chartData[0]?.time;
    updateAllSeries(chartData);

    chart.timeScale().fitContent();

    // Subscribe to visible range changes for scroll-based loading
    const rangeHandler = (range: { from: number; to: number } | null) => {
      if (rangeTimerRef.current) clearTimeout(rangeTimerRef.current);
      rangeTimerRef.current = setTimeout(() => {
        if (range && range.from <= SCROLL_LOAD_THRESHOLD) {
          handleScrollLoadMore();
        }
      }, RANGE_CHANGE_DEBOUNCE_MS);
    };
    chart.timeScale().subscribeVisibleLogicalRangeChange(rangeHandler);
    rangeUnsubRef.current = () => {
      chart.timeScale().unsubscribeVisibleLogicalRangeChange(rangeHandler);
    };

    // Resize observer
    const ro = new ResizeObserver(() => {
      if (containerRef.current && chartRef.current) {
        chartRef.current.applyOptions({ width: containerRef.current.clientWidth });
      }
    });
    ro.observe(containerRef.current);

    return () => {
      ro.disconnect();
      if (rangeTimerRef.current) clearTimeout(rangeTimerRef.current);
      if (rangeUnsubRef.current) { rangeUnsubRef.current(); rangeUnsubRef.current = null; }
      if (chartRef.current) {
        chartRef.current.remove();
        chartRef.current = null;
      }
    };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [initialOhlcv, chartInterval, updateAllSeries, handleScrollLoadMore]);

  if (!initialOhlcv?.length) {
    return <div style={{ color: TEXT_COLOR, padding: 16 }}>{t('toolArtifact.noPriceData')}</div>;
  }

  const INTERVAL_LABELS: Record<string, string> = { '5min': '5m', '15min': '15m', '30min': '30m', '1hour': '1H', '4hour': '4H', daily: 'D' };

  return (
    <div>
      <div className="flex items-center gap-3 mb-2 flex-wrap" style={{ fontSize: 13, color: TEXT_COLOR }}>
        <span style={{ fontWeight: 600, color: 'var(--color-text-primary)' }}>{data.symbol as string}</span>
        {chartInterval && (
          <span style={{
            fontSize: 11,
            padding: '1px 6px',
            borderRadius: 3,
            background: 'var(--color-bg-surface)',
            color: TEXT_COLOR,
          }}>
            {INTERVAL_LABELS[chartInterval] || chartInterval}
          </span>
        )}
        {(data.stats as Record<string, unknown> | undefined)?.period_change_pct != null && (
          <span style={{ color: ((data.stats as Record<string, unknown>).period_change_pct as number) >= 0 ? GREEN : RED }}>
            {formatPct((data.stats as Record<string, unknown>).period_change_pct as number)}
          </span>
        )}
        {chartInterval === 'daily' && (
          <>
            <span className="flex items-center gap-1">
              <span style={{ width: 12, height: 2, background: MA_BLUE, display: 'inline-block' }} /> MA20
            </span>
            <span className="flex items-center gap-1">
              <span style={{ width: 12, height: 2, background: MA_ORANGE, display: 'inline-block' }} /> MA50
            </span>
          </>
        )}
        <OpenInMarketLink symbol={data.symbol as string} />
      </div>
      <div ref={containerRef} className="[&_*]:outline-none" style={{ width: '100%', height: 360 }} />
      <StockStatsCard stats={data.stats as Record<string, unknown> | undefined} />
    </div>
  );
}

// ─── StockStatsCard ─────────────────────────────────────────────────

interface StockStatsCardProps {
  stats: Record<string, unknown> | undefined;
}

function StockStatsCard({ stats }: StockStatsCardProps): React.ReactElement | null {
  const { t } = useTranslation();
  if (!stats) return null;

  const items = [
    { label: t('toolArtifact.periodChange'), value: (stats.period_change_pct as number | undefined) != null ? formatPct(stats.period_change_pct as number) : null, color: (stats.period_change_pct as number) >= 0 ? GREEN : RED },
    { label: t('toolArtifact.periodHigh'), value: (stats.period_high as number | undefined) != null ? `$${(stats.period_high as number).toFixed(2)}` : null },
    { label: t('toolArtifact.periodLow'), value: (stats.period_low as number | undefined) != null ? `$${(stats.period_low as number).toFixed(2)}` : null },
    { label: t('toolArtifact.avgVolume'), value: (stats.avg_volume as number | undefined) != null ? formatNumber(stats.avg_volume as number).replace('$', '') : null },
    { label: t('toolArtifact.volatility'), value: (stats.volatility as number | undefined) != null ? `${((stats.volatility as number) * 100).toFixed(1)}%` : null },
    { label: 'MA 20', value: (stats.ma_20 as number | undefined) != null ? `$${(stats.ma_20 as number).toFixed(2)}` : null, labelColor: MA_BLUE },
    { label: 'MA 50', value: (stats.ma_50 as number | undefined) != null ? `$${(stats.ma_50 as number).toFixed(2)}` : null, labelColor: MA_ORANGE },
  ].filter((i) => i.value != null);

  if (items.length === 0) return null;

  return (
    <div
      style={{
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fill, minmax(100px, 1fr))',
        gap: '8px 16px',
        marginTop: 12,
        padding: '10px 12px',
        background: 'var(--color-bg-surface)',
        border: '1px solid var(--color-border-default)',
        borderRadius: 6,
        fontSize: 12,
      }}
    >
      {items.map((item) => (
        <div key={item.label}>
          <div style={{ color: item.labelColor || TEXT_COLOR, opacity: item.labelColor ? 1 : 0.7, marginBottom: 2 }}>
            {item.label}
          </div>
          <div style={{ color: item.color || 'var(--color-text-primary)', fontWeight: 500 }}>
            {item.value}
          </div>
        </div>
      ))}
    </div>
  );
}

// ─── SectorPerformanceChart ─────────────────────────────────────────

const SECTOR_ABBREVIATIONS: Record<string, string> = {
  'Consumer Cyclical': 'Cons. Cyclical',
  'Consumer Defensive': 'Cons. Defensive',
  'Communication Services': 'Comm. Services',
  'Financial Services': 'Financial Svcs',
};

export function SectorPerformanceChart({ data }: DataProps): React.ReactElement {
  const { t } = useTranslation();
  const sectors = data?.sectors as Record<string, unknown>[] | undefined;
  if (!sectors?.length) {
    return <div style={{ color: TEXT_COLOR, padding: 16 }}>{t('toolArtifact.noSectorData')}</div>;
  }

  const chartData = sectors.map((s) => {
    const name = (s.sector as string) || 'N/A';
    return {
      name: SECTOR_ABBREVIATIONS[name] || name,
      value: (s.changesPercentage as number) || 0,
      fill: ((s.changesPercentage as number) || 0) >= 0 ? GREEN : RED,
      label: formatPct((s.changesPercentage as number) || 0),
    };
  });

  return (
    <div>
      <h4 style={{ color: 'var(--color-text-primary)', fontSize: 14, fontWeight: 600, marginBottom: 12 }}>
        {t('toolArtifact.sectorPerformance')}
      </h4>
      <ResponsiveContainer width="100%" minWidth={1} height={Math.max(chartData.length * 36, 200)}>
        <BarChart data={chartData} layout="vertical" margin={{ left: 10, right: 50 }}>
          <CartesianGrid strokeDasharray="3 3" stroke={GRID_COLOR} horizontal={false} />
          <XAxis
            type="number"
            tick={{ fill: TEXT_COLOR, fontSize: 11 }}
            axisLine={{ stroke: GRID_COLOR }}
            tickFormatter={(v: number) => `${v.toFixed(1)}%`}
          />
          <YAxis
            type="category"
            dataKey="name"
            width={120}
            tick={{ fill: TEXT_COLOR, fontSize: 11 }}
            axisLine={{ stroke: GRID_COLOR }}
          />
          <Tooltip content={<DarkTooltip formatter={(v: number) => formatPct(v)} />} />
          <Bar dataKey="value" radius={[0, 4, 4, 0]}>
            {chartData.map((entry, i) => (
              <Cell key={i} fill={entry.fill} />
            ))}
            <LabelList
              dataKey="label"
              position="right"
              style={{ fill: TEXT_COLOR, fontSize: 11 }}
            />
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

// ─── PerformanceBarChart ────────────────────────────────────────────

interface PerformanceBarChartProps {
  performance: Record<string, number> | undefined;
}

const PERF_LABELS: Record<string, string> = { '1D': '1D', '5D': '5D', '1M': '1M', '3M': '3M', '6M': '6M', 'ytd': 'YTD', '1Y': '1Y', '3Y': '3Y', '5Y': '5Y' };

export const PerformanceBarChart = memo(function PerformanceBarChart({ performance }: PerformanceBarChartProps): React.ReactElement | null {
  const { t } = useTranslation();

  const chartData = useMemo(() => {
    if (!performance) return [];
    return Object.entries(PERF_LABELS)
      .filter(([key]) => performance[key] != null)
      .map(([key, label]) => ({
        name: label,
        value: performance[key],
        fill: performance[key] >= 0 ? GREEN : RED,
      }));
  }, [performance]);

  if (!performance || chartData.length === 0) return null;

  return (
    <div>
      <h4 style={{ color: 'var(--color-text-primary)', fontSize: 13, fontWeight: 600, marginBottom: 8 }}>
        {t('toolArtifact.pricePerformance')}
      </h4>
      <ResponsiveContainer width="100%" minWidth={1} height={180}>
        <BarChart data={chartData} margin={{ left: -20, right: 10 }}>
          <CartesianGrid strokeDasharray="3 3" stroke={GRID_COLOR} vertical={false} />
          <XAxis
            dataKey="name"
            tick={{ fill: TEXT_COLOR, fontSize: 11 }}
            axisLine={{ stroke: GRID_COLOR }}
          />
          <YAxis
            tick={{ fill: TEXT_COLOR, fontSize: 11 }}
            axisLine={{ stroke: GRID_COLOR }}
            tickFormatter={(v: number) => `${v.toFixed(0)}%`}
          />
          <Tooltip content={<DarkTooltip formatter={(v: number) => formatPct(v)} />} />
          <Bar dataKey="value" radius={[4, 4, 0, 0]}>
            {chartData.map((entry, i) => (
              <Cell key={i} fill={entry.fill} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
});

// ─── AnalystRatingsChart ────────────────────────────────────────────

interface AnalystRatingsChartProps {
  ratings: Record<string, unknown> | undefined;
}

export const AnalystRatingsChart = memo(function AnalystRatingsChart({ ratings }: AnalystRatingsChartProps): React.ReactElement | null {
  const { t } = useTranslation();

  const { chartData, total } = useMemo(() => {
    if (!ratings) return { chartData: [], total: 0 };
    const cd = [
      { name: 'Strong Buy', value: (ratings.strongBuy as number) || 0 },
      { name: 'Buy', value: (ratings.buy as number) || 0 },
      { name: 'Hold', value: (ratings.hold as number) || 0 },
      { name: 'Sell', value: (ratings.sell as number) || 0 },
      { name: 'Strong Sell', value: (ratings.strongSell as number) || 0 },
    ].filter((d) => d.value > 0);
    return { chartData: cd, total: cd.reduce((s, d) => s + d.value, 0) };
  }, [ratings]);

  if (!ratings || chartData.length === 0) return null;

  return (
    <div>
      <h4 style={{ color: 'var(--color-text-primary)', fontSize: 13, fontWeight: 600, marginBottom: 8 }}>
        {t('toolArtifact.analystRatings')}
      </h4>
      <div style={{ position: 'relative' }}>
        <ResponsiveContainer width="100%" minWidth={1} height={200}>
          <PieChart>
            <Pie
              data={chartData}
              cx="50%"
              cy="50%"
              innerRadius={50}
              outerRadius={75}
              dataKey="value"
              stroke="none"
            >
              {chartData.map((entry) => (
                <Cell key={entry.name} fill={ANALYST_COLORS[entry.name] || 'var(--color-icon-muted)'} />
              ))}
            </Pie>
            <Legend
              wrapperStyle={{ fontSize: 11, color: TEXT_COLOR }}
              formatter={(val: string) => <span style={{ color: TEXT_COLOR }}>{val}</span>}
            />
            <Tooltip content={<DarkTooltip formatter={(v: number) => `${v} (${((v / total) * 100).toFixed(0)}%)`} />} />
          </PieChart>
        </ResponsiveContainer>
        {/* Center consensus label */}
        <div
          style={{
            position: 'absolute',
            top: '38%',
            left: '50%',
            transform: 'translate(-50%, -50%)',
            textAlign: 'center',
          }}
        >
          <div style={{ fontSize: 14, fontWeight: 700, color: 'var(--color-text-primary)', textTransform: 'uppercase' }}>
            {(ratings.consensus as string) || ''}
          </div>
          <div style={{ fontSize: 11, color: TEXT_COLOR }}>{t('toolArtifact.nRatings', { count: total })}</div>
        </div>
      </div>
    </div>
  );
});

// ─── RevenueBreakdownChart ──────────────────────────────────────────

interface RevenueBreakdownChartProps {
  revenueByProduct: Record<string, number> | undefined;
  revenueByGeo: Record<string, number> | undefined;
}

const buildPieData = (obj: Record<string, number>) => {
  return Object.entries(obj)
    .map(([name, value]) => ({ name, value }))
    .sort((a, b) => b.value - a.value);
};

export const RevenueBreakdownChart = memo(function RevenueBreakdownChart({ revenueByProduct, revenueByGeo }: RevenueBreakdownChartProps): React.ReactElement | null {
  const { t } = useTranslation();
  const hasProduct = revenueByProduct && Object.keys(revenueByProduct).length > 0;
  const hasGeo = revenueByGeo && Object.keys(revenueByGeo).length > 0;

  if (!hasProduct && !hasGeo) return null;

  const renderPie = (data: Array<{ name: string; value: number }>, title: string) => {
    const total = data.reduce((s, d) => s + d.value, 0);
    return (
      <div style={{ flex: 1, minWidth: 200 }}>
        <h5 style={{ color: TEXT_COLOR, fontSize: 12, fontWeight: 500, marginBottom: 4 }}>
          {title}
        </h5>
        <ResponsiveContainer width="100%" minWidth={1} height={180}>
          <PieChart>
            <Pie
              data={data}
              cx="50%"
              cy="50%"
              innerRadius={35}
              outerRadius={55}
              dataKey="value"
              stroke="none"
            >
              {data.map((_, i) => (
                <Cell key={i} fill={PIE_COLORS[i % PIE_COLORS.length]} />
              ))}
            </Pie>
            <Legend
              wrapperStyle={{ fontSize: 10, color: TEXT_COLOR }}
              formatter={(val: string) => <span style={{ color: TEXT_COLOR }}>{val}</span>}
            />
            <Tooltip
              content={<DarkTooltip formatter={(v: number) => `${formatNumber(v)} (${((v / total) * 100).toFixed(1)}%)`} />}
            />
          </PieChart>
        </ResponsiveContainer>
      </div>
    );
  };

  return (
    <div>
      <h4 style={{ color: 'var(--color-text-primary)', fontSize: 13, fontWeight: 600, marginBottom: 8 }}>
        {t('toolArtifact.revenueBreakdown')}
      </h4>
      <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap' }}>
        {hasProduct && renderPie(buildPieData(revenueByProduct), t('toolArtifact.byProduct'))}
        {hasGeo && renderPie(buildPieData(revenueByGeo), t('toolArtifact.byGeography'))}
      </div>
    </div>
  );
});

// ─── QuarterlyRevenueChart ───────────────────────────────────────────

interface ChartArrayDataProps {
  data: Record<string, unknown>[] | undefined;
}

export const QuarterlyRevenueChart = memo(function QuarterlyRevenueChart({ data }: ChartArrayDataProps): React.ReactElement | null {
  const { t } = useTranslation();
  if (!data?.length) return null;

  return (
    <div>
      <h4 style={{ color: 'var(--color-text-primary)', fontSize: 13, fontWeight: 600, marginBottom: 8 }}>
        {t('toolArtifact.quarterlyRevenue')}
      </h4>
      <ResponsiveContainer width="100%" minWidth={1} height={220}>
        <BarChart data={data} margin={{ left: -10, right: 10 }}>
          <CartesianGrid strokeDasharray="3 3" stroke={GRID_COLOR} vertical={false} />
          <XAxis dataKey="period" tick={{ fill: TEXT_COLOR, fontSize: 10 }} axisLine={{ stroke: GRID_COLOR }} />
          <YAxis tick={{ fill: TEXT_COLOR, fontSize: 11 }} axisLine={{ stroke: GRID_COLOR }} tickFormatter={(v: number) => formatNumber(v).replace('$', '')} />
          <Tooltip content={<DarkTooltip formatter={(v: number) => formatNumber(v)} />} />
          <Legend wrapperStyle={{ fontSize: 11, color: TEXT_COLOR }} formatter={(val: string) => <span style={{ color: TEXT_COLOR }}>{val}</span>} />
          <Bar dataKey="revenue" name={t('toolArtifact.revenue')} fill="var(--color-accent-primary)" radius={[4, 4, 0, 0]} />
          <Bar dataKey="netIncome" name={t('toolArtifact.netIncome')} fill={GREEN} radius={[4, 4, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
});

// ─── MarginsChart ───────────────────────────────────────────────────

export const MarginsChart = memo(function MarginsChart({ data }: ChartArrayDataProps): React.ReactElement | null {
  const { t } = useTranslation();

  const chartData = useMemo(() => {
    if (!data?.length) return [];
    return data.map((d) => ({
      period: d.period as string,
      grossMargin: (d.grossMargin as number | undefined) != null ? (d.grossMargin as number) * 100 : null,
      operatingMargin: (d.operatingMargin as number | undefined) != null ? (d.operatingMargin as number) * 100 : null,
      netMargin: (d.netMargin as number | undefined) != null ? (d.netMargin as number) * 100 : null,
    }));
  }, [data]);

  if (!data?.length) return null;

  return (
    <div>
      <h4 style={{ color: 'var(--color-text-primary)', fontSize: 13, fontWeight: 600, marginBottom: 8 }}>
        {t('toolArtifact.profitMargins')}
      </h4>
      <ResponsiveContainer width="100%" minWidth={1} height={220}>
        <LineChart data={chartData} margin={{ left: -10, right: 10 }}>
          <CartesianGrid strokeDasharray="3 3" stroke={GRID_COLOR} vertical={false} />
          <XAxis dataKey="period" tick={{ fill: TEXT_COLOR, fontSize: 10 }} axisLine={{ stroke: GRID_COLOR }} />
          <YAxis tick={{ fill: TEXT_COLOR, fontSize: 11 }} axisLine={{ stroke: GRID_COLOR }} tickFormatter={(v: number) => `${v.toFixed(0)}%`} />
          <Tooltip content={<DarkTooltip formatter={(v: number) => `${v?.toFixed(1)}%`} />} />
          <Legend wrapperStyle={{ fontSize: 11, color: TEXT_COLOR }} formatter={(val: string) => <span style={{ color: TEXT_COLOR }}>{val}</span>} />
          <Line type="monotone" dataKey="grossMargin" name={t('toolArtifact.grossMargin')} stroke="var(--color-accent-primary)" strokeWidth={2} dot={{ r: 3 }} connectNulls />
          <Line type="monotone" dataKey="operatingMargin" name={t('toolArtifact.operatingMargin')} stroke={MA_ORANGE} strokeWidth={2} dot={{ r: 3 }} connectNulls />
          <Line type="monotone" dataKey="netMargin" name={t('toolArtifact.netMargin')} stroke={GREEN} strokeWidth={2} dot={{ r: 3 }} connectNulls />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
});

// ─── EarningsSurpriseChart ──────────────────────────────────────────

export const EarningsSurpriseChart = memo(function EarningsSurpriseChart({ data }: ChartArrayDataProps): React.ReactElement | null {
  const { t } = useTranslation();
  if (!data?.length) return null;

  return (
    <div>
      <h4 style={{ color: 'var(--color-text-primary)', fontSize: 13, fontWeight: 600, marginBottom: 8 }}>
        {t('toolArtifact.epsActualVsEstimate')}
      </h4>
      <ResponsiveContainer width="100%" minWidth={1} height={220}>
        <BarChart data={data} margin={{ left: -10, right: 10 }}>
          <CartesianGrid strokeDasharray="3 3" stroke={GRID_COLOR} vertical={false} />
          <XAxis dataKey="period" tick={{ fill: TEXT_COLOR, fontSize: 10 }} axisLine={{ stroke: GRID_COLOR }} />
          <YAxis tick={{ fill: TEXT_COLOR, fontSize: 11 }} axisLine={{ stroke: GRID_COLOR }} tickFormatter={(v: number) => `$${v.toFixed(2)}`} />
          <Tooltip content={<DarkTooltip formatter={(v: number) => `$${v?.toFixed(2)}`} />} />
          <Legend wrapperStyle={{ fontSize: 11, color: TEXT_COLOR }} formatter={(val: string) => <span style={{ color: TEXT_COLOR }}>{val}</span>} />
          <Bar dataKey="epsActual" name={t('toolArtifact.epsActual')} fill={GREEN} radius={[4, 4, 0, 0]} />
          <Bar dataKey="epsEstimate" name={t('toolArtifact.epsEstimate')} fill="var(--color-icon-muted)" radius={[4, 4, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
});

// ─── CashFlowChart ──────────────────────────────────────────────────

export const CashFlowChart = memo(function CashFlowChart({ data }: ChartArrayDataProps): React.ReactElement | null {
  const { t } = useTranslation();
  if (!data?.length) return null;

  return (
    <div>
      <h4 style={{ color: 'var(--color-text-primary)', fontSize: 13, fontWeight: 600, marginBottom: 8 }}>
        {t('toolArtifact.cashFlowQuarterly')}
      </h4>
      <ResponsiveContainer width="100%" minWidth={1} height={220}>
        <BarChart data={data} margin={{ left: -10, right: 10 }}>
          <CartesianGrid strokeDasharray="3 3" stroke={GRID_COLOR} vertical={false} />
          <XAxis dataKey="period" tick={{ fill: TEXT_COLOR, fontSize: 10 }} axisLine={{ stroke: GRID_COLOR }} />
          <YAxis tick={{ fill: TEXT_COLOR, fontSize: 11 }} axisLine={{ stroke: GRID_COLOR }} tickFormatter={(v: number) => formatNumber(v).replace('$', '')} />
          <Tooltip content={<DarkTooltip formatter={(v: number) => formatNumber(v)} />} />
          <Legend wrapperStyle={{ fontSize: 11, color: TEXT_COLOR }} formatter={(val: string) => <span style={{ color: TEXT_COLOR }}>{val}</span>} />
          <ReferenceLine y={0} stroke={GRID_COLOR} />
          <Bar dataKey="operatingCashFlow" name={t('toolArtifact.operatingCF')} fill="var(--color-accent-primary)" radius={[4, 4, 0, 0]} />
          <Bar dataKey="capitalExpenditure" name={t('toolArtifact.capEx')} fill={RED} radius={[4, 4, 0, 0]} />
          <Bar dataKey="freeCashFlow" name={t('toolArtifact.freeCF')} fill={GREEN} radius={[4, 4, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
});

// ─── CompanyOverviewCard ────────────────────────────────────────────

const DETAIL_STATUS_ICONS: Record<string, typeof Sunrise | typeof Sunset> = {
  early_trading: Sunrise,
  late_trading: Sunset,
};
const DETAIL_STATUS_LABELS: Record<string, string> = {
  early_trading: 'Pre-Market',
  open: 'Regular Hours',
  late_trading: 'After-Hours',
  closed: 'Market Closed',
};
const DETAIL_STATUS_COLORS: Record<string, string> = {
  early_trading: '#f59e0b',
  open: GREEN,
  late_trading: '#3b82f6',
  closed: TEXT_COLOR,
};

/** Defers rendering until the element scrolls into view (with 200px pre-load margin) */
function LazyChart({ height, children, root }: { height: number; children: React.ReactNode; root?: React.RefObject<HTMLDivElement | null> }): React.ReactElement {
  const ref = useRef<HTMLDivElement>(null);
  const [visible, setVisible] = useState(false);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const observer = new IntersectionObserver(
      ([entry]) => { if (entry.isIntersecting) { setVisible(true); observer.disconnect(); } },
      { root: root?.current ?? null, rootMargin: '200px' },
    );
    observer.observe(el);
    return () => observer.disconnect();
  // eslint-disable-next-line react-hooks/exhaustive-deps -- root is a stable ref object, observer is one-shot
  }, []);

  if (!visible) return <div ref={ref} style={{ minHeight: height, width: '100%' }} />;
  return <>{children}</>;
}

export const CompanyOverviewCard = memo(function CompanyOverviewCard({ data, scrollContainerRef }: DataProps & { scrollContainerRef?: React.RefObject<HTMLDivElement | null> }): React.ReactElement {
  const { t } = useTranslation();
  const {
    symbol, name, quote, performance, analystRatings,
    revenueByProduct, revenueByGeo,
    quarterlyFundamentals, earningsSurprises, cashFlow,
    float: floatData, shortInterest, shortVolume,
  } = (data || {}) as Record<string, unknown>;

  const quoteObj = quote as Record<string, unknown> | undefined;

  // Resolve display price: snapshot -> regularClose, FMP fallback -> price
  const displayPrice = (quoteObj?.regularClose ?? quoteObj?.price) as number | undefined;
  const displayChange = (quoteObj?.regularChange ?? quoteObj?.change) as number | undefined;
  const displayChangePct = (quoteObj?.regularChangePct ?? quoteObj?.changePct) as number | undefined;
  const changeColor = (displayChange ?? 0) >= 0 ? GREEN : RED;

  // Extended hours
  const marketStatus = quoteObj?.marketStatus as string | undefined;
  const isExtended = marketStatus === 'early_trading' || marketStatus === 'late_trading';
  const extPrice = quoteObj?.lastTradePrice as number | undefined;
  const hasExtPrice = isExtended && extPrice != null && displayPrice != null && extPrice !== displayPrice;
  const extDiff = hasExtPrice ? extPrice - displayPrice : 0;
  const extDiffPct = hasExtPrice && displayPrice ? (extDiff / displayPrice * 100) : 0;

  // Float / short interest / short volume
  const floatObj = floatData as Record<string, unknown> | undefined;
  const hasFloat = floatObj && typeof floatObj === 'object' && (floatObj.free_float as number | undefined) != null;
  // shortInterest: single object (new) or array (legacy backward compat)
  const latestSI = Array.isArray(shortInterest)
    ? ((shortInterest as Record<string, unknown>[]).length ? (shortInterest as Record<string, unknown>[])[(shortInterest as Record<string, unknown>[]).length - 1] : null)
    : ((shortInterest as Record<string, unknown>) || null);
  const hasSI = latestSI && (latestSI.short_interest as number | undefined) != null;
  const siPctOfFloat = (hasSI && hasFloat && (floatObj!.free_float as number))
    ? ((latestSI!.short_interest as number) / (floatObj!.free_float as number) * 100) : null;
  const shortVolumeObj = shortVolume as Record<string, unknown> | undefined;
  const hasSV = shortVolumeObj && typeof shortVolumeObj === 'object' && (shortVolumeObj.short_volume_ratio as number | undefined) != null;

  return (
    <div className="space-y-5">
      {/* Quote summary */}
      {quoteObj && (
        <div>
          <div className="flex items-baseline gap-3 mb-3 flex-wrap">
            <span style={{ fontSize: 20, fontWeight: 700, color: 'var(--color-text-primary)', minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: '100%' }}>
              {(name as string) || (symbol as string)}
            </span>
            <span style={{ fontSize: 14, color: TEXT_COLOR, flexShrink: 0 }}>{symbol as string}</span>
            <OpenInMarketLink symbol={symbol as string} />
            {marketStatus && (() => {
              const StatusIcon = DETAIL_STATUS_ICONS[marketStatus];
              return (
                <span style={{
                  display: 'inline-flex', alignItems: 'center', gap: 4,
                  fontSize: 11, fontWeight: 600, padding: '2px 8px', borderRadius: 4,
                  color: DETAIL_STATUS_COLORS[marketStatus] || TEXT_COLOR,
                  border: `1px solid ${DETAIL_STATUS_COLORS[marketStatus] || TEXT_COLOR}`,
                  whiteSpace: 'nowrap',
                }}>
                  {StatusIcon && <StatusIcon size={11} />}
                  {DETAIL_STATUS_LABELS[marketStatus] || marketStatus}
                </span>
              );
            })()}
          </div>

          {/* Regular close price */}
          <div className="flex items-baseline gap-3 mb-1">
            <span style={{ fontSize: 24, fontWeight: 700, color: 'var(--color-text-primary)' }}>
              ${displayPrice?.toFixed(2) || 'N/A'}
            </span>
            {displayChange != null && (
              <span style={{ fontSize: 14, color: changeColor }}>
                {displayChange >= 0 ? '+' : ''}{displayChange?.toFixed(2)} ({displayChangePct?.toFixed(2)}%)
              </span>
            )}
            {marketStatus && hasExtPrice && (
              <span style={{ fontSize: 11, color: TEXT_COLOR }}>Close</span>
            )}
          </div>

          {/* Extended-hours price */}
          {hasExtPrice && (
            <div className="flex items-center gap-2 mb-3" style={{ fontSize: 14 }}>
              <span style={{ display: 'inline-flex', alignItems: 'center', color: DETAIL_STATUS_COLORS[marketStatus!] || TEXT_COLOR }}>
                {marketStatus === 'early_trading' ? <Sunrise size={14} /> : <Sunset size={14} />}
              </span>
              <span style={{ fontWeight: 600, color: 'var(--color-text-primary)' }}>
                ${extPrice.toFixed(2)}
              </span>
              <span style={{ color: extDiff >= 0 ? GREEN : RED, fontWeight: 500 }}>
                {extDiff >= 0 ? '+' : ''}{extDiff.toFixed(2)} ({extDiffPct >= 0 ? '+' : ''}{extDiffPct.toFixed(2)}%)
              </span>
            </div>
          )}

          {!hasExtPrice && <div className="mb-3" />}

          <div
            className="grid grid-cols-2 gap-x-6 gap-y-1"
            style={{ fontSize: 12, color: TEXT_COLOR }}
          >
            {(quoteObj.open as number | undefined) != null && <QuoteStat label={t('toolArtifact.open')} value={`$${(quoteObj.open as number).toFixed(2)}`} />}
            {(quoteObj.previousClose as number | undefined) != null && <QuoteStat label={t('toolArtifact.prevClose')} value={`$${(quoteObj.previousClose as number).toFixed(2)}`} />}
            {(quoteObj.dayLow as number | undefined) != null && (quoteObj.dayHigh as number | undefined) != null && (
              <QuoteStat label={t('toolArtifact.dayRange')} value={`$${(quoteObj.dayLow as number).toFixed(2)} - $${(quoteObj.dayHigh as number).toFixed(2)}`} />
            )}
            {(quoteObj.yearLow as number | undefined) != null && (quoteObj.yearHigh as number | undefined) != null && (
              <QuoteStat label={t('toolArtifact.52wRange')} value={`$${(quoteObj.yearLow as number).toFixed(2)} - $${(quoteObj.yearHigh as number).toFixed(2)}`} />
            )}
            {(quoteObj.volume as number | undefined) != null && <QuoteStat label={t('toolArtifact.volume')} value={formatNumber(quoteObj.volume as number).replace('$', '')} />}
            {(quoteObj.marketCap as number | undefined) != null && <QuoteStat label={t('toolArtifact.marketCap')} value={formatNumber(quoteObj.marketCap as number)} />}
          </div>
        </div>
      )}

      {/* Float & Short Data */}
      {(hasFloat || hasSI || hasSV) && (
        <div>
          <h4 style={{ fontSize: 13, fontWeight: 600, color: 'var(--color-text-primary)', marginBottom: 8 }}>
            {t('toolArtifact.shareStructure', 'Share Structure')}
          </h4>
          <div
            className="grid grid-cols-2 gap-x-6 gap-y-1"
            style={{ fontSize: 12, color: TEXT_COLOR }}
          >
            {hasFloat && (
              <QuoteStat label={t('toolArtifact.float', 'Float')} value={formatNumber(floatObj!.free_float as number).replace('$', '')} />
            )}
            {hasFloat && (floatObj!.free_float_percent as number | undefined) != null && (
              <QuoteStat label={t('toolArtifact.floatPct', 'Float %')} value={`${(floatObj!.free_float_percent as number).toFixed(1)}%`} />
            )}
            {hasSI && (
              <QuoteStat
                label={`${t('toolArtifact.shortInterest', 'Short Interest')}${(latestSI!.settlement_date as string | undefined) ? ` (${latestSI!.settlement_date as string})` : ''}`}
                value={(latestSI!.short_interest as number).toLocaleString()}
              />
            )}
            {siPctOfFloat != null && (
              <QuoteStat label={t('toolArtifact.shortPctFloat', 'SI % of Float')} value={`${siPctOfFloat.toFixed(2)}%`} />
            )}
            {(latestSI?.days_to_cover as number | undefined) != null && (
              <QuoteStat label={t('toolArtifact.daysToCover', 'Days to Cover')} value={(latestSI!.days_to_cover as number).toFixed(2)} />
            )}
            {hasSV && (
              <QuoteStat
                label={`${t('toolArtifact.shortVolRatio', 'Short Vol Ratio')}${(shortVolumeObj!.date as string | undefined) ? ` (${shortVolumeObj!.date as string})` : ''}`}
                value={`${(shortVolumeObj!.short_volume_ratio as number).toFixed(1)}%`}
              />
            )}
          </div>
        </div>
      )}

      {/* Performance */}
      <PerformanceBarChart performance={performance as Record<string, number> | undefined} />

      {/* Analyst Ratings */}
      <AnalystRatingsChart ratings={analystRatings as Record<string, unknown> | undefined} />

      {/* Quarterly Revenue & Net Income + Profit Margins (same data source) */}
      {(quarterlyFundamentals as unknown[])?.length > 0 && (
        <>
          <LazyChart height={250} root={scrollContainerRef}>
            <QuarterlyRevenueChart data={quarterlyFundamentals as Record<string, unknown>[] | undefined} />
          </LazyChart>
          <LazyChart height={250} root={scrollContainerRef}>
            <MarginsChart data={quarterlyFundamentals as Record<string, unknown>[] | undefined} />
          </LazyChart>
        </>
      )}

      {/* EPS Actual vs Estimate */}
      {(earningsSurprises as unknown[])?.length > 0 && (
        <LazyChart height={250} root={scrollContainerRef}>
          <EarningsSurpriseChart data={earningsSurprises as Record<string, unknown>[] | undefined} />
        </LazyChart>
      )}

      {/* Cash Flow */}
      {(cashFlow as unknown[])?.length > 0 && (
        <LazyChart height={250} root={scrollContainerRef}>
          <CashFlowChart data={cashFlow as Record<string, unknown>[] | undefined} />
        </LazyChart>
      )}

      {/* Revenue Breakdown */}
      {((revenueByProduct as Record<string, number> | undefined) && Object.keys(revenueByProduct as Record<string, number>).length > 0) ||
       ((revenueByGeo as Record<string, number> | undefined) && Object.keys(revenueByGeo as Record<string, number>).length > 0) ? (
        <LazyChart height={220} root={scrollContainerRef}>
          <RevenueBreakdownChart revenueByProduct={revenueByProduct as Record<string, number> | undefined} revenueByGeo={revenueByGeo as Record<string, number> | undefined} />
        </LazyChart>
      ) : null}
    </div>
  );
});

interface QuoteStatProps {
  label: string;
  value: string;
}

function QuoteStat({ label, value }: QuoteStatProps): React.ReactElement {
  return (
    <div className="flex justify-between py-0.5">
      <span style={{ opacity: 0.7 }}>{label}</span>
      <span style={{ color: 'var(--color-text-primary)' }}>{value}</span>
    </div>
  );
}

// ─── MarketIndicesChart ─────────────────────────────────────────────

export function MarketIndicesChart({ data }: DataProps): React.ReactElement {
  const { t } = useTranslation();
  const indices = data?.indices as Record<string, Record<string, unknown>> | undefined;
  if (!indices || Object.keys(indices).length === 0) {
    return <div style={{ color: TEXT_COLOR, padding: 16 }}>{t('toolArtifact.noIndexData')}</div>;
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      {Object.entries(indices).map(([symbol, indexData]) => {
        const ohlcv = indexData.ohlcv as Record<string, unknown>[] | undefined;
        const lastClose = ohlcv?.[ohlcv.length - 1]?.close as number | undefined;
        const changePct = (indexData.stats as Record<string, unknown> | undefined)?.period_change_pct as number | undefined;
        const changeColor = (changePct ?? 0) >= 0 ? GREEN : RED;
        const stats = indexData.stats as Record<string, unknown> | undefined;

        return (
          <div
            key={symbol}
            style={{
              background: 'var(--color-bg-surface)',
              border: '1px solid var(--color-border-default)',
              borderRadius: 8,
              padding: '10px 12px',
            }}
          >
            {/* Header: name + price/change + market link */}
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 4, gap: 8 }}>
              <span style={{ color: 'var(--color-text-primary)', fontWeight: 600, fontSize: 13, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', minWidth: 0 }}>
                {(indexData.name as string) || symbol}
              </span>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 13, flexShrink: 0 }}>
                {lastClose != null && (
                  <span style={{ color: 'var(--color-text-primary)', fontWeight: 500 }}>
                    {lastClose.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                  </span>
                )}
                {changePct != null && (
                  <span style={{ color: changeColor, fontWeight: 500 }}>
                    {formatPct(changePct)}
                  </span>
                )}
                <OpenInMarketLink symbol={symbol} />
              </div>
            </div>

            {/* Stats row */}
            {stats && (
              <div style={{ display: 'flex', gap: 12, fontSize: 11, color: TEXT_COLOR, marginBottom: 6 }}>
                {(stats.ma_20 as number | undefined) != null && <span>MA20: {(stats.ma_20 as number).toFixed(2)}</span>}
                {(stats.ma_50 as number | undefined) != null && <span>MA50: {(stats.ma_50 as number).toFixed(2)}</span>}
                {(stats.volatility as number | undefined) != null && <span>Vol: {((stats.volatility as number) * 100).toFixed(1)}%</span>}
              </div>
            )}

            <MiniCandlestick
              ohlcv={((indexData.chart_ohlcv as Record<string, unknown>[] | undefined)?.length ?? 0) > 0 ? indexData.chart_ohlcv as Record<string, unknown>[] : indexData.ohlcv as Record<string, unknown>[] | undefined}
              height={160}
            />
          </div>
        );
      })}
    </div>
  );
}

// ─── StockScreenerTable ──────────────────────────────────────────────

export function StockScreenerTable({ data }: DataProps): React.ReactElement {
  const { t } = useTranslation();
  const { results = [], filters = {}, count = 0 } = (data || {}) as {
    results?: Record<string, unknown>[];
    filters?: Record<string, unknown>;
    count?: number;
  };
  const [sortKey, setSortKey] = useState<string | null>(null);
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc');

  const handleSort = useCallback((key: string) => {
    if (sortKey === key) {
      setSortDir((d) => (d === 'desc' ? 'asc' : 'desc'));
    } else {
      setSortKey(key);
      setSortDir('desc');
    }
  }, [sortKey]);

  const sortedResults = useMemo(() => {
    if (!sortKey) return results as Record<string, unknown>[];
    return [...(results as Record<string, unknown>[])].sort((a, b) => {
      const aVal = a[sortKey];
      const bVal = b[sortKey];
      if (aVal == null && bVal == null) return 0;
      if (aVal == null) return 1;
      if (bVal == null) return -1;
      if (typeof aVal === 'string') return sortDir === 'asc' ? aVal.localeCompare(bVal as string) : (bVal as string).localeCompare(aVal);
      return sortDir === 'asc' ? (aVal as number) - (bVal as number) : (bVal as number) - (aVal as number);
    });
  }, [results, sortKey, sortDir]);

  if (!(results as Record<string, unknown>[]).length) {
    return <div style={{ color: TEXT_COLOR, padding: 16 }}>{t('toolArtifact.noScreenerResults')}</div>;
  }

  const filterTags = Object.entries(filters as Record<string, unknown>).map(([k, v]) => `${k}: ${v}`);

  interface Column {
    key: string;
    label: string;
    width: number;
    format?: (v: unknown) => string;
    color?: (v: unknown) => string;
  }

  const columns: Column[] = [
    { key: 'symbol', label: t('toolArtifact.symbol'), width: 70 },
    { key: 'companyName', label: t('toolArtifact.company'), width: 160 },
    { key: 'price', label: t('toolArtifact.price'), width: 70, format: (v) => v != null ? `$${(v as number).toFixed(2)}` : 'N/A' },
    { key: 'marketCap', label: t('toolArtifact.mktCap'), width: 80, format: (v) => formatNumber(v as number) },
    { key: 'sector', label: t('toolArtifact.sector'), width: 110 },
    { key: 'industry', label: t('toolArtifact.industry'), width: 120 },
    { key: 'beta', label: t('toolArtifact.beta'), width: 55, format: (v) => v != null ? (v as number).toFixed(2) : 'N/A' },
    { key: 'volume', label: t('toolArtifact.volume'), width: 75, format: (v) => v != null ? formatNumber(v as number).replace('$', '') : 'N/A' },
    { key: 'lastAnnualDividend', label: t('toolArtifact.dividend'), width: 65, format: (v) => v != null ? `$${(v as number).toFixed(2)}` : 'N/A' },
    { key: 'exchangeShortName', label: t('toolArtifact.exchange'), width: 70 },
    { key: 'country', label: t('toolArtifact.country'), width: 55 },
    { key: 'changes', label: t('toolArtifact.changePct'), width: 70, format: (v) => v != null ? formatPct(v as number) : 'N/A', color: (v) => v != null ? ((v as number) >= 0 ? GREEN : RED) : TEXT_COLOR },
  ];

  const SortArrow = ({ col }: { col: string }): React.ReactElement | null => {
    if (sortKey !== col) return null;
    return <span style={{ marginLeft: 2, fontSize: 10 }}>{sortDir === 'asc' ? '\u25B2' : '\u25BC'}</span>;
  };

  return (
    <div>
      <h4 style={{ color: 'var(--color-text-primary)', fontSize: 14, fontWeight: 600, marginBottom: 8 }}>
        {t('toolArtifact.stockScreener')} — {t('toolArtifact.nResults', { count })}
      </h4>

      {/* Filter summary */}
      {filterTags.length > 0 && (
        <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', marginBottom: 12 }}>
          {filterTags.map((tag, i) => (
            <span
              key={i}
              style={{
                fontSize: 11,
                padding: '2px 8px',
                borderRadius: 12,
                backgroundColor: 'var(--color-accent-soft)',
                color: 'var(--color-text-primary)',
                border: '1px solid var(--color-accent-soft)',
              }}
            >
              {tag}
            </span>
          ))}
        </div>
      )}

      {/* Scrollable table */}
      <div style={{ overflowX: 'auto', overflowY: 'auto', maxHeight: '70vh' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
          <thead>
            <tr>
              {columns.map((col) => (
                <th
                  key={col.key}
                  onClick={() => handleSort(col.key)}
                  style={{
                    position: 'sticky',
                    top: 0,
                    background: 'var(--color-bg-card)',
                    color: TEXT_COLOR,
                    fontWeight: 500,
                    padding: '6px 8px',
                    textAlign: 'left',
                    borderBottom: '1px solid var(--color-border-muted)',
                    cursor: 'pointer',
                    whiteSpace: 'nowrap',
                    minWidth: col.width,
                    userSelect: 'none',
                  }}
                >
                  {col.label}<SortArrow col={col.key} />
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {sortedResults.map((stock, i) => (
              <tr
                key={(stock.symbol as string) || i}
                style={{
                  borderBottom: '1px solid var(--color-border-muted)',
                }}
                onMouseEnter={(e) => (e.currentTarget.style.backgroundColor = 'var(--color-border-muted)')}
                onMouseLeave={(e) => (e.currentTarget.style.backgroundColor = 'transparent')}
              >
                {columns.map((col) => {
                  const raw = stock[col.key];
                  const display = col.format ? col.format(raw) : ((raw as string) ?? 'N/A');
                  const cellColor = col.color ? col.color(raw) : (col.key === 'symbol' ? 'var(--color-text-primary)' : TEXT_COLOR);
                  return (
                    <td
                      key={col.key}
                      style={{
                        padding: '5px 8px',
                        color: cellColor,
                        fontWeight: col.key === 'symbol' ? 600 : 400,
                        whiteSpace: 'nowrap',
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                        maxWidth: col.key === 'companyName' ? 160 : col.key === 'industry' ? 120 : undefined,
                      }}
                    >
                      {display}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

interface MiniCandlestickProps {
  ohlcv: Record<string, unknown>[] | undefined;
  height?: number;
}

function MiniCandlestick({ ohlcv, height = 180 }: MiniCandlestickProps): React.ReactElement | null {
  const { theme } = useTheme();
  const ct = CANVAS_THEMES[theme] || CANVAS_THEMES.dark;
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);

  // Detect if data is intraday (numeric timestamps are always intraday from our API)
  const firstBar = ohlcv?.[0];
  const firstTime = firstBar && ((firstBar as Record<string, unknown>).time ?? (firstBar as Record<string, unknown>).date);
  const isIntraday = typeof firstTime === 'number' || (typeof firstTime === 'string' && (firstTime.includes(' ') || firstTime.includes('T')));

  useEffect(() => {
    if (!containerRef.current || !ohlcv?.length) return;

    if (chartRef.current) {
      chartRef.current.remove();
      chartRef.current = null;
    }

    const chart = createChart(containerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: ct.bg },
        textColor: ct.text,
      },
      width: containerRef.current.clientWidth,
      height,
      grid: {
        vertLines: { color: ct.grid },
        horzLines: { color: ct.grid },
      },
      rightPriceScale: { borderColor: ct.grid },
      timeScale: { borderColor: ct.grid, timeVisible: isIntraday },
    });
    chartRef.current = chart;

    const series = chart.addCandlestickSeries({
      upColor: ct.up,
      downColor: ct.down,
      borderDownColor: ct.down,
      borderUpColor: ct.up,
      wickDownColor: ct.down,
      wickUpColor: ct.up,
    });
    series.setData(
      ohlcv.map((d) => ({
        time: toChartTime((d as Record<string, unknown>).time ?? (d as Record<string, unknown>).date),
        open: (d as Record<string, unknown>).open as number,
        high: (d as Record<string, unknown>).high as number,
        low: (d as Record<string, unknown>).low as number,
        close: (d as Record<string, unknown>).close as number,
      }))
    );

    chart.timeScale().fitContent();

    const ro = new ResizeObserver(() => {
      if (containerRef.current && chartRef.current) {
        chartRef.current.applyOptions({ width: containerRef.current.clientWidth });
      }
    });
    ro.observe(containerRef.current);

    return () => {
      ro.disconnect();
      if (chartRef.current) {
        chartRef.current.remove();
        chartRef.current = null;
      }
    };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [ohlcv, height, theme]);

  if (!ohlcv?.length) return null;
  return <div ref={containerRef} className="[&_*]:outline-none" style={{ width: '100%', height }} />;
}
