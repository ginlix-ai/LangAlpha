/**
 * Inline preview for an agent ``chart_annotation`` artifact in the chat
 * transcript.
 *
 * On the standalone ChatAgent page there is no chart, so this renders a live
 * lightweight-charts mini candlestick with the annotations drawn on it; the
 * card expands into MarketView (carrying symbol + thread + workspace so the
 * conversation continues there). Inside the MarketView desktop panel the real
 * chart already shows the drawing live, so the card collapses to a one-line
 * confirmation chip (see ChartSurfaceContext).
 */

import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useLocation, useNavigate, useParams } from 'react-router-dom';
import { createChart, ColorType, CrosshairMode, LineStyle } from 'lightweight-charts';
import type { IChartApi, ISeriesApi, Time } from 'lightweight-charts';
import { LineChart, ExternalLink, Check } from 'lucide-react';

import type { ChartDataPoint } from '@/types/market';
import { fetchStockData } from '@/pages/MarketView/utils/api';
import type { StoredAnnotation } from '@/pages/MarketView/stores/chartAnnotationStore';
import {
  chartAnnotationStore,
  makeChartId,
  useDisplayCleared,
} from '@/pages/MarketView/stores/chartAnnotationStore';
import { AgentAnnotationsPrimitive } from '@/pages/MarketView/utils/agentAnnotationsPrimitive';
import {
  DEFAULT_LINE_COLOR,
  DEFAULT_TRENDLINE_COLOR,
  buildMarkers,
  buildPrimitiveData,
  isPriceLine,
  isTrendline,
  resolveTrendlineData,
  styleToLwc,
  toUnixSeconds,
} from '@/pages/MarketView/utils/annotationGeometry';

import { useWorkspaceId } from '../../contexts/WorkspaceContext';
import { useChartSurface } from '../../contexts/ChartSurfaceContext';

const CARD_BG = 'var(--color-bg-tool-card)';
const CARD_BORDER = 'var(--color-border-muted)';
const TEXT_COLOR = 'var(--color-text-tertiary)';
const ACCENT = 'var(--color-accent-primary)';
const CHART_HEIGHT = 184;

interface InlineChartAnnotationCardProps {
  artifact: Record<string, unknown> | null | undefined;
  onClick?: () => void;
}

/** Gather every ISO time referenced by an annotation set (for chart range). */
function collectTimes(annotations: StoredAnnotation[]): number[] {
  const out: number[] = [];
  for (const a of annotations) {
    if ('time' in a && typeof a.time === 'string') {
      const t = toUnixSeconds(a.time);
      if (t != null) out.push(t);
    }
    if ('point1' in a && 'point2' in a) {
      const t1 = toUnixSeconds(a.point1.time);
      const t2 = toUnixSeconds(a.point2.time);
      if (t1 != null) out.push(t1);
      if (t2 != null) out.push(t2);
    }
  }
  return out;
}

function fmtDate(unixSeconds: number): string {
  return new Date(unixSeconds * 1000).toISOString().slice(0, 10);
}

/** Resolve a chart color from a CSS variable on the element, with fallback. */
function cssVar(el: HTMLElement, name: string, fallback: string): string {
  const v = getComputedStyle(el).getPropertyValue(name).trim();
  return v || fallback;
}

export function InlineChartAnnotationCard({
  artifact,
}: InlineChartAnnotationCardProps): React.ReactElement | null {
  const navigate = useNavigate();
  const location = useLocation();
  const params = useParams();
  const ctxWorkspaceId = useWorkspaceId();
  const { chartPresent } = useChartSurface();

  const symbol = ((artifact?.symbol as string) || '').toUpperCase();
  const timeframe = (artifact?.timeframe as string) || '1day';
  const annotations = useMemo(
    () => (artifact?.annotations as StoredAnnotation[] | undefined) ?? [],
    [artifact],
  );
  const workspaceId = (artifact?.workspace_id as string | undefined) || ctxWorkspaceId || undefined;
  const threadId = params.threadId as string | undefined;

  // Whether this instance is currently cleared from the chart (MarketView only).
  const displayCleared = useDisplayCleared(workspaceId, symbol, timeframe);

  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const [status, setStatus] = useState<'loading' | 'ready' | 'error'>('loading');

  // Stable dependency key so the chart only rebuilds when the set changes.
  const annKey = useMemo(
    () => annotations.map((a) => a.annotation_id).join(','),
    [annotations],
  );

  // Re-apply a cleared drawing to the adjacent MarketView chart.
  const handleRestore = useCallback(() => {
    if (!workspaceId || !symbol) return;
    chartAnnotationStore.restoreDisplay(workspaceId, makeChartId(symbol, timeframe));
  }, [workspaceId, symbol, timeframe]);

  const handleExpand = useCallback(() => {
    if (!symbol) return;
    const sp = new URLSearchParams();
    sp.set('symbol', symbol);
    sp.set('tf', timeframe);
    sp.set('mode', 'ptc');
    if (workspaceId) sp.set('ws', workspaceId);
    if (threadId && threadId !== '__default__') sp.set('thread', threadId);
    sp.set('returnTo', location.pathname + location.search);
    navigate(`/market?${sp.toString()}`);
  }, [symbol, timeframe, workspaceId, threadId, location, navigate]);

  // Fetch data + build the mini chart. Skipped entirely when a real chart is
  // present (MarketView) — the chip variant renders instead.
  useEffect(() => {
    if (chartPresent || !symbol) return;
    let cancelled = false;
    const controller = new AbortController();
    setStatus('loading');

    (async () => {
      // Range: span the annotation times (padded), else backend default.
      const times = collectTimes(annotations);
      let fromDate: string | undefined;
      let toDate: string | undefined;
      if (times.length > 0) {
        const min = Math.min(...times);
        const max = Math.max(...times);
        const span = Math.max(max - min, 30 * 86400);
        const pad = Math.round(span * 0.2);
        fromDate = fmtDate(min - pad);
        toDate = fmtDate(max + pad + 5 * 86400);
      }

      const result = await fetchStockData(symbol, timeframe, fromDate, toDate, {
        signal: controller.signal,
      });
      if (cancelled) return;

      const data = result.data as ChartDataPoint[];
      const container = containerRef.current;
      if (result.error || !data?.length || !container) {
        setStatus('error');
        return;
      }

      // Tear down a prior chart (set changed / re-render).
      if (chartRef.current) {
        chartRef.current.remove();
        chartRef.current = null;
      }

      let chart: IChartApi;
      try {
        chart = createChart(container, {
          layout: {
            background: { type: ColorType.Solid, color: cssVar(container, '--color-bg-tool-card', '#0e0f12') },
            textColor: cssVar(container, '--color-text-tertiary', '#8b8f98'),
          },
          autoSize: true,
          grid: {
            vertLines: { color: cssVar(container, '--color-border-muted', '#23262e') },
            horzLines: { color: cssVar(container, '--color-border-muted', '#23262e') },
          },
          crosshair: { mode: CrosshairMode.Hidden },
          handleScroll: false,
          handleScale: false,
          rightPriceScale: { borderColor: cssVar(container, '--color-border-muted', '#23262e') },
          timeScale: {
            borderColor: cssVar(container, '--color-border-muted', '#23262e'),
            timeVisible: false,
          },
        });
      } catch {
        setStatus('error');
        return;
      }
      chartRef.current = chart;

      const upColor = cssVar(container, '--color-profit', '#0FEDBE');
      const downColor = cssVar(container, '--color-loss', '#FF383C');
      const series: ISeriesApi<'Candlestick'> = chart.addCandlestickSeries({
        upColor,
        downColor,
        borderVisible: false,
        wickUpColor: upColor,
        wickDownColor: downColor,
      });
      series.setData(
        data.map((d) => ({ time: d.time as Time, open: d.open, high: d.high, low: d.low, close: d.close })),
      );

      // Native annotations: price lines + trendlines.
      for (const ann of annotations) {
        if (isPriceLine(ann)) {
          series.createPriceLine({
            price: ann.price,
            title: ann.label ?? '',
            color: ann.color ?? DEFAULT_LINE_COLOR,
            lineWidth: 1,
            lineStyle: styleToLwc(ann.style),
            axisLabelVisible: true,
            lineVisible: true,
          });
        } else if (isTrendline(ann)) {
          const lineData = resolveTrendlineData(ann, data);
          if (!lineData) continue;
          const ls = chart.addLineSeries({
            color: ann.color ?? DEFAULT_TRENDLINE_COLOR,
            lineWidth: 2,
            lineStyle: LineStyle.Dashed,
            lastValueVisible: false,
            priceLineVisible: false,
            crosshairMarkerVisible: false,
            // Label drawn as a chip at the line end (buildPrimitiveData).
          });
          ls.setData(lineData);
        }
      }

      // Markers + canvas-primitive shapes (rect / vline / text / fib).
      const markers = buildMarkers(annotations, data);
      if (markers.length) series.setMarkers(markers);
      const prim = new AgentAnnotationsPrimitive();
      series.attachPrimitive(prim);
      prim.setTheme(
        document.documentElement.getAttribute('data-theme') === 'light' ? 'light' : 'dark',
      );
      prim.setData(buildPrimitiveData(annotations, data));

      chart.timeScale().fitContent();
      setStatus('ready');
    })();

    return () => {
      cancelled = true;
      controller.abort();
      if (chartRef.current) {
        chartRef.current.remove();
        chartRef.current = null;
      }
    };
  }, [chartPresent, symbol, timeframe, annKey, annotations]);

  if (!artifact || !symbol) return null;

  const count = annotations.length;
  const countLabel = `${count} annotation${count === 1 ? '' : 's'}`;

  // Inside MarketView: the real chart shows the drawing — collapse to a chip.
  // The chip is clickable: when the user has cleared the drawing from the chart,
  // clicking re-applies it; otherwise it's a confirmation that it's on the chart.
  if (chartPresent) {
    return (
      <button
        type="button"
        onClick={handleRestore}
        title={displayCleared ? 'Show these annotations on the chart' : 'Shown on the chart'}
        style={{
          display: 'inline-flex',
          alignItems: 'center',
          gap: 8,
          background: CARD_BG,
          border: `1px solid ${displayCleared ? ACCENT : CARD_BORDER}`,
          borderRadius: 8,
          padding: '6px 12px',
          fontSize: 12,
          color: TEXT_COLOR,
          cursor: 'pointer',
          transition: 'border-color 0.15s',
        }}
        onMouseEnter={(e) => (e.currentTarget.style.borderColor = ACCENT)}
        onMouseLeave={(e) =>
          (e.currentTarget.style.borderColor = displayCleared ? ACCENT : CARD_BORDER)
        }
      >
        {displayCleared
          ? <LineChart size={14} style={{ color: ACCENT, flexShrink: 0 }} />
          : <Check size={14} style={{ color: 'var(--color-profit)', flexShrink: 0 }} />}
        <span>
          <span style={{ color: 'var(--color-text-primary)', fontWeight: 600 }}>{symbol}</span>
          <span style={{ color: TEXT_COLOR }}>{` · ${timeframe}`}</span>
          {' · '}
          {displayCleared ? `Show ${countLabel} on chart` : `${countLabel} on chart`}
        </span>
      </button>
    );
  }

  return (
    <div
      role="button"
      tabIndex={0}
      onClick={handleExpand}
      onKeyDown={(e) => {
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault();
          handleExpand();
        }
      }}
      style={{
        background: CARD_BG,
        border: `1px solid ${CARD_BORDER}`,
        borderRadius: 8,
        padding: 10,
        cursor: 'pointer',
        transition: 'border-color 0.15s',
        outline: 'none',
        userSelect: 'none',
      }}
      onMouseEnter={(e) => (e.currentTarget.style.borderColor = ACCENT)}
      onMouseLeave={(e) => (e.currentTarget.style.borderColor = CARD_BORDER)}
    >
      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
        <LineChart size={15} style={{ color: ACCENT, flexShrink: 0 }} />
        <span style={{ fontWeight: 700, color: 'var(--color-text-primary)', fontSize: 14 }}>{symbol}</span>
        <span style={{ fontSize: 11, color: TEXT_COLOR }}>{timeframe}</span>
        <span style={{ fontSize: 11, color: TEXT_COLOR }}>·</span>
        <span style={{ fontSize: 11, color: TEXT_COLOR }}>{countLabel}</span>
        <span
          style={{
            marginLeft: 'auto',
            display: 'inline-flex',
            alignItems: 'center',
            gap: 4,
            fontSize: 11,
            fontWeight: 600,
            color: ACCENT,
          }}
        >
          Open in MarketView
          <ExternalLink size={12} />
        </span>
      </div>

      {/* Chart / fallback */}
      {status === 'error' ? (
        <div style={{ fontSize: 12, color: TEXT_COLOR, padding: '12px 2px' }}>
          Chart preview unavailable — open in MarketView to view the {countLabel}.
        </div>
      ) : (
        <div style={{ position: 'relative', width: '100%', height: CHART_HEIGHT }}>
          <div ref={containerRef} className="[&_*]:outline-none" style={{ width: '100%', height: CHART_HEIGHT }} />
          {status === 'loading' && (
            <div
              style={{
                position: 'absolute',
                inset: 0,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                fontSize: 12,
                color: TEXT_COLOR,
              }}
            >
              Loading chart…
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export default InlineChartAnnotationCard;
