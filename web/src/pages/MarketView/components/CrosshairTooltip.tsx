import React from 'react';
import { currencySymbol } from '@/lib/bars';
import type { ValueStore } from '@/lib/valueStore';
import './CrosshairTooltip.css';

interface TooltipData {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume?: number;
  maValues?: Record<number, number>;
  rsiValue?: number | null;
}

export interface CrosshairTooltipState {
  visible: boolean;
  x: number;
  y: number;
  data: TooltipData | null;
}

interface CrosshairTooltipProps {
  visible: boolean;
  x: number;
  y: number;
  data: TooltipData | null;
  containerWidth?: number;
  containerHeight?: number;
  /** ISO currency code — prefixes O/H/L/C with its symbol when provided. */
  currency?: string;
  /** Price decimal places (defaults to 2). */
  decimals?: number;
}

function CrosshairTooltip({ visible, x, y, data, containerWidth, containerHeight, currency, decimals }: CrosshairTooltipProps) {
  if (!visible || !data) return null;

  const isUp = data.close >= data.open;
  const dirColor = isUp ? 'var(--color-profit)' : 'var(--color-loss)';

  // Clamp position to stay within container
  const tooltipWidth = 200;
  const tooltipHeight = 180;
  const clampedX = Math.min(Math.max(x + 16, 0), (containerWidth || 800) - tooltipWidth - 8);
  const clampedY = Math.min(Math.max(y - 10, 0), (containerHeight || 500) - tooltipHeight - 8);

  const priceDecimals = typeof decimals === 'number' && decimals >= 0 ? decimals : 2;
  const priceSymbol = currency ? currencySymbol(currency) : '';
  const formatPrice = (v: number | null | undefined): string => v != null ? `${priceSymbol}${v.toFixed(priceDecimals)}` : '\u2014';
  const formatVol = (v: number | null | undefined): string => {
    if (v == null) return '\u2014';
    if (v >= 1e6) return `${(v / 1e6).toFixed(1)}M`;
    if (v >= 1e3) return `${(v / 1e3).toFixed(1)}K`;
    return String(v);
  };

  const formatDate = (ts: number | null | undefined): string => {
    if (!ts) return '';
    const d = new Date(ts * 1000);
    // Chart times encode venue wall clock as fake UTC — read them back in UTC.
    // A browser-local read shifts the date for users away from the venue.
    return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC' });
  };

  return (
    <div
      className="crosshair-tooltip"
      style={{ left: clampedX, top: clampedY }}
    >
      <div className="crosshair-tooltip-date">{formatDate(data.time)}</div>
      <div className="crosshair-tooltip-grid">
        <span className="crosshair-tooltip-label">O</span>
        <span style={{ color: dirColor }}>{formatPrice(data.open)}</span>
        <span className="crosshair-tooltip-label">H</span>
        <span style={{ color: dirColor }}>{formatPrice(data.high)}</span>
        <span className="crosshair-tooltip-label">L</span>
        <span style={{ color: dirColor }}>{formatPrice(data.low)}</span>
        <span className="crosshair-tooltip-label">C</span>
        <span style={{ color: dirColor }}>{formatPrice(data.close)}</span>
      </div>
      <div className="crosshair-tooltip-row">
        <span className="crosshair-tooltip-label">Vol</span>
        <span>{formatVol(data.volume)}</span>
      </div>
      {data.maValues && Object.keys(data.maValues).length > 0 && (
        <div className="crosshair-tooltip-ma-section">
          {Object.entries(data.maValues).map(([period, val]: [string, number]) => (
            <div className="crosshair-tooltip-row" key={period}>
              <span className="crosshair-tooltip-label">MA{period}</span>
              <span>{val != null ? val.toFixed(2) : '\u2014'}</span>
            </div>
          ))}
        </div>
      )}
      {data.rsiValue != null && (
        <div className="crosshair-tooltip-row">
          <span className="crosshair-tooltip-label">RSI</span>
          <span>{data.rsiValue.toFixed(0)}</span>
        </div>
      )}
    </div>
  );
}

export default React.memo(CrosshairTooltip);

interface CrosshairTooltipLayerProps {
  store: ValueStore<CrosshairTooltipState>;
  containerRef: React.RefObject<HTMLDivElement | null>;
  currency?: string;
  decimals?: number;
}

/**
 * Subscribes to the crosshair store so per-mousemove updates re-render only
 * this leaf, not the chart component that owns the lightweight-charts
 * subscription. All props are identity-stable — the memo shields the layer
 * from parent re-renders while the store drives its own.
 */
export const CrosshairTooltipLayer = React.memo(function CrosshairTooltipLayer({
  store,
  containerRef,
  currency,
  decimals,
}: CrosshairTooltipLayerProps) {
  const state = React.useSyncExternalStore(store.subscribe, store.get);
  return (
    <CrosshairTooltip
      visible={state.visible}
      x={state.x}
      y={state.y}
      data={state.data}
      containerWidth={containerRef.current?.clientWidth}
      containerHeight={containerRef.current?.clientHeight}
      currency={currency}
      decimals={decimals}
    />
  );
});

