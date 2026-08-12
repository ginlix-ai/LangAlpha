/**
 * A compact area/line preview of a stock's price — the resting state of the
 * chat annotation card. Built as a plain SVG from fetched OHLC bars (no
 * lightweight-charts), so it's cheap to render inline in the transcript.
 * Theme-aware via CSS variables.
 */

import React, { useId, useMemo } from 'react';

import type { ChartDataPoint } from '@/types/market';

const PAD_L = 6;
const PAD_R = 6;
const PAD_T = 8;
const PAD_B = 6;
const VB_W = 600;
const VB_H = 200;

interface AnnotationPreviewChartProps {
  bars: ChartDataPoint[];
  /** Area / line color (price up vs down over the window). */
  trendColor: string;
  /** Pulse a dot at the latest close. Parent must be ``position: relative``. */
  showLastPrice?: boolean;
}

export function AnnotationPreviewChart({
  bars,
  trendColor,
  showLastPrice = false,
}: AnnotationPreviewChartProps): React.ReactElement | null {
  const gradId = useId();

  const model = useMemo(() => {
    if (bars.length < 2) return null;

    let pmin = Infinity;
    let pmax = -Infinity;
    for (const b of bars) {
      if (b.low < pmin) pmin = b.low;
      if (b.high > pmax) pmax = b.high;
    }
    if (!Number.isFinite(pmin) || !Number.isFinite(pmax)) return null;

    const span = pmax - pmin || 1;
    pmin -= span * 0.06;
    pmax += span * 0.06;

    const innerW = VB_W - PAD_L - PAD_R;
    const innerH = VB_H - PAD_T - PAD_B;
    const n = bars.length;
    const x = (i: number) => PAD_L + (n > 1 ? i / (n - 1) : 0.5) * innerW;
    const y = (p: number) => PAD_T + innerH - ((p - pmin) / (pmax - pmin)) * innerH;

    const linePts = bars.map((b, i) => `${x(i).toFixed(1)},${y(b.close).toFixed(1)}`).join(' ');
    const areaPath = `M${PAD_L},${(PAD_T + innerH).toFixed(1)} L${linePts} L${(PAD_L + innerW).toFixed(1)},${(PAD_T + innerH).toFixed(1)} Z`;
    // Latest close, as a % of the viewBox, for the (non-distorting) HTML dot overlay.
    const lastXPct = (x(n - 1) / VB_W) * 100;
    const lastYPct = (y(bars[n - 1].close) / VB_H) * 100;
    return { linePts, areaPath, lastXPct, lastYPct };
  }, [bars]);

  if (!model) return null;

  return (
    <>
      <svg viewBox={`0 0 ${VB_W} ${VB_H}`} preserveAspectRatio="none" style={{ width: '100%', height: '100%', display: 'block' }} aria-hidden="true">
        <defs>
          <linearGradient id={gradId} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0" stopColor={trendColor} stopOpacity={0.2} />
            <stop offset="1" stopColor={trendColor} stopOpacity={0} />
          </linearGradient>
        </defs>
        <path d={model.areaPath} fill={`url(#${gradId})`} />
        <polyline
          points={model.linePts}
          fill="none"
          stroke={trendColor}
          strokeWidth={2}
          strokeLinejoin="round"
          vectorEffect="non-scaling-stroke"
        />
      </svg>
      {showLastPrice && (
        <span
          aria-hidden="true"
          style={{
            position: 'absolute',
            left: `${model.lastXPct}%`,
            top: `${model.lastYPct}%`,
            width: 8,
            height: 8,
            transform: 'translate(-50%, -50%)',
            pointerEvents: 'none',
          }}
        >
          <span
            className="animate-ping motion-reduce:animate-none"
            style={{ position: 'absolute', inset: 0, borderRadius: 9999, backgroundColor: trendColor, opacity: 0.55 }}
          />
          <span
            style={{
              position: 'absolute',
              inset: 0,
              borderRadius: 9999,
              backgroundColor: trendColor,
              boxShadow: `0 0 6px ${trendColor}`,
            }}
          />
        </span>
      )}
    </>
  );
}

export default AnnotationPreviewChart;
