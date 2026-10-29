import { useTranslation } from 'react-i18next';
import { cn } from '@/lib/utils';
import { ease, useSceneTime } from './clock';
import { signColor, useSvgId } from './kit';

/* The charts the scenes draw, each after the app's own: the price card a
   market-data tool leaves in chat (`InlineStockPriceCard`), and the bar
   chart an agent puts in a report or a reply, its own name in the accent
   among grey peers, as the README's figures draw them. Both trace in on the
   scene clock. */

/** `series` across a 100×`h` box, high values up, as a smooth path (the
    monotone curve the app's charts take, here a Catmull-Rom spline). */
function curve(series: number[], h: number, pad = 1.5): { line: string; area: string } {
  const lo = Math.min(...series);
  const hi = Math.max(...series);
  const span = hi - lo || 1;
  const pts = series.map((v, i) => ({
    x: (i / (series.length - 1)) * 100,
    y: pad + (1 - (v - lo) / span) * (h - 2 * pad),
  }));
  let line = `M${pts[0].x},${pts[0].y}`;
  for (let i = 0; i < pts.length - 1; i++) {
    const p0 = pts[Math.max(i - 1, 0)];
    const p1 = pts[i];
    const p2 = pts[i + 1];
    const p3 = pts[Math.min(i + 2, pts.length - 1)];
    const c1 = { x: p1.x + (p2.x - p0.x) / 6, y: p1.y + (p2.y - p0.y) / 6 };
    const c2 = { x: p2.x - (p3.x - p1.x) / 6, y: p2.y - (p3.y - p1.y) / 6 };
    line += ` C${c1.x},${c1.y} ${c2.x},${c2.y} ${p2.x},${p2.y}`;
  }
  return { line, area: `${line} L100,${h} L0,${h} Z` };
}

/**
 * A price line over a fill that fades from 30% to 2%, as the app's price
 * card draws it, traced left to right over `[at, at + dur]`. Fills its box's
 * width; `height` in px.
 */
function PriceArea({
  series,
  color,
  at = 0,
  dur = 900,
  height = 32,
  className,
}: {
  series: number[];
  color: string;
  at?: number;
  dur?: number;
  height?: number;
  className?: string;
}) {
  const id = useSvgId();
  const p = ease(useSceneTime(), at, dur);
  const h = 30;
  const { line, area } = curve(series, h);
  return (
    <svg
      viewBox={`0 0 100 ${h}`}
      preserveAspectRatio="none"
      className={cn('block w-full', className)}
      style={{ height }}
      aria-hidden
    >
      <defs>
        <linearGradient id={`${id}-fill`} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={color} stopOpacity={0.3} />
          <stop offset="100%" stopColor={color} stopOpacity={0.02} />
        </linearGradient>
        <clipPath id={`${id}-clip`}>
          <rect x="0" y="0" width={100 * p} height={h} />
        </clipPath>
      </defs>
      <g clipPath={`url(#${id}-clip)`}>
        <path d={area} fill={`url(#${id}-fill)`} />
        <path
          d={line}
          fill="none"
          stroke={color}
          strokeWidth="1.5"
          vectorEffect="non-scaling-stroke"
          strokeLinejoin="round"
          strokeLinecap="round"
        />
      </g>
    </svg>
  );
}

/** A symbol's price over a period (`InlineStockPriceCard`): symbol, last
    close and the period's change over the range it covers, the area chart,
    then the period's high, low and average volume. */
export function StockPriceCard({
  sym,
  price,
  change,
  period,
  series,
  high,
  low,
  vol,
  at = 0,
}: {
  sym: string;
  price: string;
  /** The period's change, in %. */
  change: number;
  period: string;
  series: number[];
  high: string;
  low: string;
  vol: string;
  /** When the line starts tracing. */
  at?: number;
}) {
  const { t } = useTranslation();
  const color = signColor(change);
  const label = { color: 'var(--color-text-tertiary)' };
  return (
    <div
      className="flex flex-col gap-1 rounded-md border px-2 py-1.5 tabular-nums"
      style={{
        background: 'var(--color-bg-tool-card)',
        borderColor: 'var(--color-border-muted)',
      }}
    >
      {/* wraps in a narrow column, as the app's does: the period drops to
          its own line, flush right */}
      <div className="flex flex-wrap items-baseline gap-x-1 gap-y-0.5 leading-none">
        <span className="text-[8px] font-bold" style={{ color: 'var(--color-text-primary)' }}>
          {sym}
        </span>
        <span className="text-[8px] font-semibold" style={{ color: 'var(--color-text-primary)' }}>
          {price}
        </span>
        <span className="text-[7px] font-semibold" style={{ color }}>
          {change >= 0 ? '+' : ''}
          {change.toFixed(2)}%
        </span>
        <span className="ml-auto whitespace-nowrap text-[6px]" style={label}>
          {period}
        </span>
      </div>
      <PriceArea series={series} color={color} at={at} height={30} />
      {/* wraps whole stats in a narrow column, never a label from its value */}
      <div
        className="flex flex-wrap gap-x-2 gap-y-0.5 whitespace-nowrap text-[6px] leading-none"
        style={label}
      >
        <span>
          {t('toolArtifact.high')}: {high}
        </span>
        <span>
          {t('toolArtifact.low')}: {low}
        </span>
        <span>
          {t('toolArtifact.vol')}: {vol}
        </span>
      </div>
    </div>
  );
}

/** One bar of a `BarChart`. `mark` draws it in the accent: the name the
    chart is about. */
export interface Bar {
  label: string;
  value: number;
  /** As printed at the bar's end. */
  shown: string;
  mark?: boolean;
}

/**
 * A horizontal bar chart as an agent draws one into a report or a reply:
 * labels on the left, grey bars with the subject in the accent, each value
 * at its bar's end, and a dashed line at `median` when given. Bars grow in
 * one after another from `at`, `per` ms apart. Values sit on `surface`, the
 * fill under the chart, so a reference line passing a value runs behind it.
 */
export function BarChart({
  bars,
  max,
  median,
  at = 0,
  per = 70,
  labelWidth = 26,
  surface = 'var(--color-bg-tool-card)',
  className,
}: {
  bars: Bar[];
  max: number;
  median?: number;
  at?: number;
  per?: number;
  /** Width of the label column, in px. */
  labelWidth?: number;
  surface?: string;
  className?: string;
}) {
  const t = useSceneTime();
  return (
    <div className={cn('relative flex flex-col gap-[3px]', className)}>
      {bars.map((bar, i) => {
        const p = ease(t, at + i * per, 520);
        return (
          <div key={bar.label} className="flex items-center gap-1 leading-none">
            <span
              className="shrink-0 truncate text-right text-[6px]"
              style={{
                width: labelWidth,
                color: bar.mark ? 'var(--color-text-primary)' : 'var(--color-text-tertiary)',
                fontWeight: bar.mark ? 600 : 400,
              }}
            >
              {bar.label}
            </span>
            <span className="relative flex h-[5px] flex-1 items-center">
              <span
                className="h-full rounded-r-[1px]"
                style={{
                  // the last 14px of the track hold the value
                  width: `calc((100% - 14px) * ${(bar.value / max) * p})`,
                  background: bar.mark ? 'var(--color-accent-primary)' : 'var(--iv-line-2)',
                }}
              />
              <span
                className="relative z-[1] ml-[2px] shrink-0 px-[1px] text-[5px] tabular-nums"
                style={{
                  background: surface,
                  color: bar.mark ? 'var(--color-text-primary)' : 'var(--color-text-tertiary)',
                  opacity: p > 0.6 ? 1 : 0,
                  transition: 'opacity var(--motion-quick) var(--ease-out)',
                }}
              >
                {bar.shown}
              </span>
            </span>
          </div>
        );
      })}
      {median != null && (
        <span
          className="pointer-events-none absolute bottom-[-2px] top-[-2px] border-l border-dashed"
          style={{
            left: `calc(${labelWidth + 4}px + (100% - ${labelWidth + 4}px - 14px) * ${median / max})`,
            borderColor: 'var(--color-text-tertiary)',
            opacity: ease(t, at + bars.length * per, 300),
          }}
          aria-hidden
        />
      )}
    </div>
  );
}
