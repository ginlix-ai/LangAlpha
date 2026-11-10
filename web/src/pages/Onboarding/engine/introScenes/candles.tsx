import type { ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { MessageSquarePlus, X } from 'lucide-react';
import { PAST, useSceneTime } from './clock';
import { useSvgId } from './kit';
import { cardPosition, type CardMetrics } from '@/pages/MarketView/utils/layerCardPosition';
import {
  AXIS_W,
  TIME_H,
  TIME_MARKS,
  type ChartEvent,
  type ChartScale,
  type Series,
} from './nvdaSeries';

/* The candlestick chart the market scenes and the file panel's chart tab
   share: MarketChart in miniature, as lightweight-charts paints it with the
   app's chart theme. Candles over a volume histogram in the bottom fifth of
   the price pane, MA20 and MA50, an RSI pane beneath, the price axis on the
   right with the last price on it, the time axis below. Over it go the
   agent's chart layer, a label on the bar each piece of news hit, and the
   user's selection (chartTools.tsx). The series and its scale are nvdaSeries.ts. */

/* ── the chart ─────────────────────────────────────────────────────────── */

const GRID = 'var(--color-border-default)';
const AXIS_TEXT = 'var(--color-text-secondary)';
const UP = 'var(--color-profit)';
const DOWN = 'var(--color-loss)';

/** A value on the price axis in a filled box, as the chart marks its last
    price and each price line. */
function AxisLabel({ x, y, fill, text }: { x: number; y: number; fill: string; text: string }) {
  return (
    <g>
      <rect x={x} y={y - 4} width={AXIS_W} height={8} rx={1} fill={fill} />
      <text x={x + 3} y={y + 1.8} fontSize={5} fill="#fff" className="tabular-nums">
        {text}
      </text>
    </g>
  );
}

/** The chart: `s` laid out by `k`. `children` are overlays, positioned in
    the chart's px. */
export function Candles({
  series: s,
  scale: k,
  children,
}: {
  series: Series;
  scale: ChartScale;
  children?: ReactNode;
}) {
  const id = useSvgId();
  const { bars } = s;
  const n = bars.length;
  const end = bars[n - 1];
  const close = end.c;
  const vmax = Math.max(...bars.map((b) => b.v));
  const volTop = k.priceH * 0.8;
  const body = Math.max(k.step * 0.68, 1.2);
  const plotH = k.height - TIME_H;
  const line = (vals: number[], f: (v: number) => number) =>
    vals.map((v, i) => `${k.x(i).toFixed(2)},${f(v).toFixed(2)}`).join(' ');
  const rsiLine = line(s.rsi, k.ry);
  const rsiArea = `${rsiLine} ${k.x(n - 1)},${k.rsiTop + k.rsiH} ${k.x(0)},${k.rsiTop + k.rsiH}`;
  const rsiNow = s.rsi[n - 1];
  return (
    // overlays may run past the chart, as a badge's detail does over the toolbar
    <div
      className="relative shrink-0"
      style={{ width: k.width, height: k.height, background: 'var(--color-bg-card)' }}
    >
      <svg width={k.width} height={k.height} className="absolute inset-0 block" aria-hidden>
        <defs>
          <linearGradient id={`${id}-rsi`} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="var(--color-accent-primary)" stopOpacity={0.2} />
            <stop offset="100%" stopColor="var(--color-accent-primary)" stopOpacity={0.02} />
          </linearGradient>
        </defs>
        {/* grid */}
        {k.ticks.map((tick) => (
          <line
            key={tick}
            x1={0}
            x2={k.paneW}
            y1={k.y(tick)}
            y2={k.y(tick)}
            stroke={GRID}
            strokeWidth={0.5}
          />
        ))}
        {TIME_MARKS.map(({ i }) => (
          <line key={i} x1={k.x(i)} x2={k.x(i)} y1={0} y2={plotH} stroke={GRID} strokeWidth={0.5} />
        ))}
        <g>
          {/* volume, in the bottom fifth of the price pane */}
          {bars.map((b, i) => {
            const h = (b.v / vmax) * (k.priceH - volTop);
            return (
              <rect
                key={`v${i}`}
                x={k.x(i) - body / 2}
                y={k.priceH - h}
                width={body}
                height={h}
                fill={b.c >= b.o ? UP : DOWN}
                fillOpacity={0.3}
              />
            );
          })}
          {/* candles: no borders, wicks in the body's color */}
          {bars.map((b, i) => {
            const color = b.c >= b.o ? UP : DOWN;
            const top = k.y(Math.max(b.o, b.c));
            return (
              <g key={`c${i}`}>
                <line
                  x1={k.x(i)}
                  x2={k.x(i)}
                  y1={k.y(b.h)}
                  y2={k.y(b.l)}
                  stroke={color}
                  strokeWidth={0.7}
                />
                <rect
                  x={k.x(i) - body / 2}
                  y={top}
                  width={body}
                  height={Math.max(k.y(Math.min(b.o, b.c)) - top, 0.7)}
                  fill={color}
                />
              </g>
            );
          })}
          {s.ma.map((m) => (
            <polyline
              key={m.color}
              points={line(m.values, k.y)}
              fill="none"
              stroke={m.color}
              strokeWidth={0.9}
              strokeLinejoin="round"
            />
          ))}
          <polygon points={rsiArea} fill={`url(#${id}-rsi)`} />
          <polyline
            points={rsiLine}
            fill="none"
            stroke="var(--color-accent-primary)"
            strokeWidth={1}
            strokeLinejoin="round"
          />
        </g>
        {/* the last price's line, dotted in the last bar's color */}
        <line
          x1={0}
          x2={k.paneW}
          y1={k.y(close)}
          y2={k.y(close)}
          stroke={close >= end.o ? UP : DOWN}
          strokeWidth={0.6}
          strokeDasharray="1 1.5"
        />
        {/* pane separator, then the axes' borders */}
        <line x1={0} x2={k.width} y1={k.priceH + 0.5} y2={k.priceH + 0.5} stroke={GRID} />
        <line x1={k.paneW + 0.5} x2={k.paneW + 0.5} y1={0} y2={plotH} stroke={GRID} />
        <line x1={0} x2={k.width} y1={plotH + 0.5} y2={plotH + 0.5} stroke={GRID} />
        {k.ticks
          .filter((tick) => Math.abs(k.y(tick) - k.y(close)) > 8)
          .map((tick) => (
            <text
              key={tick}
              x={k.paneW + 3}
              y={k.y(tick) + 1.8}
              fontSize={5}
              fill={AXIS_TEXT}
              className="tabular-nums"
            >
              {tick.toFixed(2)}
            </text>
          ))}
        <AxisLabel
          x={k.paneW}
          y={k.y(close)}
          fill={close >= end.o ? UP : DOWN}
          text={close.toFixed(2)}
        />
        {/* the RSI pane: its label top left, its value on its own axis */}
        <text x={3} y={k.rsiTop + 6.5} fontSize={5} fill={AXIS_TEXT}>
          RSI (14)
        </text>
        <AxisLabel
          x={k.paneW}
          y={k.ry(rsiNow)}
          fill="var(--color-accent-primary)"
          text={rsiNow.toFixed(0)}
        />
        {TIME_MARKS.map(({ i, label }) => (
          <text key={i} x={k.x(i)} y={plotH + 7} fontSize={5} fill={AXIS_TEXT} textAnchor="middle">
            {label}
          </text>
        ))}
      </svg>
      {children}
    </div>
  );
}

/* ── the agent's layer ─────────────────────────────────────────────────── */

/** A layer element's color when the agent names none: the layer theme's
    neutral. */
const LAYER_COLOR = 'var(--color-info)';
/** A label's center keeps this far from the pane's edges. */
const EDGE_X = 15;
/** A label's chip sits this far above its point, center to point. */
const CHIP_RISE = 7;

/** Where a pointer lands on a label, in chart px: on its chip, over its bar.
    The labels the scenes point at sit clear of the pane's edges. */
// eslint-disable-next-line react-refresh/only-export-components -- the anchor travels with the label's geometry
export function labelAnchor(k: ChartScale, el: ChartEvent): [number, number] {
  return [k.x(el.i), k.y(el.price) - CHIP_RISE];
}

/**
 * A label in the agent's chart layer as the layer renderer paints it
 * (`chartLayer/kinds/label`), at half the app's size: a dot on the bar and
 * price, and the text on a pill chip above it with the element's color as its
 * dot. Pops in at `at`. `hover` gives the chip the element's edge and opens
 * the explain card (`ChartLayerOverlay`) with its hint; `open` pins the card,
 * with Ask about this. `layer` is the layer's title, which the card names.
 */
export function LayerLabel({
  scale: k,
  el,
  layer,
  at = PAST,
  hover = false,
  open = false,
}: {
  scale: ChartScale;
  el: ChartEvent;
  layer: string;
  at?: number;
  hover?: boolean;
  open?: boolean;
}) {
  if (useSceneTime() < at) return null;
  const x = Math.max(EDGE_X, Math.min(k.x(el.i), k.paneW - EDGE_X));
  const y = k.y(el.price);
  return (
    <>
      <span className="absolute" style={{ left: x, top: y }}>
        <span
          className="intro-pop absolute block h-[2.5px] w-[2.5px] rounded-full"
          style={{ left: -1.25, top: -1.25, background: LAYER_COLOR }}
        />
        {/* centered on the anchor; the pop animates the chip's own transform */}
        <span
          className="absolute left-0 -translate-x-1/2 -translate-y-1/2"
          style={{ top: -CHIP_RISE }}
        >
          <span
            className="intro-pop flex h-[9.5px] items-center gap-[2.5px] whitespace-nowrap rounded-full border px-[4px] text-[5.5px] font-semibold leading-none transition-colors"
            style={{
              background: 'var(--color-bg-elevated)',
              borderColor: hover || open ? LAYER_COLOR : 'var(--color-border-elevated)',
              color: 'var(--color-text-primary)',
              boxShadow: '0 0.5px 3px rgba(0, 0, 0, 0.18)',
            }}
          >
            <span
              className="h-[3px] w-[3px] shrink-0 rounded-full"
              style={{ background: LAYER_COLOR }}
            />
            {el.title}
          </span>
        </span>
      </span>
      {(hover || open) && (
        <ExplainCard k={k} el={el} layer={layer} at={[x, y - CHIP_RISE]} pinned={open} />
      )}
    </>
  );
}

/** The overlay's card at half size; its height as placed, before it is measured. */
const CARD: CardMetrics = { width: 140, height: 60, gap: 7, edge: 4 };

/** The explain card (`.chart-layer-card`, half size): the element's kind and
    its layer, its text, the agent's note; then the hint while it is only
    hovered, or Ask about this once pinned. Up and to the left of the
    pointer, as the overlay places it, else beside or below it. */
function ExplainCard({
  k,
  el,
  layer,
  at: [px, py],
  pinned,
}: {
  k: ChartScale;
  el: ChartEvent;
  layer: string;
  at: [number, number];
  pinned: boolean;
}) {
  const { t } = useTranslation();
  const { left, top } = cardPosition(px, py, k.width, k.height, CARD);
  return (
    <span
      className="intro-pop absolute z-10 flex flex-col gap-[2px] rounded-[4px] border px-[6px] py-[5px]"
      style={{
        left,
        top,
        width: CARD.width,
        background: 'var(--color-bg-elevated)',
        borderColor: 'var(--color-border-muted)',
        boxShadow: 'var(--shadow-card)',
      }}
    >
      <span
        className="flex min-w-0 items-center gap-[3px] font-mono text-[5px] leading-none tracking-[0.02em]"
        style={{ color: 'var(--color-text-tertiary)' }}
      >
        <span
          className="h-[3.5px] w-[3.5px] shrink-0 rounded-full"
          style={{ background: LAYER_COLOR }}
        />
        <span className="shrink-0 uppercase">{t('marketView.layerExplain.types.label')}</span>
        <span className="shrink-0">·</span>
        <span className="min-w-0 truncate">{layer}</span>
        {pinned && <X className="ml-auto h-[6.5px] w-[6.5px] shrink-0" />}
      </span>
      <span
        className="text-[6.5px] font-semibold leading-[1.35]"
        style={{ color: 'var(--color-text-primary)' }}
      >
        {el.title}
      </span>
      {el.note && (
        <span className="text-[6px] leading-[1.5]" style={{ color: 'var(--color-text-secondary)' }}>
          {el.note}
        </span>
      )}
      {pinned ? (
        <span
          className="mt-[3px] flex h-[13px] w-fit items-center gap-[3px] rounded-full border px-[5px] text-[6px] font-medium leading-none"
          style={{ borderColor: 'var(--color-border-elevated)', color: 'var(--color-text-primary)' }}
        >
          <MessageSquarePlus className="h-[6.5px] w-[6.5px]" />
          {t('marketView.layerExplain.ask')}
        </span>
      ) : (
        <span
          className="mt-px text-[5.5px] leading-none"
          style={{ color: 'var(--color-text-quaternary)' }}
        >
          {t('marketView.layerExplain.pinHint')}
        </span>
      )}
    </span>
  );
}
