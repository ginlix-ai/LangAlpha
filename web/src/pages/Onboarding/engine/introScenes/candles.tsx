import type { ReactNode } from 'react';
import { PAST, useSceneTime } from './clock';
import { useSvgId } from './kit';
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
   agent's event badges, each pinned to the bar its news hit, and the user's
   selection (chartTools.tsx). The series and its scale are nvdaSeries.ts. */

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

/* ── the agent's event badges ──────────────────────────────────────────── */

/** An event's color when the agent names none (`DEFAULT_EVENT_COLOR`): the
    annotation amber, apart from the slate blue of technical marks. */
const EVENT_COLOR = '#E9954A';
/** A badge's center keeps this far from the pane's edges (`EDGE_X`). */
const EDGE_X = 15;
/** An anchor higher than this hangs its badge under it (`FLIP_Y`). */
const FLIP_Y = 22;

/** Where a click lands on an event's badge, in chart px: over its bar, at
    the middle of a badge hung above its dot (the 4.5px gap and half the
    badge's height). The badges the scenes click sit clear of the pane's
    edges and its top, so neither clamp nor flip applies. */
// eslint-disable-next-line react-refresh/only-export-components -- the anchor travels with the badge's geometry
export function badgeAnchor(k: ChartScale, ev: ChartEvent): [number, number] {
  return [k.x(ev.i), k.y(ev.price) - 10.5];
}

/**
 * An event annotation as `AgentEventOverlay` draws it, at half the app's
 * size: a dot ringed in the card color on the bar and price, and the
 * headline on an elevated badge whose caret points down at the dot (or up,
 * hung under a dot near the pane's top). Pops in at `at`. `hover` gives it
 * the accent edge; `open` shows the detail in a popover beside it.
 */
export function EventBadge({
  scale: k,
  ev,
  at = PAST,
  hover = false,
  open = false,
}: {
  scale: ChartScale;
  ev: ChartEvent;
  at?: number;
  hover?: boolean;
  open?: boolean;
}) {
  if (useSceneTime() < at) return null;
  const x = Math.max(EDGE_X, Math.min(k.x(ev.i), k.paneW - EDGE_X));
  const y = k.y(ev.price);
  const below = y < FLIP_Y;
  const edge = hover || open ? 'var(--color-accent-primary)' : 'var(--color-border-muted)';
  return (
    <span className="absolute" style={{ left: x, top: y, zIndex: open ? 1 : undefined }}>
      <span
        className="intro-pop absolute block h-[3.5px] w-[3.5px] rounded-full"
        style={{
          left: -1.75,
          top: -1.75,
          background: EVENT_COLOR,
          boxShadow: '0 0 0 1px var(--color-bg-card)',
        }}
      />
      {/* centered on the anchor; the pop animates the badge's own transform */}
      <span
        className="absolute left-0 -translate-x-1/2"
        style={below ? { top: 4.5 } : { bottom: 4.5 }}
      >
        <span
          className="intro-pop relative flex items-center gap-[3px] whitespace-nowrap rounded-[3px] border px-[4.5px] py-[1.5px] text-[5.5px] font-semibold leading-[1.3]"
          style={{
            background: 'var(--color-bg-elevated)',
            borderColor: edge,
            color: 'var(--color-text-primary)',
            boxShadow: '0 1px 3px rgba(0, 0, 0, 0.18)',
          }}
        >
          <span
            className="h-[3.5px] w-[3.5px] shrink-0 rounded-full"
            style={{ background: EVENT_COLOR }}
          />
          {ev.title}
          <span
            className="absolute left-1/2 h-[3.5px] w-[3.5px] -translate-x-1/2 rotate-45"
            style={{
              ...(below ? { top: -2 } : { bottom: -2 }),
              background: 'var(--color-bg-elevated)',
              borderColor: edge,
              borderStyle: 'solid',
              borderWidth: below ? '1px 0 0 1px' : '0 1px 1px 0',
            }}
          />
        </span>
        {open && ev.detail && (
          <EventDetail
            ev={ev}
            below={below}
            // kept inside the chart, as the popover's collision padding keeps it on screen
            shift={Math.max(DETAIL_W / 2 + 2, Math.min(x, k.width - DETAIL_W / 2 - 2)) - x}
          />
        )}
      </span>
    </span>
  );
}

const DETAIL_W = 140;

/** The badge's detail (`agent-event-pop`, a popover on its far side from the
    dot): the dot and headline, then the story in a few sentences. `shift`
    moves it off center. */
function EventDetail({ ev, below, shift }: { ev: ChartEvent; below: boolean; shift: number }) {
  return (
    <span
      className="intro-pop absolute left-1/2 z-10 flex flex-col gap-[3.5px] whitespace-normal rounded-[3px] border px-[7px] py-[6px]"
      style={{
        ...(below ? { top: 'calc(100% + 5px)' } : { bottom: 'calc(100% + 5px)' }),
        width: DETAIL_W,
        marginLeft: shift - DETAIL_W / 2,
        background: 'var(--color-bg-popover)',
        borderColor: 'var(--color-border-default)',
        boxShadow: '0 2px 6px rgba(0, 0, 0, 0.12)',
      }}
    >
      <span className="flex items-center gap-[3.5px]">
        <span className="h-1 w-1 shrink-0 rounded-full" style={{ background: EVENT_COLOR }} />
        <span
          className="text-[6.5px] font-bold leading-none"
          style={{ color: 'var(--color-text-primary)' }}
        >
          {ev.title}
        </span>
      </span>
      <span
        className="text-[6.25px] font-normal leading-[1.55]"
        style={{ color: 'var(--color-text-secondary)' }}
      >
        {ev.detail}
      </span>
    </span>
  );
}
