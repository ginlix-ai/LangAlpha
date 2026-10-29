import { useState, type ReactNode } from 'react';
import {
  GripVertical,
  HelpCircle,
  MoreVertical,
  Paperclip,
  Pencil,
  Search,
} from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { getCardStyle, type CardStyle } from '@/lib/cardStyle';
import { cn } from '@/lib/utils';
// The dashboard's own card stylesheets, so the drawing's widgets are its
// cards (TermCard brings the chat's two with it).
import '@/pages/ChatAgent/components/charts/termCard.css';
import '@/pages/ChatAgent/components/charts/termCardStyles.css';
import '@/pages/Dashboard/widgets/framework/WidgetFrame.css';
import '@/pages/Dashboard/components/RowAttachButton.css';
import { InputDock, rv } from './kit';

/* The dashboard intro's shared parts: invented quotes, sparklines, the page
   and its header, the floating chat, and the widget frame. The page's
   chrome is drawn after its real counterpart under pages/Dashboard, in
   miniature; the widgets are the real cards, with the dashboard's own
   classes, scaled down (`Real`). Labels are the app's own strings. The
   scenes show the page through a camera, so it is drawn at its own size and
   the camera picks the crop. */
/* eslint-disable react-refresh/only-export-components -- shared parts:
   scene components plus data and style helpers */

/* ── live quotes ───────────────────────────────────────────────────────── */

/** A quote's % change over the script: each value holds from its `at`. */
type Ticks = { at: number; v: number }[];

/** The change `ticks` holds at `t`; pages that sit still show the last one. */
export function changeAt(ticks: Ticks, t: number, live: boolean): number {
  if (!live) return ticks[ticks.length - 1].v;
  let i = 0;
  while (i + 1 < ticks.length && ticks[i + 1].at <= t) i++;
  return ticks[i].v;
}

/** The day the scenes are set on, a Monday, and the time on the page: the
    news ran and the brief was written before it, the earnings are later in
    the week. */
export const TODAY = new Date(2026, 9, 26, 10, 30);

/** The session the index figures are for, as the index cards print it. */
export const SESSION = `${TODAY.getMonth() + 1}/${TODAY.getDate()}`;

/** The index cards: the symbol in the card's edge, the name beside it. Their
    ticks run on DashboardGrid's script; the final signs are the static
    design's (up, up, down, up, down). Each `spark` runs three points past
    the seven on screen (see `Spark`). */
export const INDICES: {
  sym: string;
  name: string;
  prev: number;
  ticks: Ticks;
  spark: number[];
}[] = [
  {
    sym: 'GSPC',
    name: 'S&P 500',
    prev: 5820.1,
    ticks: [
      { at: 0, v: 0.31 },
      { at: 1400, v: 0.38 },
      { at: 4800, v: 0.42 },
    ],
    spark: [20, 22, 19, 18, 16, 17, 13, 12, 9, 8],
  },
  {
    sym: 'NDX',
    name: 'Nasdaq 100',
    prev: 20480.55,
    ticks: [
      { at: 0, v: 0.62 },
      { at: 4400, v: 0.71 },
    ],
    spark: [24, 21, 22, 18, 17, 14, 15, 11, 9, 6],
  },
  {
    sym: 'DJI',
    name: 'Dow Jones',
    prev: 42810.2,
    ticks: [
      { at: 0, v: 0.05 },
      { at: 2300, v: -0.02 },
    ],
    spark: [14, 12, 15, 13, 16, 14, 15, 17, 15, 17],
  },
  {
    sym: 'RUT',
    name: 'Russell 2000',
    prev: 2268.4,
    ticks: [
      { at: 0, v: -0.14 },
      { at: 3200, v: 0.09 },
    ],
    spark: [16, 19, 18, 21, 20, 18, 17, 18, 15, 13],
  },
  {
    sym: 'VIX',
    name: 'VIX',
    prev: 16.12,
    ticks: [
      { at: 0, v: -1.2 },
      { at: 3600, v: -1.64 },
    ],
    spark: [8, 10, 9, 12, 14, 13, 17, 18, 21, 22],
  },
];

export const WATCH: { sym: string; prev: number; ticks: Ticks }[] = [
  {
    sym: 'NVDA',
    prev: 179.53,
    ticks: [
      { at: 0, v: 1.42 },
      { at: 4000, v: 1.61 },
    ],
  },
  {
    sym: 'AAPL',
    prev: 226.8,
    ticks: [
      { at: 0, v: 0.12 },
      { at: 4500, v: -0.08 },
    ],
  },
  {
    sym: 'TSLA',
    prev: 248.1,
    ticks: [
      { at: 0, v: 2.31 },
      { at: 5000, v: 2.05 },
    ],
  },
  { sym: 'MSFT', prev: 431.2, ticks: [{ at: 0, v: 0.34 }] },
  { sym: 'AMZN', prev: 186.4, ticks: [{ at: 0, v: 0.84 }] },
];

/* ── sparklines ────────────────────────────────────────────────────────── */

/** Steps across a sparkline: seven points on screen. */
const SPARK_STEPS = 6;

/** The line slid `slid` steps left and traced out to x = `reach` (of 100),
    clipped to the chart, plus where it ends. */
function sparkLine(series: number[], slid: number, reach: number) {
  const step = 100 / SPARK_STEPS;
  const pts = series.map((y, i) => ({ x: (i - slid) * step, y }));
  const out: string[] = [];
  let end = pts[0];
  for (let i = 1; i < pts.length; i++) {
    const a = pts[i - 1];
    const b = pts[i];
    if (b.x <= 0 || a.x >= reach) continue;
    const yAt = (x: number) => a.y + ((b.y - a.y) * (x - a.x)) / (b.x - a.x);
    const x0 = Math.max(a.x, 0);
    const x1 = Math.min(b.x, reach);
    if (out.length === 0) out.push(`${x0},${yAt(x0)}`);
    out.push(`${x1},${yAt(x1)}`);
    end = { x: x1, y: yAt(x1) };
  }
  return { points: out.join(' '), end };
}

/**
 * A sparkline over `series`. A series longer than the seven points on screen
 * starts slid to its last seven; each point that arrives slides the window a
 * step left. `stretch` fills its box (the index cards, no end dot); otherwise
 * the box keeps the 10:3 viewBox and the line ends on a dot, as the mini
 * chart tiles draw it. The stroke is the cards' own weight at any size.
 */
export function Spark({
  series,
  color,
  slid = series.length - 1 - SPARK_STEPS,
  reach = 100,
  stretch = false,
  className = '',
}: {
  series: number[];
  color: string;
  slid?: number;
  reach?: number;
  stretch?: boolean;
  className?: string;
}) {
  const { points, end } = sparkLine(series, slid, reach);
  return (
    <svg
      viewBox="0 0 100 30"
      preserveAspectRatio={stretch ? 'none' : undefined}
      className={className}
      aria-hidden
    >
      {points && (
        <>
          <polyline
            points={points}
            fill="none"
            stroke={color}
            strokeWidth="1.25"
            vectorEffect="non-scaling-stroke"
            strokeLinejoin="round"
            strokeLinecap="round"
            className="transition-colors duration-quick"
          />
          {!stretch && <circle cx={end.x} cy={end.y} r="3" fill={color} />}
        </>
      )}
    </svg>
  );
}

/* ── page chrome ───────────────────────────────────────────────────────── */

/**
 * The dashboard header: symbol search on the left, then the edit-layout
 * pencil and help. `edit` fills the pencil, as edit mode does; `pressed` is
 * its transform while a click holds it down.
 */
export function DashTopbar({ edit = false, pressed }: { edit?: boolean; pressed?: string }) {
  const { t } = useTranslation();
  return (
    <>
      <span
        className="flex h-[17px] w-[44%] items-center gap-1 rounded-md border px-1.5"
        style={{ background: 'var(--color-bg-input)', borderColor: 'var(--color-border-muted)' }}
      >
        <Search className="h-2 w-2 shrink-0" style={{ color: 'var(--color-icon-muted)' }} />
        <span
          className="truncate text-[6px] leading-none"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {t('dashboard.searchPlaceholder')}
        </span>
        <span
          className="ml-auto rounded-[2px] border px-[2px] text-[5px] leading-[1.3]"
          style={{
            borderColor: 'var(--color-border-default)',
            color: 'var(--color-text-quaternary)',
          }}
        >
          /
        </span>
      </span>
      <span className="ml-auto flex items-center gap-1.5">
        <span className="flex transition-transform duration-press" style={{ transform: pressed }}>
          <span
            className="flex h-3.5 w-3.5 items-center justify-center rounded-[4px] transition-colors duration-quick"
            style={{
              background: edit ? 'var(--color-btn-primary-bg)' : 'transparent',
              color: edit ? 'var(--color-btn-primary-text)' : 'var(--color-text-secondary)',
            }}
          >
            <Pencil className="h-2 w-2" />
          </span>
        </span>
        <HelpCircle className="h-2.5 w-2.5" style={{ color: 'var(--color-text-secondary)' }} />
      </span>
    </>
  );
}

/** The page's width, in page px: what the camera's shots are measured in. */
export const PAGE_W = 400;
/** The page's padding and the gap between its cards, in page px. */
export const PAD = 10;
export const GAP = 6;

/**
 * The dashboard page with no app around it: `header` over the content when
 * a step is about the header, the content padded under it. `height` holds
 * the page to a window's height, for a chat that floats at its foot; what
 * runs past it is cut off, as the page scrolls on under the fold.
 */
export function DashPage({
  header,
  height,
  children,
}: {
  header?: ReactNode;
  height?: number;
  children: ReactNode;
}) {
  return (
    <div className="flex flex-col overflow-hidden" style={{ width: PAGE_W, height }}>
      {header && (
        <div
          className="flex h-7 shrink-0 items-center gap-2 border-b px-2.5"
          style={{ borderColor: 'var(--color-border-muted)' }}
        >
          {header}
        </div>
      )}
      <div className="relative flex min-h-0 flex-1 flex-col gap-1.5" style={{ padding: PAD }}>
        {children}
      </div>
    </div>
  );
}

/** The floating chat's own lift off the page (Dashboard.css), scaled down. */
const FLOAT_SHADOW = '0 4px 12px rgba(0, 0, 0, 0.12)';

/** What floats over the foot of a `DashPage`, as the real chat is fixed
    over the content and the page runs on under it. */
export function Floating({ children }: { children: ReactNode }) {
  return (
    <div className="absolute inset-x-0 flex justify-center" style={{ bottom: PAD }}>
      {children}
    </div>
  );
}

/** The floating chat, as wide as the real one sits on the content column. */
export function Composer({
  delay,
  focused,
  chip,
  text,
}: {
  delay?: number;
  focused?: boolean;
  chip?: ReactNode;
  text?: ReactNode;
}) {
  return (
    <div className="w-[236px] rounded-[11px]" style={{ boxShadow: FLOAT_SHADOW }}>
      <InputDock delay={delay} focused={focused} chip={chip} text={text} />
    </div>
  );
}


/* ── widgets ───────────────────────────────────────────────────────────── */

/** Page px per px of the real dashboard. The widgets are drawn with the
    dashboard's own card classes at the app's size and scaled down to the
    page, so they follow the reader's card style and theme as the real ones
    do. Just over half: about the real page's columns at the width they
    have on a laptop, with its figures still readable through the camera. */
const REAL_SCALE = 0.55;

/**
 * Its children at the real dashboard's size, scaled down to the page's.
 * Zoom rather than a transform, so the box keeps its place in the page's
 * layout; and divided by the app's text scale, since the cards set their
 * type in rem and the page's geometry, which the camera and pointer are
 * measured in, must not move with it.
 */
export function Real({ className, children }: { className?: string; children: ReactNode }) {
  return (
    <div
      className={className}
      style={{ zoom: `calc(${REAL_SCALE} / var(--app-font-scale, 1))` }}
    >
      {children}
    </div>
  );
}

/** How far a row under a widget's tabs sits from where the default card
    style puts it, in page px: Quiet sets the title inside the card, over
    the body, and Terminal's tabs are a line of text rather than pills.
    Pointer targets on a row add it. */
const ROW_SHIFT: Record<CardStyle, number> = { balanced: 0, terminal: -2, quiet: 9.5 };

/** The reader's card style, read once: it is set in Settings, never while
    an intro plays. */
export function useRowShift(): number {
  const [style] = useState(getCardStyle);
  return ROW_SHIFT[style];
}

/**
 * A widget on the grid, drawn as `WidgetFrame` draws it: the chat's data
 * card, its title set into the top edge (inside it, in Quiet), the widget's
 * `sub` beside the title and its `meta` at the edge's far end. In edit mode
 * the edge goes dashed in the accent, the drag grip comes before the title
 * and the widget's actions after it. The cell's size is the caller's: the
 * card fills it, and a body taller than the cell is cut off, as a real cell
 * scrolls it.
 */
export function Widget({
  title,
  sub,
  meta,
  edit = false,
  delay,
  className,
  children,
}: {
  title: string;
  sub?: ReactNode;
  meta?: ReactNode;
  edit?: boolean;
  delay?: number;
  className?: string;
  children?: ReactNode;
}) {
  return (
    <div
      className={cn('flex min-h-0 min-w-0 flex-col', delay != null && 'intro-rv', className)}
      style={delay != null ? rv(delay) : undefined}
    >
      <Real className="min-h-0 flex-1">
        <div className={cn('widget-frame', edit && 'widget-frame--edit')}>
          <section className="term-card widget-card">
            <header className="term-card-head" data-keep="title">
              {edit && (
                <span className="widget-card__grip">
                  <GripVertical size={12} />
                </span>
              )}
              <span className="term-card-title">
                <span className="term-bracket">[</span>
                <span className="term-card-title-text">{title}</span>
                <span className="term-bracket">]</span>
              </span>
              {sub != null && <span className="term-card-sub">{sub}</span>}
              {edit && (
                <span className="widget-card__actions">
                  <span className="widget-card__icon-btn">
                    <Paperclip size={12} />
                  </span>
                  <span className="widget-card__icon-btn">
                    <MoreVertical size={12} />
                  </span>
                </span>
              )}
              {meta != null && <span className="term-card-meta">{meta}</span>}
            </header>
            <div className="widget-card__body" style={{ overflow: 'hidden' }}>
              <div className="h-full">{children}</div>
            </div>
          </section>
        </div>
      </Real>
    </div>
  );
}

/** A widget's tabs, as its chips: `[Watchlist] [Holdings]`, the first one
    picked. */
export function Chips({ labels, picked = 0 }: { labels: string[]; picked?: number }) {
  return (
    <div className="dash-chips">
      {labels.map((label, i) => (
        <span key={label} className="term-chip" aria-pressed={i === picked}>
          {label}
        </span>
      ))}
    </div>
  );
}
