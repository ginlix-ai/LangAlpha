import React, { useId, useRef } from 'react';
import { useNarrowContainer } from '@/hooks/useNarrowContainer';
import { cn } from '@/lib/utils';
import { percentMove } from './termChange';
import { CardNarrowContext, useCardNarrow, useTermPanel } from './termContext';
import './termCard.css';
import './termCardStyles.css';

/** Below this, a card's full layout (three columns of figures, a table's
 *  every column) no longer fits its column. */
const CARD_FULL_WIDTH = 640;

/** A transcript column, measured once for every card in it. */
export function CardColumn({ className, children }: { className?: string; children: React.ReactNode }): React.ReactElement {
  const ref = useRef<HTMLDivElement>(null);
  const narrow = useNarrowContainer(ref, CARD_FULL_WIDTH) === true;
  return (
    <CardNarrowContext value={narrow}>
      <div ref={ref} className={className}>{children}</div>
    </CardNarrowContext>
  );
}

/**
 * The frame every data card in the transcript is drawn in: a text-UI box with
 * its title set into the top edge, `[ NVDA ] US EQUITY ........ OCT 5 – 23`.
 * The pieces below it are the marks the cards share, so a change or a range
 * reads the same on every card. Inside the detail panel it is one of the
 * panel's sections, the same frame a size up.
 */
export function TermCard({
  title,
  sub,
  meta,
  metaTitle,
  keep = 'title',
  onClick,
  className,
  children,
  testid,
}: {
  title: React.ReactNode;
  /** Beside the title, in its cut: the exchange, the filing kind. */
  sub?: React.ReactNode;
  /** At the far end of the edge: the date range, the freshness, a count. A
   *  list drops its empty entries, and nothing left draws no meta. */
  meta?: React.ReactNode;
  /** The meta's tooltip: when the rows were fetched. */
  metaTitle?: string;
  /** The part of the edge that keeps every letter when it is short: the
   *  title (a ticker, a short label), or the sub when the title is a long
   *  name and the sub its ticker. */
  keep?: 'title' | 'sub';
  onClick?: () => void;
  className?: string;
  children?: React.ReactNode;
  testid?: string;
}): React.ReactElement {
  const narrow = useCardNarrow();
  const panel = useTermPanel();
  // An entry is keyed by its place in the list, so one that comes and goes
  // does not remount the ones after it.
  const metaParts = (Array.isArray(meta) ? meta : [meta]).flatMap((part, i) =>
    part == null || typeof part === 'boolean' || part === '' ? [] : [<React.Fragment key={i}>{part}</React.Fragment>],
  );
  // The whole card answers a click; the title is the button a keyboard or a
  // screen reader reaches. A card made a button itself would hold its links
  // and freshness tags inside a button, and read its whole table as its name.
  const open = onClick
    ? (e: React.MouseEvent) => {
        // A link on the card goes where it says, a tag says what it says in
        // its tooltip, and a drag that selected figures was copying them, not
        // asking for the panel.
        if ((e.target as Element).closest('a, button, input, [tabindex]')) return;
        if (window.getSelection()?.toString()) return;
        onClick();
      }
    : undefined;
  const titleParts = (
    <>
      <span className="term-bracket" aria-hidden="true">[</span>
      <span className="term-card-title-text">{title}</span>
      <span className="term-bracket" aria-hidden="true">]</span>
    </>
  );
  return (
    <section
      className={cn('term-card', className)}
      data-size={narrow ? 'sm' : undefined}
      data-scale={panel ? 'panel' : undefined}
      data-clickable={onClick ? '' : undefined}
      data-testid={testid}
      onClick={open}
    >
      <header className="term-card-head" data-keep={keep}>
        {onClick ? (
          <button type="button" className="term-card-title" onClick={onClick}>{titleParts}</button>
        ) : (
          <span className="term-card-title">{titleParts}</span>
        )}
        {sub != null && sub !== '' && <span className="term-card-sub">{sub}</span>}
        {metaParts.length > 0 && <span className="term-card-meta" title={metaTitle}>{metaParts}</span>}
        {onClick && <span className="term-card-go" aria-hidden="true">›</span>}
      </header>
      {children}
    </section>
  );
}

/** A word in brackets, `[EOD]`, with an optional state dot. */
export function TermTag({
  children,
  tone,
}: {
  children: React.ReactNode;
  /** Draws the dot: green live, gray neutral, amber-free warning. */
  tone?: 'live' | 'neutral' | 'warning';
}): React.ReactElement {
  return (
    <span className="term-tag">
      {tone && <span className="term-dot" data-tone={tone} aria-hidden="true" />}
      {children}
    </span>
  );
}

/**
 * The ink a state is said in. Profit and loss are a market's; success and
 * danger a delivery's verdict; muted is the quiet default, faint quieter
 * still, for a state the vocabulary does not know.
 */
export type TermTone = 'profit' | 'loss' | 'warning' | 'info' | 'success' | 'danger' | 'muted' | 'faint';

/** A state in brackets in its own color: `[FILLED]`, `[PAUSED]`. */
export function TermStatus({
  children,
  tone,
  testid,
}: {
  children: React.ReactNode;
  tone?: TermTone;
  testid?: string;
}): React.ReactElement {
  return <span className="term-status" data-tone={tone} data-testid={testid}>{children}</span>;
}

/**
 * A move with its direction drawn: ▲ 1.70%, ▼ 0.24%, · 0.00%. The figure is
 * unsigned beside an arrow, since the arrow already says which way.
 * `children` replaces the default percent text, for a move in money.
 */
export function TermChange({
  value,
  children,
}: {
  /** Decides the direction; for a percent, also the text. */
  value: number | null | undefined;
  children?: React.ReactNode;
}): React.ReactElement | null {
  if (value == null || !Number.isFinite(value)) return null;
  const { dir, sign, size } = percentMove(value);
  return (
    <span className="term-chg" data-dir={dir}>
      {dir === 'flat' ? (
        <span aria-hidden="true">·</span>
      ) : (
        <svg className="term-chg-mark" viewBox="0 0 10 9" aria-hidden="true">
          <path d={dir === 'up' ? 'M5 0 10 9H0Z' : 'M5 9 0 0h10Z'} />
        </svg>
      )}
      {/* The arrow is a picture; the sign is what a screen reader and a copy hear. */}
      {sign && <span className="sr-only">{sign}</span>}
      <span>{children ?? size}</span>
    </span>
  );
}

/** Labels and values in columns, joined by a dotted leader. */
export function TermKV({
  pairs,
  cols = 2,
}: {
  pairs: ReadonlyArray<readonly [label: React.ReactNode, value: React.ReactNode] | null | false | undefined>;
  cols?: number;
}): React.ReactElement | null {
  const rows = pairs.filter((p): p is readonly [React.ReactNode, React.ReactNode] => !!p);
  if (!rows.length) return null;
  return (
    <div className="term-kv" style={{ '--term-cols': cols } as React.CSSProperties}>
      {rows.map(([label, value], i) => (
        <div key={i}>
          <span className="term-k">{label}</span>
          <span className="term-kv-leader" aria-hidden="true" />
          <span className="term-v">{value}</span>
        </div>
      ))}
    </div>
  );
}

/** Where a price sits between a low and a high: `├───●──┤`. */
export function TermRange({
  low,
  high,
  value,
  fill = false,
}: {
  low: number | null | undefined;
  high: number | null | undefined;
  value: number | null | undefined;
  /** Take the line's free width rather than a fixed nine characters. */
  fill?: boolean;
}): React.ReactElement | null {
  if (low == null || high == null || value == null || !(high > low)) return null;
  const at = Math.min(1, Math.max(0, (value - low) / (high - low)));
  return (
    <span className="term-range" data-fill={fill ? '' : undefined} aria-hidden="true">
      <span className="term-range-at" style={{ '--at': at } as React.CSSProperties} />
    </span>
  );
}

/** `── SECTOR PERFORMANCE · 1D ─────`: a second section in the same box. */
export function TermDivider({ children }: { children: React.ReactNode }): React.ReactElement {
  return <div className="term-divider">{children}</div>;
}

/** `+15 MORE` under a list that was cut. */
export function TermMore({ children }: { children: React.ReactNode }): React.ReactElement {
  return <div className="term-more">{children}</div>;
}

/** A row of `LABEL value` stats. */
export function TermStats({
  stats,
}: {
  stats: ReadonlyArray<readonly [label: React.ReactNode, value: React.ReactNode] | null | false | undefined>;
}): React.ReactElement | null {
  const rows = stats.filter((s): s is readonly [React.ReactNode, React.ReactNode] => !!s);
  if (!rows.length) return null;
  return (
    <div className="term-stats">
      {rows.map(([label, value], i) => (
        <span key={i}>
          <span>{label}</span>
          <b>{value}</b>
        </span>
      ))}
    </div>
  );
}

/**
 * Bars either side of a zero axis, the longest at full half-width. Each row's
 * label sits on one line, and its figure beside its bar.
 */
export function TermBars({
  rows,
}: {
  rows: ReadonlyArray<{ label: string; value: number; text: string }>;
}): React.ReactElement | null {
  if (!rows.length) return null;
  const max = Math.max(...rows.map((r) => Math.abs(r.value))) || 1;
  return (
    <div className="term-bars">
      {rows.map((r) => {
        const side = r.value < 0 ? 'down' : 'up';
        // Leaves the figure its room beside the longest bar.
        const width = `calc((100% - 4.5em) * ${Math.abs(r.value) / max})`;
        const bar = <span className="term-bar" style={{ width }} />;
        const text = <span className="term-bars-value">{r.text}</span>;
        return (
          <React.Fragment key={r.label}>
            <span className="term-bars-label">{r.label}</span>
            <div className="term-bars-row">
              <div className="term-bars-side" data-side="down">{side === 'down' && <>{text}{bar}</>}</div>
              <div className="term-bars-side" data-side="up">{side === 'up' && <>{bar}{text}</>}</div>
            </div>
          </React.Fragment>
        );
      })}
    </div>
  );
}

/**
 * A price line, its high and low read off a right axis, with the dates under
 * it. Drawn in one SVG stretched to the width; the stroke keeps its weight.
 */
export function TermPriceLine({
  values,
  color,
  height,
  axis,
  dates,
}: {
  values: readonly number[];
  color: string;
  height: number;
  /** The high and the low, as printed. */
  axis: readonly [high: string, low: string];
  /** First, middle and last date labels. */
  dates?: readonly string[];
}): React.ReactElement | null {
  const gradientId = useId();
  if (values.length < 2) return null;
  const W = 600;
  const pad = 2;
  const hi = Math.max(...values);
  const lo = Math.min(...values);
  const span = hi - lo || 1;
  const y = (v: number) => pad + ((hi - v) / span) * (height - pad * 2);
  const x = (i: number) => (i / (values.length - 1)) * W;
  const line = values.map((v, i) => `${x(i).toFixed(1)},${y(v).toFixed(1)}`).join(' ');
  const area = `0,${height} ${line} ${W},${height}`;
  return (
    <div className="term-plot">
      <svg
        className="term-plot-svg"
        viewBox={`0 0 ${W} ${height}`}
        preserveAspectRatio="none"
        style={{ height }}
        aria-hidden="true"
      >
        <defs>
          <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={color} stopOpacity={0.14} />
            <stop offset="100%" stopColor={color} stopOpacity={0} />
          </linearGradient>
        </defs>
        <line className="term-plot-grid" x1={0} x2={W} y1={pad} y2={pad} vectorEffect="non-scaling-stroke" />
        <line className="term-plot-grid" x1={0} x2={W} y1={height - pad} y2={height - pad} vectorEffect="non-scaling-stroke" />
        <polygon points={area} fill={`url(#${gradientId})`} />
        <polyline
          points={line}
          fill="none"
          stroke={color}
          strokeWidth={1.25}
          strokeLinejoin="round"
          vectorEffect="non-scaling-stroke"
        />
      </svg>
      <div className="term-plot-axis" style={{ height }}>
        <span>{axis[0]}</span>
        <span>{axis[1]}</span>
      </div>
      {dates && dates.length > 0 && (
        <div className="term-plot-dates">
          {dates.map((d, i) => (
            <span key={i}>{d}</span>
          ))}
        </div>
      )}
    </div>
  );
}
