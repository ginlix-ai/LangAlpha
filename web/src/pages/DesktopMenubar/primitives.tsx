/**
 * The pieces every section of the menu bar panel is drawn from: a figure
 * still loading, a move in its direction's color, a section's frame, and a
 * quote row.
 */
import type { ReactNode } from 'react';
import { DASH } from '@/lib/format';
import { cn } from '@/lib/utils';
import { changeDirection } from '@/pages/ChatAgent/components/charts/termChange';
import { signedPercent, type ExtendedMove } from './glanceSummary';

/** A value still on its way: a quiet bar where it will land, not the dash that means none. */
export function Pending({ className }: { className?: string }) {
  return (
    <span className={className} aria-hidden="true">
      <span className="mb-pending" />
    </span>
  );
}

/** A percent move in its direction's color: profit, loss, or quiet for none. */
export function Move({ value, pending, className }: { value: number | null | undefined; pending?: boolean; className?: string }) {
  if (pending) return <Pending className={className} />;
  if (value == null || !Number.isFinite(value)) return <span className={cn('mb-t3', className)}>{DASH}</span>;
  return (
    <span className={cn('mb-move', className)} data-dir={changeDirection(value)}>
      {signedPercent(value)}
    </span>
  );
}

/** The pre-market or after-hours move under a row's figures, as the Dashboard rows print it: `AH 199.10 +0.34%`. */
export function ExtendedLine({ ext, price, className }: { ext: Pick<ExtendedMove, 'label' | 'pct'>; price?: string; className?: string }) {
  return (
    <span className={cn('mb-ext', className)}>
      <span className="mb-ext-label">{ext.label}</span>
      {price && <span className="mb-num">{price}</span>}
      <Move value={ext.pct} />
    </span>
  );
}

export function Section({ title, aside, children }: { title: string; aside?: ReactNode; children: ReactNode }) {
  return (
    <section className="mb-section" aria-label={title}>
      <header className="mb-section-head">
        <h2 className="mb-kicker">{title}</h2>
        {aside}
      </header>
      {children}
    </section>
  );
}

/**
 * A holding or watchlist row: the symbol, an optional figure beside it, the
 * price and move, and the extended-hours line under them when there is one.
 * Opens the symbol's page.
 */
export function QuoteRowButton({
  symbol,
  aside,
  price,
  pct,
  ext,
  extPrice,
  pending = false,
  onOpen,
}: {
  symbol: string;
  /** Between the symbol and the figures: a holding's weight. */
  aside?: ReactNode;
  /** The formatted price, or null for none. */
  price: string | null;
  pct: number | null;
  ext: ExtendedMove | null | undefined;
  extPrice?: string;
  pending?: boolean;
  onOpen: () => void;
}) {
  return (
    <li>
      <button type="button" className="mb-row mb-row-btn" onClick={onOpen} aria-busy={pending || undefined}>
        <span className="mb-sym">{symbol}</span>
        {aside}
        <span className="mb-figs">
          <span className="mb-figs-main">
            {pending ? <Pending className="mb-price" /> : <span className="mb-price mb-num">{price ?? DASH}</span>}
            <Move value={pct} pending={pending} className="mb-row-move" />
          </span>
          {ext && <ExtendedLine ext={ext} price={extPrice} />}
        </span>
      </button>
    </li>
  );
}
