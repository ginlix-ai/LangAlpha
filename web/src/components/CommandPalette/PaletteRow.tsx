import type { ReactNode } from 'react';
import { Ellipsis } from 'lucide-react';
import { TickerLogo } from '@/components/TickerLogo';
import { useLocale } from '@/hooks/useLocale';
import { grouped2, signedFixed2 } from '@/lib/format';
import { snapshotToStockPrice, type QuoteRow } from '@/lib/quotes';
import { cn } from '@/lib/utils';
import type { PaletteItem } from './types';

interface RowProps {
  id: string;
  index: number;
  active: boolean;
  icon: PaletteItem['icon'];
  accent?: boolean;
  symbol?: string;
  title: string;
  spans?: [number, number][];
  meta?: string;
  detail?: PaletteItem['detail'];
  /** Drawn at the row's end, in a box of its own that takes `.command-palette-trailing`. */
  trailing?: ReactNode;
  /** Opens the row's other actions, for a pointer: the keyboard has →. Unset for a row with none. */
  onMore?: () => void;
  moreLabel?: string;
  onHover: () => void;
  onPick: () => void;
}

export function PaletteRow({
  id,
  index,
  active,
  icon: Icon,
  accent,
  symbol,
  title,
  spans,
  meta,
  detail,
  trailing,
  onMore,
  moreLabel,
  onHover,
  onPick,
}: RowProps) {
  return (
    <div
      id={id}
      data-row={index}
      role="option"
      aria-selected={active}
      className={cn('command-palette-row', active && 'is-active', detail && 'has-detail')}
      // Moving, not entering: rows arriving under a still pointer take nothing.
      onMouseMove={active ? undefined : onHover}
      onMouseDown={(e) => e.preventDefault()}
      onClick={onPick}
    >
      {/* A ticker's row leads with its logo, in the icon's column. */}
      {symbol ? (
        <TickerLogo symbol={symbol} className="command-palette-row-icon" />
      ) : (
        <Icon className={cn('command-palette-row-icon', accent && 'is-accent')} aria-hidden />
      )}
      <span className="command-palette-row-body">
        <span className="command-palette-row-line">
          {symbol && <span className="command-palette-symbol">{symbol}</span>}
          {title && (
            <span className="command-palette-title">
              <Highlighted text={title} spans={spans} />
            </span>
          )}
          {meta && <span className="command-palette-meta">{meta}</span>}
        </span>
        {detail && (
          <span className="command-palette-detail">
            <span className="command-palette-detail-label">{detail.label}</span>
            <Highlighted text={detail.text} spans={detail.spans} />
          </span>
        )}
      </span>
      {/* No ↵ on the selected row: the footer names what Enter does, and a
          key drawn here would push the row's quote aside as the keys move. */}
      {trailing}
      {onMore && (
        // For a pointer only, and hidden from the accessibility tree: the
        // keyboard opens the same actions with →, which the field's
        // description names. It never takes focus, so typing goes on.
        <button
          type="button"
          tabIndex={-1}
          aria-hidden
          title={moreLabel}
          className="command-palette-more tap-target"
          onMouseDown={(e) => e.preventDefault()}
          onClick={(e) => {
            e.stopPropagation();
            onMore();
          }}
        >
          <Ellipsis aria-hidden />
        </button>
      )}
    </div>
  );
}

/** `text` with the spans bolded; spans outside it, or overlapping, are clamped rather than trusted. */
export function Highlighted({ text, spans }: { text: string; spans?: [number, number][] }) {
  if (!spans?.length) return <>{text}</>;
  const parts: ReactNode[] = [];
  let at = 0;
  spans.forEach(([from, to], i) => {
    const start = Math.max(from, at);
    const end = Math.min(to, text.length);
    if (end <= start) return;
    if (start > at) parts.push(text.slice(at, start));
    parts.push(<mark key={i}>{text.slice(start, end)}</mark>);
    at = end;
  });
  if (at < text.length) parts.push(text.slice(at));
  return <>{parts}</>;
}

/**
 * A ticker's price and day's change, as a row shows it (the change in
 * percent) or the preview does (the change and its percent), else `fallback`
 * while there is no quote.
 */
export function QuoteFigures({
  symbol,
  quote,
  className,
  full = false,
  fallback = null,
}: {
  symbol: string;
  quote: QuoteRow | undefined;
  className: string;
  full?: boolean;
  fallback?: ReactNode;
}) {
  const locale = useLocale();
  const price = quote ? snapshotToStockPrice(symbol, quote) : null;
  if (!price?.quoteAvailable) return fallback;
  const percent = `${signedFixed2(price.changePercent, locale)}%`;
  return (
    <span className={className}>
      <span>{grouped2(price.price, locale)}</span>
      <span className={cn(price.change > 0 && 'is-up', price.change < 0 && 'is-down')}>
        {full ? `${signedFixed2(price.change, locale)} (${percent})` : percent}
      </span>
    </span>
  );
}
