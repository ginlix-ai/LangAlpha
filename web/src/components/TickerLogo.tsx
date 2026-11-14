import { useEffect, useState, type ReactNode } from 'react';
import { cn } from '@/lib/utils';
import { tickerInitial, tickerLogoUrl } from '@/lib/tickerLogo';
import './TickerLogo.css';

type FailureStage = 'waiting' | 'retrying' | 'given-up';

/** The relay's `Retry-After` on a transient failure. */
const RETRY_AFTER_MS = 60_000;

/**
 * A ticker's mark: its logo, over a tile with its first letter that shows
 * while the image loads and stays when there is none. The tile is sized from
 * the text around it, so rows in one list keep the same slot whichever they
 * get. Decorative: the ticker's own text stays the accessible name.
 */
export function TickerLogo({ symbol, className }: { symbol: string; className?: string }): React.ReactElement {
  const src = tickerLogoUrl(symbol);
  // Keyed by the URL, so a row that is handed another symbol starts over.
  const [failure, setFailure] = useState<{ src: string; stage: FailureStage } | null>(null);
  const stage = failure?.src === src ? failure.stage : null;
  // A symbol the data service is still resolving answers an uncached 503
  // the first time, so a failed image is asked once more after the relay's
  // Retry-After. A miss is a 404 the browser keeps, so that second ask is
  // answered from its cache and the letter stays.
  useEffect(() => {
    if (!src || stage !== 'waiting') return;
    const id = window.setTimeout(() => setFailure({ src, stage: 'retrying' }), RETRY_AFTER_MS);
    return () => window.clearTimeout(id);
  }, [src, stage]);
  // The letter is drawn by CSS from `data-letter`, under the image: it is
  // never copied with the row's text.
  return (
    <span className={cn('ticker-logo', className)} data-letter={tickerInitial(symbol)} aria-hidden="true">
      {src && (stage === null || stage === 'retrying') && (
        <img
          // A new element per URL: a browser may keep painting the previous
          // image while a reused element loads the next one.
          key={src}
          src={src}
          alt=""
          loading="lazy"
          decoding="async"
          draggable={false}
          onError={() => setFailure({ src, stage: stage === 'retrying' ? 'given-up' : 'waiting' })}
        />
      )}
    </span>
  );
}

/**
 * A ticker written after its logo, in the line's flow: where the line is
 * short the text wraps or cuts as its container says, the logo staying at
 * its start. The text is the symbol unless `children` says otherwise.
 */
export function TickerLabel({
  symbol,
  children,
  className,
}: {
  /** The listing whose logo leads; none draws the text alone. */
  symbol: string | null | undefined;
  children?: ReactNode;
  className?: string;
}): React.ReactElement {
  return (
    <span className={cn('ticker-label', className)}>
      {symbol && <TickerLogo symbol={symbol} />}
      {children ?? symbol}
    </span>
  );
}
