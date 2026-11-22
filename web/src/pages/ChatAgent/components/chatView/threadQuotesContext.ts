/**
 * A thread's quotes as its surfaces read them (the tray, the Status panel, a
 * wakeup's row), provided by `ThreadQuotesProvider`.
 */
import { createContext, useContext, useEffect } from 'react';
import { formatMoney } from '@/lib/bars';
import { DASH } from '@/lib/format';
import type { MarketPhase } from '@/lib/marketUtils';
import type { QuoteTile } from '@/lib/quotes/useQuoteTiles';

/** A tile as a thread reads it, its currency the code its figures print in:
 *  null for an index, whose level is points. */
export type ThreadQuote = Pick<QuoteTile, 'price' | 'pct' | 'ext' | 'places' | 'currency' | 'line' | 'live'>;

export interface ThreadQuotes {
  /** By symbol, as the watch list or the wakeup spells it. */
  bySymbol: Readonly<Record<string, ThreadQuote>>;
  phase: MarketPhase;
}

export const ThreadQuotesContext = createContext<ThreadQuotes>({ bySymbol: {}, phase: 'unknown' });

/** Asks for the session lines while the caller is mounted; returns its release. */
export const QuoteLinesContext = createContext<(() => () => void) | null>(null);

export function useThreadQuotes(): ThreadQuotes {
  return useContext(ThreadQuotesContext);
}

/**
 * Today's line in each quote, while `on`: a read per ticker on a minute's
 * poll, so only a surface that draws the lines asks for them.
 */
export function useThreadQuoteLines(on: boolean): void {
  const ask = useContext(QuoteLinesContext);
  useEffect(() => (on && ask ? ask() : undefined), [on, ask]);
}

/** The last trade, which a price wakeup's distance is read at. Before the
 *  open or after the close the headline is the official close and the trade
 *  is the extended-hours print beside it, the one the wakeup's monitor reads.
 *  Null until the quote has a price, for the monitor's own reading to stand in. */
export function lastTrade(quote: ThreadQuote | undefined): number | null {
  return quote?.ext?.price ?? quote?.price ?? null;
}

/** A quote's price as the menu bar prints its rows: in its currency, to the
 *  listing's places, so a sub-cent or crypto price keeps its digits. */
export function quotePrice(quote: ThreadQuote | undefined, locale: string): string {
  return quote?.price != null ? formatMoney(quote.price, quote.currency, locale, { upTo: quote.places }) : DASH;
}
