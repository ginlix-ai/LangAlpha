/**
 * A quote row split the way every Dashboard row splits one (`sessionQuote`):
 * the official figure as the headline and the extended-hours print beside it.
 * The menu bar panel's rows and tiles, its lines beside the icon, and a chat's
 * watched tickers all read it through here.
 */
import { sessionQuote, type MarketStatusData, type SessionQuote } from '@/lib/marketUtils';
import { snapshotToStockPrice, type QuoteRow } from '@/lib/quotes';

/** A pre-market or after-hours move (`PM`/`AH`), measured off the official price beside it. */
export type ExtendedMove = Omit<NonNullable<SessionQuote['ext']>, 'type'>;

export interface SplitQuote {
  /** False when the symbol has no quote to be had. */
  quoteAvailable: boolean;
  price: number | null;
  change: number | null;
  pct: number | null;
  ext: ExtendedMove | null;
  places: number;
  currency: string | null;
}

export function splitQuote(
  symbol: string,
  row: QuoteRow | undefined | null,
  marketStatus: MarketStatusData | null,
): SplitQuote {
  const p = snapshotToStockPrice(symbol, row);
  // A zero or negative price is a partial snapshot, not a quote.
  const quoteAvailable = p.quoteAvailable !== false && p.price != null && p.price > 0;
  if (!quoteAvailable) {
    return { quoteAvailable, price: null, change: null, pct: null, ext: null, places: p.places, currency: p.currency ?? null };
  }
  const { headline, ext } = sessionQuote(marketStatus, p, { shortLabels: true });
  // A row the day's session has not printed yet stands at the previous close
  // (`snapshotToStockPrice`), which is no move at all rather than a 0.00% one.
  const unprinted = row?.price == null;
  return {
    quoteAvailable,
    price: headline.price,
    change: unprinted ? null : headline.change,
    pct: unprinted ? null : headline.pct,
    ext,
    places: p.places,
    currency: p.currency ?? null,
  };
}
