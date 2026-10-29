import { formatMoney } from '@/lib/bars/currencyDisplay';
import { quoteCurrency } from '@/lib/bars/exchanges';

export interface SelectionPriceShape {
  selectionType: 'region' | 'price_level';
  symbol: string;
  priceLow: number;
  priceHigh: number;
  /** The chart axis's decimals at draw time; absent on an older snapshot, which prints 2. */
  decimals?: number;
}

/**
 * A chart selection's price bounds on one line: the level for a price line,
 * `low - high` for a region, with no currency on an index level, at the places
 * the chart axis showed. The image caption the agent reads and the cards and
 * preview that show what it was sent all print it, so it is built once and
 * they cannot drift apart.
 */
export function selectionPriceBounds(sel: SelectionPriceShape, locale: string): string {
  const code = quoteCurrency(null, sel.symbol);
  const opts = { decimals: sel.decimals ?? 2 };
  const low = formatMoney(sel.priceLow, code, locale, opts);
  return sel.selectionType === 'region' ? `${low} - ${formatMoney(sel.priceHigh, code, locale, opts)}` : low;
}
