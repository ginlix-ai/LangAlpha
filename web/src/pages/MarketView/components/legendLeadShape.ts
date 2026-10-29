import { signedFixed2 } from '@/lib/format';
import { DASH, fixed2OrDash as fmt, type StockQuoteModel } from '../hooks/useStockQuoteModel';

export const headlineChange = (h: StockQuoteModel['headline'], locale: string): string =>
  h.change != null && h.pct != null ? `${signedFixed2(h.change, locale)} (${signedFixed2(h.pct, locale)}%)` : DASH;

/**
 * Changes only when the lead's intrinsic width can: the lead prints in
 * tabular figures, so its width follows the length of what it prints, not
 * the value, and the session word is translated, so the locale counts too.
 * A host that lays the lead in a measured row keys its re-measure on this
 * rather than on the quote, which changes every tick.
 */
export function legendLeadShapeKey(symbol: string, q: StockQuoteModel, locale: string): string {
  const ext = q.ext
    ? `${fmt(q.ext.price, locale).length}:${q.ext.change != null ? signedFixed2(q.ext.change, locale).length : 0}:${signedFixed2(q.ext.pct, locale).length}`
    : '';
  return `${locale}|${symbol}|${fmt(q.headline.price, locale).length}|${q.currency}|${headlineChange(q.headline, locale).length}|${ext}|${q.status}|${q.quoteBadge?.key ?? ''}:${q.quoteBadge?.minutes ?? ''}`;
}
