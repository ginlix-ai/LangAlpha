import { DASH, signedFixed2 } from '@/lib/format';
import { fixedUpTo } from '@/lib/bars';
import { priceOrDash, type StockQuoteModel } from '../hooks/useStockQuoteModel';

export const headlineChange = (h: StockQuoteModel['headline'], places: number, locale: string): string =>
  h.change != null && h.pct != null
    ? `${fixedUpTo(h.change, places, locale, { signed: true })} (${signedFixed2(h.pct, locale)}%)`
    : DASH;

/**
 * Changes only when the lead's intrinsic width can: the lead prints in
 * tabular figures, so its width follows the length of what it prints, not
 * the value, and the session word is translated, so the locale counts too.
 * A host that lays the lead in a measured row keys its re-measure on this
 * rather than on the quote, which changes every tick.
 */
export function legendLeadShapeKey(symbol: string, q: StockQuoteModel, locale: string): string {
  const { places } = q;
  const ext = q.ext
    ? `${priceOrDash(q.ext.price, places, locale).length}:${q.ext.change != null ? fixedUpTo(q.ext.change, places, locale, { signed: true }).length : 0}:${signedFixed2(q.ext.pct, locale).length}`
    : '';
  return `${locale}|${symbol}|${priceOrDash(q.headline.price, places, locale).length}|${q.currency}|${headlineChange(q.headline, places, locale).length}|${ext}|${q.status}|${q.quoteBadge?.key ?? ''}:${q.quoteBadge?.minutes ?? ''}`;
}

/**
 * The room the lead asks of the chart's toolbar row (`measureToolbarLead`):
 * its full width, less the company name, which gives way to the controls down
 * to nothing, plus whatever the quote, cut at its end, is holding back. So the
 * demand is the width of what the lead will not give up, however much of the
 * name shows, and a name filling spare room never reads to the toolbar as
 * room taken.
 */
export function legendLeadDemand(lead: HTMLElement): number {
  let w = lead.scrollWidth;
  for (const el of lead.querySelectorAll<HTMLElement>('.legend-lead-name')) w -= el.getBoundingClientRect().width;
  for (const el of lead.querySelectorAll<HTMLElement>('.legend-lead-quote')) w += el.scrollWidth - el.clientWidth;
  return w;
}
