import { displaySpelling } from '@/lib/bars/exchanges';
import type { StockSearchHit } from '@/lib/marketUtils';

/** The server's matches that say the query names the row: all but `other`. */
const NAMED: ReadonlySet<StockSearchHit['match']> = new Set(['exact', 'symbol_prefix', 'name_word']);

const CJK = /[㐀-鿿豈-﫿]/;
const compact = (text: string) => text.toUpperCase().replace(/[^A-Z0-9^]/g, '');

/**
 * Whether `query` names a listing the search did not return, read as the
 * server reads its rows (`symbol_search.matcher`): its symbol starts with it,
 * punctuation aside, or a word of one of its names does. A name in a script
 * written without spaces matches anywhere in it.
 */
export function namesListing(query: string, symbol: string, names: readonly (string | null | undefined)[]): boolean {
  const q = query.trim();
  if (!q) return false;
  const typed = compact(q);
  if (typed && compact(symbol).startsWith(typed)) return true;
  const lowered = q.toLowerCase();
  const wordStart = new RegExp(`(?<![\\p{L}\\p{N}_])${lowered.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}`, 'u');
  return names.some((name) => {
    const text = (name ?? '').toLowerCase();
    return !!text && (wordStart.test(text) || (CJK.test(lowered) && text.includes(lowered)));
  });
}

/**
 * The search's hits with the user's own tickers (`own`: held, watched, opened
 * lately, in that order) that `query` names put first, after the exact
 * listing: the symbol typed is the one meant, and a short query names more
 * own tickers than the palette shows. A returned own ticker counts when the
 * server matched it on its symbol or a name word, and keeps the search's
 * order among the others; one the search did not return is read here and
 * follows them. The rest keep the search's order.
 */
export function boostOwnTickers(hits: readonly StockSearchHit[], query: string, own: readonly StockSearchHit[]): StockSearchHit[] {
  const mine = new Map<string, StockSearchHit>();
  for (const ticker of own) {
    const key = displaySpelling(ticker.symbol);
    if (key && !mine.has(key)) mine.set(key, ticker);
  }
  if (mine.size === 0) return [...hits];
  const exact: StockSearchHit[] = [];
  const boosted: StockSearchHit[] = [];
  const rest: StockSearchHit[] = [];
  const found = new Set<string>();
  for (const hit of hits) {
    const key = displaySpelling(hit.symbol);
    found.add(key);
    if (hit.match === 'exact') exact.push(hit);
    else (mine.has(key) && NAMED.has(hit.match) ? boosted : rest).push(hit);
  }
  const unfound = [...mine.entries()]
    .filter(([key, ticker]) => !found.has(key) && namesListing(query, ticker.symbol, [ticker.name, ticker.nameLocal, ticker.nameEn]))
    .map(([, ticker]) => ticker);
  return [...exact, ...boosted, ...unfound, ...rest];
}
