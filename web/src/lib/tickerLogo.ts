/**
 * Where a ticker's logo image lives. The server answers with an opaque square
 * tile that reads on either theme, or a 404 when no source has one, so the
 * caller draws the ticker's letter until the image loads and keeps it when it
 * does not.
 */
import { api } from '@/api/client';
import { isIndexListing } from '@/lib/bars/exchanges';

/**
 * The logo URL for `symbol`, or null when there is none to ask for: an index
 * has no company, and neither does an option contract (`O:…`). One size for
 * every tile, large enough for the biggest at 3x, so a symbol is one cached image.
 */
export function tickerLogoUrl(symbol: string): string | null {
  const s = symbol.trim();
  // A spelling with no letter or digit (`.`, `..`) would be read as a path step.
  if (!/[A-Za-z0-9]/.test(s) || isIndexListing(s) || /^O:/i.test(s)) return null;
  return api.getUri({ url: `/api/v1/market-data/logos/${encodeURIComponent(s)}`, params: { size: 128 } });
}

/** The letter a tile shows: the ticker's first, without an index caret. */
export function tickerInitial(symbol: string): string {
  const first = symbol.trim().replace(/^\^/, '').match(/[A-Za-z0-9]/)?.[0];
  return first ? first.toUpperCase() : '?';
}
