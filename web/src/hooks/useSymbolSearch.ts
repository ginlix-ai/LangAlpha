import { useEffect, useState } from 'react';
import { searchStocks, type StockSearchHit } from '@/lib/marketUtils';
import { useDebouncedValue } from '@/lib/useDebouncedValue';

/** How long a symbol query rests before it is sent; every symbol field shares it. */
export const SYMBOL_SEARCH_DEBOUNCE_MS = 300;

interface SymbolSearchOptions {
  /** How long the query has to rest before it is sent. */
  delayMs?: number;
  /** A false value clears the hits and sends nothing, for a field that is not showing. */
  enabled?: boolean;
}

interface SymbolSearchState {
  hits: StockSearchHit[];
  loading: boolean;
}

/**
 * Ticker search behind every symbol field: debounce, fetch, and stale-response
 * discard in one place. `loading` covers the rest period too, so a caller can
 * show a spinner from the first keystroke rather than from the request.
 */
export function useSymbolSearch(
  query: string,
  limit = 8,
  { delayMs = SYMBOL_SEARCH_DEBOUNCE_MS, enabled = true }: SymbolSearchOptions = {},
): SymbolSearchState {
  const live = enabled ? query.trim() : '';
  const rested = useDebouncedValue(live, delayMs);
  const [result, setResult] = useState<{ query: string; hits: StockSearchHit[] }>({ query: '', hits: [] });

  useEffect(() => {
    if (!rested) {
      setResult({ query: '', hits: [] });
      return;
    }
    // A superseded query aborts its request outright; the flag covers the
    // window between a resolved promise and a cleanup that already ran.
    const controller = new AbortController();
    let stale = false;
    // A failed request settles like an empty one: `loading` must not outlive it.
    searchStocks(rested, limit, { signal: controller.signal })
      .then((res) => {
        if (!stale) setResult({ query: rested, hits: Array.isArray(res.results) ? res.results : [] });
      })
      .catch(() => {
        if (!stale) setResult({ query: rested, hits: [] });
      });
    return () => {
      stale = true;
      controller.abort();
    };
  }, [rested, limit]);

  const hits = live ? result.hits : [];
  const loading = live !== '' && (rested !== live || result.query !== rested);
  return { hits, loading };
}
