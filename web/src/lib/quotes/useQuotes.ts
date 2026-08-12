/**
 * useQuote / useQuotes — the unified frontend quote layer.
 *
 * Every symbol lives under a single React Query entry (['quote', SYMBOL]), so
 * overlapping watchlists / portfolios / charts share one cache entry and one
 * poll instead of each fetching its own overlapping batch. The queryFn delegates
 * to the coalescing batcher, which merges concurrent same-window requests into
 * one HTTP call and fans the result back out across the per-symbol keys.
 *
 * Missing rows (unknown/unresolvable symbols the backend drops) surface as
 * `quote === undefined` — never an error.
 */
import { useCallback, useMemo } from 'react';
import { useQueries, useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { getQuoteBatcher, quoteKey, type QuoteRow } from './quoteBatcher';

export interface UseQuotesOptions {
  /** Route through the index snapshot endpoint (and strip a leading '^'). */
  isIndex?: boolean;
  /** Disable fetching entirely (e.g. no symbol selected yet). */
  enabled?: boolean;
  /** Freshness window. Defaults to the 30s cadence the widgets used. */
  staleTime?: number;
  /** Background poll cadence. Defaults to 60s; pass `false` to disable. */
  refetchInterval?: number | false;
}

export interface UseQuotesResult {
  /** Normalized-key → quote row (undefined when the symbol has no quote). */
  quotes: Record<string, QuoteRow | undefined>;
  isLoading: boolean;
  isFetching: boolean;
  refetch: () => void;
}

const DEFAULT_STALE_MS = 30_000;
const DEFAULT_REFETCH_MS = 60_000;

/**
 * Subscribe to quotes for a set of symbols. Symbols are normalized + de-duped;
 * each distinct key becomes one shared React Query entry.
 */
export function useQuotes(symbols: string[], options: UseQuotesOptions = {}): UseQuotesResult {
  const {
    isIndex = false,
    enabled = true,
    staleTime = DEFAULT_STALE_MS,
    refetchInterval = DEFAULT_REFETCH_MS,
  } = options;
  const queryClient = useQueryClient();

  const keys = useMemo(() => {
    const seen = new Set<string>();
    const out: string[] = [];
    for (const s of symbols || []) {
      const k = quoteKey(s, isIndex);
      if (k && !seen.has(k)) {
        seen.add(k);
        out.push(k);
      }
    }
    return out;
  }, [symbols, isIndex]);

  const results = useQueries({
    queries: keys.map((key) => ({
      queryKey: queryKeys.quote.detail(key),
      queryFn: () => getQuoteBatcher(queryClient).request(key, { isIndex }),
      enabled,
      staleTime,
      refetchInterval: enabled ? refetchInterval : (false as const),
      refetchIntervalInBackground: false,
    })),
  });

  // Cheap change-signature so `quotes` stays referentially stable across no-op
  // re-renders (dataUpdatedAt moves on every fetch AND every WS write-through).
  const signature = results.map((r) => `${r.status}:${r.dataUpdatedAt}`).join('|');

  const quotes = useMemo(() => {
    const out: Record<string, QuoteRow | undefined> = {};
    results.forEach((r, i) => {
      out[keys[i]] = (r.data ?? undefined) as QuoteRow | undefined;
    });
    return out;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [signature, keys]);

  const isLoading = results.some((r) => r.isLoading);
  const isFetching = results.some((r) => r.isFetching);

  const refetch = useCallback(() => {
    keys.forEach((key) => {
      void queryClient.refetchQueries({ queryKey: queryKeys.quote.detail(key) });
    });
  }, [keys, queryClient]);

  return { quotes, isLoading, isFetching, refetch };
}

export interface UseQuoteOptions extends UseQuotesOptions {}

export interface UseQuoteResult {
  quote: QuoteRow | undefined;
  isLoading: boolean;
  isFetching: boolean;
  refetch: () => void;
}

/** Single-symbol convenience wrapper over useQuotes. */
export function useQuote(symbol: string | null | undefined, options: UseQuoteOptions = {}): UseQuoteResult {
  const enabled = (options.enabled ?? true) && !!symbol;
  const list = useMemo(() => (symbol ? [symbol] : []), [symbol]);
  const { quotes, isLoading, isFetching, refetch } = useQuotes(list, { ...options, enabled });
  const key = symbol ? quoteKey(symbol, options.isIndex) : '';
  return { quote: key ? quotes[key] : undefined, isLoading, isFetching, refetch };
}
