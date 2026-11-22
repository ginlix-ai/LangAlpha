/**
 * The panel's reads. Each is the Dashboard's own source (the quote layer, the
 * holdings and watchlist entries, the intraday line) under the panel's own
 * rule for when to poll: only while it is on screen. The window stays alive
 * hidden for as long as the app runs, so a poll that ran on regardless would
 * fetch quotes all day for a panel nobody opened.
 *
 * A section the user hid is never mounted, so none of its reads run.
 */
import { useEffect, useMemo, useRef } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { isLastingRefusal } from '@/api/errors';
import { portfolioHoldingsQuery, watchlistItemsByIdQuery, watchlistItemsQuery } from '@/hooks/membershipQueries';
import type { MarketStatusData } from '@/lib/marketUtils';
import { stockKey, type QuoteRow } from '@/lib/quotes';
import { splitQuote, type SplitQuote } from '@/lib/quotes/splitQuote';
import { useStockQuotes } from '@/lib/quotes/useQuoteTiles';
import { recentThreadsQuery } from '@/pages/ChatAgent/utils/threadQueries';
import type { HoldingQuote } from './glanceSummary';

/** Recent threads read for their titles; the run states come from the user feed. */
export const AGENT_THREAD_LIMIT = 30;

/** The lists themselves (holdings, watchlist entries) change rarely. */
const LIST_POLL_MS = 60_000;

/** A read that runs, and polls, only while `on` (the panel on screen, its section wanting it). */
function pollWhile(on: boolean, ms: number) {
  return { enabled: on, refetchInterval: on ? ms : (false as const) };
}

/**
 * Opening the panel refreshes what it shows, however fresh the cache thinks
 * it is: it was closed, so its numbers are as old as the last open. Not
 * `refetchOnWindowFocus`: the panel opens without taking focus.
 */
export function useRefetchOnOpen(active: boolean): void {
  const queryClient = useQueryClient();
  const wasActive = useRef(active);
  useEffect(() => {
    // Runs after the sections' own effects, so their queries are enabled by now.
    if (active && !wasActive.current) void queryClient.refetchQueries({ type: 'active' }, { cancelRefetch: false });
    wasActive.current = active;
  }, [active, queryClient]);
}

export function usePortfolioGlance(active: boolean, marketStatus: MarketStatusData | null) {
  const { data: holdings = [], isSuccess: loaded, isError } = useQuery({
    ...portfolioHoldingsQuery,
    ...pollWhile(active, LIST_POLL_MS),
  });
  const symbols = useMemo(() => holdings.map((h) => stockKey(h.symbol)).filter(Boolean), [holdings]);
  const { quotes, pending } = useStockQuotes(symbols, active);

  const rows = useMemo<HoldingQuote[]>(() => holdingQuotes(holdings, quotes, marketStatus), [holdings, quotes, marketStatus]);
  const quotesPending = symbols.some((symbol) => quotes[symbol] === undefined && pending[symbol] !== false);
  return { rows, count: holdings.length, loaded, isError, quotesPending };
}

/** Holdings joined to their quotes at the official price, the shape the summary reads. */
export function holdingQuotes(
  holdings: readonly { symbol?: string | null; quantity?: number | null; currency?: string | null }[],
  quotes: Record<string, QuoteRow | undefined>,
  marketStatus: MarketStatusData | null,
): HoldingQuote[] {
  return holdings.map((h) => {
    const symbol = stockKey(h.symbol ?? '');
    const q = splitQuote(symbol, quotes[symbol], marketStatus);
    return {
      symbol,
      quantity: Number(h.quantity || 0),
      price: q.price,
      change: q.change,
      changePercent: q.pct,
      currency: q.currency ?? h.currency ?? 'USD',
      ext: q.ext,
    };
  });
}

export interface WatchGlanceRow extends SplitQuote {
  symbol: string;
  assetClass: string | null;
  /** No quote yet, as opposed to none to be had. */
  pending: boolean;
}

/**
 * Whether the panel shows the default watchlist instead of the chosen one:
 * none is chosen, it never loaded, or the server refused it for good (gone,
 * not this user's). A refresh that failed in passing keeps the list it had.
 */
export function showsDefaultWatchlist(
  watchlistId: string | null,
  chosen: { isError: boolean; data: unknown; error: unknown },
): boolean {
  if (watchlistId == null) return true;
  return chosen.isError && (chosen.data === undefined || isLastingRefusal(chosen.error));
}

/**
 * The chosen watchlist, or the default one (the app's shared entry) when none
 * is chosen or the chosen one is gone.
 */
function useWatchlistEntries(watchlistId: string | null, active: boolean) {
  const chosen = useQuery({
    ...watchlistItemsByIdQuery(watchlistId ?? ''),
    ...pollWhile(active && watchlistId != null, LIST_POLL_MS),
    retry: 1,
  });
  const useDefault = showsDefaultWatchlist(watchlistId, chosen);
  const fallback = useQuery({ ...watchlistItemsQuery, ...pollWhile(active && useDefault, LIST_POLL_MS) });
  return useDefault
    ? { items: fallback.data?.items, loaded: fallback.isSuccess }
    : { items: chosen.data?.items, loaded: chosen.isSuccess };
}

export function useWatchlistGlance(
  active: boolean,
  watchlistId: string | null,
  shown: number,
  marketStatus: MarketStatusData | null,
) {
  const { items, loaded } = useWatchlistEntries(watchlistId, active);
  const all = useMemo(() => (items ?? []).map((i) => stockKey(i.symbol)).filter(Boolean), [items]);
  // Quotes only for the rows on screen.
  const symbols = useMemo(() => all.slice(0, shown), [all, shown]);
  const { quotes, pending } = useStockQuotes(symbols, active);
  const rows = useMemo<WatchGlanceRow[]>(
    () =>
      symbols.map((symbol) => ({
        ...splitQuote(symbol, quotes[symbol], marketStatus),
        symbol,
        assetClass: quotes[symbol]?.asset_class ?? null,
        pending: quotes[symbol] === undefined && pending[symbol] !== false,
      })),
    [symbols, quotes, pending, marketStatus],
  );
  return { rows, total: all.length, loaded };
}

export function useRecentThreadsGlance(active: boolean) {
  const { data, isSuccess: loaded } = useQuery({ ...recentThreadsQuery(AGENT_THREAD_LIMIT), enabled: active });
  return { threads: data?.threads ?? [], loaded };
}
