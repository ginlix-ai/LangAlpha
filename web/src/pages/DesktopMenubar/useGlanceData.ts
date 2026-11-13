/**
 * The panel's reads. Each is the Dashboard's own source (the quote layer, the
 * holdings and watchlist entries, the intraday line) under the panel's own
 * rule for when to poll: only while it is on screen. The window stays alive
 * hidden for as long as the app runs, so a poll that ran on regardless would
 * fetch quotes all day for a panel nobody opened.
 *
 * A section the user hid is never mounted, so none of its reads run.
 */
import { useEffect, useMemo, useRef, useSyncExternalStore } from 'react';
import { useQueries, useQuery, useQueryClient } from '@tanstack/react-query';
import { isLastingRefusal } from '@/api/errors';
import { portfolioHoldingsQuery, watchlistItemsByIdQuery, watchlistItemsQuery } from '@/hooks/membershipQueries';
import { isUSEquity } from '@/lib/bars/exchanges';
import { sessionLineQuery } from '@/lib/bars/sessionQuery';
import type { QuoteRef } from '@/lib/desktop';
import { marketStatusQuery, type MarketStatusData } from '@/lib/marketUtils';
import { quoteKey, stockKey, useQuotes, type QuoteRow } from '@/lib/quotes';
import { recentThreadsQuery } from '@/pages/ChatAgent/utils/threadQueries';
import type { HoldingQuote } from './glanceSummary';
import { usePanelQuotes } from './liveQuotes';
import { splitQuote, type SplitQuote } from './quoteSplit';

/** Recent threads read for their titles; the run states come from the user feed. */
export const AGENT_THREAD_LIMIT = 30;

/** The lists themselves (holdings, watchlist entries, the session) change rarely. */
const LIST_POLL_MS = 60_000;
/** Quotes with nothing streaming them. */
const REST_POLL_MS = 15_000;
/** Quotes the socket keeps live: REST only refreshes the figures a tick does not carry. */
const STREAMED_POLL_MS = 60_000;

/** A read that runs, and polls, only while `on` (the panel on screen, its section wanting it). */
function pollWhile(on: boolean, ms: number) {
  return { enabled: on, refetchInterval: on ? ms : (false as const) };
}

function subscribeActive(onChange: () => void): () => void {
  document.addEventListener('visibilitychange', onChange);
  return () => document.removeEventListener('visibilitychange', onChange);
}

/**
 * Visible is open: the shell hides the window whenever the panel closes, and
 * leaves it throttled so its visibility tells. Focus is no test: on macOS the
 * panel is a non-activating panel, whose page may report no focus while open.
 */
function readActive(): boolean {
  return document.visibilityState === 'visible';
}

/** Whether the panel is on screen, as the window lets the page tell. */
export function usePanelActive(): boolean {
  return useSyncExternalStore(subscribeActive, readActive, () => true);
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

export function useMarketStatusGlance(active: boolean): MarketStatusData | null {
  const { data = null } = useQuery({ ...marketStatusQuery(), ...pollWhile(active, LIST_POLL_MS) });
  return data;
}

/**
 * Quotes for stocks, streamed while the panel shows where the socket is up,
 * polled where it is not. The socket carries US equities only, so the rest
 * keep the REST pace in a list that mixes the two.
 */
function useStockQuotes(symbols: string[], active: boolean) {
  const streaming = usePanelQuotes(symbols);
  const streamable = useMemo(() => symbols.filter((s) => isUSEquity(stockKey(s))), [symbols]);
  const polled = useMemo(() => symbols.filter((s) => !isUSEquity(stockKey(s))), [symbols]);
  const options = { enabled: active, staleTime: 10_000 };
  const live = useQuotes(streamable, { ...options, refetchInterval: streaming ? STREAMED_POLL_MS : REST_POLL_MS });
  const rest = useQuotes(polled, { ...options, refetchInterval: REST_POLL_MS });
  return useMemo(
    () => ({ quotes: { ...rest.quotes, ...live.quotes }, pending: { ...rest.pending, ...live.pending } }),
    [live.quotes, live.pending, rest.quotes, rest.pending],
  );
}

export interface GlanceTile extends SplitQuote {
  ref: QuoteRef;
  /** No quote yet, as opposed to none to be had. */
  pending: boolean;
  /** The latest session's closes, oldest first; empty for none. */
  line: number[];
}

export function useTilesGlance(tiles: readonly QuoteRef[], active: boolean, marketStatus: MarketStatusData | null): GlanceTile[] {
  const indexSymbols = useMemo(() => tiles.filter((t) => t.kind === 'index').map((t) => t.symbol), [tiles]);
  const stockSymbols = useMemo(() => tiles.filter((t) => t.kind === 'stock').map((t) => t.symbol), [tiles]);
  // Indices are never streamed, so they poll at the REST pace.
  usePanelQuotes(indexSymbols, { isIndex: true });
  const indexQuotes = useQuotes(indexSymbols, {
    isIndex: true,
    enabled: active,
    staleTime: 10_000,
    refetchInterval: REST_POLL_MS,
  });
  const stockQuotes = useStockQuotes(stockSymbols, active);

  // The Dashboard's session line, asked for by the caret spelling that routes an index to its endpoint.
  const lines = useQueries({
    queries: tiles.map((ref) => ({
      ...sessionLineQuery(ref.kind === 'index' ? `^${ref.symbol}` : ref.symbol),
      ...pollWhile(active, LIST_POLL_MS),
    })),
  });

  return tiles.map((ref, i) => {
    const isIndex = ref.kind === 'index';
    const key = quoteKey(ref.symbol, isIndex);
    const { quotes, pending } = isIndex ? indexQuotes : stockQuotes;
    const row = quotes[key];
    return {
      ...splitQuote(key, row, marketStatus),
      ref,
      pending: row === undefined && pending[key] !== false,
      line: lines[i]?.data?.values ?? [],
    };
  });
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
