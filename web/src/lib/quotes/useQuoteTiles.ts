/**
 * Quotes for a set of tickers, live where the market-data socket carries them
 * and polled where it does not, under the caller's rule for when to read at
 * all. The menu bar panel and a chat's watched tickers read through here, so
 * one ticker on both ticks from one cache.
 */
import { useMemo } from 'react';
import { useQueries } from '@tanstack/react-query';
import { isUSEquity } from '@/lib/bars/exchanges';
import { sessionLineQuery } from '@/lib/bars/sessionQuery';
import type { QuoteRef } from '@/lib/desktop';
import type { MarketStatusData } from '@/lib/marketUtils';
import { useLiveQuotes, useShownQuotes } from './liveQuotes';
import { quoteKey, stockKey } from './quoteBatcher';
import { useQuotes } from './useQuotes';
import { splitQuote, type SplitQuote } from './splitQuote';

/** Quotes with nothing streaming them. */
const REST_POLL_MS = 15_000;
/** Quotes the socket keeps live: REST only refreshes the figures a tick does not carry. */
const STREAMED_POLL_MS = 60_000;
/** The intraday line, which a minute bar moves at most once a minute. */
const LINE_POLL_MS = 60_000;

/**
 * Quotes for stocks, streamed while `active` where the socket is up, polled
 * where it is not. The socket carries US equities only, so the rest keep the
 * REST pace in a list that mixes the two.
 */
export function useStockQuotes(symbols: string[], active: boolean) {
  const streaming = useLiveQuotes(symbols);
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

export interface QuoteTile extends SplitQuote {
  ref: QuoteRef;
  /** No quote yet, as opposed to none to be had. */
  pending: boolean;
  /** A tick reached it inside the socket's freshness window: what the menu
   *  bar's liveness badge calls streaming. */
  live: boolean;
  /** The latest session's closes, oldest first; empty for none, or when not asked for. */
  line: number[];
}

/**
 * Each ticker's quote, split at the session. With `lines`, today's line too,
 * a query per ticker on a minute's poll, so only a surface that draws it asks.
 */
export function useQuoteTiles(
  tiles: readonly QuoteRef[],
  active: boolean,
  marketStatus: MarketStatusData | null,
  { lines = false }: { lines?: boolean } = {},
): QuoteTile[] {
  const indexSymbols = useMemo(() => tiles.filter((t) => t.kind === 'index').map((t) => t.symbol), [tiles]);
  const stockSymbols = useMemo(() => tiles.filter((t) => t.kind === 'stock').map((t) => t.symbol), [tiles]);
  // Indices are never streamed, so they poll at the REST pace.
  useLiveQuotes(indexSymbols, { isIndex: true });
  const { quotes: indexRows, pending: indexPending } = useQuotes(indexSymbols, {
    isIndex: true,
    enabled: active,
    staleTime: 10_000,
    refetchInterval: REST_POLL_MS,
  });
  const { quotes: stockRows, pending: stockPending } = useStockQuotes(stockSymbols, active);
  const { isLive } = useShownQuotes();

  // The Dashboard's session line, asked for by the caret spelling that routes an index to its endpoint.
  const reading = active && lines;
  const sessionLines = useQueries({
    queries: tiles.map((ref) => ({
      ...sessionLineQuery(ref.kind === 'index' ? `^${ref.symbol}` : ref.symbol),
      enabled: reading,
      refetchInterval: reading ? LINE_POLL_MS : (false as const),
    })),
    combine: lineValues,
  });

  return useMemo(() => tiles.map((ref, i) => {
    const isIndex = ref.kind === 'index';
    const key = quoteKey(ref.symbol, isIndex);
    const row = (isIndex ? indexRows : stockRows)[key];
    return {
      ...splitQuote(key, row, marketStatus),
      ref,
      pending: row === undefined && (isIndex ? indexPending : stockPending)[key] !== false,
      live: isLive({ key, isIndex }),
      line: lines ? sessionLines[i] ?? NO_LINE : NO_LINE,
    };
  }), [tiles, indexRows, indexPending, stockRows, stockPending, isLive, sessionLines, marketStatus, lines]);
}

const NO_LINE: number[] = [];

/** Each line's closes, kept by identity while they hold (the combine's result is shared deeply). */
function lineValues(results: readonly { data?: { values: number[] } | null }[]): number[][] {
  return results.map((result) => result.data?.values ?? NO_LINE);
}
