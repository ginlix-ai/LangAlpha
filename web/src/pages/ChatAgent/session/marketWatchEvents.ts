/**
 * Market-watch SSE handling: the live watch-chip snapshot. Moves to
 * session/marketWatchEvents.ts with the live-router carve (5.7).
 */

/**
 * Latest market-watch snapshot for a thread. Seeded from the GET
 * `/market-watch` endpoint on thread load and overwritten live by
 * `market_watch_update` SSE events; drives the persistent "Watching …" chip.
 */
export interface MarketWatchState {
  symbols: string[];
  content?: string;
  timestamp?: number;
}

/**
 * Coerce an untrusted `symbols` field (SSE event or GET response) to a
 * `string[]`: a missing/non-array value becomes an empty list ("watch off")
 * and non-string entries are dropped.
 */
export function coerceSymbols(value: unknown): string[] {
  return Array.isArray(value)
    ? value.filter((s): s is string => typeof s === 'string')
    : [];
}

/**
 * Handles `market_watch_update` custom SSE events during streaming. Parses the
 * live watch snapshot (symbols + content + timestamp) off the event and hands
 * it to the caller's setter so the persistent watch chip stays current
 * mid-turn. Non-string symbol entries are dropped and a missing/non-array
 * `symbols` field coerces to an empty list (the "watch off" signal).
 */
export function handleMarketWatchUpdate({ event, setMarketWatch }: {
  event: { symbols?: unknown; content?: unknown; timestamp?: unknown };
  setMarketWatch: (next: MarketWatchState) => void;
}): boolean {
  const symbols = coerceSymbols(event.symbols);
  setMarketWatch({
    symbols,
    content: typeof event.content === 'string' ? event.content : undefined,
    timestamp: typeof event.timestamp === 'number' ? event.timestamp : undefined,
  });
  return true;
}
