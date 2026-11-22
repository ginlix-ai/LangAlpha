/**
 * The quotes a surface shows, and live prices for them while it is on screen
 * (the menu bar panel, a chat's watched tickers). Each part of it names its
 * quotes here; the stocks among them stream over one market-data socket, open
 * only while `active`, which writes each tick into the shared quote cache
 * (`useMarketDataWS`'s write-through) that the rows already read. Where the
 * socket is down or the deployment has none, the rows keep polling REST
 * instead. The menu bar's liveness badge reads the same list, so it speaks
 * for exactly what is on screen.
 *
 * The socket lives in its own component, mounted only while active, so
 * leaving closes it at once rather than after the hook's minute of grace for
 * a hidden tab.
 */
import { createContext, useContext, useEffect, useId, useMemo, useState, useSyncExternalStore, type ReactNode } from 'react';
import { isUSEquity } from '@/lib/bars/exchanges';
import { createEmitter } from '@/lib/emitter';
import { quoteKey } from './quoteBatcher';
import useMarketDataWS from '@/pages/MarketView/hooks/useMarketDataWS';

/** A quote on screen: its cache key, and whether it is an index's. */
export interface ShownQuote {
  key: string;
  isIndex: boolean;
}

/** Each distinct set, sorted and joined, so a snapshot is one string per set. */
function joined(lists: Iterable<readonly string[]>): string {
  const all = new Set<string>();
  for (const list of lists) for (const s of list) all.add(s);
  return [...all].sort().join(',');
}

class LiveQuotes {
  private readonly streamedBySection = new Map<string, readonly string[]>();
  private readonly shownBySection = new Map<string, readonly string[]>();
  private streaming = false;
  private tickingKey = '';
  private streamedKey = '';
  private shownKey = '';
  private readonly changes = createEmitter();

  subscribe = (listener: () => void) => this.changes.subscribe(listener);

  set(id: string, shown: readonly string[], streamed: readonly string[]): void {
    for (const [map, list] of [[this.shownBySection, shown], [this.streamedBySection, streamed]] as const) {
      if (list.length) map.set(id, list);
      else map.delete(id);
    }
    const shownKey = joined(this.shownBySection.values());
    const streamedKey = joined(this.streamedBySection.values());
    if (shownKey === this.shownKey && streamedKey === this.streamedKey) return;
    this.shownKey = shownKey;
    this.streamedKey = streamedKey;
    this.changes.emit();
  }

  /** The stocks to stream, as `AAPL,MSFT`. */
  readStreamed = () => this.streamedKey;

  /** Every quote on screen, as `i:GSPC,s:AAPL`. */
  readShown = () => this.shownKey;

  setStreaming(on: boolean): void {
    if (on === this.streaming) return;
    this.streaming = on;
    this.changes.emit();
  }

  readStreaming = () => this.streaming;

  /** The stocks with a tick within the socket's freshness window, as one string. */
  setTicking(symbols: Iterable<string>): void {
    const key = [...new Set(symbols)].sort().join(',');
    if (key === this.tickingKey) return;
    this.tickingKey = key;
    this.changes.emit();
  }

  readTicking = () => this.tickingKey;
}

const LiveQuotesContext = createContext<LiveQuotes | null>(null);

export function LiveQuotesProvider({ active, children }: { active: boolean; children: ReactNode }) {
  const [quotes] = useState(() => new LiveQuotes());
  // No socket at all for a surface showing no stock.
  const named = useSyncExternalStore(quotes.subscribe, quotes.readStreamed, quotes.readStreamed) !== '';
  return (
    <LiveQuotesContext value={quotes}>
      {active && named && <LivePump quotes={quotes} />}
      {children}
    </LiveQuotesContext>
  );
}

const noSubscribe = () => () => {};
const notStreaming = () => false;
const nothingShown = () => '';

function useStreaming(quotes: LiveQuotes | null): boolean {
  return useSyncExternalStore(quotes?.subscribe ?? noSubscribe, quotes?.readStreaming ?? notStreaming, notStreaming);
}

/**
 * Name the quotes this part of the surface shows. Stocks are streamed while
 * it is active; answers whether they are, so the caller polls REST only when
 * not.
 */
// eslint-disable-next-line react-refresh/only-export-components
export function useLiveQuotes(symbols: readonly string[], { isIndex = false }: { isIndex?: boolean } = {}): boolean {
  const quotes = useContext(LiveQuotesContext);
  const id = useId();
  const keys = symbols.map((s) => quoteKey(s, isIndex)).filter(Boolean);
  const shown = keys.map((k) => `${isIndex ? 'i' : 's'}:${k}`).join(',');
  // The feed is US equities only; anything else stays on REST.
  const streamed = isIndex ? '' : keys.filter((k) => isUSEquity(k)).join(',');
  useEffect(() => {
    if (!quotes) return;
    quotes.set(id, shown ? shown.split(',') : [], streamed ? streamed.split(',') : []);
    return () => quotes.set(id, [], []);
  }, [quotes, id, shown, streamed]);
  return useStreaming(quotes) && streamed !== '';
}

/**
 * Every quote on screen, and the stocks the socket is live for: connected, with
 * a tick for that symbol inside its freshness window, as Market View reads it.
 * A connected socket alone says nothing, since a name with no prints sends none.
 */
// eslint-disable-next-line react-refresh/only-export-components
export function useShownQuotes(): { shown: ShownQuote[]; isLive: (quote: ShownQuote) => boolean } {
  const quotes = useContext(LiveQuotesContext);
  const subscribe = quotes?.subscribe ?? noSubscribe;
  const shownKey = useSyncExternalStore(subscribe, quotes?.readShown ?? nothingShown, nothingShown);
  const tickingKey = useSyncExternalStore(subscribe, quotes?.readTicking ?? nothingShown, nothingShown);
  const streaming = useStreaming(quotes);
  const shown = useMemo(
    () => (shownKey ? shownKey.split(',').map((s) => ({ key: s.slice(2), isIndex: s.startsWith('i:') })) : []),
    [shownKey],
  );
  // The socket ticks stocks under their cache key, and never an index. Kept
  // while the ticking set holds, so a reader memoized on it holds too.
  const isLive = useMemo(() => {
    const live = new Set(streaming && tickingKey ? tickingKey.split(',') : []);
    return ({ key, isIndex }: ShownQuote) => !isIndex && live.has(key);
  }, [streaming, tickingKey]);
  return { shown, isLive };
}

function LivePump({ quotes }: { quotes: LiveQuotes }): null {
  const { prices, subscribe, unsubscribe, connectionStatus } = useMarketDataWS();
  const key = useSyncExternalStore(quotes.subscribe, quotes.readStreamed, quotes.readStreamed);

  useEffect(() => {
    if (!key) return;
    const list = key.split(',');
    subscribe(list);
    return () => unsubscribe(list);
  }, [key, subscribe, unsubscribe]);

  useEffect(() => {
    quotes.setStreaming(connectionStatus === 'connected');
    return () => quotes.setStreaming(false);
  }, [quotes, connectionStatus]);

  // The socket keeps a symbol only while its last tick is within the window.
  useEffect(() => {
    quotes.setTicking(prices.keys());
    return () => quotes.setTicking([]);
  }, [quotes, prices]);

  return null;
}
