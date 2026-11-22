/**
 * Prices for the tickers a thread follows: the ones it watches and the ones an
 * armed price wakeup waits on. Read from the app's shared quote cache,
 * streamed over the market-data socket where the feed carries the symbol and
 * polled where it does not, the way the menu bar's panel reads its own. So the
 * rows tick while the chat is in front and the page on screen, whether or not
 * the agent is working, where the agent's own watch block only arrives during
 * a turn while a venue is open. Read through `threadQuotesContext`.
 */
import { useCallback, useMemo, useState, type ReactNode } from 'react';
import { usePageVisible } from '@/hooks/usePageVisible';
import { quoteCurrency } from '@/lib/bars';
import type { QuoteRef } from '@/lib/desktop';
import { marketPhase } from '@/lib/marketUtils';
import { LiveQuotesProvider } from '@/lib/quotes/liveQuotes';
import { useMarketStatus } from '@/lib/quotes/useMarketStatus';
import { useQuoteTiles } from '@/lib/quotes/useQuoteTiles';
import { armedPrice } from '../wakeupLabels';
import { QuoteLinesContext, ThreadQuotesContext, type ThreadQuotes } from './threadQuotesContext';
import type { TrayContents } from './useTrayContents';

/** Fixed quotes for everything inside, for a fixture with no market data to read. */
export function FixedThreadQuotes({ quotes, children }: { quotes: ThreadQuotes; children: ReactNode }) {
  return <ThreadQuotesContext value={quotes}>{children}</ThreadQuotesContext>;
}

/** A caret names an index, which the quote API asks for without it. */
function quoteRef(symbol: string): QuoteRef {
  return symbol.startsWith('^') ? { kind: 'index', symbol: symbol.slice(1) } : { kind: 'stock', symbol };
}

/** The watched tickers, then each armed price wakeup's not already watched,
 *  as one key so the list keeps its identity while the tray's changes around it. */
function quotedSymbolsKey(contents: TrayContents | null): string {
  const symbols = new Set(contents?.watch?.symbols ?? []);
  for (const w of contents?.wakeups?.items ?? []) {
    const price = armedPrice(w);
    if (price) symbols.add(price.symbol);
  }
  return [...symbols].join(',');
}

function Quotes({ symbols, active, children }: { symbols: readonly string[]; active: boolean; children: ReactNode }) {
  const refs = useMemo(() => symbols.map(quoteRef), [symbols]);
  const [lineReaders, setLineReaders] = useState(0);
  const askLines = useCallback(() => {
    setLineReaders((n) => n + 1);
    return () => setLineReaders((n) => n - 1);
  }, []);
  const status = useMarketStatus(active);
  const tiles = useQuoteTiles(refs, active, status, { lines: lineReaders > 0 });
  const value = useMemo<ThreadQuotes>(() => ({
    phase: marketPhase(status),
    bySymbol: Object.fromEntries(symbols.map((symbol, i) => {
      const { price, pct, ext, places, currency, line, live } = tiles[i];
      return [symbol, { price, pct, ext, places, currency: quoteCurrency(currency, symbol), line, live }];
    })),
  }), [symbols, tiles, status]);
  return (
    <QuoteLinesContext value={askLines}>
      <ThreadQuotesContext value={value}>{children}</ThreadQuotesContext>
    </QuoteLinesContext>
  );
}

/**
 * Quotes for what the tray follows, for everything inside. Read only while
 * the chat is the one in front (`active`: the page keeps others mounted out of
 * sight) and the page is on screen.
 */
export function ThreadQuotesProvider({ contents, active, children }: {
  contents: TrayContents | null;
  active: boolean;
  children: ReactNode;
}) {
  const key = quotedSymbolsKey(contents);
  const symbols = useMemo(() => (key ? key.split(',') : []), [key]);
  const visible = usePageVisible();
  const reading = active && visible && symbols.length > 0;
  return (
    <LiveQuotesProvider active={reading}>
      <Quotes symbols={symbols} active={reading}>{children}</Quotes>
    </LiveQuotesProvider>
  );
}
