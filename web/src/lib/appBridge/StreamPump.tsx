import { useEffect, useRef } from 'react';
import { useLocale } from '@/hooks/useLocale';
import { useQuotes, type QuoteRow } from '@/lib/quotes';
import useMarketDataWS from '@/pages/MarketView/hooks/useMarketDataWS';
import { toQuote, type QuoteArgs } from './methods';
import type { Quote } from './protocol';

function keysOf(subs: ReadonlyMap<string, QuoteArgs>, isIndex: boolean): string[] {
  const keys = new Set<string>();
  for (const sub of subs.values()) if (sub.isIndex === isIndex) sub.keys.forEach((k) => keys.add(k));
  return [...keys].sort();
}

interface StreamPumpProps {
  /** The frame's `quotes.stream` subscriptions by id. */
  subs: ReadonlyMap<string, QuoteArgs>;
  onQuote: (id: string, quote: Quote) => void;
}

/**
 * The streamed quotes, mounted only while a frame streams something. They are
 * the page's own quote rows: the market socket writes each tick into its
 * symbol's row and the rows poll while it is down, so every row that changes
 * goes to the subscriptions that name it, and a new subscription gets the rows
 * there already.
 */
export function StreamPump({ subs, onQuote }: StreamPumpProps): null {
  const { subscribe, unsubscribe } = useMarketDataWS();
  const stockKeys = keysOf(subs, false);
  const { quotes: stocks } = useQuotes(stockKeys);
  const { quotes: indexes } = useQuotes(keysOf(subs, true), { isIndex: true });

  const socketKey = stockKeys.join(',');
  useEffect(() => {
    if (!socketKey) return;
    const list = socketKey.split(',');
    subscribe(list);
    return () => unsubscribe(list);
  }, [socketKey, subscribe, unsubscribe]);

  // A quote carries words in the app's language (its freshness label), made
  // as it is sent: a switch sends every row again in the new one, since a
  // closed market may not tick again today.
  const locale = useLocale();
  const sentIn = useRef(locale);
  const sent = useRef(new Map<string, Map<string, QuoteRow>>());
  useEffect(() => {
    if (sentIn.current !== locale) {
      sentIn.current = locale;
      sent.current.clear();
    }
    for (const id of [...sent.current.keys()]) if (!subs.has(id)) sent.current.delete(id);
    for (const [id, { keys, isIndex }] of subs) {
      const rows = isIndex ? indexes : stocks;
      let mine = sent.current.get(id);
      if (!mine) sent.current.set(id, (mine = new Map()));
      for (const key of keys) {
        const row = rows[key];
        if (!row || mine.get(key) === row) continue;
        mine.set(key, row);
        onQuote(id, toQuote(key, row));
      }
    }
  }, [subs, stocks, indexes, onQuote, locale]);

  return null;
}
