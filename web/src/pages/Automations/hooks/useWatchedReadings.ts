import { useMemo } from 'react';
import { useQuotes } from '@/lib/quotes';
import type { Automation } from '@/types/automation';
import { meterReading, watchedQuote, type MeterReading } from '../utils/price';

export interface WatchedReading {
  symbol: string;
  reading: MeterReading | null;
  /** The quote lookup has answered, so a missing price means there is none
   *  rather than that it has not arrived yet. */
  settled: boolean;
  quoted: boolean;
}

/**
 * Live distance to the trigger for every price automation in the list. Stocks
 * and indices go to different snapshot endpoints, so they are asked for in two
 * batches; both share the app-wide per-symbol quote cache.
 */
export function useWatchedReadings(automations: Automation[]): Map<string, WatchedReading> {
  const watched = useMemo(
    () =>
      automations
        .filter((a) => a.trigger_type === 'price')
        .map((a) => ({ a, q: watchedQuote(a.trigger_config) }))
        .filter((x): x is { a: Automation; q: NonNullable<ReturnType<typeof watchedQuote>> } => x.q != null),
    [automations],
  );
  const stocks = useMemo(() => watched.filter((w) => !w.q.isIndex).map((w) => w.q.symbol), [watched]);
  const indices = useMemo(() => watched.filter((w) => w.q.isIndex).map((w) => w.q.symbol), [watched]);

  const { quotes: stockQuotes, isLoading: stocksLoading } = useQuotes(stocks, { enabled: stocks.length > 0 });
  const { quotes: indexQuotes, isLoading: indicesLoading } = useQuotes(indices, {
    isIndex: true,
    enabled: indices.length > 0,
  });

  return useMemo(() => {
    const out = new Map<string, WatchedReading>();
    for (const { a, q } of watched) {
      const quote = (q.isIndex ? indexQuotes : stockQuotes)[q.symbol];
      out.set(a.automation_id, {
        symbol: a.trigger_config?.symbol.toUpperCase() ?? q.symbol,
        reading: meterReading(a.trigger_config, quote),
        settled: !(q.isIndex ? indicesLoading : stocksLoading),
        quoted: quote?.price != null && quote.price > 0,
      });
    }
    return out;
  }, [watched, stockQuotes, indexQuotes, stocksLoading, indicesLoading]);
}
