/**
 * React Query wrapper around ``fetchStockData`` for read-only OHLC bars.
 *
 * The live MarketView chart loads bars imperatively; this hook exists for
 * lightweight consumers (e.g. the chat transcript's annotation preview) that
 * just want a cached array of bars for a ``(symbol, interval)`` without the
 * websocket / quote / overview machinery. Cached and deduped by query key so
 * repeated cards for the same symbol share one request.
 */

import { useQuery } from '@tanstack/react-query';

import { queryKeys } from '@/lib/queryKeys';
import type { ChartDataPoint } from '@/types/market';

import { fetchStockData } from '../utils/api';
import { INITIAL_LOAD_DAYS } from '../utils/chartConstants';

const FIVE_MIN = 5 * 60 * 1000;
const THIRTY_MIN = 30 * 60 * 1000;

/**
 * The same initial history window the live chart loads for this interval, so the
 * preview agrees with what opens. ``0`` days (e.g. daily) means full history.
 */
function previewRange(interval: string): { from?: string; to?: string } {
  const days = INITIAL_LOAD_DAYS[interval] ?? 90;
  if (days <= 0) return {};
  const fmt = (d: Date) => d.toLocaleDateString('en-CA', { timeZone: 'America/New_York' });
  const to = new Date();
  const from = new Date();
  from.setDate(from.getDate() - days);
  return { from: fmt(from), to: fmt(to) };
}

interface UseStockBarsOptions {
  /** Gate the fetch (e.g. only when the card is the resting preview). */
  enabled?: boolean;
}

interface UseStockBarsResult {
  bars: ChartDataPoint[];
  isLoading: boolean;
  isError: boolean;
}

/** Fetch cached OHLC bars for a symbol + interval. */
export function useStockBars(
  symbol: string | null | undefined,
  interval: string,
  { enabled = true }: UseStockBarsOptions = {},
): UseStockBarsResult {
  const sym = (symbol || '').trim().toUpperCase();
  const active = enabled && !!sym;

  const query = useQuery({
    queryKey: queryKeys.marketData.bars(sym, interval),
    queryFn: async ({ signal }) => {
      const { from, to } = previewRange(interval);
      const result = await fetchStockData(sym, interval, from, to, { signal });
      // fetchStockData reports soft errors in-band; surface a real failure so
      // React Query marks the query errored instead of caching an empty array.
      if (!result.data?.length && result.error) throw new Error(result.error);
      return result.data;
    },
    enabled: active,
    staleTime: FIVE_MIN,
    gcTime: THIRTY_MIN,
    retry: 1,
  });

  return {
    bars: query.data ?? [],
    isLoading: active && query.isLoading,
    isError: query.isError,
  };
}
