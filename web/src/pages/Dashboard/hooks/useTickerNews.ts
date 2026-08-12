import { useQuery } from '@tanstack/react-query';
import { getNews } from '../utils/api';
import {
  type DashboardNewsItem,
  NEWS_POLL_INTERVAL_MS,
  NEWS_STALE_MS,
  mapNewsResults,
} from '../utils/newsItem';

// Back-compat alias — the news-item shape is shared across all dashboard feeds.
export type TickerNewsItem = DashboardNewsItem;

interface TickerRow {
  symbol: string;
  [key: string]: unknown;
}

/**
 * Fetches news for a list of ticker rows via React Query — same cache/poll
 * mechanism as useDashboardData (no hand-rolled module cache or setInterval).
 * @param rows - Array of objects with a `symbol` property
 * @param cacheKey - Distinguishes feeds that share tickers (e.g. 'portfolio', 'watchlist')
 * @param provider - Optional news provider to target (e.g. 'tickertick')
 */
export function useTickerNews(rows: TickerRow[], cacheKey: string, provider?: string): { items: TickerNewsItem[]; loading: boolean } {
  const tickers = (rows || []).map((r) => r.symbol).filter(Boolean);
  // Sorted so row reordering doesn't churn the query key. The ticker set,
  // cacheKey, and provider are all in the key, so a change refetches and the
  // Classic (chain) and Custom ('tickertick') feeds never serve each other's
  // articles.
  const tickerKey = [...tickers].sort().join(',');
  const hasTickers = tickers.length > 0;

  const query = useQuery<TickerNewsItem[]>({
    queryKey: ['dashboard', 'tickerNews', cacheKey, provider ?? null, tickerKey],
    queryFn: async () => {
      const data = await getNews({ tickers, limit: 50, provider });
      return data.results?.length > 0 ? mapNewsResults(data.results) : [];
    },
    enabled: hasTickers,
    staleTime: NEWS_STALE_MS,
    refetchInterval: NEWS_POLL_INTERVAL_MS,
    refetchIntervalInBackground: false,
  });

  return {
    items: hasTickers ? (query.data ?? []) : [],
    loading: hasTickers && query.isLoading,
  };
}
