import { useQuery } from '@tanstack/react-query';
import { getNews, getIndices, INDEX_SYMBOLS, fallbackIndex, normalizeIndexSymbol } from '../utils/api';
import { fetchMarketStatus } from '@/lib/marketUtils';

/**
 * Formats a given timestamp to a relative time string (e.g. "just now", "10 min ago").
 */
function formatRelativeTime(timestamp) {
  if (!timestamp) return '';
  const now = new Date();
  const then = new Date(timestamp);
  const diffMs = now - then;
  const diffMin = Math.floor(diffMs / 60000);
  if (diffMin < 1) return 'just now';
  if (diffMin < 60) return `${diffMin} min ago`;
  const diffHr = Math.floor(diffMin / 60);
  if (diffHr < 24) return `${diffHr} hr${diffHr > 1 ? 's' : ''} ago`;
  const diffDay = Math.floor(diffHr / 24);
  return `${diffDay} day${diffDay > 1 ? 's' : ''} ago`;
}

/**
 * useDashboardData Hook
 * Uses TanStack Query to manage fetching, caching, and auto-polling of data.
 * Eliminates race conditions and reduces boilerplate of manual useEffects.
 */
export function useDashboardData() {
  // 1. Market Status (Polls every 60s, cached globally)
  const { data: marketStatus = null } = useQuery({
    queryKey: ['dashboard', 'marketStatus'],
    queryFn: fetchMarketStatus,
    refetchInterval: 60000,
    staleTime: 30000,
  });

  // 2. Market Indices (Adaptive Polling: 30s open / 60s closed)
  const isMarketOpen = marketStatus?.market === 'open' ||
    (marketStatus && !marketStatus.afterHours && !marketStatus.earlyHours && marketStatus.market !== 'closed');

  const { data: indices, isLoading: indicesLoading } = useQuery({
    queryKey: ['dashboard', 'indices', INDEX_SYMBOLS],
    queryFn: async () => {
      const { indices: next } = await getIndices(INDEX_SYMBOLS);
      return next;
    },
    // Using initialData provides standard fallback values instantly
    initialData: () => INDEX_SYMBOLS.map((s) => fallbackIndex(normalizeIndexSymbol(s))),
    refetchInterval: isMarketOpen ? 30000 : 60000,
    staleTime: 10000,
  });

  // 3. News Feed (Fetched once, cached for 5 minutes)
  const { data: newsItems = [], isLoading: newsLoading } = useQuery({
    queryKey: ['dashboard', 'news'],
    queryFn: async () => {
      const data = await getNews({ limit: 50 });
      if (data.results && data.results.length > 0) {
        return data.results.map((r) => ({
          id: r.id,
          title: r.title,
          time: formatRelativeTime(r.published_at),
          isHot: r.has_sentiment,
          source: r.source?.name || '',
          favicon: r.source?.favicon_url || null,
          image: r.image_url || null,
          tickers: r.tickers || [],
        }));
      }
      return [];
    },
    staleTime: 5 * 60 * 1000, // 5 minutes fresh cache
  });

  return {
    indices,
    indicesLoading,
    newsItems,
    newsLoading,
    marketStatus,
    // Kept for backward compatibility with components that might use MarketStatusRef
    marketStatusRef: { current: marketStatus }
  };
}
