import { useQuery } from '@tanstack/react-query';
import { getNews, getIndices, INDEX_SYMBOLS, fallbackIndex, normalizeIndexSymbol } from '../utils/api';
import { fetchMarketStatus } from '@/lib/marketUtils';
import type { IndexData } from '@/types/market';

interface MarketStatusData {
  market?: string;
  afterHours?: boolean;
  earlyHours?: boolean;
  [key: string]: unknown;
}

interface NewsItem {
  id: string;
  title: string;
  time: string;
  isHot: boolean;
  source: string;
  favicon: string | null;
  image: string | null;
  tickers: string[];
  articleUrl?: string | null;
}

interface DashboardData {
  indices: IndexData[] | undefined;
  indicesLoading: boolean;
  newsItems: NewsItem[];
  newsLoading: boolean;
  marketStatus: MarketStatusData | null;
  marketStatusRef: { current: MarketStatusData | null };
}

/**
 * Formats a given timestamp to a relative time string (e.g. "just now", "10 min ago").
 */
function formatRelativeTime(timestamp: string | number | null | undefined): string {
  if (!timestamp) return '';
  const now = new Date();
  const then = new Date(timestamp);
  const diffMs = now.getTime() - then.getTime();
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
export function useDashboardData(): DashboardData {
  // 1. Market Status (Polls every 60s, cached globally)
  const { data: marketStatus = null } = useQuery<MarketStatusData | null>({
    queryKey: ['dashboard', 'marketStatus'],
    queryFn: fetchMarketStatus,
    refetchInterval: 60000,
    refetchIntervalInBackground: false,
    staleTime: 30000,
  });

  // 2. Market Indices (Adaptive Polling: 30s open / 60s closed)
  const isMarketOpen = marketStatus?.market === 'open' ||
    (marketStatus && !marketStatus.afterHours && !marketStatus.earlyHours && marketStatus.market !== 'closed');

  const { data: indices, isLoading: indicesLoading } = useQuery<IndexData[]>({
    queryKey: ['dashboard', 'indices', INDEX_SYMBOLS],
    queryFn: async () => {
      const { indices: next } = await getIndices(INDEX_SYMBOLS);
      return next;
    },
    // Using placeholderData provides standard fallback values instantly 
    // without populating the cache as "fresh", thereby triggering an immediate background fetch
    placeholderData: (): IndexData[] => INDEX_SYMBOLS.map((s) => fallbackIndex(normalizeIndexSymbol(s))),
    refetchInterval: isMarketOpen ? 30000 : 60000,
    refetchIntervalInBackground: false,
    staleTime: 10000,
  });

  // 3. News Feed (Fetched once, cached for 5 minutes)
  const { data: newsItems = [], isLoading: newsLoading } = useQuery<NewsItem[]>({
    queryKey: ['dashboard', 'news'],
    queryFn: async (): Promise<NewsItem[]> => {
      const data = await getNews({ limit: 50 });
      if (data.results && data.results.length > 0) {
        return data.results.map((r: Record<string, unknown>) => ({
          id: r.id as string,
          title: r.title as string,
          time: formatRelativeTime(r.published_at as string | null | undefined),
          isHot: r.has_sentiment as boolean,
          source: (r.source as Record<string, unknown> | undefined)?.name as string || '',
          favicon: (r.source as Record<string, unknown> | undefined)?.favicon_url as string || null,
          image: r.image_url as string || null,
          tickers: (r.tickers as string[]) || [],
          articleUrl: (r.article_url as string) || null,
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
