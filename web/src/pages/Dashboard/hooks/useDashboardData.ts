import { useQuery, useInfiniteQuery } from '@tanstack/react-query';
import { useMemo } from 'react';
import { getNews, getIndex, INDEX_SYMBOLS, normalizeIndexSymbol, buildIndexData } from '../utils/api';
import { useQuotes } from '@/lib/quotes';
import { fetchMarketStatus } from '@/lib/marketUtils';
import type { IndexData, SparklinePoint } from '@/types/market';
import {
  type DashboardNewsItem,
  NEWS_POLL_INTERVAL_MS,
  NEWS_STALE_MS,
  mapNewsResults,
} from '../utils/newsItem';

type NewsItem = DashboardNewsItem;

interface MarketStatusData {
  market?: string;
  afterHours?: boolean;
  earlyHours?: boolean;
  [key: string]: unknown;
}

interface DashboardData {
  indices: IndexData[] | undefined;
  indicesLoading: boolean;
  newsItems: NewsItem[];
  newsLoading: boolean;
  curatedItems: NewsItem[];
  curatedLoading: boolean;
  curatedHasNextPage: boolean;
  curatedIsFetchingNextPage: boolean;
  curatedFetchNextPage: () => void;
  marketStatus: MarketStatusData | null;
  marketStatusRef: { current: MarketStatusData | null };
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
  const indexRefetch = isMarketOpen ? 30000 : 60000;

  // Index price/change flows through the shared quote layer (['quote', NORM]),
  // so an index viewed in MarketView and shown here shares one cache entry.
  const { quotes: indexQuotes, isLoading: indexQuotesLoading } = useQuotes(INDEX_SYMBOLS, {
    isIndex: true,
    staleTime: 10000,
    refetchInterval: indexRefetch,
  });

  // Sparklines are the intraday series — not part of the quote layer — so they
  // keep their own batched fetch, refreshed on the same adaptive cadence.
  const { data: sparklineMap = {} } = useQuery<Record<string, { sparklineData: SparklinePoint[]; asOfDate?: string }>>({
    queryKey: ['dashboard', 'indexSparklines', INDEX_SYMBOLS],
    queryFn: async () => {
      const entries = await Promise.all(INDEX_SYMBOLS.map(async (s) => {
        const norm = normalizeIndexSymbol(s);
        try {
          const r = await getIndex(norm);
          return [norm, { sparklineData: r.sparklineData, asOfDate: r.asOfDate }] as const;
        } catch {
          return [norm, { sparklineData: [] as SparklinePoint[], asOfDate: undefined }] as const;
        }
      }));
      return Object.fromEntries(entries);
    },
    refetchInterval: indexRefetch,
    refetchIntervalInBackground: false,
    staleTime: 10000,
  });

  // Combine snapshot + sparkline into the IndexData cards. Always an array
  // (fallback cards for symbols with no quote yet), matching the old
  // placeholderData behavior of rendering instantly.
  const indices = useMemo<IndexData[]>(
    () => INDEX_SYMBOLS.map((s) => {
      const norm = normalizeIndexSymbol(s);
      const sp = sparklineMap[norm];
      return buildIndexData(norm, indexQuotes[norm], sp?.sparklineData ?? [], sp?.asOfDate);
    }),
    [indexQuotes, sparklineMap]
  );
  const indicesLoading = indexQuotesLoading;

  // 3. Market General Feed — kept warm server-side by the news poller, so we
  //    re-poll every 60s to surface the latest articles in an open tab.
  const { data: newsItems = [], isLoading: newsLoading } = useQuery<NewsItem[]>({
    queryKey: ['dashboard', 'news'],
    queryFn: async (): Promise<NewsItem[]> => {
      const data = await getNews({ limit: 50 });
      return data.results?.length ? mapNewsResults(data.results) : [];
    },
    staleTime: NEWS_STALE_MS,
    refetchInterval: NEWS_POLL_INTERVAL_MS,
    refetchIntervalInBackground: false,
  });

  // 4. Curated "Top" Feed (TickerTick) — cursor-paginated for infinite scroll,
  //    also kept warm server-side. Auto-refresh ONLY page 1 (the warm buffer):
  //    refetchInterval refetches every loaded page, and pages 2+ bypass the
  //    server cache and hit upstream directly, so we stop polling once the user
  //    scrolls past page 1.
  const curated = useInfiniteQuery({
    queryKey: ['dashboard', 'curatedNews'],
    queryFn: ({ pageParam }) => getNews({ provider: 'tickertick', limit: 50, cursor: pageParam }),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (lastPage) => lastPage.next_cursor ?? undefined,
    staleTime: NEWS_STALE_MS,
    refetchInterval: (query) =>
      (query.state.data?.pages.length ?? 0) <= 1 ? NEWS_POLL_INTERVAL_MS : false,
    refetchIntervalInBackground: false,
  });

  // Flatten loaded pages, de-duping by id (guards against feed rotation between
  // page fetches reintroducing a story).
  const curatedItems = useMemo<NewsItem[]>(() => {
    const rows = curated.data?.pages.flatMap((p) => p.results) ?? [];
    const seen = new Set<string>();
    const unique = rows.filter((r) => {
      const id = r.id as string;
      if (!id || seen.has(id)) return false;
      seen.add(id);
      return true;
    });
    return mapNewsResults(unique);
  }, [curated.data]);

  return {
    indices,
    indicesLoading,
    newsItems,
    newsLoading,
    curatedItems,
    curatedLoading: curated.isLoading,
    curatedHasNextPage: !!curated.hasNextPage,
    curatedIsFetchingNextPage: curated.isFetchingNextPage,
    curatedFetchNextPage: () => {
      void curated.fetchNextPage();
    },
    marketStatus,
    // Kept for backward compatibility with components that might use MarketStatusRef
    marketStatusRef: { current: marketStatus }
  };
}
