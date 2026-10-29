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
import { useCnNewsEligibility } from './useCnNewsEligibility';

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
  newsHasNextPage: boolean;
  newsIsFetchingNextPage: boolean;
  newsFetchNextPage: () => void;
  curatedItems: NewsItem[];
  curatedLoading: boolean;
  curatedHasNextPage: boolean;
  curatedIsFetchingNextPage: boolean;
  curatedFetchNextPage: () => void;
  marketStatus: MarketStatusData | null;
  marketStatusRef: { current: MarketStatusData | null };
}

// Flatten infinite-query pages into one list, de-duping by id (guards against
// feed rotation between page fetches reintroducing a story).
function flattenNewsPages(
  pages: { results: Record<string, unknown>[] }[] | undefined,
): NewsItem[] {
  const rows = pages?.flatMap((p) => p.results ?? []) ?? [];
  const seen = new Set<string>();
  const unique = rows.filter((r) => {
    const id = r.id as string;
    if (!id || seen.has(id)) return false;
    seen.add(id);
    return true;
  });
  return mapNewsResults(unique);
}

/**
 * Cursor-paginated infinite news feed with a page-1-only polling policy.
 *
 * Auto-refresh runs ONLY while page 1 (the warm server-side buffer) is the sole
 * loaded page: refetchInterval refetches every loaded page, and pages 2+ bypass
 * the server cache and hit upstream directly, so we stop polling once the user
 * scrolls past page 1.
 */
function useInfiniteNewsFeed(queryKey: (string | null)[], provider?: string, enabled = true) {
  const query = useInfiniteQuery({
    queryKey,
    queryFn: ({ pageParam }) => getNews({ limit: 50, provider, cursor: pageParam }),
    enabled,
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (lastPage) => lastPage.next_cursor ?? undefined,
    staleTime: NEWS_STALE_MS,
    refetchInterval: (q) =>
      (q.state.data?.pages.length ?? 0) <= 1 ? NEWS_POLL_INTERVAL_MS : false,
    refetchIntervalInBackground: false,
  });
  const items = useMemo<NewsItem[]>(() => flattenNewsPages(query.data?.pages), [query.data]);
  return {
    items,
    // A feed held back until its provider is decided is still loading.
    isLoading: query.isLoading || (!enabled && !query.data),
    hasNextPage: !!query.hasNextPage,
    isFetchingNextPage: query.isFetchingNextPage,
    fetchNextPage: () => {
      void query.fetchNextPage();
    },
  };
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

  // 3. Market General Feed — cursor-paginated for infinite scroll, kept warm
  //    server-side by the news poller.
  //    Eligible CN users (A-share pack + zh locale) get the tushare CN feed;
  //    the backend re-checks eligibility and falls back to the chain if not.
  //    The chain feed serves next_cursor=null (it can't paginate), so
  //    load-more simply never triggers for non-CN users. The feed waits while
  //    eligibility is undecided, so a cold load fetches one feed, not two.
  const cnNewsEligible = useCnNewsEligibility();
  const newsProvider = cnNewsEligible ? 'tushare' : undefined;
  const news = useInfiniteNewsFeed(
    ['dashboard', 'news', newsProvider ?? null], newsProvider, cnNewsEligible !== null,
  );

  // 4. Curated "Top" Feed (TickerTick) — cursor-paginated for infinite scroll,
  //    also kept warm server-side.
  const curated = useInfiniteNewsFeed(['dashboard', 'curatedNews'], 'tickertick');

  return {
    indices,
    indicesLoading,
    newsItems: news.items,
    newsLoading: news.isLoading,
    newsHasNextPage: news.hasNextPage,
    newsIsFetchingNextPage: news.isFetchingNextPage,
    newsFetchNextPage: news.fetchNextPage,
    curatedItems: curated.items,
    curatedLoading: curated.isLoading,
    curatedHasNextPage: curated.hasNextPage,
    curatedIsFetchingNextPage: curated.isFetchingNextPage,
    curatedFetchNextPage: curated.fetchNextPage,
    marketStatus,
    // Kept for backward compatibility with components that might use MarketStatusRef
    marketStatusRef: { current: marketStatus }
  };
}
