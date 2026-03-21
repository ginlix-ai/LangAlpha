import { useCallback, useEffect, useState } from 'react';
import { getNews } from '../utils/api';

export interface TickerNewsItem {
  id: string;
  title: string;
  time: string;
  isHot: boolean;
  image: string | null;
  source: string;
  favicon: string | null;
  tickers: string[];
  articleUrl?: string | null;
}

interface TickerRow {
  symbol: string;
  [key: string]: unknown;
}

interface CacheEntry {
  items: TickerNewsItem[];
  tickerKey: string;
}

// Module-level caches keyed by caller-provided cacheKey
const cacheMap = new Map<string, CacheEntry>();

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

function mapNewsResults(results: Record<string, unknown>[]): TickerNewsItem[] {
  return results.map((r) => ({
    id: r.id as string,
    title: r.title as string,
    time: formatRelativeTime(r.published_at as string | null | undefined),
    isHot: r.has_sentiment as boolean,
    image: r.image_url as string || null,
    source: (r.source as Record<string, unknown> | undefined)?.name as string || '',
    favicon: (r.source as Record<string, unknown> | undefined)?.favicon_url as string || null,
    tickers: (r.tickers as string[]) || [],
    articleUrl: (r.article_url as string) || null,
  }));
}

/**
 * Hook to fetch news for a list of ticker rows.
 * @param rows - Array of objects with a `symbol` property
 * @param cacheKey - Unique key for module-level caching (e.g. 'portfolio', 'watchlist')
 */
export function useTickerNews(rows: TickerRow[], cacheKey: string): { items: TickerNewsItem[]; loading: boolean } {
  const cached = cacheMap.get(cacheKey);
  const [items, setItems] = useState<TickerNewsItem[]>(() => cached?.items || []);
  const [loading, setLoading] = useState(!cached);

  const fetchNews = useCallback(async (): Promise<void> => {
    const tickers = (rows || []).map((r) => r.symbol).filter(Boolean);
    const tickerKey = [...tickers].sort().join(',');

    if (!tickers.length) {
      setItems([]);
      setLoading(false);
      cacheMap.set(cacheKey, { items: [], tickerKey: '' });
      return;
    }

    setLoading(true);
    try {
      const data = await getNews({ tickers, limit: 50 });
      const mapped: TickerNewsItem[] = data.results?.length > 0 ? mapNewsResults(data.results) : [];
      setItems(mapped);
      cacheMap.set(cacheKey, { items: mapped, tickerKey });
    } catch {
      setItems([]);
    } finally {
      setLoading(false);
    }
  }, [rows, cacheKey]);

  useEffect(() => {
    const tickers = (rows || []).map((r) => r.symbol).filter(Boolean);
    const tickerKey = [...tickers].sort().join(',');
    const cached = cacheMap.get(cacheKey);
    if (cached?.tickerKey !== tickerKey) {
      cacheMap.delete(cacheKey);
    }
    if (!cacheMap.has(cacheKey)) fetchNews();
  }, [fetchNews, rows, cacheKey]);

  return { items, loading };
}
