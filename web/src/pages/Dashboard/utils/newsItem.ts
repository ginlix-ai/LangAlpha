/** How often the dashboard news feeds re-poll their warm server buffer. */
export const NEWS_POLL_INTERVAL_MS = 60000;
/** Staleness window for news queries — just under the poll interval so an open
 *  tab refetches on the poll cadence, not on every remount. */
export const NEWS_STALE_MS = 55000;

export interface NewsSentimentItem {
  ticker: string;
  sentiment: string;
  reasoning?: string;
}

/** Normalized news row shared by every dashboard news feed (market, curated,
 *  portfolio, watchlist). Produced by mapNewsResults from the /news payload. */
export interface DashboardNewsItem {
  id: string;
  title: string;
  publishedAt: string | null;
  isHot: boolean;
  source: string;
  favicon: string | null;
  image: string | null;
  tickers: string[];
  articleUrl?: string | null;
  // Inlined article body — lets the detail modal render without a by-id fetch.
  author?: string | null;
  description?: string | null;
  keywords?: string[];
  sentiments?: NewsSentimentItem[] | null;
}

export type NewsDateRange = 'all' | '1h' | '6h' | '24h' | '7d';

const DATE_RANGE_MS: Record<Exclude<NewsDateRange, 'all'>, number> = {
  '1h': 3600 * 1000,
  '6h': 6 * 3600 * 1000,
  '24h': 24 * 3600 * 1000,
  '7d': 7 * 86400 * 1000,
};

/** Whether a story falls inside a news feed's date-range filter. Reads the raw
 *  timestamp, never the displayed "5m ago", whose wording varies by locale and
 *  source; a story without a parseable timestamp is outside every bounded range. */
export function isWithinDateRange(
  publishedAt: string | null | undefined,
  range: NewsDateRange,
  now: number,
): boolean {
  if (range === 'all') return true;
  const ts = publishedAt ? new Date(publishedAt).getTime() : NaN;
  return Number.isFinite(ts) && ts >= now - DATE_RANGE_MS[range];
}

/** Map raw /news results into the normalized DashboardNewsItem shape. The time
 *  stays a raw timestamp: a row formats it against the live clock and language,
 *  so a string stamped here would freeze at the fetch. */
export function mapNewsResults(results: Record<string, unknown>[]): DashboardNewsItem[] {
  return results.map((r) => ({
    id: r.id as string,
    title: r.title as string,
    publishedAt: (r.published_at as string) || null,
    isHot: r.has_sentiment as boolean,
    source: (r.source as Record<string, unknown> | undefined)?.name as string || '',
    favicon: (r.source as Record<string, unknown> | undefined)?.favicon_url as string || null,
    image: r.image_url as string || null,
    tickers: (r.tickers as string[]) || [],
    articleUrl: (r.article_url as string) || null,
    author: (r.author as string) ?? null,
    description: (r.description as string) ?? null,
    keywords: (r.keywords as string[]) || [],
    sentiments: (r.sentiments as NewsSentimentItem[]) ?? null,
  }));
}
