/**
 * Shared news-article snapshot helpers — used by:
 *   - NewsFeedWidget per-row attach (fetches full body on click)
 *   - NewsDetailModal attach button (uses already-loaded article)
 *
 * The ContextBus payload is identical from either entry point, so the agent
 * sees the exact same `<widget-context>` block whether the user attached from
 * the list row or from inside the detail modal.
 */

import type { WidgetContextSnapshot } from '../widgets/framework/contextSnapshot';
import {
  serializeNewsArticleToMarkdown,
  wrapWidgetContext,
  type NewsArticleDetail,
} from '../widgets/framework/snapshotSerializers';
import { getNewsArticle } from './api';

interface RawArticle {
  title?: string;
  description?: string;
  article_url?: string | null;
  author?: string;
  published_at?: string;
  keywords?: string[];
  tickers?: string[];
  sentiments?: Array<{ ticker: string; sentiment?: string; reasoning?: string }>;
  source?: { name?: string } | string;
  [key: string]: unknown;
}

export function normalizeArticle(raw: RawArticle): NewsArticleDetail {
  const sourceName =
    typeof raw.source === 'string'
      ? raw.source
      : raw.source && typeof raw.source === 'object'
        ? raw.source.name
        : undefined;
  return {
    title: raw.title ?? '',
    description: raw.description,
    author: raw.author,
    publishedAt: raw.published_at,
    url: raw.article_url ?? undefined,
    source: sourceName,
    tickers: raw.tickers,
    keywords: raw.keywords,
    sentiments: raw.sentiments,
  };
}

/**
 * Sync snapshot builder for an already-loaded news article. Used by the
 * detail-modal attach button (article is already in component state) and as
 * the final step of `buildNewsArticleSnapshot` after its async fetch.
 */
export function buildNewsWidgetSnapshot(opts: {
  instanceId: string;
  rowId: string;
  article: NewsArticleDetail;
}): WidgetContextSnapshot {
  const { article } = opts;
  const body = serializeNewsArticleToMarkdown(article);
  const text = wrapWidgetContext(
    'news.feed/row',
    {
      source: article.source,
      tickers: article.tickers?.join(','),
      published_at: article.publishedAt,
    },
    body,
  );
  return {
    widget_type: 'news.feed/row',
    widget_id: `${opts.instanceId}/${opts.rowId}`,
    label: 'News: ' + article.title,
    description: [article.source, article.publishedAt].filter(Boolean).join(' · ') || undefined,
    captured_at: new Date().toISOString(),
    text,
    data: { article },
  };
}

/**
 * Build a row-level snapshot for a single news article. Tries to fetch the
 * full body via /news/:id; falls back to whatever fields we already have on
 * the list-row item if the fetch fails or no id is available.
 */
export async function buildNewsArticleSnapshot(opts: {
  instanceId: string;
  rowId: string;
  articleId?: string | number;
  /** Fallback fields when the article fetch fails — ensures we still attach something useful. */
  fallback: NewsArticleDetail;
}): Promise<WidgetContextSnapshot> {
  let article: NewsArticleDetail = opts.fallback;
  if (opts.articleId != null) {
    try {
      const raw = (await getNewsArticle(String(opts.articleId))) as RawArticle;
      article = normalizeArticle(raw);
      // Fall back per-field if the fetched article is missing what the row had
      // (the list payload is sometimes richer in certain fields, e.g., tickers).
      if (!article.tickers?.length && opts.fallback.tickers?.length) article.tickers = opts.fallback.tickers;
      if (!article.source && opts.fallback.source) article.source = opts.fallback.source;
      if (!article.publishedAt && opts.fallback.publishedAt) article.publishedAt = opts.fallback.publishedAt;
      if (!article.url && opts.fallback.url) article.url = opts.fallback.url;
      if (!article.title && opts.fallback.title) article.title = opts.fallback.title;
    } catch (err) {
      // Article fetch is best-effort — fall back to the list-row fields so
      // the agent still gets *something* contextual rather than nothing.
      console.warn('[newsArticleFetch] full-article fetch failed, using row fallback', err);
    }
  }
  return buildNewsWidgetSnapshot({ instanceId: opts.instanceId, rowId: opts.rowId, article });
}
