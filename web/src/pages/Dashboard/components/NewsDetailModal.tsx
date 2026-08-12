import React, { useEffect, useRef, useState } from 'react';
import {
  X, Calendar, Hash, ExternalLink, TrendingUp, TrendingDown, Minus, Tag,
  Paperclip,
} from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import i18n from '@/i18n';
import { useTranslation } from 'react-i18next';
import { getNewsArticle } from '../utils/api';
import { useIsMobile } from '@/hooks/useIsMobile';
import { MobileBottomSheet } from '@/components/ui/mobile-bottom-sheet';
import { Loader } from '@/components/ui/loader';
import { useToast } from '@/components/ui/use-toast';
import { ContextBus } from '@/lib/contextBus';
import { buildNewsWidgetSnapshot, normalizeArticle } from '../utils/newsArticleFetch';

interface ArticleSource {
  name: string;
  favicon_url?: string;
}

interface ArticleSentiment {
  ticker: string;
  sentiment: string;
  reasoning?: string;
}

interface Article {
  title: string;
  description?: string;
  image_url?: string;
  article_url?: string;
  author?: string;
  published_at?: string;
  keywords?: string[];
  tickers?: string[];
  sentiments?: ArticleSentiment[];
  source?: ArticleSource;
  [key: string]: unknown;
}

/** The clicked row's full body. The list inlines description/keywords/sentiments,
 *  so a seed with a description renders the complete modal with no by-id fetch.
 *  Rows without one still fall back to the fetch (optional enrichment). */
interface NewsFallback {
  title?: string;
  source?: string;
  publishedAt?: string | null;
  tickers?: string[];
  articleUrl?: string | null;
  author?: string | null;
  description?: string | null;
  keywords?: string[];
  sentiments?: ArticleSentiment[] | null;
  imageUrl?: string | null;
  favicon?: string | null;
}

interface NewsDetailModalProps {
  newsId: string | null;
  onClose: () => void;
  /** Legacy (Classic dashboard): URL-only fallback for the empty state. */
  fallbackUrl?: string | null;
  /** Rich fallback from the clicked row — preferred. */
  fallback?: NewsFallback | null;
}

function fallbackToArticle(fb: NewsFallback | null | undefined): Article | null {
  if (!fb?.title) return null;
  return {
    title: fb.title,
    description: fb.description ?? undefined,
    image_url: fb.imageUrl ?? undefined,
    article_url: fb.articleUrl ?? undefined,
    author: fb.author ?? undefined,
    published_at: fb.publishedAt ?? undefined,
    source: fb.source ? { name: fb.source, favicon_url: fb.favicon ?? undefined } : undefined,
    keywords: fb.keywords ?? [],
    tickers: fb.tickers ?? [],
    sentiments: fb.sentiments ?? undefined,
  };
}

function attachArticleToContext(article: Article, articleId: string): void {
  const detail = normalizeArticle(article as Parameters<typeof normalizeArticle>[0]);
  ContextBus.attach(buildNewsWidgetSnapshot({
    instanceId: 'news.detail',
    rowId: articleId,
    article: detail,
  }));
}

function sentimentIcon(sentiment: string): React.ReactElement {
  switch (sentiment) {
    case 'positive':
      return <TrendingUp size={16} style={{ color: 'var(--color-profit)' }} />;
    case 'negative':
      return <TrendingDown size={16} style={{ color: 'var(--color-loss)' }} />;
    default:
      return <Minus size={16} style={{ color: 'var(--color-warning, #facc15)' }} />;
  }
}

function sentimentStyle(sentiment: string): React.CSSProperties {
  switch (sentiment) {
    case 'positive':
      return {
        color: 'var(--color-profit)',
        backgroundColor: 'var(--color-profit-soft)',
        borderColor: 'var(--color-profit-soft)',
      };
    case 'negative':
      return {
        color: 'var(--color-loss)',
        backgroundColor: 'var(--color-loss-soft)',
        borderColor: 'var(--color-loss-soft)',
      };
    default:
      return {
        color: 'var(--color-warning, #facc15)',
        backgroundColor: 'rgba(250, 204, 21, 0.1)',
        borderColor: 'rgba(250, 204, 21, 0.2)',
      };
  }
}

function formatDate(dateString: string | undefined): string {
  if (!dateString) return '';
  try {
    const d = new Date(dateString);
    return d.toLocaleDateString(i18n.language, {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
      hour: 'numeric',
      minute: '2-digit',
    });
  } catch {
    return dateString;
  }
}

/** Only allow http(s) URLs into an <a href>. Article/fallback URLs come from
 *  external news feeds, and React does NOT block javascript:/data: schemes,
 *  which would execute on click. Returns undefined for anything non-http(s). */
function safeHttpUrl(url: string | null | undefined): string | undefined {
  if (!url) return undefined;
  try {
    const protocol = new URL(url, window.location.origin).protocol;
    return protocol === 'http:' || protocol === 'https:' ? url : undefined;
  } catch {
    return undefined;
  }
}

/** Shared inner content for both mobile bottom sheet and desktop dialog */
function NewsBody({
  article,
  loading,
  fetchFailed,
  fallbackUrl,
  expandedSentiment,
  setExpandedSentiment,
  isMobile,
  onAttach,
}: {
  article: Article | null;
  loading: boolean;
  fetchFailed?: boolean;
  fallbackUrl?: string | null;
  expandedSentiment: number | null;
  setExpandedSentiment: React.Dispatch<React.SetStateAction<number | null>>;
  isMobile: boolean;
  /** Optional handler. If provided, an Attach-to-chat button is rendered in the meta row. */
  onAttach?: () => void;
}) {
  const { t: trans } = useTranslation();
  const safeFallbackUrl = safeHttpUrl(fallbackUrl);
  const safeArticleUrl = safeHttpUrl(article?.article_url);
  if (loading && !article) {
    return (
      <div className="flex items-center justify-center py-24">
        <Loader size={32} className="text-[color:var(--color-accent-primary)]" />
      </div>
    );
  }

  if (!article) {
    return (
      <div className="flex flex-col items-center justify-center gap-3 py-24">
        <p style={{ color: 'var(--color-text-secondary)' }}>
          {fetchFailed ? 'Article details not available' : 'Article not found'}
        </p>
        {fetchFailed && safeFallbackUrl && (
          <a
            href={safeFallbackUrl}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-1.5 px-4 py-2 rounded-lg text-sm font-medium transition-colors"
            style={{
              backgroundColor: 'var(--color-btn-primary-bg)',
              color: 'var(--color-btn-primary-text)',
            }}
          >
            Open article
            <ExternalLink size={14} />
          </a>
        )}
      </div>
    );
  }

  return (
    <>
      {/* Hero image */}
      {article.image_url && (
        <div className={`relative ${isMobile ? 'h-48' : 'h-64 md:h-80'} w-full ${isMobile ? '-mx-4 w-[calc(100%+32px)]' : ''}`}>
          <img
            src={article.image_url}
            alt={article.title}
            className="w-full h-full object-cover"
          />
          <div
            className="absolute inset-0"
            style={{
              background: isMobile
                ? 'linear-gradient(to top, var(--color-bg-card) 0%, var(--color-bg-card) 15%, rgba(0,0,0,0.6) 70%, rgba(0,0,0,0.3) 100%)'
                : 'linear-gradient(to top, var(--color-bg-elevated) 0%, transparent 60%)',
            }}
          />
          <div className={`absolute bottom-0 left-0 right-0 ${isMobile ? 'p-4' : 'p-6 md:p-8'}`}>
            {article.source?.name && (
              <span
                className="inline-flex items-center gap-1.5 text-[0.625rem] font-bold px-2 py-0.5 rounded uppercase tracking-wider mb-2 sm:mb-3"
                style={{
                  backgroundColor: 'var(--color-accent-primary)',
                  color: '#fff',
                }}
              >
                {article.source.favicon_url && (
                  <img
                    src={article.source.favicon_url}
                    alt=""
                    className="w-3.5 h-3.5 rounded-sm"
                    onError={(e) => { (e.target as HTMLImageElement).style.display = 'none'; }}
                  />
                )}
                {article.source.name}
              </span>
            )}
            <h1
              className={`${isMobile ? 'text-lg' : 'text-2xl md:text-3xl'} font-bold leading-tight`}
              style={{ color: '#fff', textShadow: '0 1px 4px rgba(0,0,0,0.5)' }}
            >
              {article.title}
            </h1>
          </div>
        </div>
      )}

      {/* Body */}
      <div className={isMobile ? 'pt-4' : 'p-6 md:p-8'}>
        {/* Title fallback — sources without a hero image (e.g. TickerTick) still
            need the headline + source rendered, since the hero block above is
            skipped when there's no image_url. */}
        {!article.image_url && (
          <div className="mb-5 sm:mb-6">
            {article.source?.name && (
              <span
                className="inline-flex items-center gap-1.5 text-[0.625rem] font-bold px-2 py-0.5 rounded uppercase tracking-wider mb-2 sm:mb-3"
                style={{
                  backgroundColor: 'var(--color-accent-soft)',
                  color: 'var(--color-accent-primary)',
                }}
              >
                {article.source.favicon_url && (
                  <img
                    src={article.source.favicon_url}
                    alt=""
                    className="w-3.5 h-3.5 rounded-sm"
                    onError={(e) => { (e.target as HTMLImageElement).style.display = 'none'; }}
                  />
                )}
                {article.source.name}
              </span>
            )}
            <h1
              className={`${isMobile ? 'text-lg' : 'text-2xl md:text-3xl'} font-bold leading-tight`}
              style={{ color: 'var(--color-text-primary)' }}
            >
              {article.title}
            </h1>
          </div>
        )}

        {/* Meta */}
        <div
          className={`flex items-center ${isMobile ? 'gap-3 text-xs' : 'gap-6 text-sm'} mb-6 sm:mb-8 pb-3 sm:pb-4 border-b flex-wrap`}
          style={{
            color: 'var(--color-text-secondary)',
            borderColor: 'var(--color-border-muted)',
          }}
        >
          {article.author && (
            <span className="font-semibold" style={{ color: 'var(--color-accent-light)' }}>
              By {article.author}
            </span>
          )}
          {article.published_at && (
            <span className="flex items-center gap-1.5 sm:gap-2">
              <Calendar size={isMobile ? 12 : 14} /> {formatDate(article.published_at)}
            </span>
          )}
          <div className="ml-auto flex items-center gap-3">
            {onAttach && (
              <button
                type="button"
                onClick={onAttach}
                className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full border text-[0.6875rem] sm:text-xs font-medium transition-colors"
                style={{
                  backgroundColor: 'var(--color-accent-soft)',
                  borderColor: 'var(--color-accent-overlay)',
                  color: 'var(--color-accent-primary)',
                }}
                onMouseEnter={(e) => {
                  e.currentTarget.style.opacity = '0.9';
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.opacity = '1';
                }}
                title={trans('dashboard.widgets.frame.addToContext', { defaultValue: 'Attach to chat' })}
              >
                <Paperclip size={isMobile ? 12 : 13} />
                {trans('dashboard.widgets.frame.addToContext', { defaultValue: 'Attach to chat' })}
              </button>
            )}
            {safeArticleUrl && (
              <a
                href={safeArticleUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center gap-1.5 transition-opacity hover:opacity-80"
                style={{ color: 'var(--color-text-secondary)' }}
              >
                Source <ExternalLink size={isMobile ? 12 : 14} />
              </a>
            )}
          </div>
        </div>

        <div className="space-y-6 sm:space-y-8">
          {/* Related Topics */}
          {(article.keywords?.length ?? 0) > 0 && (
            <div>
              <h3
                className={`${isMobile ? 'text-base' : 'text-lg'} font-bold mb-2 sm:mb-3 flex items-center gap-2`}
                style={{ color: 'var(--color-text-primary)' }}
              >
                <Tag size={isMobile ? 16 : 18} style={{ color: 'var(--color-accent-light)' }} />
                Related Topics
              </h3>
              <div className="flex flex-wrap gap-1.5 sm:gap-2">
                {article.keywords!.map((kw, i) => (
                  <span
                    key={i}
                    className="px-2 py-0.5 sm:px-3 sm:py-1 rounded-full border text-[0.6875rem] sm:text-xs"
                    style={{
                      backgroundColor: 'var(--color-bg-hover)',
                      borderColor: 'var(--color-border-muted)',
                      color: 'var(--color-text-secondary)',
                    }}
                  >
                    #{kw}
                  </span>
                ))}
              </div>
            </div>
          )}

          {/* Executive Summary */}
          {article.description && (
            <div>
              <h3
                className={`${isMobile ? 'text-base' : 'text-lg'} font-bold mb-2 sm:mb-3 flex items-center gap-2`}
                style={{ color: 'var(--color-text-primary)' }}
              >
                <Hash size={isMobile ? 16 : 18} style={{ color: 'var(--color-accent-primary)' }} />
                Executive Summary
              </h3>
              <p
                className="text-[0.8125rem] sm:text-sm leading-relaxed"
                style={{ color: 'var(--color-text-secondary)' }}
              >
                {article.description}
              </p>
            </div>
          )}

          {/* Ticker Impact */}
          {((article.sentiments?.length ?? 0) > 0 || (article.tickers?.length ?? 0) > 0) && (
            <div>
              <h3
                className={`${isMobile ? 'text-base' : 'text-lg'} font-bold mb-2 sm:mb-3 flex items-center gap-2`}
                style={{ color: 'var(--color-text-primary)' }}
              >
                Ticker Impact
              </h3>
              <div className={`flex flex-wrap ${isMobile ? 'gap-2' : 'gap-3'}`}>
                {(article.sentiments?.length ?? 0) > 0
                  ? article.sentiments!.slice(0, 5).map((insight, i) => (
                      <div
                        key={i}
                        className={`${isMobile ? 'p-2.5' : 'p-3'} rounded-xl border cursor-pointer transition-colors flex-1 ${isMobile ? 'min-w-[140px]' : 'min-w-[200px]'} max-w-[300px]`}
                        style={{
                          backgroundColor: 'var(--color-bg-card)',
                          borderColor: 'var(--color-border-muted)',
                        }}
                        onMouseEnter={(e) => { e.currentTarget.style.borderColor = 'var(--color-border-elevated)'; }}
                        onMouseLeave={(e) => { e.currentTarget.style.borderColor = 'var(--color-border-muted)'; }}
                        onClick={() => setExpandedSentiment(i)}
                      >
                        <div className="flex justify-between items-center mb-1.5 sm:mb-2">
                          <span
                            className="font-bold text-sm"
                            style={{ color: 'var(--color-text-primary)' }}
                          >
                            {insight.ticker}
                          </span>
                          <div
                            className="flex items-center gap-1 text-[0.625rem] font-bold px-1.5 py-0.5 rounded uppercase border"
                            style={sentimentStyle(insight.sentiment)}
                          >
                            {sentimentIcon(insight.sentiment)} {insight.sentiment || 'neutral'}
                          </div>
                        </div>
                        {insight.reasoning && (
                          <p
                            className="text-[0.6875rem] sm:text-xs leading-relaxed line-clamp-2"
                            style={{ color: 'var(--color-text-secondary)' }}
                          >
                            {insight.reasoning}
                          </p>
                        )}
                      </div>
                    ))
                  : (article.tickers?.length ?? 0) > 0 && (
                      article.tickers!.map((ticker, i) => (
                        <span
                          key={i}
                          className="px-2.5 py-1 sm:px-3 sm:py-1.5 rounded-lg border text-xs font-bold"
                          style={{
                            backgroundColor: 'var(--color-bg-card)',
                            borderColor: 'var(--color-border-muted)',
                            color: 'var(--color-text-primary)',
                          }}
                        >
                          {ticker}
                        </span>
                      ))
                    )}
              </div>

              {/* Sentiment detail modal */}
              <AnimatePresence>
                {expandedSentiment !== null && article.sentiments?.[expandedSentiment] && (() => {
                  const insight = article.sentiments[expandedSentiment];
                  return (
                    <motion.div
                      key="sentiment-overlay"
                      initial={{ opacity: 0 }}
                      animate={{ opacity: 1 }}
                      exit={{ opacity: 0 }}
                      onClick={() => setExpandedSentiment(null)}
                      className="fixed inset-0 z-[60] flex items-center justify-center p-4"
                      style={{ backgroundColor: 'rgba(0,0,0,0.5)', backdropFilter: 'blur(4px)' }}
                    >
                      <motion.div
                        initial={{ opacity: 0, scale: 0.95, y: 20 }}
                        animate={{ opacity: 1, scale: 1, y: 0 }}
                        exit={{ opacity: 0, scale: 0.95, y: 20 }}
                        onClick={(e) => e.stopPropagation()}
                        className="w-full max-w-lg rounded-2xl border p-6 shadow-2xl"
                        style={{
                          backgroundColor: 'var(--color-bg-elevated)',
                          borderColor: 'var(--color-border-muted)',
                        }}
                      >
                        <div className="flex items-center justify-between mb-4">
                          <div className="flex items-center gap-3">
                            <span
                              className="text-xl font-bold"
                              style={{ color: 'var(--color-text-primary)' }}
                            >
                              {insight.ticker}
                            </span>
                            <div
                              className="flex items-center gap-1 text-xs font-bold px-2 py-1 rounded uppercase border"
                              style={sentimentStyle(insight.sentiment)}
                            >
                              {sentimentIcon(insight.sentiment)} {insight.sentiment || 'neutral'}
                            </div>
                          </div>
                          <button
                            onClick={() => setExpandedSentiment(null)}
                            className="p-2 rounded-full transition-colors"
                            style={{ color: 'var(--color-text-secondary)' }}
                            onMouseEnter={(e) => { e.currentTarget.style.backgroundColor = 'var(--color-bg-hover)'; }}
                            onMouseLeave={(e) => { e.currentTarget.style.backgroundColor = 'transparent'; }}
                          >
                            <X size={18} />
                          </button>
                        </div>
                        {insight.reasoning && (
                          <p
                            className="text-sm leading-relaxed"
                            style={{ color: 'var(--color-text-secondary)' }}
                          >
                            {insight.reasoning}
                          </p>
                        )}
                      </motion.div>
                    </motion.div>
                  );
                })()}
              </AnimatePresence>
            </div>
          )}

        </div>
      </div>
    </>
  );
}

function NewsDetailModal({ newsId, onClose, fallbackUrl, fallback }: NewsDetailModalProps) {
  const { t } = useTranslation();
  const { toast } = useToast();
  const [article, setArticle] = useState<Article | null>(null);
  const [loading, setLoading] = useState(false);
  const [fetchFailed, setFetchFailed] = useState(false);
  const [expandedSentiment, setExpandedSentiment] = useState<number | null>(null);
  const isMobile = useIsMobile();

  // Read the latest fallback at fetch time without re-running the effect when
  // the parent hands us a fresh object for the same newsId.
  const fallbackRef = useRef(fallback);
  fallbackRef.current = fallback;

  const handleAttach = () => {
    if (!article || !newsId) return;
    attachArticleToContext(article, newsId);
    toast({
      title: t('dashboard.widgets.frame.contextAttached', { defaultValue: 'Added to context' }),
      description: 'News: ' + (article.title || ''),
    });
  };
  const canAttach = !!article && !!newsId;

  useEffect(() => {
    if (!newsId) {
      setArticle(null);
      setFetchFailed(false);
      setExpandedSentiment(null);
      return;
    }
    // Seed with the clicked row's known fields so the modal renders instantly.
    const seed = fallbackToArticle(fallbackRef.current);
    let cancelled = false;
    setArticle(seed);
    setFetchFailed(false);
    setExpandedSentiment(null);

    // The list now inlines the full article body, so when the row already
    // carries a description we render straight from it — no by-id round-trip.
    if (seed?.description) {
      setLoading(false);
      return;
    }

    // Optional enrichment: rows without an inlined body (or the Classic
    // dashboard's URL-only path) still fetch the full article by id.
    setLoading(true);
    getNewsArticle(newsId)
      .then((data) => {
        if (!cancelled) setArticle(data as Article);
      })
      .catch((err) => {
        console.error('[NewsDetailModal] fetch failed:', err?.message);
        if (!cancelled && !seed) {
          // No fallback to show → surface the empty state.
          setArticle(null);
          setFetchFailed(true);
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [newsId]);

  useEffect(() => {
    if (!newsId) return;
    const handleEsc = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', handleEsc);
    return () => window.removeEventListener('keydown', handleEsc);
  }, [newsId, onClose]);

  const body = (
    <NewsBody
      article={article}
      loading={loading}
      fetchFailed={fetchFailed}
      fallbackUrl={fallbackUrl}
      expandedSentiment={expandedSentiment}
      setExpandedSentiment={setExpandedSentiment}
      isMobile={isMobile}
      onAttach={canAttach ? handleAttach : undefined}
    />
  );

  // Mobile: use MobileBottomSheet
  if (isMobile) {
    return (
      <MobileBottomSheet
        open={!!newsId}
        onClose={onClose}
        sizing="fixed"
        height="92vh"
        style={{ paddingBottom: 'calc(var(--bottom-tab-height, 0px) + 16px)' }}
      >
        {body}
      </MobileBottomSheet>
    );
  }

  // Desktop: centered dialog
  return (
    <AnimatePresence>
      {newsId && (
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          onClick={onClose}
          className="fixed inset-0 z-50 flex items-center justify-center p-8"
          style={{ backgroundColor: 'var(--color-bg-overlay, rgba(0,0,0,0.6))', backdropFilter: 'blur(4px)' }}
        >
          <motion.div
            initial={{ opacity: 0, scale: 0.95, y: 20 }}
            animate={{ opacity: 1, scale: 1, y: 0 }}
            exit={{ opacity: 0, scale: 0.95, y: 20 }}
            onClick={(e) => e.stopPropagation()}
            className="w-full max-w-5xl max-h-[90vh] rounded-3xl overflow-hidden shadow-2xl flex flex-col relative border"
            style={{
              backgroundColor: 'var(--color-bg-elevated)',
              borderColor: 'var(--color-border-muted)',
            }}
          >
            {/* Close */}
            <button
              onClick={onClose}
              className="absolute top-4 right-4 z-20 p-2 rounded-full transition-colors"
              style={{
                backgroundColor: 'rgba(0,0,0,0.5)',
                color: '#fff',
                backdropFilter: 'blur(8px)',
              }}
            >
              <X size={20} />
            </button>

            <div className="overflow-y-auto flex-1">
              {body}
            </div>
          </motion.div>
        </motion.div>
      )}
    </AnimatePresence>
  );
}

export default NewsDetailModal;
