import React, { useEffect, useState } from 'react';
import { X, Calendar, Hash, ExternalLink, TrendingUp, TrendingDown, Minus, Tag } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import i18n from '@/i18n';
import { getNewsArticle } from '../utils/api';
import { useIsMobile } from '@/hooks/useIsMobile';
import { MobileBottomSheet } from '@/components/ui/mobile-bottom-sheet';

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

interface NewsDetailModalProps {
  newsId: string | null;
  onClose: () => void;
  fallbackUrl?: string | null;
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

/** Shared inner content for both mobile bottom sheet and desktop dialog */
function NewsBody({
  article,
  loading,
  fetchFailed,
  fallbackUrl,
  expandedSentiment,
  setExpandedSentiment,
  isMobile,
}: {
  article: Article | null;
  loading: boolean;
  fetchFailed?: boolean;
  fallbackUrl?: string | null;
  expandedSentiment: number | null;
  setExpandedSentiment: React.Dispatch<React.SetStateAction<number | null>>;
  isMobile: boolean;
}) {
  if (loading) {
    return (
      <div className="flex items-center justify-center py-24">
        <div
          className="h-8 w-8 border-2 rounded-full animate-spin"
          style={{ borderColor: 'var(--color-border-default)', borderTopColor: 'var(--color-accent-primary)' }}
        />
      </div>
    );
  }

  if (!article) {
    return (
      <div className="flex flex-col items-center justify-center gap-3 py-24">
        <p style={{ color: 'var(--color-text-secondary)' }}>
          {fetchFailed ? 'Article details not available' : 'Article not found'}
        </p>
        {fetchFailed && fallbackUrl && (
          <a
            href={fallbackUrl}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-1.5 px-4 py-2 rounded-lg text-sm font-medium transition-colors"
            style={{
              backgroundColor: 'var(--color-accent-primary)',
              color: '#fff',
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
                className="inline-flex items-center gap-1.5 text-[10px] font-bold px-2 py-0.5 rounded uppercase tracking-wider mb-2 sm:mb-3"
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
          {article.article_url && (
            <a
              href={article.article_url}
              target="_blank"
              rel="noopener noreferrer"
              className="ml-auto flex items-center gap-1.5 transition-opacity hover:opacity-80"
              style={{ color: 'var(--color-text-secondary)' }}
            >
              Source <ExternalLink size={isMobile ? 12 : 14} />
            </a>
          )}
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
                    className="px-2 py-0.5 sm:px-3 sm:py-1 rounded-full border text-[11px] sm:text-xs"
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
                className="text-[13px] sm:text-sm leading-relaxed"
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
                            className="flex items-center gap-1 text-[10px] font-bold px-1.5 py-0.5 rounded uppercase border"
                            style={sentimentStyle(insight.sentiment)}
                          >
                            {sentimentIcon(insight.sentiment)} {insight.sentiment || 'neutral'}
                          </div>
                        </div>
                        {insight.reasoning && (
                          <p
                            className="text-[11px] sm:text-xs leading-relaxed line-clamp-2"
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

function NewsDetailModal({ newsId, onClose, fallbackUrl }: NewsDetailModalProps) {
  const [article, setArticle] = useState<Article | null>(null);
  const [loading, setLoading] = useState(false);
  const [fetchFailed, setFetchFailed] = useState(false);
  const [expandedSentiment, setExpandedSentiment] = useState<number | null>(null);
  const isMobile = useIsMobile();

  useEffect(() => {
    if (!newsId) {
      setArticle(null);
      setFetchFailed(false);
      setExpandedSentiment(null);
      return;
    }
    let cancelled = false;
    setLoading(true);
    setFetchFailed(false);
    setExpandedSentiment(null);
    getNewsArticle(newsId)
      .then((data) => {
        if (!cancelled) setArticle(data as Article);
      })
      .catch((err) => {
        console.error('[NewsDetailModal] fetch failed:', err?.message);
        if (!cancelled) {
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

  // Escape key
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
