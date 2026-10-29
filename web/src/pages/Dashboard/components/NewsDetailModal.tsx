import { useEffect, useEffectEvent, useState } from 'react';
import { X } from 'lucide-react';
import { motion, AnimatePresence } from '@/lib/framer';
import { useTranslation } from 'react-i18next';
import { useLocale } from '@/hooks/useLocale';
import { getNewsArticle } from '../utils/api';
import { useBackdropDismiss } from '@/hooks/useDialogA11y';
import { DashWindowDialog } from './DashWindowDialog';
import { useIsMobile } from '@/hooks/useIsMobile';
import { MobileBottomSheet } from '@/components/ui/mobile-bottom-sheet';
import { Loader } from '@/components/ui/loader';
import { useToast } from '@/components/ui/use-toast';
import { ContextBus } from '@/lib/contextBus';
import { createDateFormatter } from '@/lib/format';
import { TermCard, TermStatus, TermTag, type TermTone } from '@/pages/ChatAgent/components/charts/TermCard';
import { TermPanelContext } from '@/pages/ChatAgent/components/charts/termContext';
import '@/pages/ChatAgent/components/charts/termPanel.css';
import { buildNewsWidgetSnapshot, normalizeArticle } from '../utils/newsArticleFetch';
import './dashWindow.css';

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
  /** The clicked row, drawn while (or instead of) the article loads. */
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

/** The ink a sentiment is said in: a market's profit and loss, else muted. */
function sentimentTone(sentiment: string): TermTone {
  if (sentiment === 'positive') return 'profit';
  if (sentiment === 'negative') return 'loss';
  return 'muted';
}

const dateTime = createDateFormatter({
  month: 'short',
  day: 'numeric',
  year: 'numeric',
  hour: 'numeric',
  minute: '2-digit',
});

function formatDate(dateString: string | undefined, locale: string): string {
  if (!dateString) return '';
  try {
    const d = new Date(dateString);
    return dateTime(d, locale);
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

/** The article's picture, beside the reading column. A feed's image link is
 *  often dead, and a broken picture says less than none, so one that fails
 *  takes its frame with it. */
function NewsThumb({ src }: { src: string }) {
  const [failed, setFailed] = useState(false);
  if (failed) return null;
  return (
    <div className="dash-window-thumb">
      <img src={src} alt="" onError={() => setFailed(true)} />
    </div>
  );
}

/** Shared inner content for both mobile bottom sheet and desktop dialog: the
 *  article in the detail panel's language, its parts as cards. */
function NewsBody({
  article,
  loading,
  fetchFailed,
  onAttach,
  onClose,
}: {
  article: Article | null;
  loading: boolean;
  fetchFailed?: boolean;
  /** Optional handler. If provided, an Add-to-chat chip is rendered in the meta line. */
  onAttach?: () => void;
  /** The window's close, at the meta line's end; the sheet closes by its own handle. */
  onClose?: () => void;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const safeArticleUrl = safeHttpUrl(article?.article_url);
  const closeButton = onClose ? (
    <button type="button" className="dash-window-x" onClick={onClose} aria-label={t('dashboard.newsDetail.close')}>
      <X size={16} />
    </button>
  ) : null;

  if (loading && !article) {
    return (
      <div className="dash-window-state">
        {closeButton}
        <Loader size={24} className="text-(--color-accent-primary)" />
      </div>
    );
  }

  if (!article) {
    return (
      <div className="dash-window-state">
        {closeButton}
        <p>{fetchFailed ? t('dashboard.newsDetail.notAvailable') : t('dashboard.newsDetail.notFound')}</p>
      </div>
    );
  }

  const sentiments = article.sentiments ?? [];
  // A story whose feed scored nothing still names what it is about.
  const impact = sentiments.length > 0
    ? sentiments.map((s) => ({ ticker: s.ticker, sentiment: s.sentiment || 'neutral', note: s.reasoning }))
    : (article.tickers ?? []).map((ticker) => ({ ticker, sentiment: null, note: undefined }));
  const keywords = article.keywords ?? [];
  const image = safeHttpUrl(article.image_url);
  const hasSide = !!image || keywords.length > 0;

  return (
    <TermPanelContext value={true}>
      <div className="dash-window-top">
        <div className="term-line dash-window-meta">
          {article.source?.name && <TermTag tone="neutral">{article.source.name}</TermTag>}
          {article.author && <span className="term-t3">{article.author}</span>}
          {article.published_at && <span className="term-t3">{formatDate(article.published_at, locale)}</span>}
          <span className="term-push dash-window-acts">
            {onAttach && (
              <button
                type="button"
                className="term-chip dash-window-chip"
                onClick={onAttach}
                aria-label={t('dashboard.newsDetail.addToChat')}
                title={t('dashboard.widgets.frame.addToContextTitle')}
              >
                {t('dashboard.newsDetail.addToChat')}
              </button>
            )}
            {safeArticleUrl && (
              <a
                className="term-chip dash-window-chip"
                href={safeArticleUrl}
                target="_blank"
                rel="noopener noreferrer"
                aria-label={t('dashboard.newsDetail.source')}
              >
                <span>{t('dashboard.newsDetail.source')}</span>
                <span aria-hidden="true">↗</span>
              </a>
            )}
            {closeButton}
          </span>
        </div>
        <h2 className="dash-window-title">{article.title}</h2>
      </div>

      <div className={hasSide ? 'dash-window-grid' : 'dash-window-grid dash-window-grid--one'}>
        <div className="dash-window-col">
          {article.description && (
            <TermCard title={t('dashboard.newsDetail.summary')} meta={t('dashboard.newsDetail.summaryMeta')}>
              <p className="dash-window-prose">{article.description}</p>
            </TermCard>
          )}
          {impact.length > 0 && (
            <TermCard title={t('dashboard.newsDetail.tickerImpact')} meta={impact.length}>
              <table className="term-table dash-window-impact">
                <tbody>
                  {impact.map((row, i) => (
                    <tr key={`${row.ticker}-${i}`}>
                      <td className="term-code term-strong">{row.ticker}</td>
                      <td>
                        {row.sentiment && (
                          <TermStatus tone={sentimentTone(row.sentiment)}>
                            {t(`dashboard.newsDetail.sentiment.${sentimentTone(row.sentiment)}`)}
                          </TermStatus>
                        )}
                      </td>
                      <td className="term-fill term-prose">{row.note}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </TermCard>
          )}
        </div>
        {hasSide && (
          <aside className="dash-window-col">
            {image && <NewsThumb key={image} src={image} />}
            {keywords.length > 0 && (
              <TermCard title={t('dashboard.newsDetail.topics')}>
                <div className="dash-window-tags">
                  {keywords.map((kw, i) => <TermTag key={i}>{kw}</TermTag>)}
                </div>
              </TermCard>
            )}
          </aside>
        )}
      </div>
    </TermPanelContext>
  );
}

function NewsDetailModal({ newsId, onClose, fallback }: NewsDetailModalProps) {
  const { t } = useTranslation();
  const { toast } = useToast();
  const [article, setArticle] = useState<Article | null>(null);
  const [loading, setLoading] = useState(false);
  const [fetchFailed, setFetchFailed] = useState(false);
  const isMobile = useIsMobile();
  const backdrop = useBackdropDismiss<HTMLDivElement>(onClose);

  // Read the latest fallback at fetch time without re-running the effect when
  // the parent hands us a fresh object for the same newsId.
  const readFallback = useEffectEvent(() => fallback);

  const handleAttach = () => {
    if (!article || !newsId) return;
    attachArticleToContext(article, newsId);
    toast({
      title: t('dashboard.widgets.frame.contextAttached', { defaultValue: 'Added to context' }),
      description: t('dashboard.newsDetail.attachedNews', { title: article.title || '' }),
    });
  };
  const canAttach = !!article && !!newsId;

  useEffect(() => {
    if (!newsId) {
      setArticle(null);
      setFetchFailed(false);
      return;
    }
    // Seed with the clicked row's known fields so the modal renders instantly.
    const seed = fallbackToArticle(readFallback());
    let cancelled = false;
    setArticle(seed);
    setFetchFailed(false);

    // The list now inlines the full article body, so when the row already
    // carries a description we render straight from it — no by-id round-trip.
    if (seed?.description) {
      setLoading(false);
      return;
    }

    // Optional enrichment: rows without an inlined body still fetch the full
    // article by id.
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
      // The same guard the dialog hook uses. An Escape that abandons an IME
      // candidate, or one a layer inside has already handled, is not a
      // dismissal, and this listener is on the window so it hears both.
      if (e.key !== 'Escape' || e.defaultPrevented || e.isComposing || e.keyCode === 229) return;
      onClose();
    };
    window.addEventListener('keydown', handleEsc);
    return () => window.removeEventListener('keydown', handleEsc);
  }, [newsId, onClose]);

  const body = (
    <NewsBody
      article={article}
      loading={loading}
      fetchFailed={fetchFailed}
      onAttach={canAttach ? handleAttach : undefined}
      onClose={isMobile ? undefined : onClose}
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
        className="dash-window dash-window--sheet"
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
          {...backdrop}
          className="fixed inset-0 z-1010 flex items-center justify-center p-8"
          style={{ backgroundColor: 'var(--color-bg-overlay, rgba(0,0,0,0.6))', backdropFilter: 'blur(4px)' }}
        >
          <DashWindowDialog label={article?.title || t('dashboard.newsDetail.title')} onClose={onClose}>
            {body}
          </DashWindowDialog>
        </motion.div>
      )}
    </AnimatePresence>
  );
}

export default NewsDetailModal;
