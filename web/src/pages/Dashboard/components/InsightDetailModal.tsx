import React, { useEffect, useState } from 'react';
import { X } from 'lucide-react';
import { motion, AnimatePresence } from '@/lib/framer';
import { useTranslation } from 'react-i18next';
import { getInsightDetail } from '../utils/api';
import { useBackdropDismiss } from '@/hooks/useDialogA11y';
import { DashWindowDialog } from './DashWindowDialog';
import { useIsMobile } from '@/hooks/useIsMobile';
import { MobileBottomSheet } from '@/components/ui/mobile-bottom-sheet';
import { Loader } from '@/components/ui/loader';
import { useToast } from '@/components/ui/use-toast';
import { ContextBus } from '@/lib/contextBus';
import { createDateFormatter } from '@/lib/format';
import { buildInsightWidgetSnapshot, normalizeInsight } from '../utils/insightFetch';
import { useLocale } from '@/hooks/useLocale';
import { TermCard, TermChange, TermTag } from '@/pages/ChatAgent/components/charts/TermCard';
import { TermLink } from '@/pages/ChatAgent/components/charts/TermPanel';
import { TermPanelContext } from '@/pages/ChatAgent/components/charts/termContext';
import './dashWindow.css';

interface InsightTopic {
  text: string;
  trend: 'up' | 'down' | 'neutral';
}

interface InsightSource {
  url: string;
  title?: string;
  favicon?: string;
}

interface InsightContentItem {
  title: string;
  body: string;
  url?: string;
}

interface InsightDetail {
  market_insight_id: string;
  headline: string;
  summary?: string;
  model?: string;
  completed_at?: string;
  topics?: InsightTopic[];
  content?: InsightContentItem[];
  sources?: InsightSource[];
  [key: string]: unknown;
}

interface InsightDetailModalProps {
  marketInsightId: string | null;
  onClose: () => void;
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

function hostOf(url: string): string {
  try {
    return new URL(url).hostname.replace('www.', '');
  } catch {
    return url;
  }
}

/** How many sources show before the list is asked to open. */
const SOURCES_SHOWN = 5;

/** Shared inner content for both mobile bottom sheet and desktop dialog: the
 *  brief in the detail panel's language, its parts as cards. */
function InsightBody({
  detail,
  loading,
  sourcesOpen,
  setSourcesOpen,
  onAttach,
  onClose,
}: {
  detail: InsightDetail | null;
  loading: boolean;
  sourcesOpen: boolean;
  setSourcesOpen: (open: boolean) => void;
  /** Optional handler. If provided, an Add-to-chat chip is rendered in the meta line. */
  onAttach?: () => void;
  /** The window's close, at the meta line's end; the sheet closes by its own handle. */
  onClose?: () => void;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const closeButton = onClose ? (
    <button type="button" className="dash-window-x" onClick={onClose} aria-label={t('dashboard.newsDetail.close')}>
      <X size={16} />
    </button>
  ) : null;

  if (loading) {
    return (
      <div className="dash-window-state">
        {closeButton}
        <Loader size={24} className="text-(--color-accent-primary)" />
      </div>
    );
  }

  if (!detail) {
    return (
      <div className="dash-window-state">
        {closeButton}
        <p>{t('dashboard.insightDetail.notFound')}</p>
      </div>
    );
  }

  const topics = detail.topics ?? [];
  const content = detail.content ?? [];
  // Model-written addresses: only a web page becomes a link.
  const sources = (detail.sources ?? []).filter((source) => /^https?:/i.test(source.url ?? ''));
  const shownSources = sourcesOpen ? sources : sources.slice(0, SOURCES_SHOWN);
  const hasSide = topics.length > 0 || sources.length > 0;

  return (
    <TermPanelContext value={true}>
      <div className="dash-window-top">
        <div className="term-line dash-window-meta">
          <TermTag tone="neutral">{t('dashboard.insightDetail.eyebrow')}</TermTag>
          {detail.model && <span className="term-t3">{detail.model}</span>}
          {detail.completed_at && <span className="term-t3">{formatDate(detail.completed_at, locale)}</span>}
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
            {closeButton}
          </span>
        </div>
        <h2 className="dash-window-title">{detail.headline}</h2>
      </div>

      <div className={hasSide ? 'dash-window-grid' : 'dash-window-grid dash-window-grid--one'}>
        <div className="dash-window-col">
          {detail.summary && (
            <TermCard title={t('dashboard.newsDetail.summary')} meta={t('dashboard.newsDetail.summaryMeta')}>
              <p className="dash-window-prose">{detail.summary}</p>
            </TermCard>
          )}
          {content.length > 0 && (
            <TermCard title={t('dashboard.insightDetail.stories')} meta={content.length}>
              <ol className="dash-window-stories">
                {content.map((item, i) => {
                  const url = item.url && /^https?:/i.test(item.url) ? item.url : undefined;
                  return (
                    <li key={i}>
                      <div className="term-line">
                        <span className="term-t3">{String(i + 1).padStart(2, '0')}</span>
                        <span className="dash-window-story-title">{item.title}</span>
                        {url && <span className="term-push"><TermLink href={url}>{hostOf(url)}</TermLink></span>}
                      </div>
                      <p className="dash-window-prose dash-window-prose--sub">{item.body}</p>
                    </li>
                  );
                })}
              </ol>
            </TermCard>
          )}
        </div>
        {hasSide && (
          <aside className="dash-window-col">
            {topics.length > 0 && (
              <TermCard title={t('dashboard.newsDetail.topics')}>
                <div className="dash-window-tags">
                  {topics.map((topic) => (
                    <span key={topic.text} className="term-tag dash-window-topic">
                      <TermChange value={topic.trend === 'up' ? 1 : topic.trend === 'down' ? -1 : 0}>{''}</TermChange>
                      {topic.text}
                    </span>
                  ))}
                </div>
              </TermCard>
            )}
            {sources.length > 0 && (
              <TermCard title={t('dashboard.insightDetail.sources')} meta={sources.length}>
                <ul className="dash-window-sources">
                  {shownSources.map((source, i) => (
                    <li key={i}>
                      <a href={source.url} target="_blank" rel="noopener noreferrer" title={source.title || source.url}>
                        <span className="term-fill">{source.title || hostOf(source.url)}</span>
                        <span className="term-t3">{hostOf(source.url)}</span>
                      </a>
                    </li>
                  ))}
                </ul>
                {sources.length > SOURCES_SHOWN && (
                  <button
                    type="button"
                    className="term-more dash-window-more"
                    aria-expanded={sourcesOpen}
                    onClick={() => setSourcesOpen(!sourcesOpen)}
                  >
                    {sourcesOpen
                      ? t('dashboard.insightDetail.fewerSources')
                      : t('dashboard.insightDetail.allSources', { count: sources.length })}
                  </button>
                )}
              </TermCard>
            )}
          </aside>
        )}
      </div>
    </TermPanelContext>
  );
}

function InsightDetailModal({ marketInsightId, onClose }: InsightDetailModalProps) {
  const { t } = useTranslation();
  const { toast } = useToast();
  const [detail, setDetail] = useState<InsightDetail | null>(null);
  const [loading, setLoading] = useState(false);
  // Which brief its sources were opened for, so another brief opens folded.
  const [sourcesOpenFor, setSourcesOpenFor] = useState<string | null>(null);
  const sourcesOpen = !!marketInsightId && sourcesOpenFor === marketInsightId;
  const setSourcesOpen = (open: boolean) => setSourcesOpenFor(open ? marketInsightId ?? null : null);
  const isMobile = useIsMobile();
  const backdrop = useBackdropDismiss<HTMLDivElement>(onClose);

  const handleAttach = () => {
    if (!detail || !marketInsightId) return;
    const insight = normalizeInsight(detail as Parameters<typeof normalizeInsight>[0]);
    const snapshot = buildInsightWidgetSnapshot({
      instanceId: 'insight.detail',
      rowId: marketInsightId,
      insight,
    });
    ContextBus.attach(snapshot);
    toast({
      title: t('dashboard.widgets.frame.contextAttached', { defaultValue: 'Added to context' }),
      description: t('dashboard.insightDetail.attachedBrief', { title: detail.headline || '' }),
    });
  };
  const canAttach = !!detail && !!marketInsightId;

  useEffect(() => {
    if (!marketInsightId) {
      setDetail(null);
      return;
    }
    let cancelled = false;
    setLoading(true);
    getInsightDetail(marketInsightId)
      .then((data) => {
        if (!cancelled) setDetail(data as InsightDetail);
      })
      .catch((err) => {
        console.error('[InsightDetailModal] fetch failed:', err?.message);
        if (!cancelled) setDetail(null);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => { cancelled = true; };
  }, [marketInsightId]);

  useEffect(() => {
    if (!marketInsightId) return;
    const handleEsc = (e: KeyboardEvent) => {
      // The guard the dialog hook uses: an Escape that abandons an IME
      // candidate, or one a layer inside already handled, is not a dismissal.
      if (e.key !== 'Escape' || e.defaultPrevented || e.isComposing || e.keyCode === 229) return;
      onClose();
    };
    window.addEventListener('keydown', handleEsc);
    return () => window.removeEventListener('keydown', handleEsc);
  }, [marketInsightId, onClose]);

  const body = (
    <InsightBody
      detail={detail}
      loading={loading}
      sourcesOpen={sourcesOpen}
      setSourcesOpen={setSourcesOpen}
      onAttach={canAttach ? handleAttach : undefined}
      onClose={isMobile ? undefined : onClose}
    />
  );

  // Mobile: use MobileBottomSheet
  if (isMobile) {
    return (
      <MobileBottomSheet
        open={!!marketInsightId}
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
      {marketInsightId && (
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          {...backdrop}
          className="fixed inset-0 z-1010 flex items-center justify-center p-8"
          style={{ backgroundColor: 'var(--color-bg-overlay, rgba(0,0,0,0.6))', backdropFilter: 'blur(4px)' }}
        >
          <DashWindowDialog label={detail?.headline || t('dashboard.insightDetail.eyebrow')} onClose={onClose}>
            {body}
          </DashWindowDialog>
        </motion.div>
      )}
    </AnimatePresence>
  );
}

export default InsightDetailModal;
