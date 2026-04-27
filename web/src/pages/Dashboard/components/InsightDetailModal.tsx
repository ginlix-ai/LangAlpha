import React, { useEffect, useState } from 'react';
import { X, ExternalLink, Sparkles, ChevronDown, Paperclip } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import { useTranslation } from 'react-i18next';
import TopicBadge from './TopicBadge';
import { getInsightDetail } from '../utils/api';
import { useIsMobile } from '@/hooks/useIsMobile';
import { MobileBottomSheet } from '@/components/ui/mobile-bottom-sheet';
import { useToast } from '@/components/ui/use-toast';
import { ContextBus } from '@/lib/contextBus';
import { buildInsightWidgetSnapshot, normalizeInsight } from '../utils/insightFetch';
import i18n from '@/i18n';

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
function InsightBody({
  detail,
  loading,
  sourcesOpen,
  setSourcesOpen,
  isMobile,
  onAttach,
}: {
  detail: InsightDetail | null;
  loading: boolean;
  sourcesOpen: boolean;
  setSourcesOpen: React.Dispatch<React.SetStateAction<boolean>>;
  isMobile: boolean;
  /** Optional handler. If provided, an Attach-to-chat button is rendered next to the eyebrow. */
  onAttach?: () => void;
}) {
  const { t } = useTranslation();
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

  if (!detail) {
    return (
      <div className="flex items-center justify-center py-24">
        <p style={{ color: 'var(--color-text-secondary)' }}>{t('dashboard.insightDetail.notFound')}</p>
      </div>
    );
  }

  return (
    <>
      <div className={isMobile ? '' : 'p-6 md:p-8 pb-0'}>
        <div className="flex items-center gap-2 sm:gap-3 mb-3 sm:mb-4 flex-wrap">
          <div
            className="px-2.5 py-0.5 sm:px-3 sm:py-1 rounded-full border flex items-center gap-1.5 sm:gap-2 text-[10px] sm:text-xs font-semibold uppercase tracking-wider"
            style={{
              backgroundColor: 'var(--color-accent-soft)',
              borderColor: 'var(--color-accent-overlay)',
              color: 'var(--color-accent-light)',
            }}
          >
            <Sparkles size={isMobile ? 10 : 12} />
            {t('dashboard.insightDetail.eyebrow')}
          </div>
          {detail.model && (
            <span
              className="px-2 py-0.5 rounded text-[10px] font-bold uppercase tracking-wider"
              style={{
                backgroundColor: 'var(--color-bg-hover)',
                color: 'var(--color-text-secondary)',
              }}
            >
              {detail.model}
            </span>
          )}
          {detail.completed_at && (
            <span className="text-[11px] sm:text-xs" style={{ color: 'var(--color-text-secondary)' }}>
              {formatDate(detail.completed_at)}
            </span>
          )}
          {onAttach && (
            <button
              type="button"
              onClick={onAttach}
              className="ml-auto inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full border text-[11px] sm:text-xs font-medium transition-colors"
              style={{
                backgroundColor: 'var(--color-accent-soft)',
                borderColor: 'var(--color-accent-overlay)',
                color: 'var(--color-accent-primary)',
              }}
              onMouseEnter={(e) => {
                e.currentTarget.style.backgroundColor = 'var(--color-accent-primary)';
                e.currentTarget.style.color = '#fff';
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.backgroundColor = 'var(--color-accent-soft)';
                e.currentTarget.style.color = 'var(--color-accent-primary)';
              }}
              title={t('dashboard.widgets.frame.addToContext', { defaultValue: 'Attach to chat' })}
            >
              <Paperclip size={isMobile ? 12 : 13} />
              {t('dashboard.widgets.frame.addToContext', { defaultValue: 'Attach to chat' })}
            </button>
          )}
        </div>

        <h1
          className="text-xl sm:text-2xl md:text-3xl font-bold leading-tight mb-3 sm:mb-4"
          style={{ color: 'var(--color-text-primary)' }}
        >
          {detail.headline}
        </h1>

        {(detail.topics?.length ?? 0) > 0 && (
          <div className="flex flex-wrap gap-1.5 sm:gap-2 mb-4 sm:mb-6">
            {detail.topics!.map((topic) => (
              <TopicBadge key={topic.text} text={topic.text} trend={topic.trend} />
            ))}
          </div>
        )}

        {detail.summary && (
          <p
            className="text-[13px] sm:text-sm leading-relaxed mb-4 sm:mb-6"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            {detail.summary}
          </p>
        )}
      </div>

      <div className={isMobile ? '' : 'px-6 md:px-8 pb-6 md:pb-8'}>
        {(detail.content?.length ?? 0) > 0 && (
          <div
            className="rounded-xl border overflow-hidden"
            style={{
              backgroundColor: 'var(--color-bg-subtle)',
              borderColor: 'var(--color-border-muted)',
            }}
          >
            {detail.content!.map((item, i) => {
              const domain = item.url ? (() => { try { return new URL(item.url).hostname.replace('www.', ''); } catch { return ''; } })() : '';
              const favicon = domain ? `https://www.google.com/s2/favicons?domain=${domain}&sz=32` : undefined;

              return (
                <div
                  key={i}
                  className={isMobile ? 'px-3 py-3 flex gap-3' : 'px-5 py-4 flex gap-4'}
                  style={i > 0 ? { borderTop: '1px solid var(--color-border-muted)' } : undefined}
                >
                  <span
                    className="text-[11px] sm:text-xs font-bold mt-0.5 shrink-0 w-4 sm:w-5 text-right"
                    style={{ color: 'var(--color-text-tertiary)' }}
                  >
                    {i + 1}
                  </span>
                  <div className="min-w-0 flex-1">
                    <div className={isMobile ? 'flex flex-col gap-1' : 'flex items-start justify-between gap-3'}>
                      <h3
                        className="text-[13px] sm:text-sm font-semibold leading-snug"
                        style={{ color: 'var(--color-text-primary)' }}
                      >
                        {item.title}
                      </h3>
                      {item.url && (
                        <a
                          href={item.url}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="inline-flex items-center gap-1.5 text-[11px] sm:text-xs shrink-0 transition-opacity hover:opacity-80"
                          style={{ color: 'var(--color-text-tertiary)' }}
                        >
                          {favicon ? (
                            <img src={favicon} alt="" className="w-3.5 h-3.5 rounded-sm" />
                          ) : (
                            <ExternalLink size={12} />
                          )}
                          <span>{domain}</span>
                        </a>
                      )}
                    </div>
                    <p
                      className="text-[13px] sm:text-sm leading-relaxed mt-1"
                      style={{ color: 'var(--color-text-secondary)' }}
                    >
                      {item.body}
                    </p>
                  </div>
                </div>
              );
            })}
          </div>
        )}

        {/* Collapsible All Sources */}
        {(detail.sources?.length ?? 0) > 0 && (
          <div className="mt-6">
            <button
              onClick={() => setSourcesOpen((v) => !v)}
              className="flex items-center gap-2 text-xs font-semibold uppercase tracking-wider w-full py-2 transition-colors"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              <ChevronDown
                size={14}
                className="transition-transform"
                style={{ transform: sourcesOpen ? 'rotate(180deg)' : 'rotate(0deg)' }}
              />
              {t('dashboard.insightDetail.allSources', { count: detail.sources!.length })}
              {!sourcesOpen && (
                <span className="flex items-center -space-x-1 ml-1">
                  {detail.sources!
                    .filter((s) => s.favicon)
                    .slice(0, 5)
                    .map((s, i) => (
                      <img
                        key={i}
                        src={s.favicon}
                        alt=""
                        className="w-4 h-4 rounded-full ring-1 ring-[var(--color-bg-elevated)]"
                      />
                    ))}
                </span>
              )}
            </button>
            {sourcesOpen && (
              <div className="mt-2 space-y-1">
                {detail.sources!.map((source, i) => {
                  const domain = (() => { try { return new URL(source.url).hostname.replace('www.', ''); } catch { return source.url; } })();
                  return (
                    <a
                      key={i}
                      href={source.url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="flex items-center gap-2 text-xs py-1.5 px-3 rounded-lg transition-colors"
                      style={{ color: 'var(--color-text-secondary)' }}
                      onMouseEnter={(e) => { e.currentTarget.style.backgroundColor = 'var(--color-bg-hover)'; }}
                      onMouseLeave={(e) => { e.currentTarget.style.backgroundColor = 'transparent'; }}
                    >
                      {source.favicon ? (
                        <img src={source.favicon} alt="" className="w-4 h-4 rounded-sm shrink-0" />
                      ) : (
                        <ExternalLink size={14} className="shrink-0 opacity-50" />
                      )}
                      <span className="truncate">{source.title || domain}</span>
                      <span className="ml-auto shrink-0 opacity-40">{domain}</span>
                    </a>
                  );
                })}
              </div>
            )}
          </div>
        )}
      </div>
    </>
  );
}

function InsightDetailModal({ marketInsightId, onClose }: InsightDetailModalProps) {
  const { t } = useTranslation();
  const { toast } = useToast();
  const [detail, setDetail] = useState<InsightDetail | null>(null);
  const [loading, setLoading] = useState(false);
  const [sourcesOpen, setSourcesOpen] = useState(false);
  const isMobile = useIsMobile();

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
      description: 'Brief: ' + (detail.headline || ''),
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
      if (e.key === 'Escape') onClose();
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
      isMobile={isMobile}
      onAttach={canAttach ? handleAttach : undefined}
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
          onClick={onClose}
          className="fixed inset-0 z-50 flex items-center justify-center p-8"
          style={{ backgroundColor: 'var(--color-bg-overlay, rgba(0,0,0,0.6))', backdropFilter: 'blur(4px)' }}
        >
          <motion.div
            initial={{ opacity: 0, scale: 0.95, y: 20 }}
            animate={{ opacity: 1, scale: 1, y: 0 }}
            exit={{ opacity: 0, scale: 0.95, y: 20 }}
            onClick={(e) => e.stopPropagation()}
            className="w-full max-w-4xl max-h-[90vh] rounded-3xl overflow-hidden shadow-2xl flex flex-col relative border"
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

export default InsightDetailModal;
