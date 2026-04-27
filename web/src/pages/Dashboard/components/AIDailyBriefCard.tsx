import React, { useEffect, useState, useCallback, useRef } from 'react';
import { Sparkles, ArrowRight, Newspaper, Clock, ChevronDown, Loader2 } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import { useTranslation } from 'react-i18next';
import TopicBadge from './TopicBadge';
import { getTodayInsights, getInsightDetail, generatePersonalizedInsight } from '../utils/api';
import { useIsMobile } from '@/hooks/useIsMobile';
import i18n from '@/i18n';
import { RowAttachButton } from './RowAttachButton';

interface InsightTopic {
  text: string;
  trend: 'up' | 'down' | 'neutral';
}

interface Insight {
  market_insight_id: string;
  type: string;
  headline: string;
  summary: string;
  completed_at?: string;
  topics?: InsightTopic[];
  [key: string]: unknown;
}

interface AIDailyBriefCardProps {
  onReadFull?: (marketInsightId: string) => void;
  /** Widget instance id — when provided, per-row paperclip attach buttons render. */
  instanceId?: string;
}

interface TypeConfigEntry {
  labelKey: string;
  accent: string;
}

// Module-level cache (survives navigation, clears on page refresh)
let insightsCache: Insight[] | null = null;

/**
 * Read the latest insight brief data from the module cache. Used by the
 * InsightBriefWidget's `useWidgetContextExport` snapshot serializer so the
 * agent gets the actual headline + summary + topics, not just the widget label.
 * Returns null if the brief hasn't loaded yet.
 */
export function getCachedInsights(): Insight[] | null {
  return insightsCache;
}

const TYPE_CONFIG: Record<string, TypeConfigEntry> = {
  pre_market: { labelKey: 'dashboard.brief.typeLabel.preMarket', accent: 'var(--color-profit)' },
  market_update: { labelKey: 'dashboard.brief.typeLabel.marketUpdate', accent: 'var(--color-accent-primary)' },
  post_market: { labelKey: 'dashboard.brief.typeLabel.postMarket', accent: '#a78bfa' },
  personalized: { labelKey: 'dashboard.brief.typeLabel.personalized', accent: '#f59e0b' },
};

function formatRelativeTime(timestamp: string | undefined): string {
  if (!timestamp) return '';
  const now = new Date();
  const then = new Date(timestamp);
  const diffMs = now.getTime() - then.getTime();
  const diffMin = Math.floor(diffMs / 60000);
  if (diffMin < 1) return i18n.t('dashboard.widgets.common.relativeNow');
  if (diffMin < 60) return i18n.t('dashboard.widgets.common.relativePast', { when: `${diffMin}m` });
  const diffHr = Math.floor(diffMin / 60);
  if (diffHr < 24) return i18n.t('dashboard.widgets.common.relativePast', { when: `${diffHr}h` });
  const diffDay = Math.floor(diffHr / 24);
  return i18n.t('dashboard.widgets.common.relativePast', { when: `${diffDay}d` });
}

function formatTime(timestamp: string | undefined): string {
  if (!timestamp) return '';
  try {
    const d = new Date(timestamp);
    return d.toLocaleTimeString(i18n.language, { hour: 'numeric', minute: '2-digit' });
  } catch {
    return '';
  }
}

/** On mobile: show tags in a single row, overflow hidden with "+N more". Desktop: wrap freely. */
function MobileTopicRow({ topics }: { topics: InsightTopic[] }) {
  const { t } = useTranslation();
  const isMobile = useIsMobile();
  const rowRef = useRef<HTMLDivElement>(null);
  const [visibleCount, setVisibleCount] = useState(topics.length);

  useEffect(() => {
    const row = rowRef.current;
    if (!row) return;

    function measure() {
      if (!isMobile) {
        setVisibleCount(topics.length);
        return;
      }
      const children = Array.from(row!.children) as HTMLElement[];
      const rowTop = row!.getBoundingClientRect().top;
      let count = 0;
      for (const child of children) {
        if (child.dataset.overflow) continue; // skip the "+N" badge
        if (child.getBoundingClientRect().top - rowTop > 4) break; // wrapped to next line
        count++;
      }
      setVisibleCount(count < topics.length ? (count || 1) : topics.length);
    }

    measure();
    const ro = new ResizeObserver(measure);
    ro.observe(row);
    return () => ro.disconnect();
  }, [isMobile, topics.length]);

  const overflow = topics.length - visibleCount;

  if (!isMobile) {
    return (
      <div className="flex flex-wrap gap-3">
        {topics.map((t) => <TopicBadge key={t.text} text={t.text} trend={t.trend} />)}
      </div>
    );
  }

  return (
    <div ref={rowRef} className="flex flex-wrap gap-1.5 overflow-hidden" style={{ maxHeight: 28 }}>
      {topics.slice(0, visibleCount).map((t) => (
        <TopicBadge key={t.text} text={t.text} trend={t.trend} />
      ))}
      {overflow > 0 && (
        <span
          data-overflow="true"
          className="px-1.5 py-0.5 rounded text-[10px] font-medium"
          style={{ color: 'var(--color-text-tertiary)', backgroundColor: 'var(--color-bg-tag)', border: '1px solid var(--color-bg-tag)' }}
        >
          {t('dashboard.brief.overflowMore', { count: overflow })}
        </span>
      )}
    </div>
  );
}

function AIDailyBriefCard({ onReadFull, instanceId }: AIDailyBriefCardProps) {
  const { t } = useTranslation();
  const [insights, setInsights] = useState<Insight[]>(insightsCache || []);
  const [loading, setLoading] = useState(!insightsCache);
  const [expanded, setExpanded] = useState(false);
  const [generating, setGenerating] = useState(false);
  const [generateError, setGenerateError] = useState<string | null>(null);
  const mountedRef = useRef(true);

  useEffect(() => {
    return () => { mountedRef.current = false; };
  }, []);

  useEffect(() => {
    if (insightsCache) return;
    let cancelled = false;
    getTodayInsights().then((data) => {
      if (cancelled) return;
      const typedData = data as unknown as Insight[];
      if (typedData?.length) {
        insightsCache = typedData;
        setInsights(typedData);
      }
      setLoading(false);
    });
    return () => { cancelled = true; };
  }, []);

  const latest: Insight | null = insights[0] || null;
  const older = insights.slice(1);

  const handleCardClick = useCallback((e: React.MouseEvent<HTMLDivElement>) => {
    // Don't expand if clicking the CTA button or a link
    if ((e.target as HTMLElement).closest('button') || (e.target as HTMLElement).closest('a')) return;
    if (older.length > 0) setExpanded((v) => !v);
  }, [older.length]);

  const handleGeneratePersonalized = useCallback(async (e: React.MouseEvent) => {
    e.stopPropagation();
    if (generating) return;
    setGenerating(true);
    setGenerateError(null);
    try {
      const row = await generatePersonalizedInsight() as unknown as Insight;
      if (!row?.market_insight_id) return;
      const insightId = row.market_insight_id;

      // Poll until completed or failed
      const poll = async () => {
        const maxAttempts = 120; // 10 min at 5s intervals
        for (let i = 0; i < maxAttempts; i++) {
          if (!mountedRef.current) return;
          await new Promise((r) => setTimeout(r, 5000));
          if (!mountedRef.current) return;
          try {
            const detail = await getInsightDetail(insightId) as unknown as Insight & { status?: string };
            if (detail.status === 'completed') {
              // Prepend to insights list (functional update to avoid stale closure)
              setInsights(prev => {
                const updated = [detail, ...prev.filter((ins) => ins.market_insight_id !== insightId)];
                return updated;
              });
              // Update module cache outside the updater (side-effect-free updater)
              insightsCache = null; // invalidate — next mount will refetch
              onReadFull?.(insightId);
              return;
            }
            if (detail.status === 'failed') {
              if (mountedRef.current) {
                setGenerateError(t('dashboard.brief.errors.generationFailed'));
              }
              return;
            }
          } catch {
            // 404 means row not visible yet, keep polling
          }
        }
        // Poll exhausted without completion
        if (mountedRef.current) {
          setGenerateError(t('dashboard.brief.errors.timeout'));
        }
      };
      await poll();
    } catch (err: unknown) {
      console.error('[Insights] Failed to start personalized brief:', err);
      const status = (err as Record<string, unknown>)?.response
        ? ((err as Record<string, unknown>).response as Record<string, unknown>)?.status
        : undefined;
      if (status === 409) {
        setGenerateError(t('dashboard.brief.errors.alreadyGenerating'));
      } else if (status === 429) {
        setGenerateError(t('dashboard.brief.errors.creditLimit'));
      } else {
        setGenerateError(t('dashboard.brief.errors.generic'));
      }
    } finally {
      setGenerating(false);
    }
  }, [generating, onReadFull, t]);

  if (loading) {
    return (
      <div
        className="relative rounded-3xl overflow-hidden border p-8"
        style={{
          borderColor: 'var(--color-accent-overlay)',
          background: 'var(--color-bg-card)',
        }}
      >
        <div className="animate-pulse space-y-4">
          <div className="flex items-center gap-2">
            <div className="h-6 w-40 rounded-full" style={{ backgroundColor: 'var(--color-bg-hover)' }} />
            <div className="h-4 w-16 rounded" style={{ backgroundColor: 'var(--color-bg-hover)' }} />
          </div>
          <div className="h-8 w-3/4 rounded" style={{ backgroundColor: 'var(--color-bg-hover)' }} />
          <div className="h-4 w-full rounded" style={{ backgroundColor: 'var(--color-bg-hover)' }} />
          <div className="h-4 w-2/3 rounded" style={{ backgroundColor: 'var(--color-bg-hover)' }} />
          <div className="flex gap-3">
            <div className="h-8 w-28 rounded-lg" style={{ backgroundColor: 'var(--color-bg-hover)' }} />
            <div className="h-8 w-24 rounded-lg" style={{ backgroundColor: 'var(--color-bg-hover)' }} />
            <div className="h-8 w-20 rounded-lg" style={{ backgroundColor: 'var(--color-bg-hover)' }} />
          </div>
        </div>
      </div>
    );
  }

  if (!latest) {
    return (
      <div
        className="relative rounded-3xl overflow-hidden border p-8 flex items-center justify-center"
        style={{
          borderColor: 'var(--color-accent-overlay)',
          background: 'var(--color-bg-card)',
          minHeight: 200,
        }}
      >
        <div className="text-center">
          <Newspaper size={40} className="mx-auto mb-3 opacity-30" style={{ color: 'var(--color-accent-primary)' }} />
          <p style={{ color: 'var(--color-text-secondary)' }}>{t('dashboard.brief.generatingFirst')}</p>
        </div>
      </div>
    );
  }

  const updatedAgo = formatRelativeTime(latest.completed_at);
  const topics = latest.topics || [];
  const latestType = TYPE_CONFIG[latest.type] || TYPE_CONFIG.market_update;
  const isPersonalized = latest.type === 'personalized';

  return (
    <div className="relative">
      {/* Stacked card shadows (visible only when collapsed and there are older insights) */}
      {!expanded && older.length > 0 && (
        <>
          <div
            className="absolute left-3 right-3 bottom-0 h-full rounded-3xl border pointer-events-none"
            style={{
              borderColor: 'var(--color-border-muted)',
              background: 'var(--color-bg-card)',
              transform: 'translateY(8px) scale(0.98)',
              opacity: 0.6,
              zIndex: 0,
            }}
          />
          {older.length > 1 && (
            <div
              className="absolute left-6 right-6 bottom-0 h-full rounded-3xl border pointer-events-none"
              style={{
                borderColor: 'var(--color-border-muted)',
                background: 'var(--color-bg-card)',
                transform: 'translateY(16px) scale(0.96)',
                opacity: 0.35,
                zIndex: -1,
              }}
            />
          )}
        </>
      )}

      <motion.div
        layout
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.2 }}
        className="relative group rounded-3xl overflow-hidden border"
        style={{
          borderColor: 'var(--color-accent-overlay)',
          background: `linear-gradient(135deg, var(--color-bg-card) 0%, var(--color-bg-card) 60%, var(--color-accent-soft) 100%)`,
          cursor: older.length > 0 ? 'pointer' : 'default',
          zIndex: 1,
        }}
        onClick={handleCardClick}
      >
        <div className="absolute top-0 right-0 p-6 opacity-20 group-hover:opacity-40 transition-opacity pointer-events-none hidden sm:block">
          <Newspaper size={120} style={{ color: 'var(--color-accent-primary)' }} />
        </div>

        <div className="relative z-10 p-4 sm:p-8 flex flex-col gap-8 items-start">
          <div className="flex-1">
            <div className="flex items-center gap-2 mb-4">
              <div
                className="px-3 py-1 rounded-full border flex items-center gap-2 text-xs font-semibold uppercase tracking-wider"
                style={{
                  backgroundColor: 'var(--color-accent-soft)',
                  borderColor: 'var(--color-accent-overlay)',
                  color: 'var(--color-accent-light)',
                }}
              >
                <Sparkles size={12} />
                {isPersonalized ? t('dashboard.brief.personalizedBrief') : t('dashboard.brief.eyebrow')}
              </div>
              {isPersonalized && (
                <span
                  className="px-2 py-0.5 rounded-full text-[10px] font-semibold uppercase tracking-wider"
                  style={{
                    color: latestType.accent,
                    backgroundColor: `color-mix(in srgb, ${latestType.accent} 15%, transparent)`,
                  }}
                >
                  {t('dashboard.brief.basedOnWatchlistPortfolio')}
                </span>
              )}
              {updatedAgo && (
                <span className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
                  {t('dashboard.brief.updatedWhen', { when: updatedAgo })}
                </span>
              )}
            </div>

            <h2
              className="text-xl sm:text-3xl font-bold mb-4 leading-tight"
              style={{ color: 'var(--color-text-primary)' }}
            >
              {latest.headline}
            </h2>

            <p
              className="mb-6 leading-relaxed line-clamp-3 sm:line-clamp-none"
              style={{ color: 'var(--color-text-secondary)' }}
            >
              {latest.summary}
            </p>

            <MobileTopicRow topics={topics} />

            {/* Mobile: full-width CTA + stack indicator */}
            <div className="flex flex-col gap-2 mt-4 sm:hidden">
              <button
                onClick={(e) => {
                  e.stopPropagation();
                  onReadFull?.(latest.market_insight_id);
                }}
                className="group/btn flex items-center justify-center gap-1.5 w-full py-2.5 rounded-lg text-sm font-semibold transition-colors"
                style={{
                  backgroundColor: 'var(--color-btn-primary-bg, var(--color-accent-primary))',
                  color: 'var(--color-btn-primary-text, #fff)',
                }}
              >
                {t('dashboard.brief.readFullBrief')}
                <ArrowRight size={14} className="group-hover/btn:translate-x-1 transition-transform" />
              </button>
              <button
                onClick={handleGeneratePersonalized}
                disabled={generating}
                title={t('dashboard.brief.generateTooltip')}
                className="group/btn flex items-center justify-center gap-1.5 w-full py-2.5 rounded-lg text-sm font-semibold transition-colors border"
                style={{
                  borderColor: 'var(--color-border-default)',
                  color: 'var(--color-text-secondary)',
                  opacity: generating ? 0.6 : 1,
                }}
              >
                {generating ? <Loader2 size={14} className="animate-spin" /> : <Sparkles size={14} />}
                {generating ? t('dashboard.brief.generating') : t('dashboard.brief.generatePersonalized')}
              </button>
              {generateError && (
                <p className="text-xs mt-1" style={{ color: 'var(--color-loss, #ef4444)' }}>{generateError}</p>
              )}
              {older.length > 0 && (
                <button
                  onClick={(e) => {
                    e.stopPropagation();
                    setExpanded((v) => !v);
                  }}
                  className="flex items-center justify-center gap-1.5 text-xs transition-colors"
                  style={{ color: 'var(--color-text-tertiary)' }}
                >
                  <Clock size={12} />
                  {t('dashboard.brief.earlierCount', { count: older.length })}
                  <ChevronDown
                    size={14}
                    className="transition-transform"
                    style={{ transform: expanded ? 'rotate(180deg)' : 'rotate(0deg)' }}
                  />
                </button>
              )}
            </div>
          </div>

          {/* Desktop CTA + stack indicator */}
          <div className="hidden sm:flex w-auto flex-col items-end justify-end gap-3 self-stretch">
            <div className="flex gap-2">
              <button
                onClick={handleGeneratePersonalized}
                disabled={generating}
                title={t('dashboard.brief.generateTooltip')}
                className="group/btn flex items-center gap-2 px-5 py-3 rounded-xl font-semibold transition-colors border"
                style={{
                  borderColor: 'var(--color-border-default)',
                  color: 'var(--color-text-secondary)',
                  opacity: generating ? 0.6 : 1,
                }}
                onMouseEnter={(e) => { if (!generating) e.currentTarget.style.backgroundColor = 'var(--color-bg-hover)'; }}
                onMouseLeave={(e) => { e.currentTarget.style.backgroundColor = 'transparent'; }}
              >
                {generating ? <Loader2 size={16} className="animate-spin" /> : <Sparkles size={16} />}
                {generating ? t('dashboard.brief.generating') : t('dashboard.brief.generatePersonalized')}
              </button>
              <button
                onClick={(e) => {
                  e.stopPropagation();
                  onReadFull?.(latest.market_insight_id);
                }}
                className="group/btn flex items-center gap-2 px-6 py-3 rounded-xl font-semibold transition-colors shadow-lg"
                style={{
                  backgroundColor: 'var(--color-btn-primary-bg, var(--color-accent-primary))',
                  color: 'var(--color-btn-primary-text, #fff)',
                }}
                onMouseEnter={(e) => (e.currentTarget.style.opacity = '0.9')}
                onMouseLeave={(e) => (e.currentTarget.style.opacity = '1')}
              >
                {t('dashboard.brief.readFullBrief')}
                <ArrowRight size={16} className="group-hover/btn:translate-x-1 transition-transform" />
              </button>
            </div>
            {generateError && (
              <p className="text-xs text-right" style={{ color: 'var(--color-loss, #ef4444)' }}>{generateError}</p>
            )}

            {older.length > 0 && (
              <button
                onClick={(e) => {
                  e.stopPropagation();
                  setExpanded((v) => !v);
                }}
                className="flex items-center gap-1.5 text-xs transition-colors"
                style={{ color: 'var(--color-text-tertiary)' }}
                onMouseEnter={(e) => (e.currentTarget.style.color = 'var(--color-text-secondary)')}
                onMouseLeave={(e) => (e.currentTarget.style.color = 'var(--color-text-tertiary)')}
              >
                <Clock size={12} />
                {t('dashboard.brief.earlierToday', { count: older.length })}
                <ChevronDown
                  size={14}
                  className="transition-transform"
                  style={{ transform: expanded ? 'rotate(180deg)' : 'rotate(0deg)' }}
                />
              </button>
            )}
          </div>
        </div>

        {/* Expanded timeline — inside the main card */}
        <AnimatePresence>
          {expanded && older.length > 0 && (
            <motion.div
              initial={{ height: 0, opacity: 0 }}
              animate={{ height: 'auto', opacity: 1 }}
              exit={{ height: 0, opacity: 0 }}
              transition={{ duration: 0.3, ease: [0.4, 0, 0.2, 1] }}
              className="overflow-hidden"
            >
              <div
                className="mx-8 mb-6 border-t pt-5"
                style={{ borderColor: 'var(--color-border-muted)' }}
              >
                <div className="flex items-center gap-2 mb-4">
                  <span
                    className="text-[10px] font-semibold uppercase tracking-wider"
                    style={{ color: 'var(--color-text-tertiary)' }}
                  >
                    {t('dashboard.brief.earlierInsights')}
                  </span>
                </div>

                <div className="space-y-1">
                  {older.map((item) => {
                    const cfg = TYPE_CONFIG[item.type] || TYPE_CONFIG.market_update;
                    return (
                      <div key={item.market_insight_id} className="row-attach-host relative">
                        <button
                          onClick={(e) => {
                            e.stopPropagation();
                            onReadFull?.(item.market_insight_id);
                          }}
                          className="w-full flex items-center gap-4 px-4 py-3 rounded-xl text-left transition-colors group/item"
                          style={{ color: 'var(--color-text-secondary)' }}
                          onMouseEnter={(e) => (e.currentTarget.style.backgroundColor = 'var(--color-bg-hover)')}
                          onMouseLeave={(e) => (e.currentTarget.style.backgroundColor = 'transparent')}
                        >
                          <span
                            className="text-xs font-medium shrink-0 w-16 text-right tabular-nums"
                            style={{ color: 'var(--color-text-tertiary)' }}
                          >
                            {formatTime(item.completed_at)}
                          </span>

                          <span
                            className="w-2 h-2 rounded-full shrink-0"
                            style={{ backgroundColor: cfg.accent }}
                          />

                          <span
                            className="text-[10px] font-semibold uppercase tracking-wider shrink-0 px-2 py-0.5 rounded"
                            style={{
                              color: cfg.accent,
                              backgroundColor: `color-mix(in srgb, ${cfg.accent} 15%, transparent)`,
                            }}
                          >
                            {t(cfg.labelKey)}
                          </span>

                          <span
                            className="text-sm truncate flex-1 group-hover/item:text-[var(--color-text-primary)] transition-colors"
                          >
                            {item.headline}
                          </span>
                        </button>
                        {instanceId && (
                          <span className="absolute right-2 top-1/2 -translate-y-1/2 z-10">
                            <RowAttachButton instanceId={instanceId} rowId={item.market_insight_id} />
                          </span>
                        )}
                      </div>
                    );
                  })}
                </div>
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </motion.div>

      {/* Bottom padding for stacked shadow effect */}
      {!expanded && older.length > 0 && (
        <div style={{ height: older.length > 1 ? 16 : 8 }} />
      )}
    </div>
  );
}

export default AIDailyBriefCard;
