import React, { useEffect, useState, useCallback, useRef } from 'react';
import { ChevronDown } from 'lucide-react';
import { Disclosure } from '@/components/ui/Disclosure';
import { SwapCell } from '@/components/ui/crossfade';
import { SkeletonBar } from '@/components/ui/skeleton';
import { useLastHeight } from '@/lib/lastShape';
import { Loader } from '@/components/ui/loader';
import { useTranslation } from 'react-i18next';
import { getTodayInsights, getInsightDetail, generatePersonalizedInsight } from '../utils/api';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { clockTime, relativeTime } from '@/lib/format';
import { TermChange, TermDivider } from '@/pages/ChatAgent/components/charts/TermCard';
import { WidgetCardMeta, WidgetCardSub } from '../widgets/framework/widgetCardSlots';
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
  /** The widget instance: keys the row attach buttons and the remembered height. */
  instanceId: string;
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

const TYPE_LABEL_KEY: Record<string, string> = {
  pre_market: 'dashboard.brief.typeLabel.preMarket',
  market_update: 'dashboard.brief.typeLabel.marketUpdate',
  post_market: 'dashboard.brief.typeLabel.postMarket',
  personalized: 'dashboard.brief.typeLabel.personalized',
};


function formatTime(timestamp: string | undefined, locale: string): string {
  if (!timestamp) return '';
  try {
    const d = new Date(timestamp);
    return clockTime(d, locale);
  } catch {
    return '';
  }
}

/** A topic with its direction drawn, the cards' arrow beside the word. */
function Topic({ topic }: { topic: InsightTopic }) {
  const value = topic.trend === 'up' ? 1 : topic.trend === 'down' ? -1 : 0;
  return (
    <span className="term-chip dash-topic">
      <TermChange value={value}>{''}</TermChange>
      {topic.text}
    </span>
  );
}

function AIDailyBriefCard({ onReadFull, instanceId }: AIDailyBriefCardProps) {
  const { t } = useTranslation();
  const locale = useLocale();
  const now = useNow();
  const [insights, setInsights] = useState<Insight[]>(insightsCache || []);
  const [loading, setLoading] = useState(!insightsCache);
  const [expanded, setExpanded] = useState(false);
  const [generating, setGenerating] = useState(false);
  const [generateError, setGenerateError] = useState<string | null>(null);
  const mountedRef = useRef(true);
  // The loaded card's height, kept for the skeleton of the next visit: a brief
  // reads much like yesterday's, so the skeleton holds about the room the card
  // will take and what sits below does not move when it lands. Not while the
  // earlier insights are open, which the next visit does not start with.
  const { ref: readyRef, height: lastHeight } = useLastHeight(`dashboard:brief.height.${instanceId}`, !expanded);

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

  // Each state below is a layer of one cell, so the brief crossfades from its
  // skeleton in place rather than cutting to a card that then rises in.
  if (loading) {
    return (
      <SwapCell as="div" swapKey="loading">
        <div data-fit-hold className="widget-term" style={{ height: lastHeight ?? undefined }}>
          <p className="dash-brief-lede"><SkeletonBar style={{ width: '70%' }} /></p>
          <p className="dash-brief-summary">
            <SkeletonBar style={{ width: '100%' }} />
            <br />
            <SkeletonBar style={{ width: '85%' }} />
          </p>
          <div className="term-line dash-brief-foot">
            <SkeletonBar style={{ width: '8ch' }} />
            <SkeletonBar className="term-push" style={{ width: '12ch' }} />
          </div>
        </div>
      </SwapCell>
    );
  }

  if (!latest) {
    return (
      <SwapCell as="div" swapKey="empty">
        <div className="widget-term dash-empty">
          <span>{t('dashboard.brief.generatingFirst')}</span>
        </div>
      </SwapCell>
    );
  }

  const updatedAgo = relativeTime(latest.completed_at, locale, now);
  const topics = latest.topics || [];
  const typeLabelKey = TYPE_LABEL_KEY[latest.type] || TYPE_LABEL_KEY.market_update;
  const isPersonalized = latest.type === 'personalized';

  return (
    <SwapCell as="div" swapKey="ready">
      <WidgetCardSub>{t(typeLabelKey)}</WidgetCardSub>
      <WidgetCardMeta>
        <span title={updatedAgo ? t('dashboard.brief.updatedWhen', { when: updatedAgo }) : undefined}>
          {formatTime(latest.completed_at, locale)}
        </span>
      </WidgetCardMeta>
      <div
        ref={readyRef}
        className="widget-term"
        style={{ cursor: older.length > 0 ? 'pointer' : undefined }}
        onClick={handleCardClick}
      >
        <p className="dash-brief-lede">{latest.headline}</p>
        <p className="dash-brief-summary">{latest.summary}</p>
        {isPersonalized && (
          <p className="term-t3 mb-2">{t('dashboard.brief.basedOnWatchlistPortfolio')}</p>
        )}

        {topics.length > 0 && (
          <div className="term-chips">
            {topics.map((topic) => <Topic key={topic.text} topic={topic} />)}
          </div>
        )}

        <div className="term-line dash-brief-foot">
          {older.length > 0 && (
            <button
              type="button"
              className="dash-link"
              aria-expanded={expanded}
              onClick={(e) => {
                e.stopPropagation();
                setExpanded((v) => !v);
              }}
            >
              {t('dashboard.brief.earlierToday', { count: older.length })}
              <ChevronDown
                size={11}
                className="transition-transform"
                style={{ transform: expanded ? 'rotate(180deg)' : 'rotate(0deg)' }}
              />
            </button>
          )}
          <button
            type="button"
            className="dash-link term-push"
            onClick={handleGeneratePersonalized}
            disabled={generating}
            title={t('dashboard.brief.generateTooltip')}
          >
            {generating && (
              <span aria-hidden="true" className="shrink-0">
                <Loader size={11} className="text-current" />
              </span>
            )}
            {generating ? t('dashboard.brief.generating') : t('dashboard.brief.generatePersonalized')}
          </button>
          <button
            type="button"
            className="dash-link term-ink"
            onClick={(e) => {
              e.stopPropagation();
              onReadFull?.(latest.market_insight_id);
            }}
          >
            {t('dashboard.brief.readFullBrief')} <span aria-hidden="true">›</span>
          </button>
        </div>
        {generateError && <p className="dash-error">{generateError}</p>}

        {/* The earlier briefs of the day, folded under the latest. */}
        <Disclosure open={expanded && older.length > 0}>
          <TermDivider>{t('dashboard.brief.earlierInsights')}</TermDivider>
          <table className="term-table dash-table">
            <tbody>
              {older.map((item) => (
                <tr
                  key={item.market_insight_id}
                  className="row-attach-host"
                  data-clickable=""
                  onClick={(e) => {
                    e.stopPropagation();
                    onReadFull?.(item.market_insight_id);
                  }}
                >
                  <td className="term-t3">{formatTime(item.completed_at, locale)}</td>
                  <td className="term-label term-t3">
                    {t(TYPE_LABEL_KEY[item.type] || TYPE_LABEL_KEY.market_update)}
                  </td>
                  <td className="term-fill dash-attach-cell">
                    {/* The row takes the pointer; the headline is its button for
                        the keyboard, and its click reaches the row. */}
                    <button type="button" className="dash-row-link" title={item.headline}>
                      {item.headline}
                    </button>
                    <RowAttachButton instanceId={instanceId} rowId={item.market_insight_id} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </Disclosure>
      </div>
    </SwapCell>
  );
}

export default AIDailyBriefCard;
