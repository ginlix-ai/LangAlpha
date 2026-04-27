/**
 * Shared insight-brief snapshot helpers — used by:
 *   - InsightBriefWidget per-row attach (fetches full content[] on click)
 *   - InsightDetailModal attach button (uses already-loaded detail)
 *
 * Mirrors the pattern in `newsArticleFetch.ts` so the agent gets the same
 * `<widget-context>` block whether the user attached from a row or from
 * inside the detail modal.
 */

import type { WidgetContextSnapshot } from '../widgets/framework/contextSnapshot';
import {
  serializeInsightToMarkdown,
  wrapWidgetContext,
  type InsightDetail,
} from '../widgets/framework/snapshotSerializers';
import { getInsightDetail } from './api';

interface RawInsight {
  market_insight_id?: string;
  type?: string;
  headline?: string;
  summary?: string;
  completed_at?: string;
  model?: string;
  topics?: Array<{ text: string; trend?: 'up' | 'down' | 'neutral' }>;
  content?: Array<{ title: string; body: string; url?: string }>;
  sources?: Array<{ url: string; title?: string }>;
  [key: string]: unknown;
}

export function normalizeInsight(raw: RawInsight): InsightDetail {
  return {
    market_insight_id: raw.market_insight_id ?? '',
    type: raw.type ?? 'market_update',
    headline: raw.headline ?? '',
    summary: raw.summary,
    completed_at: raw.completed_at,
    model: raw.model,
    topics: raw.topics,
    content: raw.content,
    sources: raw.sources,
  };
}

export function buildInsightWidgetSnapshot(opts: {
  instanceId: string;
  rowId: string;
  insight: InsightDetail;
  widgetType?: string;
}): WidgetContextSnapshot {
  const widgetType = opts.widgetType ?? 'insight.brief/row';
  const body = serializeInsightToMarkdown(opts.insight);
  const text = wrapWidgetContext(
    widgetType,
    {
      insightId: opts.insight.market_insight_id || undefined,
      insightType: opts.insight.type,
      completedAt: opts.insight.completed_at,
    },
    body,
  );
  return {
    widget_type: widgetType,
    widget_id: `${opts.instanceId}/${opts.rowId}`,
    label: 'Brief: ' + opts.insight.headline,
    description: [opts.insight.type, opts.insight.completed_at].filter(Boolean).join(' · ') || undefined,
    captured_at: new Date().toISOString(),
    text,
    data: { insight: opts.insight },
  };
}

/**
 * Build a row-level snapshot for a single insight. Tries to fetch the full
 * detail via `getInsightDetail`; falls back to whatever fields the cached
 * list-row has if the fetch fails.
 */
export async function buildInsightSnapshot(opts: {
  instanceId: string;
  rowId: string;
  insightId: string;
  fallback: InsightDetail;
}): Promise<WidgetContextSnapshot> {
  let insight: InsightDetail = opts.fallback;
  try {
    const raw = (await getInsightDetail(opts.insightId)) as RawInsight;
    insight = normalizeInsight(raw);
    if (!insight.topics?.length && opts.fallback.topics?.length) insight.topics = opts.fallback.topics;
    if (!insight.summary && opts.fallback.summary) insight.summary = opts.fallback.summary;
    if (!insight.completed_at && opts.fallback.completed_at) insight.completed_at = opts.fallback.completed_at;
    if (!insight.headline && opts.fallback.headline) insight.headline = opts.fallback.headline;
  } catch (err) {
    console.warn('[insightFetch] full-detail fetch failed, using row fallback', err);
  }
  return buildInsightWidgetSnapshot({
    instanceId: opts.instanceId,
    rowId: opts.rowId,
    insight,
  });
}
