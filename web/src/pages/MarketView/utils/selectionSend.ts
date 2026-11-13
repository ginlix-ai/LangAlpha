/**
 * Builds the chart-selection payload for a MarketView send: the backend
 * `additional_context` items, the message-bubble snapshots, and the outgoing
 * message text (a lone selection's note is promoted when the user typed
 * nothing). Shared by both send paths — the desktop `MarketChatPanel` and the
 * mobile / FAB path in `MarketView` — so the build is defined once. Clearing
 * the selections after send stays the caller's job (the two paths clear at
 * different points in their flow).
 */
import { chartSelectionToContext, describeSelectionImage, type AttachmentMeta } from '../../ChatAgent/utils/fileUpload';
import { marketViewAnnotationContext } from '../constants/annotationPrompt';
import type { ChartView } from '../types/chartView';
import { normalizeTimeframe } from './chartInstanceKey';
import {
  chartSelectionStore,
  promoteSelectionComment,
  toSelectionSnapshot,
  type ChartSelectionSnapshot,
} from '../stores/chartSelectionStore';

export interface ChartSelectionSend {
  /** `additional_context` items to append to the send (empty when none confirmed). */
  contexts: Record<string, unknown>[];
  /** Per-selection snapshots so the sent message renders selection cards. */
  snapshots: ChartSelectionSnapshot[];
  /**
   * Display-only attachment metas for region crops, so the live user bubble
   * shows the screenshot immediately from the base64 `dataUrl`. The durable
   * replay copy is the backend-persisted attachment, not these.
   */
  attachments: AttachmentMeta[];
  /** Message to send — the lone selection's note when the user typed nothing. */
  outgoingMessage: string;
}

export function buildChartSelectionSend(
  symbol: string,
  timeframe: string,
  message: string,
): ChartSelectionSend {
  const confirmed = chartSelectionStore.getConfirmedFor(symbol, timeframe);
  const contexts: Record<string, unknown>[] = [];
  const attachments: AttachmentMeta[] = [];
  for (const selection of confirmed) {
    contexts.push(...chartSelectionToContext(selection, { symbol, timeframe }));
    if (selection.croppedImage) {
      attachments.push({
        name: describeSelectionImage(selection),
        type: 'image',
        size: 0,
        dataUrl: selection.croppedImage,
      });
    }
  }
  return {
    contexts,
    snapshots: confirmed.map(toSelectionSnapshot),
    attachments,
    outgoingMessage: promoteSelectionComment(message, confirmed),
  };
}

export interface MarketSendContexts extends ChartSelectionSend {
  /** The chart-annotation skill, first in the send's `additional_context`. */
  skill: Record<string, unknown>;
  /** What the charts beside the composer show (`chart_views`). */
  chartViews: ChartView[];
}

/**
 * What every MarketView send carries about the chart on screen: the
 * chart-annotation skill, for its drawing guide and to say which ticker and
 * timeframe "the chart" is (drawing on another timeframe would not show on
 * this one), the confirmed selections for that chart, and its view.
 */
export function buildMarketSendContexts(
  symbol: string,
  interval: string,
  message: string,
  chartViews: ChartView[],
): MarketSendContexts {
  const sym = symbol.toUpperCase();
  const tf = normalizeTimeframe(interval);
  return {
    skill: {
      type: 'skills',
      name: 'chart-annotation',
      instruction: sym ? marketViewAnnotationContext(sym, tf) : undefined,
    },
    ...buildChartSelectionSend(sym, tf, message),
    chartViews,
  };
}
