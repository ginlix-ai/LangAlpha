/**
 * Builds the chart-selection payload for a MarketView send: the backend
 * `additional_context` items, the message-bubble snapshots, and the outgoing
 * message text (a lone selection's note is promoted when the user typed
 * nothing). Shared by both send paths — the desktop `MarketChatPanel` and the
 * mobile / FAB path in `MarketView` — so the build is defined once. Clearing
 * the selections after send stays the caller's job (the two paths clear at
 * different points in their flow).
 */
import { chartSelectionToContext, describeSelectionImage } from '../../ChatAgent/utils/fileUpload';
import {
  chartSelectionStore,
  promoteSelectionComment,
  toSelectionSnapshot,
  type ChartSelectionSnapshot,
} from '../stores/chartSelectionStore';

/**
 * Display-only metadata for a region crop's thumbnail in the live user bubble.
 * The index signature mirrors the consumers' `AttachmentMeta` shape so this is
 * assignable to both send paths' `metaItems` without a cast.
 */
export interface SelectionAttachmentMeta {
  name: string;
  type: string;
  size: number;
  preview: string;
  dataUrl: string;
  [key: string]: unknown;
}

export interface ChartSelectionSend {
  /** `additional_context` items to append to the send (empty when none confirmed). */
  contexts: Record<string, unknown>[];
  /** Per-selection snapshots so the sent message renders selection cards. */
  snapshots: ChartSelectionSnapshot[];
  /**
   * Display-only attachment metas for region crops, so the live user bubble
   * shows the screenshot immediately from the base64 `preview`. The durable
   * replay copy is the backend-persisted attachment, not these.
   */
  attachments: SelectionAttachmentMeta[];
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
  const attachments: SelectionAttachmentMeta[] = [];
  for (const selection of confirmed) {
    contexts.push(
      ...(chartSelectionToContext(selection, { symbol, timeframe }) as unknown as Record<
        string,
        unknown
      >[]),
    );
    if (selection.croppedImage) {
      attachments.push({
        name: describeSelectionImage(selection),
        type: 'image',
        size: 0,
        preview: selection.croppedImage,
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
