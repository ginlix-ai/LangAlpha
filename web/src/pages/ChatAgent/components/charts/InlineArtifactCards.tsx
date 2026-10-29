import type React from 'react';
import { readTypedTicker } from '@/lib/marketUtils';
import type { InlineCardProps } from './cardReaders';
import { InlineAutomationCard } from './InlineAutomationCards';
import { InlinePreviewCard } from './InlinePreviewCard';
import { InlineChartAnnotationCard } from './InlineChartAnnotationCard';
import { InlineQuoteCard } from './InlineQuoteCard';
import { InlineStockPriceCard } from './InlineStockPriceCard';
import { InlineCompanyOverviewCard } from './InlineCompanyOverviewCard';
import {
  InlineMarketIndicesCard,
  InlineMarketOverviewCard,
  InlineSectorPerformanceCard,
} from './InlineMarketCards';
import { InlineStockScreenerCard } from './InlineScreenerCard';
import { InlineOptionsChainCard } from './InlineOptionsChainCard';
import { InlineMarketMoversCard } from './InlineMarketMoversCard';
import { InlineSecFilingCard } from './InlineSecFilingCard';
import { InlineWebSearchCard } from './InlineWebSearchCard';
import { OrderReceiptCard } from '../mcp/OrderReceiptCard';
import { MessageDeliveryCard } from '../messaging/MessageDeliveryCard';

export const INLINE_ARTIFACT_TOOLS = new Set([
  'get_daily_prices',
  'get_stock_daily_prices',
  'get_company_overview',
  'get_quote',
  // Composite tool, renders InlineMarketOverviewCard, which nests the legacy
  // indices + sector cards. Requires the `market_overview` entry in
  // INLINE_ARTIFACT_MAP (ActivityBlock.tsx / MessageList.tsx). Unlike the other
  // cards, InlineMarketOverviewCard never returns null, a completed
  // get_market_overview call routed here always renders (the unknown-region
  // error path falls back to a minimal region card rather than nothing).
  'get_market_overview',
  // Legacy names (pre-consolidation), kept for SSE replay of historical threads
  'get_market_indices',
  'get_sector_performance',
  'get_sec_filing',
  'screen_stocks',
  // Both draw a card for an error too, so a failed lookup keeps its place.
  'get_options_chain',
  'get_market_movers',
  'check_automations',
  'create_automation',
  'GetPreviewUrl',
  'WebSearch',
  'draw_chart_annotation',
]);

// ─── Artifact-type dispatch ─────────────────────────────────────────

/**
 * Maps an artifact `type` to its inline card component. Single source of truth
 * for both the activity timeline (ActivityBlock) and the message list
 * (MessageList), a new inline card is registered here (plus its tool-name gate
 * in INLINE_ARTIFACT_TOOLS) rather than in each surface separately. Both
 * surfaces pass the call's arguments as `toolArgs`; a card that needs only its
 * artifact ignores them.
 */
export const INLINE_ARTIFACT_MAP: Record<string, React.ComponentType<InlineCardProps>> = {
  stock_prices: InlineStockPriceCard,
  company_overview: InlineCompanyOverviewCard,
  quote: InlineQuoteCard,
  market_indices: InlineMarketIndicesCard,
  sector_performance: InlineSectorPerformanceCard,
  market_overview: InlineMarketOverviewCard,
  sec_filing: InlineSecFilingCard,
  stock_screener: InlineStockScreenerCard,
  options_chain: InlineOptionsChainCard,
  market_movers: InlineMarketMoversCard,
  automations: InlineAutomationCard,
  preview_url: InlinePreviewCard,
  web_search: InlineWebSearchCard,
  chart_annotation: InlineChartAnnotationCard,
  order_receipt: OrderReceiptCard,
  message_delivery: MessageDeliveryCard,
};

/**
 * The compact cards whose data is a price, and so read better as the live
 * chart. An overview is prose and fundamentals: the chart is a neighbour of
 * that, not a bigger view of it, so its card opens the result itself.
 */
const CHART_CARD_TYPES = new Set(['quote', 'stock_prices']);

/** The symbol whose live chart a card opens, or null for a card about no one stock. */
export function chartSymbolOf(artifact: Record<string, unknown> | null | undefined): string | null {
  if (!artifact || !CHART_CARD_TYPES.has(artifact.type as string)) return null;
  // A symbol no chart tab would take falls back to the result, rather than
  // opening the panel on nothing.
  return typeof artifact.symbol === 'string' ? readTypedTicker(artifact.symbol) : null;
}

/**
 * What a click on a card does: a card about one stock opens that stock's live
 * chart, and every other card falls back to the raw tool result. Either way the
 * tool-call row beside the card stays the way to the result itself.
 */
export function openCardTarget(
  artifact: Record<string, unknown> | null | undefined,
  onOpenChart: ((spec: { symbol: string }) => void) | null | undefined,
  fallback: () => void,
): void {
  const symbol = chartSymbolOf(artifact);
  if (symbol && onOpenChart) onOpenChart({ symbol });
  else fallback();
}

/**
 * Whether a completed tool call has an artifact this build draws a card for.
 *
 * The artifact's own type is asked first, which is what lets a tool nobody can
 * enumerate register a card: a direct MCP tool is named
 * `mcp__<server>__<tool>`, one name per user per connection, so the name set
 * below could never hold it. The name set stays because it is also the gate a
 * tool passes before its artifact has arrived.
 */
export function isInlineArtifactReady(
  toolName: string | null | undefined,
  artifact: unknown,
): boolean {
  if (!artifact || typeof artifact !== 'object') return false;
  const type = (artifact as { type?: unknown }).type;
  if (typeof type === 'string' && INLINE_ARTIFACT_MAP[type]) return true;
  return INLINE_ARTIFACT_TOOLS.has(toolName || '');
}
