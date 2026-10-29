import type React from 'react';
import { MessageDeliveryPanel } from '../messaging/MessageDeliveryDetail';
import { readMessageDelivery } from '../messaging/messageDelivery';
import AutomationDetailPanel from './AutomationDetailPanel';
import { unwrapMarketOverview, type PanelProps } from './cardReaders';
import { CompanyOverviewCard } from './CompanyOverviewDetail';
import { QuotePanel } from './marketLinks';
import { MarketOverviewPanel } from './MarketOverviewPanel';
import { MarketIndicesChart, SectorPerformanceChart } from './MarketOverviewParts';
import { MarketMoversPanel, OptionsChainPanel } from './OptionsMoversPanels';
import { StockScreenerTable } from './ScreenerPanel';
import SecFilingViewer from './SecFilingViewer';
import { StockPriceChart } from './StockPricePanel';

interface PanelEntry {
  Panel: React.ComponentType<PanelProps>;
  /** False for an artifact with nothing to draw, which the tool's text then says. */
  accepts?: (data: Record<string, unknown>, toolArgs?: Record<string, unknown>) => boolean;
}

const hasRows = (key: string) => (data: Record<string, unknown>): boolean => {
  const rows = data[key];
  return Array.isArray(rows) && rows.length > 0;
};

/**
 * Maps an artifact `type` to the detail panel that draws it, as
 * `INLINE_ARTIFACT_MAP` does its card. A module of its own, since the panels
 * draw charts and the cards are on the chat's first paint. The host frames
 * every panel in one `TermPanel`.
 */
const PANEL_ARTIFACT_MAP: Record<string, PanelEntry> = {
  stock_prices: { Panel: StockPriceChart },
  company_overview: { Panel: CompanyOverviewCard },
  market_indices: { Panel: MarketIndicesChart },
  sector_performance: { Panel: SectorPerformanceChart },
  // An error carries only its region.
  market_overview: {
    Panel: MarketOverviewPanel,
    accepts: (data) => {
      const { hasIndices, hasSectors } = unwrapMarketOverview(data);
      return hasIndices || hasSectors;
    },
  },
  // An empty answer ("No quote data available") reads better as its text.
  quote: { Panel: QuotePanel, accepts: hasRows('quotes') },
  stock_screener: { Panel: StockScreenerTable },
  // An error or an empty result has no rows.
  options_chain: { Panel: OptionsChainPanel, accepts: hasRows('results') },
  market_movers: { Panel: MarketMoversPanel, accepts: hasRows('results') },
  sec_filing: { Panel: SecFilingViewer },
  automations: { Panel: AutomationDetailPanel },
  message_delivery: { Panel: MessageDeliveryPanel, accepts: (data, toolArgs) => readMessageDelivery(data, toolArgs) != null },
};

/** The panel an artifact opens, or null for one the tool's text should say instead. */
export function artifactPanel(
  artifact: Record<string, unknown> | undefined,
  toolArgs?: Record<string, unknown>,
): React.ComponentType<PanelProps> | null {
  const type = artifact?.type;
  if (!artifact || typeof type !== 'string' || !Object.hasOwn(PANEL_ARTIFACT_MAP, type)) return null;
  const { Panel, accepts } = PANEL_ARTIFACT_MAP[type];
  return !accepts || accepts(artifact, toolArgs) ? Panel : null;
}
