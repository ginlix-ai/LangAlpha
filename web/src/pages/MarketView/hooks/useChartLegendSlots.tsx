/**
 * The chart's quote header as MarketChart's toolbar slots: the ticker legend
 * leading the toolbar row and the day's figures in the thin row beneath. The
 * MarketView page and the chat's chart panel both mount it, so the two read
 * as one component and differ only in how much each has room to show.
 */

import React, { useMemo } from 'react';
import { useLocale } from '@/hooks/useLocale';
import type { StockSearchHit } from '@/lib/marketUtils';
import { LegendLead, LegendStats, type LegendSource } from '../components/StockLegendStrip';
import { legendLeadDemand, legendLeadShapeKey } from '../components/legendLeadShape';
import type { StockQuoteModel } from './useStockQuoteModel';

export interface ChartLegendSlotsInputs {
  symbol: string;
  quote: StockQuoteModel;
  onSwitchSymbol?: (symbol: string, hit?: StockSearchHit) => void;
  source: LegendSource;
  /** `full` for a wide host: more figures in the subrow. The company name shows wherever there is room. */
  detail?: 'compact' | 'full';
}

export interface ChartLegendSlots {
  toolbarLead: React.ReactNode;
  toolbarLeadKey: string;
  /** What the lead asks of the toolbar row, for MarketChart's `measureToolbarLead`. */
  measureToolbarLead: (lead: HTMLElement) => number;
  toolbarSubrow: React.ReactNode;
}

export function useChartLegendSlots({
  symbol, quote, onSwitchSymbol, source, detail = 'compact',
}: ChartLegendSlotsInputs): ChartLegendSlots {
  const locale = useLocale();
  const { wsStatus, wsHasData, wsDataLevel, chartFreshness } = source;
  // Memoized so MarketChart's React.memo holds while the feed is idle: fresh
  // slot JSX each render would re-render the chart on every unrelated tick.
  const toolbarLead = useMemo(
    () => (
      <LegendLead
        symbol={symbol}
        quote={quote}
        onSwitchSymbol={onSwitchSymbol}
        showName
        source={{ wsStatus, wsHasData, wsDataLevel, chartFreshness }}
      />
    ),
    [symbol, quote, onSwitchSymbol, wsStatus, wsHasData, wsDataLevel, chartFreshness],
  );
  const toolbarSubrow = useMemo(() => <LegendStats quote={quote} detail={detail} />, [quote, detail]);
  // The lead re-renders on every tick; the toolbar only needs to re-measure
  // when its width can have moved.
  const toolbarLeadKey = legendLeadShapeKey(symbol, quote, locale);
  return { toolbarLead, toolbarLeadKey, measureToolbarLead: legendLeadDemand, toolbarSubrow };
}
