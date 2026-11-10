/**
 * A self-contained replica of the MarketView chart — the candlestick / MA /
 * volume / RSI chart with the ticker legend in its toolbar — wired with its
 * own market-data websocket and REST data (the chart reads its layers itself).
 *
 * Drop it anywhere outside the MarketView page (e.g. the chat's chart tab) to
 * show the *same* chart the user sees on MarketView, including the agent's
 * chart layers. It mirrors the data wiring of ``MarketView``'s desktop
 * left panel; it's read-only (no chat capture, no watchlist), but interval
 * switching works in place. Provides its own ``MarketDataWSProvider`` so it
 * can live on any page.
 *
 * It is sized for a host that keeps its height for the chart: the ticker
 * legend and the host's actions ride the chart's toolbar row, the day's
 * figures a thin row beneath, the host frames the chart as a card, bars pack
 * across the width, light chart only, and no company overview, whose
 * full-height sheet has no room beside a narrow chart.
 */

import React, { useCallback, useEffect, useState } from 'react';
import type { StockSearchHit } from '@/lib/marketUtils';
import type { Freshness } from '@/types/market';
import { displayOverrideFromHit, type SymbolDisplayOverride } from '@/lib/displayName';
import { displaySpelling } from '@/lib/bars/exchanges';

import MarketChart from './MarketChart';
import { MarketDataWSProvider, useMarketDataWSContext } from '../contexts/MarketDataWSContext';
import { useStockData } from '../hooks/useStockData';
import { useStockQuoteModel } from '../hooks/useStockQuoteModel';
import { useChartLegendSlots } from '../hooks/useChartLegendSlots';

interface MarketChartSurfaceProps {
  symbol: string;
  /** Initial chart interval (e.g. '1day'); switchable in place via the toolbar. */
  timeframe?: string;
  /** Workspace whose agent chart layers to show. */
  workspaceId?: string | null;
  /** Follow the toolbar's interval switch, so a host can remember where the chart was left. */
  onIntervalChange?: (interval: string) => void;
  /** Lets the legend's ticker be changed in place; the host decides what that means. */
  onSwitchSymbol?: (symbol: string, hit?: StockSearchHit) => void;
  /** A host's own buttons. They land in the chart's toolbar row, so a host
   *  passes toolbar-sized icon buttons there (`ChartToolButton`). */
  headerActions?: React.ReactNode;
  /** Region and price-level selection, for a host whose composer sends them. */
  selectionTools?: boolean;
}

/** The chart sits as a card inset on the panel's own page ground, not a
 *  gray well, so the panel reads as one surface; the host draws the card
 *  edge, this is the gutter around it. */
const PADDING = '8px 10px 10px';

function MarketChartSurfaceInner({
  symbol,
  timeframe = '1day',
  workspaceId,
  onIntervalChange,
  onSwitchSymbol,
  headerActions,
  selectionTools = false,
}: MarketChartSurfaceProps): React.ReactElement {
  const {
    prices: wsPrices,
    connectionStatus: wsStatus,
    dataLevel: wsDataLevel,
    subscribe: wsSubscribe,
    unsubscribe: wsUnsubscribe,
  } = useMarketDataWSContext();

  const [selectedInterval, setSelectedInterval] = useState<string>(timeframe);
  // The tab owns the timeframe: a card opened at 1min onto a tab showing 1day
  // moves the chart. User changes flow back through `onIntervalChange`, and the
  // guard keeps a chart that downgraded a prop it cannot show from looping.
  useEffect(() => {
    setSelectedInterval((cur) => (cur === timeframe ? cur : timeframe));
  }, [timeframe]);
  const [chartMeta, setChartMeta] = useState<Record<string, unknown> | null>(null);
  // Venue phase from the chart's bars responses, the way the MarketView page
  // reads it; the chart resets it to null on a symbol switch.
  const [marketPhase, setMarketPhase] = useState<string | null>(null);
  const wsHasData = wsPrices.has(symbol);
  const streamsLive = wsStatus === 'connected' && wsHasData;

  const {
    stockInfo,
    realTimePrice,
    snapshotData,
    overviewData,
    overlayData,
    marketStatus,
  } = useStockData({ selectedStock: symbol, streamsLive });

  // Subscribe to the live feed for this symbol.
  useEffect(() => {
    if (!symbol) return;
    wsSubscribe([symbol]);
    return () => wsUnsubscribe([symbol]);
  }, [symbol, wsSubscribe, wsUnsubscribe]);

  const handleIntervalChange = useCallback((interval: string) => {
    setSelectedInterval(interval);
    onIntervalChange?.(interval);
  }, [onIntervalChange]);

  const handleStockMeta = useCallback((meta: unknown) => {
    setChartMeta(meta as Record<string, unknown> | null);
  }, []);

  // The legend's pick names the company before its quote lands, the way the
  // MarketView page does; it is kept only while the symbol it named is up.
  const [picked, setPicked] = useState<{ symbol: string; display: SymbolDisplayOverride } | null>(null);
  const handleSwitchSymbol = useCallback((next: string, hit?: StockSearchHit) => {
    setPicked(hit ? { symbol: displaySpelling(next), display: displayOverrideFromHit(hit) } : null);
    onSwitchSymbol?.(next, hit);
  }, [onSwitchSymbol]);

  // useStockData returns only the rows of the symbol on screen, so on an
  // in-place switch the legend renders dashes until the new quote lands.
  // Prefer the live WS price; fall back to REST.
  const displayPrice = wsPrices.get(symbol) || realTimePrice;
  const displayOverride = picked && picked.symbol === symbol.trim().toUpperCase() ? picked.display : null;
  const quote = overviewData?.quote || null;
  // The chart's own bars, measured apart from the quote: the legend shows both.
  const [chartFreshness, setChartFreshness] = useState<Freshness | null>(null);

  // Derived once; the two strips only print it.
  const q = useStockQuoteModel({
    symbol,
    stockInfo,
    realTimePrice: displayPrice,
    quoteData: quote,
    snapshot: snapshotData,
    marketStatus,
    wsStatus,
    wsHasData,
    marketPhase,
    displayOverride,
  });
  // The same toolbar slots the MarketView page mounts.
  const legend = useChartLegendSlots({
    symbol,
    quote: q,
    onSwitchSymbol: handleSwitchSymbol,
    source: { wsStatus, wsHasData, wsDataLevel, chartFreshness },
  });

  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        height: '100%',
        minHeight: 0,
        overflow: 'hidden',
        background: 'var(--color-bg-page)',
      }}
    >
      <div style={{ position: 'relative', flex: 1, minHeight: 0, display: 'flex', padding: PADDING }}>
        <MarketChart
          symbol={symbol}
          interval={selectedInterval}
          workspaceId={workspaceId ?? null}
          onIntervalChange={handleIntervalChange}
          onStockMeta={handleStockMeta}
          onMarketPhase={setMarketPhase}
          onChartFreshness={setChartFreshness}
          quoteData={quote}
          earningsData={overviewData?.earningsSurprises || null}
          overlayData={overlayData as Record<string, unknown> | null}
          stockMeta={chartMeta}
          snapshot={snapshotData}
          liveTick={wsPrices.get(symbol)?.barData || null}
          wsStatus={wsStatus}
          marketStatus={marketStatus}
          defaultView="fill"
          modeSwitcher={false}
          selectionTools={selectionTools}
          toolbarLead={legend.toolbarLead}
          toolbarLeadKey={legend.toolbarLeadKey}
          measureToolbarLead={legend.measureToolbarLead}
          toolbarTrail={headerActions}
          toolbarSubrow={legend.toolbarSubrow}
        />
      </div>
    </div>
  );
}

export function MarketChartSurface(props: MarketChartSurfaceProps): React.ReactElement {
  return (
    <MarketDataWSProvider>
      <MarketChartSurfaceInner {...props} />
    </MarketDataWSProvider>
  );
}

export default MarketChartSurface;
