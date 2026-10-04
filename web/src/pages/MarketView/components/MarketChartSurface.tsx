/**
 * A self-contained replica of the MarketView chart surface — the stock header
 * plus the candlestick / MA / volume / RSI chart — wired with its own
 * market-data websocket, REST data and annotation sync.
 *
 * Drop it anywhere outside the MarketView page (e.g. the chat's chart tab) to
 * show the *same* chart the user sees on MarketView, including the agent's
 * drawn annotations. It mirrors the data wiring of ``MarketView``'s desktop
 * left panel; it's read-only (no chat capture, no watchlist), but interval
 * switching and the company-overview panel work in place. Provide its own ``MarketDataWSProvider`` so it can live on any page.
 */

import React, { useCallback, useEffect, useMemo, useState } from 'react';
import type { StockSearchHit } from '@/lib/marketUtils';
import type { Freshness } from '@/types/market';
import { displayOverrideFromHit, type SymbolDisplayOverride } from '@/lib/displayName';
import { displaySpelling } from '@/lib/bars/exchanges';

import { useLocale } from '@/hooks/useLocale';
import StockHeader from './StockHeader';
import { LegendLead, LegendStats } from './StockLegendStrip';
import { legendLeadShapeKey } from './legendLeadShape';
import MarketChart from './MarketChart';
import CompanyOverviewPanel from './CompanyOverviewPanel';
import { MarketDataWSProvider, useMarketDataWSContext } from '../contexts/MarketDataWSContext';
import { useStockData } from '../hooks/useStockData';
import { useChartAnnotationSync } from '../hooks/useChartAnnotationSync';
import { useStockQuoteModel } from '../hooks/useStockQuoteModel';

interface MarketChartSurfaceProps {
  symbol: string;
  /** Initial chart interval (e.g. '1day'); switchable in place via the toolbar. */
  timeframe?: string;
  /** Workspace whose agent-drawn annotations to show. */
  workspaceId?: string | null;
  /** Follow the toolbar's interval switch, so a host can remember where the chart was left. */
  onIntervalChange?: (interval: string) => void;
  /** Lets the ticker in the header be changed in place; the host decides what that means. */
  onSwitchSymbol?: (symbol: string, hit?: StockSearchHit) => void;
  /** A host's own buttons in the header, beside Company Overview. In the
   *  `compact` variant they land in the chart's toolbar row, so a host passes
   *  toolbar-sized icon buttons there (`ChartToolButton`). */
  headerActions?: React.ReactNode;
  /** Region and price-level selection, for a host whose composer sends them. */
  selectionTools?: boolean;
  /**
   * `full` reproduces the MarketView page (metrics grid, centered latest bar,
   * Light / Advanced switch). `compact` is for a host that keeps its height
   * for the chart: the ticker legend and the actions ride the chart's toolbar
   * row, the day's figures a thin row beneath, the host frames the chart as a
   * card, bars pack across the width, light chart only, and no company
   * overview, whose full-height sheet has no room beside a narrow chart.
   */
  variant?: 'full' | 'compact';
}

/** The compact chart sits as a card inset on the plain canvas ground; the
 *  host draws the card edge, this is the gutter around it. */
const COMPACT_PADDING = '8px 10px 10px';

function MarketChartSurfaceInner({
  symbol,
  timeframe = '1day',
  workspaceId,
  onIntervalChange,
  onSwitchSymbol,
  headerActions,
  selectionTools = false,
  variant = 'full',
}: MarketChartSurfaceProps): React.ReactElement {
  const compact = variant === 'compact';
  const locale = useLocale();
  const {
    prices: wsPrices,
    connectionStatus: wsStatus,
    dataLevel: wsDataLevel,
    ginlixDataEnabled,
    subscribe: wsSubscribe,
    unsubscribe: wsUnsubscribe,
    setPreviousClose,
    setDayOpen,
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
  const [showOverview, setShowOverview] = useState(false);

  const {
    stockInfo,
    realTimePrice,
    snapshotData,
    overviewData,
    overviewLoading,
    overlayData,
    marketStatus,
  } = useStockData({ selectedStock: symbol, wsStatus, setPreviousClose, setDayOpen });

  // Load this symbol's persisted annotations (all timeframes) into the store so
  // MarketChart renders them — exactly as the live MarketView page does.
  useChartAnnotationSync(workspaceId ?? null, symbol);

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

  // The header pick names the company before its quote lands, the way the
  // MarketView page does; it is kept only while the symbol it named is up.
  const [picked, setPicked] = useState<{ symbol: string; display: SymbolDisplayOverride } | null>(null);
  const handleSwitchSymbol = useCallback((next: string, hit?: StockSearchHit) => {
    setPicked(hit ? { symbol: displaySpelling(next), display: displayOverrideFromHit(hit) } : null);
    onSwitchSymbol?.(next, hit);
  }, [onSwitchSymbol]);

  // useStockData returns only the rows of the symbol on screen, so on an
  // in-place switch the header renders dashes until the new quote lands.
  // Prefer the live WS price; fall back to REST.
  const displayPrice = wsPrices.get(symbol) || realTimePrice;
  const displayOverride = picked && picked.symbol === symbol.trim().toUpperCase() ? picked.display : null;
  const quote = overviewData?.quote || null;
  const wsHasData = !!wsPrices.get(symbol);
  // The chart's own bars, measured apart from the quote: the header shows both.
  const [chartFreshness, setChartFreshness] = useState<Freshness | null>(null);

  // Derived once; the header or the two strips only print it.
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
  const toggleOverview = useCallback(() => setShowOverview((v) => !v), []);
  // Memoized so MarketChart's React.memo holds while the feed is idle: fresh
  // slot JSX each render would re-render the chart on every unrelated state tick.
  const toolbarLead = useMemo(
    () => (compact ? <LegendLead symbol={symbol} quote={q} onSwitchSymbol={handleSwitchSymbol} /> : undefined),
    [compact, symbol, q, handleSwitchSymbol],
  );
  // The lead re-renders on every tick; the toolbar only needs to re-measure
  // when its width can have moved.
  const toolbarLeadKey = compact ? legendLeadShapeKey(symbol, q, locale) : undefined;
  const toolbarTrail = compact ? headerActions : undefined;
  const toolbarSubrow = useMemo(() => (compact ? <LegendStats quote={q} /> : undefined), [compact, q]);

  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        height: '100%',
        minHeight: 0,
        overflow: 'hidden',
        background: compact ? 'var(--color-bg-canvas)' : 'var(--color-bg-card)',
      }}
    >
      {!compact && <StockHeader
        symbol={symbol}
        quote={q}
        chartMeta={chartMeta}
        onToggleOverview={toggleOverview}
        wsStatus={wsStatus}
        wsHasData={wsHasData}
        wsDataLevel={wsDataLevel}
        ginlixDataEnabled={ginlixDataEnabled}
        onSwitchSymbol={handleSwitchSymbol}
        headerActions={headerActions}
        chartFreshness={chartFreshness}
      />}
      <div style={{ position: 'relative', flex: 1, minHeight: 0, display: 'flex', padding: compact ? COMPACT_PADDING : 0 }}>
        {showOverview && (
          <CompanyOverviewPanel
            symbol={symbol}
            visible={showOverview}
            onClose={() => setShowOverview(false)}
            data={overviewData}
            loading={overviewLoading}
          />
        )}
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
          defaultView={compact ? 'fill' : 'centered'}
          modeSwitcher={!compact}
          selectionTools={selectionTools}
          toolbarLead={toolbarLead}
          toolbarLeadKey={toolbarLeadKey}
          toolbarTrail={toolbarTrail}
          toolbarSubrow={toolbarSubrow}
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
