/**
 * A self-contained replica of the MarketView chart surface — the stock header
 * plus the candlestick / MA / volume / RSI chart — wired with its own
 * market-data websocket, REST data and annotation sync.
 *
 * Drop it anywhere outside the MarketView page (e.g. the chat transcript's
 * chart-annotation modal) to show the *same* chart the user sees on MarketView,
 * including the agent's drawn annotations. It mirrors the data wiring of
 * ``MarketView``'s desktop left panel; it's read-only (no chat capture, no
 * watchlist), but interval switching and the company-overview panel work in
 * place. Provide its own ``MarketDataWSProvider`` so it can live on any page.
 */

import React, { useCallback, useEffect, useState } from 'react';

import StockHeader from './StockHeader';
import MarketChart from './MarketChart';
import CompanyOverviewPanel from './CompanyOverviewPanel';
import { MarketDataWSProvider, useMarketDataWSContext } from '../contexts/MarketDataWSContext';
import { useStockData } from '../hooks/useStockData';
import { useChartAnnotationSync } from '../hooks/useChartAnnotationSync';

interface OverviewData {
  quote?: Record<string, unknown>;
  earningsSurprises?: unknown;
  [key: string]: unknown;
}

interface MarketChartSurfaceProps {
  symbol: string;
  /** Initial chart interval (e.g. '1day'); switchable in place via the toolbar. */
  timeframe?: string;
  /** Workspace whose agent-drawn annotations to show. */
  workspaceId?: string | null;
}

function MarketChartSurfaceInner({
  symbol,
  timeframe = '1day',
  workspaceId,
}: MarketChartSurfaceProps): React.ReactElement {
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
  const [chartMeta, setChartMeta] = useState<Record<string, unknown> | null>(null);
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
  }, []);

  const handleStockMeta = useCallback((meta: unknown) => {
    setChartMeta(meta as Record<string, unknown> | null);
  }, []);

  // Prefer the live WS price; fall back to REST (guard against a stale
  // cross-symbol value when switching tickers).
  const realTimePriceMatch = realTimePrice?.symbol === symbol ? realTimePrice : null;
  const displayPrice = wsPrices.get(symbol) || realTimePriceMatch;
  const quote = (overviewData as OverviewData | null)?.quote || null;

  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        height: '100%',
        minHeight: 0,
        background: 'var(--color-bg-card)',
        overflow: 'hidden',
      }}
    >
      <StockHeader
        symbol={symbol}
        stockInfo={stockInfo}
        realTimePrice={displayPrice}
        chartMeta={chartMeta}
        displayOverride={null}
        onToggleOverview={() => setShowOverview((v) => !v)}
        wsStatus={wsStatus}
        wsHasData={!!wsPrices.get(symbol)}
        wsDataLevel={wsDataLevel}
        ginlixDataEnabled={ginlixDataEnabled}
        quoteData={quote}
        marketStatus={marketStatus}
        snapshot={snapshotData}
      />
      <div style={{ position: 'relative', flex: 1, minHeight: 0, display: 'flex' }}>
        {showOverview && (
          <CompanyOverviewPanel
            symbol={symbol}
            visible={showOverview}
            onClose={() => setShowOverview(false)}
            data={overviewData as OverviewData | null}
            loading={overviewLoading}
          />
        )}
        <MarketChart
          symbol={symbol}
          interval={selectedInterval}
          workspaceId={workspaceId ?? null}
          onIntervalChange={handleIntervalChange}
          onStockMeta={handleStockMeta}
          quoteData={quote}
          earningsData={(overviewData as OverviewData | null)?.earningsSurprises || null}
          overlayData={overlayData as Record<string, unknown> | null}
          stockMeta={chartMeta}
          snapshot={snapshotData}
          liveTick={wsPrices.get(symbol)?.barData || null}
          wsStatus={wsStatus}
          marketStatus={marketStatus}
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
