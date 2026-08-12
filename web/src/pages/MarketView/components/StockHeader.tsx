import React, { useState, useEffect } from 'react';
import { Info, List, Sunrise, Sunset, ChevronDown } from 'lucide-react';
import './StockHeader.css';
import { isUSEquity, EXT_COLOR_PRE, EXT_COLOR_POST } from '../utils/chartConstants';
import { getExtendedHoursInfo } from '@/lib/marketUtils';
import { useIsMobile } from '@/hooks/useIsMobile';
import type { StockInfo, RealTimePrice, SnapshotData } from '@/types/market';
import type { PriceUpdate, ConnectionStatus, DataLevel } from '../hooks/useMarketDataWS';

interface ChartMeta {
  dateRange?: { from: string; to: string };
  dataPoints?: number;
  [key: string]: unknown;
}

interface QuoteData {
  previousClose?: number;
  open?: number;
  yearHigh?: number;
  yearLow?: number;
  avgVolume?: number;
  [key: string]: unknown;
}

interface DisplayOverride {
  name?: string;
  exchange?: string;
}

interface StockHeaderProps {
  symbol: string;
  stockInfo: StockInfo | null;
  realTimePrice: PriceUpdate | RealTimePrice | null;
  chartMeta: ChartMeta | null;
  displayOverride: DisplayOverride | null;
  onToggleOverview: () => void;
  onOpenWatchlist?: () => void;
  wsStatus: ConnectionStatus;
  wsHasData?: boolean;
  wsDataLevel?: DataLevel;
  ginlixDataEnabled?: boolean;
  quoteData: QuoteData | null;
  marketStatus: Record<string, unknown> | null;
  snapshot: SnapshotData | null;
  /** Venue market phase (`pre|open|post|closed`) from the chart's bars responses. */
  marketPhase?: string | null;
}

const EXCHANGE_LABELS: Record<string, string> = { HK: 'HK', SS: 'SH', SZ: 'SZ', L: 'LON', T: 'TYO', TO: 'TSX', AX: 'ASX' };
const PROVIDER_LABELS: Record<string, string> = { 'ginlix-data': 'Ginlix Data', fmp: 'FMP', yfinance: 'yfinance' };

function getVenueStatusLabel(sym: string | null | undefined, status: 'Delayed' | 'Closed'): string {
  if (!sym) return status;
  const dotIdx = sym.lastIndexOf('.');
  if (dotIdx === -1) return status;
  const suffix = sym.slice(dotIdx + 1).toUpperCase();
  return EXCHANGE_LABELS[suffix] ? `${EXCHANGE_LABELS[suffix]} ${status}` : status;
}

const StockHeader = ({ symbol, stockInfo, realTimePrice, chartMeta: _chartMeta, displayOverride, onToggleOverview, onOpenWatchlist, wsStatus, wsHasData = false, wsDataLevel = null, ginlixDataEnabled: _ginlixDataEnabled = true, quoteData, marketStatus, snapshot, marketPhase = null }: StockHeaderProps) => {
  const formatNumber = (num: number | null | undefined): string => {
    if (num == null || (num !== 0 && !num)) return '—';
    if (num >= 1e12) return (num / 1e12).toFixed(2) + 'T';
    if (num >= 1e9) return (num / 1e9).toFixed(2) + 'B';
    if (num >= 1e6) return (num / 1e6).toFixed(2) + 'M';
    if (num >= 1e3) return (num / 1e3).toFixed(2) + 'K';
    return Number(num).toFixed(2);
  };

  const price = realTimePrice?.price ?? stockInfo?.Price ?? null;
  const change = realTimePrice?.change ?? 0;
  const changePercent = realTimePrice?.changePercent ?? 0;
  const isPositive = change > 0;
  const isNegative = change < 0;
  const priceColorClass = isPositive ? 'positive' : isNegative ? 'negative' : '';

  const previousClose = snapshot?.previous_close ?? quoteData?.previousClose ?? null;
  const open = realTimePrice?.open ?? stockInfo?.Open ?? null;
  const high = realTimePrice?.high ?? stockInfo?.High ?? null;
  const low = realTimePrice?.low ?? stockInfo?.Low ?? null;
  const fiftyTwoWeekHigh = quoteData?.yearHigh ?? stockInfo?.['52WeekHigh'] ?? null;
  const fiftyTwoWeekLow = quoteData?.yearLow ?? stockInfo?.['52WeekLow'] ?? null;
  const averageVolume = quoteData?.avgVolume ?? stockInfo?.AverageVolume ?? null;
  const volume = stockInfo?.Volume ?? null;
  const hasDayRange = high != null && low != null;
  const changePct = realTimePrice?.changePercent != null ? realTimePrice.changePercent : null;

  const displayName = displayOverride?.name ?? stockInfo?.Name ?? `${symbol} Corp`;
  const displayExchange = displayOverride?.exchange ?? stockInfo?.Exchange ?? '';

  // Extended hours (market convention): the big number is the last official
  // close — today's regular close after-hours, the previous close pre-market —
  // with a coherent change pair against the previous close; the extended move
  // renders on its own labeled line against its declared anchor. A live tick
  // (has a timestamp; quote rows don't) overrides the derived ext price.
  const { extPct, extType, extPrice, extChange, extAnchor, regularClose } =
    getExtendedHoursInfo(marketStatus, snapshot);
  const tickPrice = (realTimePrice as PriceUpdate)?.timestamp != null ? (realTimePrice?.price ?? null) : null;
  const extDisplayPrice = tickPrice ?? extPrice;
  const extDisplayChange = tickPrice != null && extAnchor != null ? tickPrice - extAnchor : extChange;
  const extDisplayPct = tickPrice != null && extAnchor ? ((tickPrice - extAnchor) / extAnchor) * 100 : extPct;
  const settledClose = (extType === 'post' ? regularClose : previousClose) ?? null;
  const settledChange = extType === 'post' && regularClose != null && previousClose != null
    ? regularClose - previousClose
    : null;
  const settledChangePct = settledChange != null && previousClose ? (settledChange / previousClose) * 100 : null;
  const settledColorClass = settledChange == null ? '' : settledChange > 0 ? 'positive' : settledChange < 0 ? 'negative' : '';

  // Live = WS connected AND actually delivering aggregate data for this symbol
  const usSymbol = isUSEquity(symbol);
  const isLive = wsStatus === 'connected' && usSymbol && wsHasData;

  // The provider actually serving the displayed price: the WS feed when live
  // (ginlix-data is the only WS upstream), else whichever provider filled the
  // snapshot. Fall back to the enabled-provider list for rows without a source.
  const providers = (marketStatus?.providers ?? []) as string[];
  const activeSource = isLive ? 'ginlix-data' : (snapshot?.source ?? null);
  const dataSourceLabel = activeSource
    ? (PROVIDER_LABELS[activeSource] ?? activeSource)
    : (providers.map(p => PROVIDER_LABELS[p] ?? p).join(', ') || 'REST');
  const isMobile = useIsMobile();
  const [metricsCollapsed, setMetricsCollapsed] = useState(false);

  const [tickTime, setTickTime] = useState<Date | null>(null);
  useEffect(() => {
    if ((realTimePrice as PriceUpdate)?.timestamp) {
      setTickTime(new Date((realTimePrice as PriceUpdate).timestamp));
    }
  }, [(realTimePrice as PriceUpdate)?.timestamp]);

  const formatTickTime = (date: Date | null): string | null => {
    if (!date) return null;
    return date.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  };

  const watchlistBtn = onOpenWatchlist ? (
    <button className="stock-metrics-watchlist-pill" onClick={onOpenWatchlist}>
      <List size={13} />
      Watchlist
    </button>
  ) : null;

  return (
    <div className={`stock-header${isMobile && metricsCollapsed ? ' stock-header--compact' : ''}`}>
      <div className="stock-header-top">
        <div>
          <div className="stock-title">
            <span className="stock-symbol">{symbol}</span>
            <span className="stock-name">{displayName}</span>
            {displayExchange && <span className="stock-exchange">{displayExchange}</span>}
            <span className="stock-data-source stock-data-source--inline">
              {isLive ? (
                <>
                  <span className="data-source-dot data-source-dot--live" />
                  <span className="data-source-label">Live</span>
                  {tickTime && <span className="data-source-time">{formatTickTime(tickTime)}</span>}
                </>
              ) : marketPhase === 'closed' ? (
                <>
                  <span className="data-source-dot data-source-dot--closed" />
                  <span className="data-source-label">{getVenueStatusLabel(symbol, 'Closed')}</span>
                </>
              ) : (
                <>
                  <span className="data-source-dot data-source-dot--delayed" />
                  <span className="data-source-label">{getVenueStatusLabel(symbol, 'Delayed')}</span>
                </>
              )}
              <span className="data-source-tooltip">
                <span>Source: {dataSourceLabel}</span>
                <span>WebSocket: {wsStatus === 'connected' ? (wsHasData ? `Connected (${wsDataLevel === 'second' ? 'second' : 'minute'}-level)` : 'Connected (no data)') : wsStatus === 'disabled' ? 'Not available' : wsStatus === 'reconnecting' ? 'Reconnecting' : 'Disconnected'}</span>
              </span>
            </span>
          </div>
          <button className="stock-overview-toggle" onClick={onToggleOverview}>
            <Info size={13} />
            Company Overview
          </button>
        </div>
        <div className="stock-price-section">
          {extType && settledClose != null && extDisplayPrice != null && extDisplayPct != null ? (
            <>
              {/* Official close — prominent, stable across refreshes and intervals */}
              <div className={`stock-price ${settledColorClass}`}>{settledClose.toFixed(2)}</div>
              {settledChange != null && settledChangePct != null && (
                <div className={`stock-change ${settledColorClass}`}>
                  {settledChange >= 0 ? '+' : ''}{settledChange.toFixed(2)} {settledChange >= 0 ? '+' : ''}{settledChangePct.toFixed(2)}%
                </div>
              )}
              {/* The extended-hours move, against its own anchor, in session color */}
              <div
                className="stock-extended-hours"
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: 4,
                  fontSize: '0.8125rem',
                  color: extType === 'pre' ? EXT_COLOR_PRE : EXT_COLOR_POST,
                }}
              >
                {extType === 'pre' ? <Sunrise size={13} /> : <Sunset size={13} />}
                {extDisplayPrice.toFixed(2)}
                {extDisplayChange != null && (
                  <span>{extDisplayChange >= 0 ? '+' : ''}{extDisplayChange.toFixed(2)}</span>
                )}
                <span>({extDisplayPct >= 0 ? '+' : ''}{extDisplayPct.toFixed(2)}%)</span>
              </div>
            </>
          ) : (
            <>
              <div className={`stock-price ${priceColorClass}`}>{price != null ? price.toFixed(2) : '—'}</div>
              <div className={`stock-change ${priceColorClass}`}>
                {isPositive ? '+' : ''}{change.toFixed(2)} {isPositive ? '+' : ''}{changePercent.toFixed(2)}%
              </div>
            </>
          )}
        </div>
      </div>

      {isMobile && (
        <div className="stock-metrics-toggle-row">
          <button
            className="stock-metrics-toggle"
            onClick={() => setMetricsCollapsed(c => !c)}
            aria-expanded={!metricsCollapsed}
          >
            <span>{metricsCollapsed ? 'Show metrics' : 'Hide metrics'}</span>
            <ChevronDown size={14} className={`stock-metrics-toggle-icon${metricsCollapsed ? '' : ' stock-metrics-toggle-icon--open'}`} />
          </button>
          {watchlistBtn}
        </div>
      )}
      <div
        className={`stock-metrics-wrapper${isMobile && metricsCollapsed ? ' stock-metrics-wrapper--collapsed' : ''}`}
      >
        <div className="stock-metrics">
          <div className="metric-item">
            <span className="metric-label">Prev Close</span>
            <span className="metric-value">
              {previousClose != null ? Number(previousClose).toFixed(2) : '—'}
            </span>
          </div>
          <div className="metric-item">
            <span className="metric-label">Open
              <span className="metrics-discrepancy-hint" title="Values are aggregated from intraday data and may differ slightly from daily figures shown on the chart.">!</span>
            </span>
            <span className="metric-value">
              {open != null ? Number(open).toFixed(2) : '—'}
            </span>
          </div>
          <div className="metric-item">
            <span className="metric-label">Low</span>
            <span className="metric-value">
              {low != null ? Number(low).toFixed(2) : '—'}
            </span>
          </div>
          <div className="metric-item">
            <span className="metric-label">High</span>
            <span className="metric-value">
              {high != null ? Number(high).toFixed(2) : '—'}
            </span>
          </div>
          <div className="metric-item">
            <span className="metric-label">52 wk high</span>
            <span className="metric-value">
              {fiftyTwoWeekHigh != null ? Number(fiftyTwoWeekHigh).toFixed(2) : '—'}
            </span>
          </div>
          <div className="metric-item">
            <span className="metric-label">52 wk low</span>
            <span className="metric-value">
              {fiftyTwoWeekLow != null ? Number(fiftyTwoWeekLow).toFixed(2) : '—'}
            </span>
          </div>
          <div className="metric-item">
            <span className="metric-label">Avg Vol (3M)</span>
            <span className="metric-value">
              {averageVolume != null ? formatNumber(Number(averageVolume)) : '—'}
            </span>
          </div>
          <div className="metric-item">
            <span className="metric-label">Volume</span>
            <span className="metric-value">
              {volume != null ? formatNumber(Number(volume)) : (averageVolume != null ? formatNumber(Number(averageVolume)) : '—')}
            </span>
          </div>
          <div className="metric-item">
            <span className="metric-label">Day Range</span>
            <span className="metric-value">
              {hasDayRange ? `${Number(low).toFixed(2)} – ${Number(high).toFixed(2)}` : '—'}
            </span>
          </div>
          <div className="metric-item">
            <span className="metric-label">Change %</span>
            <span className={`metric-value ${(changePct || 0) >= 0 ? 'positive' : 'negative'}`}>
              {changePct != null ? (changePct >= 0 ? '+' : '') + changePct.toFixed(2) + '%' : '—'}
            </span>
          </div>
          {!isMobile && onOpenWatchlist && (
            <div className="metric-item metric-item--watchlist">
              {watchlistBtn}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default React.memo(StockHeader);
