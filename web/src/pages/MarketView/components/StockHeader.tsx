import React, { useState, useEffect } from 'react';
import { Info, List, Sunrise, Sunset } from 'lucide-react';
import './StockHeader.css';
import { isUSEquity, EXT_COLOR_PRE, EXT_COLOR_POST } from '../utils/chartConstants';
import { getExtendedHoursInfo } from '@/lib/marketUtils';
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
}

const EXCHANGE_LABELS: Record<string, string> = { HK: 'HK', SS: 'SH', SZ: 'SZ', L: 'LON', T: 'TYO', TO: 'TSX', AX: 'ASX' };

function getDelayedLabel(sym: string | null | undefined): string {
  if (!sym) return 'Delayed';
  const dotIdx = sym.lastIndexOf('.');
  if (dotIdx === -1) return 'Delayed';
  const suffix = sym.slice(dotIdx + 1).toUpperCase();
  return EXCHANGE_LABELS[suffix] ? `${EXCHANGE_LABELS[suffix]} Delayed` : 'Delayed';
}

const StockHeader = ({ symbol, stockInfo, realTimePrice, chartMeta, displayOverride, onToggleOverview, onOpenWatchlist, wsStatus, wsHasData = false, wsDataLevel = null, ginlixDataEnabled = true, quoteData, marketStatus, snapshot }: StockHeaderProps) => {
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

  // Pre/post-market extended hours data
  const { extPct: extendedChangePercent, extType: extendedType } = getExtendedHoursInfo(marketStatus, snapshot);

  // Live = WS connected AND actually delivering aggregate data for this symbol
  const usSymbol = isUSEquity(symbol);
  const isLive = wsStatus === 'connected' && usSymbol && wsHasData;
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

  return (
    <div className="stock-header">
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
              ) : (
                <>
                  <span className="data-source-dot data-source-dot--delayed" />
                  <span className="data-source-label">{getDelayedLabel(symbol)}</span>
                </>
              )}
              <span className="data-source-tooltip">
                <span>Source: {ginlixDataEnabled ? 'Ginlix Data' : 'FMP'}</span>
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
          {extendedType && extendedChangePercent != null ? (
            <>
              {/* Extended hours price — prominent, in session color */}
              <div className="stock-price" style={{ color: extendedType === 'pre' ? EXT_COLOR_PRE : EXT_COLOR_POST }}>
                {price != null ? price.toFixed(2) : '—'}
              </div>
              <div
                className="stock-extended-hours"
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: 4,
                  fontSize: 13,
                  color: extendedType === 'pre' ? EXT_COLOR_PRE : EXT_COLOR_POST,
                }}
              >
                {extendedType === 'pre' ? <Sunrise size={13} /> : <Sunset size={13} />}
                {change >= 0 ? '+' : ''}{change.toFixed(2)} ({extendedChangePercent >= 0 ? '+' : ''}{extendedChangePercent.toFixed(2)}%)
              </div>
              {/* Regular session close — during after-hours show today's 4 PM close, during pre-market show prev close */}
              {extendedType === 'post' && previousClose != null && (snapshot?.regular_trading_change as number | undefined) != null ? (
                <div style={{ fontSize: 11, marginTop: 2, color: 'var(--color-text-tertiary, #8b8fa3)' }}>
                  Close {(previousClose + (snapshot!.regular_trading_change as number)).toFixed(2)}
                </div>
              ) : extendedType === 'pre' && previousClose != null ? (
                <div style={{ fontSize: 11, marginTop: 2, color: 'var(--color-text-tertiary, #8b8fa3)' }}>
                  Close {previousClose.toFixed(2)}
                </div>
              ) : null}
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
        {onOpenWatchlist && (
          <div className="metric-item metric-item--watchlist">
            <button
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-medium border transition-colors"
              style={{
                borderColor: 'var(--color-border-muted)',
                color: 'var(--color-text-secondary)',
                backgroundColor: 'var(--color-bg-card)',
              }}
              onClick={onOpenWatchlist}
            >
              <List size={13} />
              Watchlist
            </button>
          </div>
        )}
      </div>
    </div>
  );
};

export default React.memo(StockHeader);
