import React, { useState, useEffect } from 'react';
import { Info } from 'lucide-react';
import './StockHeader.css';

const EXCHANGE_LABELS = { HK: 'HK', SS: 'SH', SZ: 'SZ', L: 'LON', T: 'TYO', TO: 'TSX', AX: 'ASX' };
const FOREIGN_EXCHANGES = new Set(['HK', 'SS', 'SZ', 'L', 'T', 'TO', 'AX', 'DE', 'PA', 'MC']);

function isUSSymbol(sym) {
  if (!sym) return true;
  const dotIdx = sym.lastIndexOf('.');
  if (dotIdx === -1) return true;
  const suffix = sym.slice(dotIdx + 1).toUpperCase();
  return !FOREIGN_EXCHANGES.has(suffix);
}

function getDelayedLabel(sym) {
  if (!sym) return 'Delayed';
  const dotIdx = sym.lastIndexOf('.');
  if (dotIdx === -1) return 'Delayed';
  const suffix = sym.slice(dotIdx + 1).toUpperCase();
  return EXCHANGE_LABELS[suffix] ? `${EXCHANGE_LABELS[suffix]} Delayed` : 'Delayed';
}

const StockHeader = ({ symbol, stockInfo, realTimePrice, chartMeta, displayOverride, onToggleOverview, wsStatus, quoteData }) => {
  const formatNumber = (num) => {
    if (num == null || (num !== 0 && !num)) return '—';
    if (num >= 1e12) return (num / 1e12).toFixed(2) + 'T';
    if (num >= 1e9) return (num / 1e9).toFixed(2) + 'B';
    if (num >= 1e6) return (num / 1e6).toFixed(2) + 'M';
    if (num >= 1e3) return (num / 1e3).toFixed(2) + 'K';
    return Number(num).toFixed(2);
  };

  const price = realTimePrice?.price ?? stockInfo?.Price ?? 0;
  const change = realTimePrice?.change ?? 0;
  const changePercent = realTimePrice?.changePercent ?? '0.00%';
  const isPositive = change >= 0;

  const open = realTimePrice?.open ?? stockInfo?.Open ?? null;
  const high = realTimePrice?.high ?? stockInfo?.High ?? null;
  const low = realTimePrice?.low ?? stockInfo?.Low ?? null;
  const fiftyTwoWeekHigh = quoteData?.yearHigh ?? stockInfo?.['52WeekHigh'] ?? null;
  const fiftyTwoWeekLow = quoteData?.yearLow ?? stockInfo?.['52WeekLow'] ?? null;
  const averageVolume = quoteData?.avgVolume ?? stockInfo?.AverageVolume ?? null;
  const volume = stockInfo?.Volume ?? null;
  const dayRange = (high != null && low != null) ? (Number(high) - Number(low)) : null;
  const changePct = realTimePrice?.changePercent != null ? realTimePrice.changePercent : null;

  const displayName = displayOverride?.name ?? stockInfo?.Name ?? `${symbol} Corp`;
  const displayExchange = displayOverride?.exchange ?? stockInfo?.Exchange ?? '';

  // Live timestamp — updates every second when WS is connected
  const usSymbol = isUSSymbol(symbol);
  const isLive = wsStatus === 'connected' && usSymbol;
  const [tickTime, setTickTime] = useState(null);
  useEffect(() => {
    if (realTimePrice?.timestamp) {
      setTickTime(new Date(realTimePrice.timestamp));
    }
  }, [realTimePrice?.timestamp]);

  const formatTickTime = (date) => {
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
            </span>
          </div>
          <button className="stock-overview-toggle" onClick={onToggleOverview}>
            <Info size={13} />
            Company Overview
          </button>
        </div>
        <div className="stock-price-section">
          <div className="stock-price">{price.toFixed(2)}</div>
          <div className={`stock-change ${isPositive ? 'positive' : 'negative'}`}>
            {isPositive ? '+' : ''}{change.toFixed(2)} {isPositive ? '+' : ''}{changePercent}
          </div>
        </div>
      </div>

      <div className="stock-metrics">
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
            {dayRange != null ? Number(dayRange).toFixed(2) : '—'}
          </span>
        </div>
        <div className="metric-item">
          <span className="metric-label">Change %</span>
          <span className={`metric-value ${(parseFloat(changePct) || 0) >= 0 ? 'positive' : 'negative'}`}>
            {changePct != null && changePct !== '' ? (parseFloat(changePct) >= 0 ? '+' : '') + changePct : '—'}
          </span>
        </div>
      </div>
    </div>
  );
};

export default React.memo(StockHeader);
