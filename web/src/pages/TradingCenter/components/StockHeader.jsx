import React from 'react';
import { Info } from 'lucide-react';
import './StockHeader.css';

const StockHeader = ({ symbol, stockInfo, realTimePrice, chartMeta, displayOverride, onToggleOverview }) => {
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
  const fiftyTwoWeekHigh = chartMeta?.fiftyTwoWeekHigh ?? stockInfo?.['52WeekHigh'] ?? null;
  const fiftyTwoWeekLow = chartMeta?.fiftyTwoWeekLow ?? stockInfo?.['52WeekLow'] ?? null;
  const averageVolume = stockInfo?.AverageVolume ?? null;
  const volume = stockInfo?.Volume ?? null;
  const dayRange = (high != null && low != null) ? (Number(high) - Number(low)) : null;
  const changePct = realTimePrice?.changePercent != null ? realTimePrice.changePercent : null;

  const displayName = displayOverride?.name ?? stockInfo?.Name ?? `${symbol} Corp`;
  const displayExchange = displayOverride?.exchange ?? stockInfo?.Exchange ?? 'NASDAQ';

  return (
    <div className="stock-header">
      <div className="stock-header-top">
        <div>
          <div className="stock-title">
            <span className="stock-symbol">{symbol}</span>
            <span className="stock-name">{displayName}</span>
            <span className="stock-exchange">{displayExchange}</span>
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
        <div className="metric-item view-all">
          <span className="view-all-link">View all</span>
        </div>
      </div>
    </div>
  );
};

export default React.memo(StockHeader);
