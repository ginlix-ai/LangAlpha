import React from 'react';
import './StockHeader.css';

const StockHeader = ({ symbol, stockInfo, realTimePrice }) => {
  const formatNumber = (num) => {
    if (!num && num !== 0) return 'N/A';
    if (num >= 1e12) return (num / 1e12).toFixed(2) + 'T';
    if (num >= 1e9) return (num / 1e9).toFixed(2) + 'B';
    if (num >= 1e6) return (num / 1e6).toFixed(2) + 'M';
    if (num >= 1e3) return (num / 1e3).toFixed(2) + 'K';
    return num.toFixed(2);
  };

  const price = realTimePrice?.price || stockInfo?.Price || 0;
  const change = realTimePrice?.change || 0;
  const changePercent = realTimePrice?.changePercent || '0.00%';
  const isPositive = change >= 0;

  const open = realTimePrice?.open || stockInfo?.Open || null;
  const high = realTimePrice?.high || stockInfo?.High || null;
  const low = realTimePrice?.low || stockInfo?.Low || null;

  return (
    <div className="stock-header">
      <div className="stock-header-top">
        <div className="stock-title">
          <span className="stock-symbol">{symbol}</span>
          <span className="stock-name">{stockInfo?.Name || `${symbol} Corp`}</span>
          <span className="stock-exchange">{stockInfo?.Exchange || 'NASDAQ'}</span>
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
          <span className="metric-label">Open</span>
          <span className="metric-value">
            {open !== null && open !== undefined ? open.toFixed(2) : 'N/A'}
          </span>
        </div>
        <div className="metric-item">
          <span className="metric-label">Low</span>
          <span className="metric-value">
            {low !== null && low !== undefined ? low.toFixed(2) : 'N/A'}
          </span>
        </div>
        <div className="metric-item">
          <span className="metric-label">High</span>
          <span className="metric-value">
            {high !== null && high !== undefined ? high.toFixed(2) : 'N/A'}
          </span>
        </div>
        <div className="metric-item">
          <span className="metric-label">52 wk high</span>
          <span className="metric-value">
            {stockInfo?.['52WeekHigh'] ? parseFloat(stockInfo['52WeekHigh']).toFixed(2) : 'N/A'}
          </span>
        </div>
        <div className="metric-item">
          <span className="metric-label">52 wk low</span>
          <span className="metric-value">
            {stockInfo?.['52WeekLow'] ? parseFloat(stockInfo['52WeekLow']).toFixed(2) : 'N/A'}
          </span>
        </div>
        <div className="metric-item">
          <span className="metric-label">Avg Vol (3M)</span>
          <span className="metric-value">
            {stockInfo?.AverageVolume ? formatNumber(parseFloat(stockInfo.AverageVolume)) : 'N/A'}
          </span>
        </div>
        <div className="metric-item">
          <span className="metric-label">Shares Outstanding</span>
          <span className="metric-value">
            {stockInfo?.SharesOutstanding ? formatNumber(parseFloat(stockInfo.SharesOutstanding)) : 'N/A'}
          </span>
        </div>
        <div className="metric-item">
          <span className="metric-label">Mkt Cap</span>
          <span className="metric-value">
            {stockInfo?.MarketCapitalization ? formatNumber(parseFloat(stockInfo.MarketCapitalization)) : 'N/A'}
          </span>
        </div>
        <div className="metric-item">
          <span className="metric-label">Div Yield</span>
          <span className="metric-value">
            {stockInfo?.DividendYield ? (parseFloat(stockInfo.DividendYield) * 100).toFixed(2) + '%' : 'N/A'}
          </span>
        </div>
        <div className="metric-item view-all">
          <span className="view-all-link">View all</span>
        </div>
      </div>
    </div>
  );
};

export default StockHeader;
