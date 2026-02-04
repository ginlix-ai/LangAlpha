import React, { useState } from 'react';
import './TradingPanel.css';

const TradingPanel = ({ symbol, realTimePrice }) => {
  const [orderType, setOrderType] = useState('Market Price');
  const [quantity, setQuantity] = useState(100);
  const [timeInForce, setTimeInForce] = useState('Day');
  const [stopPriceEnabled, setStopPriceEnabled] = useState(true);
  const [stopPrice, setStopPrice] = useState(400.00);
  const [isBuy, setIsBuy] = useState(true);

  const currentPrice = realTimePrice?.price || 0;
  const estimatedTotal = (currentPrice * quantity).toFixed(2);
  const estimatedLoss = stopPriceEnabled
    ? ((currentPrice - stopPrice) * quantity).toFixed(2)
    : '0.00';

  const quickQuantities = [10, 50, 100, 500];

  return (
    <div className="trading-panel">
      <div className="trading-panel-header">
        <h2>Trade</h2>
        <button type="button" className="panel-menu-btn">☰</button>
      </div>

      <div className="trading-tabs">
        <button
          type="button"
          className={`trading-tab ${isBuy ? 'active' : ''}`}
          onClick={() => setIsBuy(true)}
        >
          Buy
        </button>
        <button
          type="button"
          className={`trading-tab ${!isBuy ? 'active' : ''}`}
          onClick={() => setIsBuy(false)}
        >
          Sell
        </button>
      </div>

      <div className="trading-form">
        <div className="form-group">
          <label>Order Type</label>
          <select
            value={orderType}
            onChange={(e) => setOrderType(e.target.value)}
            className="form-select"
          >
            <option>Market Price</option>
            <option>Limit Price</option>
            <option>Stop Loss</option>
          </select>
        </div>

        <div className="form-group">
          <label>Quantity (Shares)</label>
          <div className="quantity-input-wrapper">
            <input
              type="number"
              value={quantity}
              onChange={(e) => setQuantity(parseInt(e.target.value, 10) || 0)}
              className="form-input"
            />
            <div className="quantity-arrows">
              <button type="button" onClick={() => setQuantity(q => q + 1)}>▲</button>
              <button type="button" onClick={() => setQuantity(q => Math.max(0, q - 1))}>▼</button>
            </div>
          </div>
          <div className="quick-quantities">
            {quickQuantities.map(qty => (
              <button
                key={qty}
                type="button"
                className={`quick-qty-btn ${quantity === qty ? 'active' : ''}`}
                onClick={() => setQuantity(qty)}
              >
                {qty}
              </button>
            ))}
          </div>
        </div>

        <div className="form-group">
          <label>Time-in-Force</label>
          <select
            value={timeInForce}
            onChange={(e) => setTimeInForce(e.target.value)}
            className="form-select"
          >
            <option>Day</option>
            <option>GTC</option>
            <option>IOC</option>
            <option>FOK</option>
          </select>
        </div>

        <div className="form-group">
          <div className="stop-price-header">
            <label>Stop Price</label>
            <label className="toggle-switch">
              <input
                type="checkbox"
                checked={stopPriceEnabled}
                onChange={(e) => setStopPriceEnabled(e.target.checked)}
              />
              <span className="slider"></span>
            </label>
          </div>
          {stopPriceEnabled && (
            <>
              <div className="stop-price-input-wrapper">
                <span className="currency-symbol">$</span>
                <input
                  type="number"
                  value={stopPrice}
                  onChange={(e) => setStopPrice(parseFloat(e.target.value) || 0)}
                  className="form-input"
                  step="0.01"
                />
                <div className="quantity-arrows">
                  <button type="button" onClick={() => setStopPrice(p => p + 0.01)}>▲</button>
                  <button type="button" onClick={() => setStopPrice(p => Math.max(0, p - 0.01))}>▼</button>
                </div>
              </div>
              <div className="estimated-loss">
                Est. Loss: <span className="loss-amount">${estimatedLoss}</span>
              </div>
            </>
          )}
        </div>

        <div className="financial-summary">
          <div className="summary-item">
            <span>Buying Power</span>
            <span>$122,912.50</span>
          </div>
          <div className="summary-item">
            <span>Transaction Fees</span>
            <span>$4.00</span>
          </div>
          <div className="summary-item total">
            <span>Estimated Total</span>
            <span>${estimatedTotal}</span>
          </div>
        </div>

        <button type="button" className={`trade-button ${isBuy ? 'buy' : 'sell'}`}>
          {isBuy ? 'Buy' : 'Sell'} {symbol}
        </button>

        <button type="button" className="disclaimer-link">Disclaimer &gt;</button>
      </div>

      <div className="time-sales">
        <div className="time-sales-header">
          <h3>Time &amp; Sales</h3>
          <button type="button" className="panel-menu-btn">☰</button>
        </div>
        <div className="time-sales-content">
          <div className="time-sales-item">
            <span className="time">16:59:32</span>
            <span className="price">420.56</span>
            <span className="volume">25</span>
          </div>
          <div className="time-sales-item">
            <span className="time">16:59:30</span>
            <span className="price">420.55</span>
            <span className="volume">100</span>
          </div>
          <div className="time-sales-item">
            <span className="time">16:59:28</span>
            <span className="price">420.60</span>
            <span className="volume">50</span>
          </div>
        </div>
      </div>
    </div>
  );
};

export default TradingPanel;
