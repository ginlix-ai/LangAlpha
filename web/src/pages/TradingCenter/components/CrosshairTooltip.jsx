import React from 'react';
import './CrosshairTooltip.css';

function CrosshairTooltip({ visible, x, y, data, enabledMaPeriods, containerWidth, containerHeight }) {
  if (!visible || !data) return null;

  const isUp = data.close >= data.open;
  const dirColor = isUp ? '#10b981' : '#ef4444';

  // Clamp position to stay within container
  const tooltipWidth = 200;
  const tooltipHeight = 180;
  const clampedX = Math.min(Math.max(x + 16, 0), (containerWidth || 800) - tooltipWidth - 8);
  const clampedY = Math.min(Math.max(y - 10, 0), (containerHeight || 500) - tooltipHeight - 8);

  const formatPrice = (v) => v != null ? v.toFixed(2) : '\u2014';
  const formatVol = (v) => {
    if (v == null) return '\u2014';
    if (v >= 1e6) return `${(v / 1e6).toFixed(1)}M`;
    if (v >= 1e3) return `${(v / 1e3).toFixed(1)}K`;
    return String(v);
  };

  const formatDate = (ts) => {
    if (!ts) return '';
    const d = new Date(ts * 1000);
    return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
  };

  return (
    <div
      className="crosshair-tooltip"
      style={{ left: clampedX, top: clampedY }}
    >
      <div className="crosshair-tooltip-date">{formatDate(data.time)}</div>
      <div className="crosshair-tooltip-grid">
        <span className="crosshair-tooltip-label">O</span>
        <span style={{ color: dirColor }}>{formatPrice(data.open)}</span>
        <span className="crosshair-tooltip-label">H</span>
        <span style={{ color: dirColor }}>{formatPrice(data.high)}</span>
        <span className="crosshair-tooltip-label">L</span>
        <span style={{ color: dirColor }}>{formatPrice(data.low)}</span>
        <span className="crosshair-tooltip-label">C</span>
        <span style={{ color: dirColor }}>{formatPrice(data.close)}</span>
      </div>
      <div className="crosshair-tooltip-row">
        <span className="crosshair-tooltip-label">Vol</span>
        <span>{formatVol(data.volume)}</span>
      </div>
      {data.maValues && Object.keys(data.maValues).length > 0 && (
        <div className="crosshair-tooltip-ma-section">
          {Object.entries(data.maValues).map(([period, val]) => (
            <div className="crosshair-tooltip-row" key={period}>
              <span className="crosshair-tooltip-label">MA{period}</span>
              <span>{val != null ? val.toFixed(2) : '\u2014'}</span>
            </div>
          ))}
        </div>
      )}
      {data.rsiValue != null && (
        <div className="crosshair-tooltip-row">
          <span className="crosshair-tooltip-label">RSI</span>
          <span>{data.rsiValue.toFixed(0)}</span>
        </div>
      )}
    </div>
  );
}

export default React.memo(CrosshairTooltip);
