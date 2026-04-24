import './TradingViewAttribution.css';

/**
 * Per-widget attribution caption rendered inside every TradingView card.
 *
 * Required by TradingView's embed terms — must remain visible. The
 * `tv-attribution` class allow-lists this link past the
 * `WidgetFrame.css` selector that hides the lightweight-charts watermark.
 */
export function TradingViewAttribution() {
  return (
    <a
      className="tv-attribution"
      href="https://www.tradingview.com/"
      target="_blank"
      rel="noopener noreferrer"
    >
      Powered by TradingView ↗
    </a>
  );
}
