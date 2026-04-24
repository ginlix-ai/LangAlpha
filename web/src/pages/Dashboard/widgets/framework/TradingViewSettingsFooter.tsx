import { HelpCircle } from 'lucide-react';
import { Link } from 'react-router-dom';

/**
 * Shared attribution block at the bottom of every TradingView widget's
 * settings dialog. One edit changes all 10.
 */
export function TradingViewSettingsFooter() {
  return (
    <div
      className="mt-4 pt-3 flex items-center gap-2 text-[11px]"
      style={{
        borderTop: '1px solid var(--color-border-muted)',
        color: 'var(--color-text-tertiary)',
      }}
    >
      <span>
        Provided by{' '}
        <a
          className="tv-attribution"
          href="https://www.tradingview.com/"
          target="_blank"
          rel="noopener noreferrer"
          style={{ display: 'inline', padding: 0 }}
        >
          TradingView
        </a>
        .
      </span>
      <Link
        to="/legal"
        title="Legal & attributions"
        style={{ color: 'var(--color-text-tertiary)', display: 'inline-flex' }}
      >
        <HelpCircle size={12} />
      </Link>
    </div>
  );
}
