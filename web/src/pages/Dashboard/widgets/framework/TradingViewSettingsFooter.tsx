import { useTranslation } from 'react-i18next';
import { HelpCircle } from 'lucide-react';

/**
 * Shared attribution block at the bottom of every TradingView widget's
 * settings dialog. One edit changes all 10.
 */
export function TradingViewSettingsFooter() {
  const { t } = useTranslation();
  return (
    <div
      className="mt-4 pt-3 flex items-center gap-2 text-[0.6875rem]"
      style={{
        borderTop: '1px solid var(--color-border-muted)',
        color: 'var(--color-text-tertiary)',
      }}
    >
      <span>
        {t('dashboard.widgets.tvFooter.providedBy')}
        <a
          className="tv-attribution"
          href="https://www.tradingview.com/"
          target="_blank"
          rel="noopener noreferrer"
          style={{ display: 'inline', padding: 0 }}
        >
          {t('dashboard.widgets.tvFooter.tradingView')}
        </a>
        .
      </span>
      {/* Same reason as the sign-in page's terms line: attribution is a document
          about the product, so it opens in a browser rather than replacing the
          dashboard, and the desktop shell sends it to the system browser. */}
      <a
        href="/legal"
        target="_blank"
        rel="noopener noreferrer"
        title={t('dashboard.widgets.tvFooter.legal')}
        style={{ color: 'var(--color-text-tertiary)', display: 'inline-flex' }}
      >
        <HelpCircle size={12} />
      </a>
    </div>
  );
}
