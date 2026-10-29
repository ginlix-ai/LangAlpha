import { AlertTriangle, ChevronRight, ShieldCheck } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router';
import { cn } from '@/lib/utils';
import { useTradingPermission } from '@/hooks/useTradingPermission';
import { TRADING_LEVEL_INFO, TRADING_PERMISSION_HREF } from '@/lib/tradingPermission';

/**
 * The user's trading permission, read where brokerages are connected, with the
 * way to change it. The setting itself lives in Preferences; this page is
 * where someone deciding what a connection may do goes looking for it, so the
 * level is said in full, with what it lets the agent do, rather than as a name.
 */
export function TradingPermissionBlock() {
  const { t } = useTranslation();
  const { data } = useTradingPermission();
  const info = data ? TRADING_LEVEL_INFO[data.level] : null;
  const warning = info?.warning ?? false;
  const Icon = warning ? AlertTriangle : ShieldCheck;
  return (
    <section className="detail-aside-block">
      <h3>{t('settings.tradingPermission.title')}</h3>
      {info && (
        <>
          <div className={cn('trading-level', warning && 'is-warning')}>
            <Icon aria-hidden="true" />
            <span>{t(info.labelKey)}</span>
          </div>
          <p className="accounts-aside-text">{t(info.descriptionKey)}</p>
        </>
      )}
      <Link
        to={TRADING_PERMISSION_HREF}
        // The visible words say where it changes; the label says what.
        aria-label={t('plugins.brokerages.manageTradingPermissionLabel')}
        className="trading-manage"
      >
        {t('plugins.brokerages.manageTradingPermission')}
        <ChevronRight className="h-3.5 w-3.5" aria-hidden />
      </Link>
    </section>
  );
}
