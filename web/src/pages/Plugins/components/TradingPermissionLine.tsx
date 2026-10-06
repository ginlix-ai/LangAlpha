import { AlertTriangle, ShieldCheck } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router';
import { useTradingPermission } from '@/hooks/useTradingPermission';
import { TRADING_LEVEL_INFO, TRADING_PERMISSION_HREF } from '@/lib/tradingPermission';

/**
 * The user's trading permission, read where brokerages are connected, with the
 * way to change it. The setting itself lives in Preferences; this page is
 * where someone deciding what a connection may do goes looking for it.
 */
export function TradingPermissionLine() {
  const { t } = useTranslation();
  const { data } = useTradingPermission();
  const info = data ? TRADING_LEVEL_INFO[data.level] : null;
  const warning = info?.warning ?? false;
  const Icon = warning ? AlertTriangle : ShieldCheck;
  return (
    <p
      className="flex flex-wrap items-center gap-x-1.5 gap-y-1 text-[0.6875rem]"
      style={{ color: 'var(--color-text-tertiary)' }}
    >
      <Icon
        aria-hidden="true"
        className="h-3 w-3 shrink-0"
        style={warning ? { color: 'var(--color-warning)' } : undefined}
      />
      <span>
        {info
          ? t('plugins.brokerages.tradingPermissionCurrent', { level: t(info.labelKey) })
          : t('settings.tradingPermission.title')}
      </span>
      <Link
        to={TRADING_PERMISSION_HREF}
        // "Manage" alone names nothing; the label keeps the visible word.
        aria-label={t('plugins.brokerages.manageTradingPermissionLabel')}
        className="underline underline-offset-2 transition-opacity hover:opacity-80"
        style={{ color: 'var(--color-text-secondary)' }}
      >
        {t('plugins.brokerages.manageTradingPermission')}
      </Link>
    </p>
  );
}
