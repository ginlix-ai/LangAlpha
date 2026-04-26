import { WifiOff } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { useNetworkStatus } from '@/hooks/useNetworkStatus';

/**
 * Sticky banner that surfaces the browser's offline state. Mounted by
 * DashboardRouter so it covers both Classic and Custom dashboard modes.
 *
 * Keep the visual treatment subtle — this is informational, not blocking.
 * TradingView iframes and React Query polls keep working from cache; the
 * banner just tells the user why their numbers may be stale.
 */
export default function NetworkBanner() {
  const { t } = useTranslation();
  const { online } = useNetworkStatus();
  if (online) return null;

  return (
    <div
      role="status"
      aria-live="polite"
      className="sticky top-0 z-30 flex items-center justify-center gap-2 px-4 py-2 text-xs font-medium border-b"
      style={{
        backgroundColor: 'var(--color-warning-soft)',
        color: 'var(--color-warning)',
        // Use the saturated warning color for the divider so the banner
        // visually separates from the dashboard chrome — borderColor matching
        // the soft background made the `border-b` invisible.
        borderColor: 'var(--color-warning)',
      }}
    >
      <WifiOff size={14} />
      <span>{t('dashboard.networkBanner.offlineMessage')}</span>
    </div>
  );
}
