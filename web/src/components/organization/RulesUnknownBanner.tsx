import { useEffect } from 'react';
import { useTranslation } from 'react-i18next';
import { useQueryClient } from '@tanstack/react-query';
import { Building2 } from 'lucide-react';
import { useAccount } from '@/hooks/useAccount';
import { queryKeys } from '@/lib/queryKeys';

/** How often the account is read again while its rules cannot be. */
const RECHECK_MS = 60_000;

/**
 * App-wide line for an account whose organization's settings cannot be read
 * (`/users/me` answers `policy: null`): every turn is refused and every
 * feature reads off until they can, so it says that once, where the offline
 * banner speaks, and reads the account again each minute so it goes away on
 * its own. In flow, as the offline banner is, for the same layout reason.
 */
export default function RulesUnknownBanner() {
  const { t } = useTranslation();
  const { unknown } = useAccount();
  const queryClient = useQueryClient();

  useEffect(() => {
    if (!unknown) return;
    const timer = window.setInterval(() => {
      void queryClient.invalidateQueries({ queryKey: queryKeys.user.me() });
    }, RECHECK_MS);
    return () => window.clearInterval(timer);
  }, [unknown, queryClient]);

  if (!unknown) return null;
  return (
    <div
      role="status"
      aria-live="polite"
      className="relative z-1020 shrink-0 flex items-center justify-center gap-2 px-4 py-2 text-xs font-medium border-b"
      style={{
        backgroundColor: 'var(--color-bg-card)',
        color: 'var(--color-text-secondary)',
        borderColor: 'var(--color-border-muted)',
      }}
    >
      <Building2 size={14} aria-hidden="true" style={{ color: 'var(--color-text-tertiary)' }} />
      <span>{t('organization.unknown.banner')}</span>
    </div>
  );
}
