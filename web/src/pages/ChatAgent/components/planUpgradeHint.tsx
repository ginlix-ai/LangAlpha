/**
 * Where to go when a picker holds an option the plan keeps closed. A disabled
 * option is a dead end on its own, so every picker that locks one by plan
 * shows this under it, with the same links.
 */
import { useTranslation } from 'react-i18next';

import { ErrorLink } from '@/components/ui/error-banner';
import { PLAN_URL, SUPPORT_EMAIL } from '@/config/supportLinks';
import { cn } from '@/lib/utils';

/** False when the build names neither a plan page nor a support address. */
const HAS_PLAN_UPGRADE_PATH = PLAN_URL !== null || SUPPORT_EMAIL !== null;

export function PlanUpgradeHint({
  lead,
  testId = 'spec-plan-cta',
  className,
}: {
  lead: string;
  testId?: string;
  className?: string;
}) {
  const { t } = useTranslation();
  if (!HAS_PLAN_UPGRADE_PATH) return null;
  return (
    <p className={cn('text-xs', className)} data-testid={testId} style={{ color: 'var(--color-text-secondary)' }}>
      {lead}{' '}
      <span style={{ color: 'var(--color-text-primary)' }}>
        {PLAN_URL && <ErrorLink url={PLAN_URL} label={t('computer.spec.upgradePlan', 'Upgrade plan')} external />}
        {PLAN_URL && SUPPORT_EMAIL && <span aria-hidden="true"> · </span>}
        {SUPPORT_EMAIL && (
          <a href={`mailto:${SUPPORT_EMAIL}`} style={{ textDecoration: 'underline', fontWeight: 500 }}>
            {t('computer.spec.contactSupport', 'Contact support')}
          </a>
        )}
      </span>
    </p>
  );
}
