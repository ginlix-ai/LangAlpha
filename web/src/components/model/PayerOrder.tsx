import { Trans } from 'react-i18next';
import { deploymentPayer } from '@/lib/modelPayer';
import { cn } from '@/lib/utils';
import { PayerBadge } from './PayerBadge';
import { useAccount } from '@/hooks/useAccount';
import { allows, RULE } from '@/lib/accountRules';

/**
 * The one rule the badges cannot show on their own: which pays when the
 * user's key and the deployment both serve a model. A subscription's models
 * are entries of their own, so their badge already says it all.
 */
export function PayerOrder({ className }: { className?: string }) {
  // With the user's own keys kept off by a rule, there is no order to tell.
  const { policy } = useAccount();
  if (!allows(policy, RULE.ownApiKeys)) return null;
  return (
    <p className={cn('text-xs', className)} style={{ color: 'var(--color-text-tertiary)' }}>
      <Trans
        i18nKey="settings:payerOrder.rule"
        components={{
          ownKey: <PayerBadge payer="own_key" />,
          deployment: <PayerBadge payer={deploymentPayer()} />,
        }}
      />
    </p>
  );
}
