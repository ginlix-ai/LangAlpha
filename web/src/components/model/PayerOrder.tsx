import { Trans } from 'react-i18next';
import { deploymentPayer } from '@/lib/modelPayer';
import { cn } from '@/lib/utils';
import { PayerBadge } from './PayerBadge';

/**
 * The one rule the badges cannot show on their own: which pays when the
 * user's key and the deployment both serve a model. A subscription's models
 * are entries of their own, so their badge already says it all.
 */
export function PayerOrder({ className }: { className?: string }) {
  return (
    <p className={cn('text-xs', className)} style={{ color: 'var(--color-text-tertiary)' }}>
      <Trans
        i18nKey="settings.payerOrder.rule"
        components={{
          ownKey: <PayerBadge payer="own_key" />,
          deployment: <PayerBadge payer={deploymentPayer()} />,
        }}
      />
    </p>
  );
}
