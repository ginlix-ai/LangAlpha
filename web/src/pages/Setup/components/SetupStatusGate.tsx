import type { ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import { Loader } from '@/components/ui/loader';
import type { SetupStatus } from '@/api/setup';
import { useSetupStatus } from '@/hooks/useServerSetup';

/**
 * Draws a form once the status is in, so its drafts can start from it. A status
 * that cannot be read says so and offers another try; what lies around it (the
 * step's way on, the rest of Settings) stays usable.
 */
export function SetupStatusGate({
  children,
  onFailed,
}: {
  children: (status: SetupStatus) => ReactNode;
  /** Drawn beside the failure, for the way past it. */
  onFailed?: ReactNode;
}) {
  const { t } = useTranslation();
  const { data, isPending, isFetching, refetch } = useSetupStatus();
  if (isPending && isFetching) {
    return (
      <div className="flex items-center justify-center py-10 text-(--color-text-tertiary)">
        <Loader size={20} label={t('common.loading')} />
      </div>
    );
  }
  if (!data) {
    return (
      <div className="flex flex-col items-start gap-3">
        <p role="alert" className="text-sm text-(--color-text-secondary)">
          {t('setup:errors.statusFailed')}
        </p>
        <Button type="button" variant="outline" size="sm" onClick={() => void refetch()}>
          {t('setup:errors.tryAgain')}
        </Button>
        {onFailed}
      </div>
    );
  }
  return <>{children(data)}</>;
}
