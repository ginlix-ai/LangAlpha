import { useTranslation } from 'react-i18next';
import { ToastAction } from '@/components/ui/toast';

// The stale-build toast stays up until the user acts on it, so it can outlive a
// language switch. Its copy is rendered by components that re-read the
// language, not strings taken once when the toast was raised.

export function StaleBuildToastText({ part }: { part: 'title' | 'description' }) {
  const { t } = useTranslation();
  return <>{t(`common.staleBuild.${part}`)}</>;
}

export function StaleBuildReloadAction() {
  const { t } = useTranslation();
  const label = t('common.staleBuild.reload');
  return (
    <ToastAction altText={label} onClick={() => window.location.reload()}>
      {label}
    </ToastAction>
  );
}
