import { useTranslation } from 'react-i18next';

/** A section's save state, beside the title of the group it is saving. */
export function SaveStatus({ status }: { status: 'idle' | 'saving' | 'saved' | 'error' }) {
  const { t } = useTranslation();
  if (status === 'idle') return null;
  const [text, color] = status === 'saving'
    ? [t('common.saving'), 'var(--color-text-tertiary)']
    : status === 'saved'
      ? [t('common.saved'), 'var(--color-success)']
      : [t('settings.failedToSaveSettings'), 'var(--color-loss)'];
  return (
    <span role="status" className="text-xs shrink-0" style={{ color }}>
      {text}
    </span>
  );
}
