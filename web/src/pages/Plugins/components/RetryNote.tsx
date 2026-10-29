import { AlertTriangle } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { RowNote } from './RowNote';

/** A load that failed beside a list that still renders, and its retry. */
export function RetryNote({
  text,
  onRetry,
  id,
}: {
  text: string;
  onRetry: () => void;
  /** Set when a control points here for the reason it is unavailable. */
  id?: string;
}) {
  const { t } = useTranslation();
  return (
    <RowNote icon={AlertTriangle} id={id}>
      {text}{' '}
      <button
        type="button"
        onClick={onRetry}
        className="underline underline-offset-2 hover:text-(--color-text-secondary)"
      >
        {t('common.retry')}
      </button>
    </RowNote>
  );
}
