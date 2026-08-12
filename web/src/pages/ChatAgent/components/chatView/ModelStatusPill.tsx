import type React from 'react';
import { useTranslation } from 'react-i18next';
import { Loader } from '@/components/ui/loader';
import type { ModelStatus } from '../../session/types';

/* Model resilience: the provider is retrying the current
   model, or has fallen back to a secondary. Transient —
   cleared on the first content event, on error, or on stop. */
export function ModelStatusPill({ modelStatus, isLoading }: {
  modelStatus: ModelStatus | null;
  isLoading: boolean;
}): React.ReactElement | null {
  const { t } = useTranslation();
  if (!(modelStatus && isLoading)) return null;
  return (
    <div className="flex items-center gap-2 px-3 py-1.5 text-xs"
      role="status" aria-live="polite"
      style={{ color: 'var(--color-text-tertiary)' }}>
      <span aria-hidden="true" className="flex-shrink-0">
        <Loader size={14} className="text-[color:var(--color-accent-primary)]" />
      </span>
      {modelStatus.kind === 'retrying'
        ? t('chat.modelRetrying', {
            model: modelStatus.model,
            attempt: modelStatus.attempt + 1,
            total: modelStatus.maxRetries + 1,
          })
        : t('chat.modelFallingBack', { model: modelStatus.toModel })}
    </div>
  );
}
