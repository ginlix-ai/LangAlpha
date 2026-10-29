import type React from 'react';
import { useTranslation } from 'react-i18next';
import { AlertTriangle, X } from 'lucide-react';
import { useModelName, usePaidByLabel } from '@/hooks/useModelName';
import type { FallbackSuggestion } from '../../session/types';

/* Model-fallback suggestion: the model the user sent with
   had trouble and a fallback answered the last turn. Offer
   adopting the working model for this thread. Persistent
   (survives stream end + reload) until dismissed, switched,
   or a new turn starts. Gated on composerModel, the model the
   composer shows, because that is what the next send names. */
export function FallbackSuggestionPill({
  fallbackSuggestion,
  isLoading,
  composerModel,
  onSwitchModel,
  onDismiss,
}: {
  fallbackSuggestion: FallbackSuggestion | null;
  isLoading: boolean;
  composerModel: string | null;
  onSwitchModel: (model: string) => void;
  onDismiss: () => void;
}): React.ReactElement | null {
  const { t } = useTranslation();
  const name = useModelName();
  const paidBy = usePaidByLabel();
  if (!(fallbackSuggestion && !isLoading && fallbackSuggestion.toModel !== composerModel)) {
    return null;
  }
  // A switch can move the thread onto another payer, so the button says who
  // pays: two models can share a name, one on a plan and one on credits.
  const to = name(fallbackSuggestion.toModel);
  const payer = paidBy(fallbackSuggestion.paidBy, fallbackSuggestion.toModel, fallbackSuggestion.provider);
  return (
    <div className="flex items-center gap-2 px-3 py-2 rounded-md text-sm"
      role="status" aria-live="polite"
      // Opaque: the tint is layered over the page surface, so the transcript
      // scrolling under the composer stack does not read through it.
      style={{
        backgroundColor: 'var(--color-bg-page)',
        backgroundImage: 'linear-gradient(var(--color-warning-soft), var(--color-warning-soft))',
        color: 'var(--color-text-secondary)',
        border: '1px solid var(--color-border-muted)',
      }}>
      <AlertTriangle aria-hidden="true" className="h-4 w-4 shrink-0" style={{ color: 'var(--color-warning)' }} />
      <span className="flex-1 min-w-0">
        {t('chat.modelTroubleSuggestion', {
          from: name(fallbackSuggestion.fromModel),
          to,
        })}
      </span>
      <button
        type="button"
        onClick={() => onSwitchModel(fallbackSuggestion.toModel)}
        className="text-xs font-medium whitespace-nowrap rounded-md px-2.5 py-1 shrink-0 hover:opacity-90 focus-visible:outline-hidden focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-1"
        style={{
          backgroundColor: 'var(--color-btn-primary-bg)',
          color: 'var(--color-btn-primary-text)',
        }}
      >
        {payer ? t('chat.switchToModelPaid', { model: to, payer }) : t('chat.switchToModel', { model: to })}
      </button>
      <button
        type="button"
        onClick={onDismiss}
        aria-label={t('common.close')}
        className="p-1 rounded shrink-0 hover:opacity-70"
        style={{ color: 'var(--color-text-tertiary)' }}
      >
        <X className="h-3.5 w-3.5" />
      </button>
    </div>
  );
}
