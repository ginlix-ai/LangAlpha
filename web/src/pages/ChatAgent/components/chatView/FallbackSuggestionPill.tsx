import type React from 'react';
import { useTranslation } from 'react-i18next';
import { AlertTriangle, X } from 'lucide-react';
import type { FallbackSuggestion } from '../../session/types';

/* Model-fallback suggestion: the model the user sent with
   had trouble and a fallback answered the last turn. Offer
   adopting the working model. Persistent (survives
   stream end + reload) until dismissed, switched, or a
   new turn starts. Gated on nextSendModel — the input's
   live selection — because that is what the next send
   re-uses, regardless of the durable preference. */
export function FallbackSuggestionPill({
  fallbackSuggestion,
  isLoading,
  inputModel,
  lastThreadModel,
  activePreferredModel,
  onSwitchModel,
  onDismiss,
}: {
  fallbackSuggestion: FallbackSuggestion | null;
  isLoading: boolean;
  inputModel: string | null;
  lastThreadModel: string | null;
  activePreferredModel: string | null;
  onSwitchModel: (model: string) => void;
  onDismiss: () => void;
}): React.ReactElement | null {
  const { t } = useTranslation();
  // The model the NEXT send will actually use: the chat input's live
  // selection, falling back to its initializer (thread's last model, then the
  // mode's preferred model) until it reports in. The suggestion pill gates on
  // this — not on the durable preference, which a thread's own model overrides
  // on every send.
  const nextSendModel = inputModel ?? (lastThreadModel || activePreferredModel);
  if (!(fallbackSuggestion && !isLoading && fallbackSuggestion.toModel !== nextSendModel)) {
    return null;
  }
  return (
    <div className="flex items-center gap-2 px-3 py-2 rounded-md text-sm"
      role="status" aria-live="polite"
      style={{
        backgroundColor: 'var(--color-warning-soft)',
        color: 'var(--color-text-secondary)',
        border: '1px solid var(--color-border-muted)',
      }}>
      <AlertTriangle aria-hidden="true" className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-warning)' }} />
      <span className="flex-1 min-w-0">
        {t('chat.modelTroubleSuggestion', {
          from: fallbackSuggestion.fromModel,
          to: fallbackSuggestion.toModel,
        })}
      </span>
      <button
        type="button"
        onClick={() => onSwitchModel(fallbackSuggestion.toModel)}
        className="text-xs font-medium whitespace-nowrap rounded-md px-2.5 py-1 flex-shrink-0 hover:opacity-90 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--color-accent-primary)] focus-visible:ring-offset-1"
        style={{
          backgroundColor: 'var(--color-accent-primary)',
          color: 'var(--color-text-on-accent)',
        }}
      >
        {t('chat.switchToModel', { model: fallbackSuggestion.toModel })}
      </button>
      <button
        type="button"
        onClick={onDismiss}
        aria-label={t('common.close')}
        className="p-1 rounded flex-shrink-0 hover:opacity-70"
        style={{ color: 'var(--color-text-tertiary)' }}
      >
        <X className="h-3.5 w-3.5" />
      </button>
    </div>
  );
}
