import type React from 'react';
import { useCallback, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Info, X } from 'lucide-react';
import { getModelDisplayName } from '@/components/ui/chat-input.helpers';
import { DefaultModelScopeChoice } from '@/components/model/DefaultModelScopeChoice';
import { useAllModels } from '@/hooks/useAllModels';
import { useDefaultModelChange } from '@/hooks/useDefaultModelChange';
import type { ComposerMode } from '@/lib/modelPreferences';

/** Prints a model the way the composer pill does, so the banner names it as
 *  the control right below it. */
function useComposerModelLabel(): (model: string) => string {
  const { metadata } = useAllModels();
  return useCallback((model: string) => getModelDisplayName(model, metadata), [metadata]);
}

/* The offer that follows a pick: this thread now runs on a model other than
   the account default, and the user may want every new thread to. Neutral on
   purpose: nothing is wrong, so it does not borrow the warning treatment the
   fallback pill uses. Accepting goes through the shared default change, which
   may first ask about the threads on the old default. */
interface ThreadModelBannerProps {
  model: string;
  defaultModel: string;
  mode: ComposerMode;
  onDismiss: () => void;
}

/* A pick made while the default question is open starts a new offer. The
   open question is about the model picked before, so confirming it would make
   that one the default while the thread runs another. */
export function ThreadModelBanner(props: ThreadModelBannerProps): React.ReactElement {
  return <ThreadModelOffer key={`${props.mode}:${props.model}`} {...props} />;
}

function ThreadModelOffer({ model, defaultModel, mode, onDismiss }: ThreadModelBannerProps): React.ReactElement {
  const { t } = useTranslation();
  const label = useComposerModelLabel();
  const { question, saving, request } = useDefaultModelChange(label);
  // The question replaces the button that asked it, so focus moves into the
  // question and, on cancel, back onto the button rather than the page body.
  const [asked, setAsked] = useState(false);

  if (question) return <DefaultModelScopeChoice {...question} autoFocus />;

  const name = label(model);
  return (
    <div className="flex flex-wrap items-center gap-x-2 gap-y-1.5 px-3 py-2 rounded-md text-sm"
      role="status" aria-live="polite"
      style={{
        backgroundColor: 'var(--color-bg-elevated)',
        color: 'var(--color-text-secondary)',
        border: '1px solid var(--color-border-default)',
      }}>
      <Info aria-hidden="true" className="h-4 w-4 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
      {/* The floor is what lets the row wrap: with none, a narrow composer (the
          market panel) squeezes the sentence to a word per line beside the
          button instead of moving the button below it. */}
      <span className="flex-1 min-w-48">
        {t('chat.threadModel.offer', { model: name, defaultModel: label(defaultModel) })}
      </span>
      <button
        type="button"
        onClick={() => {
          setAsked(true);
          request({ [mode]: model });
        }}
        disabled={saving}
        autoFocus={asked}
        className="text-xs font-medium whitespace-nowrap rounded-md px-2.5 py-1 shrink-0 hover:bg-(--color-bg-hover) disabled:opacity-50"
        style={{ color: 'var(--color-text-primary)', border: '1px solid var(--color-border-elevated)' }}
      >
        {t('chat.threadModel.makeDefault', { model: name })}
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

/* The thread's own model has left the catalog, so the composer fell back to
   the mode default; the next send stores that one on the thread and this row
   goes with it. A status line, not a banner: there is nothing to decide. */
export function RetiredModelNotice({
  model,
  fallback,
}: {
  model: string;
  fallback: string;
}): React.ReactElement {
  const { t } = useTranslation();
  const label = useComposerModelLabel();
  return (
    <div className="flex items-center gap-2 px-3 py-1.5 text-xs"
      role="status" aria-live="polite"
      style={{ color: 'var(--color-text-tertiary)' }}>
      <Info aria-hidden="true" className="h-3.5 w-3.5 shrink-0" />
      <span className="min-w-0">
        {t('chat.threadModel.retired', { model: label(model), fallback: label(fallback) })}
      </span>
    </div>
  );
}
