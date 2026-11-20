import type React from 'react';
import { useCallback, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useNavigate } from 'react-router';
import { Info } from 'lucide-react';
import { subscriptionKey, type SubscriptionProvider } from '@/api/oauth';
import { getModelDisplayName } from '@/components/ui/chat-input.helpers';
import { DefaultModelScopeChoice } from '@/components/model/DefaultModelScopeChoice';
import { useAllModels } from '@/hooks/useAllModels';
import { CLAUDE_CODE_CHOICE_URL } from '@/lib/claudeCode';
import { useClaudeCodeSteps } from '@/hooks/useClaudeCodeSteps';
import { useDefaultModelChange } from '@/hooks/useDefaultModelChange';
import { ThreadOfferRow } from './ThreadOfferRow';
import { StatusLine } from './StatusLine';

/** Prints a model the way the composer pill does, so the banner names it as
 *  the control right below it. */
function useComposerModelLabel(): (model: string) => string {
  const { metadata } = useAllModels();
  return useCallback((model: string) => getModelDisplayName(model, metadata), [metadata]);
}

/* The offer that follows a pick: this thread now runs on a model other than
   the account default, and the user may want every new thread to. Accepting
   goes through the shared default change, which may first ask about the
   threads on the old default. */
interface ThreadModelBannerProps {
  model: string;
  defaultModel: string;
  onDismiss: () => void;
}

/* A pick made while the default question is open starts a new offer. The
   open question is about the model picked before, so confirming it would make
   that one the default while the thread runs another. */
export function ThreadModelBanner(props: ThreadModelBannerProps): React.ReactElement {
  return <ThreadModelOffer key={props.model} {...props} />;
}

function ThreadModelOffer({ model, defaultModel, onDismiss }: ThreadModelBannerProps): React.ReactElement {
  const { t } = useTranslation();
  const label = useComposerModelLabel();
  const { question, saving, request } = useDefaultModelChange(label);
  // The question replaces the button that asked it, so focus moves into the
  // question and, on cancel, back onto the button rather than the page body.
  const [asked, setAsked] = useState(false);

  if (question) return <DefaultModelScopeChoice {...question} autoFocus />;

  const name = label(model);
  return (
    <ThreadOfferRow
      text={t('chat.threadModel.offer', { model: name, defaultModel: label(defaultModel) })}
      action={t('chat.threadModel.makeDefault', { model: name })}
      onAction={() => {
        setAsked(true);
        request({ default: model });
      }}
      busy={saving}
      focusAction={asked}
      onDismiss={onDismiss}
    />
  );
}

/* The thread's own model has left the catalog, so the composer fell back to
   the account default; the next send stores that one on the thread and this row
   goes with it. A status line, not a banner: there is nothing to decide. */
export function RetiredModelNotice({
  model,
  fallback,
  className,
}: {
  model: string;
  fallback: string;
  className?: string;
}): React.ReactElement {
  const { t } = useTranslation();
  const label = useComposerModelLabel();
  return (
    <StatusLine glyph={<Info className="h-3.5 w-3.5" />} className={className}>
      <span className="min-w-0">
        {t('chat.threadModel.retired', { model: label(model), fallback: label(fallback) })}
      </span>
    </StatusLine>
  );
}

/* The composer's model runs on a subscription whose sign-in has to be
   renewed, so a send would be refused. Said before the send, in the rows above
   the composer, with the way to Settings, where the connection's row offers the
   reconnect. Dismissed, it stays down while this view shows the same model (the
   host keys it by model); a send still meets the turn's own refusal. */
export function LapsedSignInNotice({ model, provider }: { model: string; provider: SubscriptionProvider }): React.ReactElement | null {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const label = useComposerModelLabel();
  const [dismissed, setDismissed] = useState(false);
  if (dismissed) return null;
  return (
    <ThreadOfferRow
      text={t('chat.threadModel.lapsed', {
        account: t(`modelAccess.${subscriptionKey(provider)}.name`),
        model: label(model),
      })}
      action={t('modelAccess.reconnect')}
      onAction={() => navigate('/settings?tab=model')}
      onDismiss={() => setDismissed(true)}
    />
  );
}

/* The account's Default is a Claude Code model this Chief of Staff turn
   cannot run yet, so the composer starts on another model. Said where the
   user sees the composer, with the way to set Claude Code up. Dismissed, it
   stays down while this view shows the same Default (the host keys it). */
export function HeldDefaultNotice({ model, fallback }: { model: string; fallback: string }): React.ReactElement | null {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const label = useComposerModelLabel();
  const [dismissed, setDismissed] = useState(false);
  // Only a Default the cloud steps would let run is one to set up here; held
  // for another reason (an own computer offline), the composer's model says it.
  const steps = useClaudeCodeSteps();
  if (dismissed || steps.status !== 'pending') return null;
  return (
    <ThreadOfferRow
      text={t('modelAccess.claudeCode.steps.composerHeld', { model: label(model), fallback: label(fallback) })}
      action={t('modelAccess.claudeCode.steps.setUp')}
      onAction={() => navigate(CLAUDE_CODE_CHOICE_URL)}
      onDismiss={() => setDismissed(true)}
    />
  );
}
