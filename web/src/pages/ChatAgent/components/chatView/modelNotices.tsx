import type React from 'react';
import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useNavigate } from 'react-router';
import { DefaultModelScopeChoice } from '@/components/model/DefaultModelScopeChoice';
import { NoticeAction, NoticeBody } from '@/components/ui/chat-input.notices';
import { toast } from '@/components/ui/use-toast';
import { useDefaultModelChange } from '@/hooks/useDefaultModelChange';
import type { SignInGap } from '@/hooks/useLapsedSignIn';
import { useUpdatePreferences } from '@/hooks/useUpdatePreferences';
import { CLAUDE_CODE_CHOICE_URL } from '@/lib/claudeCode';
import { subagentsDefaultPatch } from '@/lib/subagentsDefault';
import { useComposerModelLabel } from './useComposerModelLabel';

/* The composer notices that hold state, a write or the router, drawn in a
   notice's popover and, for a warning, at the top of the model picker
   (`useComposerNotices` picks which and words it; the rest it draws inline).
   The frame supplies the sentence and adds Not now where it has one. */

/* The composer's model runs on a subscription whose sign-in has to be
   renewed, or that is not connected at all, so a send would be refused. Said
   before the send, with the way to Settings, where the subscription's row
   offers the reconnect or the connect. */
export function LapsedSignInNotice({ reason }: { reason: SignInGap['reason'] }): React.ReactElement {
  const { t } = useTranslation();
  const navigate = useNavigate();
  return (
    <NoticeBody>
      <NoticeAction onClick={() => navigate('/settings?tab=model')}>
        {t(reason === 'missing' ? 'chat.threadModel.connect' : 'modelAccess.reconnect')}
      </NoticeAction>
    </NoticeBody>
  );
}

/* The account's Default is a Claude Code model this Chief of Staff turn
   cannot run yet, so the composer starts on another model, with the way to
   set Claude Code up. */
export function HeldDefaultNotice(): React.ReactElement {
  const { t } = useTranslation();
  const navigate = useNavigate();
  return (
    <NoticeBody>
      <NoticeAction onClick={() => navigate(CLAUDE_CODE_CHOICE_URL)}>
        {t('modelAccess.claudeCode.steps.setUp')}
      </NoticeAction>
    </NoticeBody>
  );
}

/* The offer that follows a pick: this thread now runs on a model other than
   the account default, and the user may want every new thread to. Accepting
   goes through the shared default change, which may first ask about the
   threads on the old default. A pick made while that question is open starts
   a new offer: the question is about the model picked before, so confirming it
   would make that one the default while the thread runs another. */
export function DefaultModelOfferNotice({ model }: { model: string }): React.ReactElement {
  return <DefaultModelOffer key={model} model={model} />;
}

function DefaultModelOffer({ model }: { model: string }): React.ReactElement {
  const { t } = useTranslation();
  const label = useComposerModelLabel();
  const { question, saving, request } = useDefaultModelChange(label);
  // The question replaces the button that asked it, so focus moves into the
  // question and, on cancel, back onto the button rather than the page body.
  const [asked, setAsked] = useState(false);

  if (question) return <DefaultModelScopeChoice {...question} autoFocus bare />;
  return (
    <NoticeBody>
      <NoticeAction
        onClick={() => {
          setAsked(true);
          request({ default: model });
        }}
        disabled={saving}
        autoFocus={asked}
      >
        {t('chat.threadModel.makeDefault', { model: label(model) })}
      </NoticeAction>
    </NoticeBody>
  );
}

/* The offer that follows a flip of the subagents switch to the side the
   user's default is not on. Accepting writes the default and asks nothing
   more: threads that follow it move with it, and threads switched on their
   own keep their value. It goes on its own, being an aside to the flip rather
   than a question the flip asked. */
export function SubagentsOfferNotice({ allowed }: { allowed: boolean }): React.ReactElement {
  const { t } = useTranslation();
  const updatePreferences = useUpdatePreferences();
  // The write moves the cached default first, which ends the offer; a refusal
  // puts it back, and the offer with it.
  const accept = () => {
    updatePreferences.mutateAsync(subagentsDefaultPatch(allowed)).catch(() => {
      toast({ variant: 'destructive', title: t('common.error'), description: t('settings:failedToSaveSettings') });
    });
  };
  return (
    <NoticeBody>
      <NoticeAction onClick={accept} disabled={updatePreferences.isPending}>
        {t(allowed ? 'chat.threadSubagents.makeOnDefault' : 'chat.threadSubagents.makeOffDefault')}
      </NoticeAction>
    </NoticeBody>
  );
}
