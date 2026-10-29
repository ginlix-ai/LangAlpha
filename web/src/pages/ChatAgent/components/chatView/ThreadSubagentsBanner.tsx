import type React from 'react';
import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import { useUpdatePreferences } from '@/hooks/useUpdatePreferences';
import { subagentsDefaultPatch } from '@/lib/subagentsDefault';
import { ThreadOfferRow } from './ThreadOfferRow';

/* The offer that follows a flip of the subagents switch to the side the
   user's default is not on. Accepting writes the default and asks nothing
   more: threads that follow it move with it, and threads switched on their
   own keep their value. It goes on its own, being an aside to the flip rather
   than a question the flip asked. */
export function ThreadSubagentsBanner({
  allowed,
  onDismiss,
  onExpire,
}: {
  /** The thread's value, which the offer would make the default. */
  allowed: boolean;
  onDismiss: () => void;
  onExpire: () => void;
}): React.ReactElement {
  const { t } = useTranslation();
  const updatePreferences = useUpdatePreferences();
  // The write moves the cached default first, which ends the offer; a refusal
  // puts it back, and the offer with it.
  const accept = () => {
    updatePreferences.mutateAsync(subagentsDefaultPatch(allowed)).catch(() => {
      toast({ variant: 'destructive', title: t('common.error'), description: t('settings.failedToSaveSettings') });
    });
  };
  return (
    <ThreadOfferRow
      text={t(allowed ? 'chat.threadSubagents.offerOn' : 'chat.threadSubagents.offerOff')}
      action={t(allowed ? 'chat.threadSubagents.makeOnDefault' : 'chat.threadSubagents.makeOffDefault')}
      onAction={accept}
      busy={updatePreferences.isPending}
      onDismiss={onDismiss}
      onExpire={onExpire}
    />
  );
}
