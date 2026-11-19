import { useEffect } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { Send } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { deliveryOptionsQuery } from '@/hooks/useLinkedChatApps';
import { useChatAppsPrompt } from './OnboardingProvider';

/**
 * "Ask from your chat apps too", under the last finished reply of a thread,
 * on hosted, while no chat app is linked: the same Chief of Staff and threads
 * reach Slack, Telegram and the rest. It stays with the first thread it shows
 * in rather than following the user into every one. Not now puts it away for
 * good; Connect opens the chat apps page.
 *
 * Chat keeps every opened thread mounted, so only the thread on screen
 * (`active`) may show it and pin it, once a reply is in (`replied`) and no
 * turn runs (`idle`).
 */
export function ChatAppsPromptCard({ threadId, active, idle, replied }: {
  threadId: string;
  active: boolean;
  idle: boolean;
  replied: boolean;
}) {
  const { t } = useTranslation();
  const prompt = useChatAppsPrompt();
  const here = active && idle && replied && !!prompt?.offered
    && (prompt.threadId === null || prompt.threadId === threadId);
  const { data } = useQuery({ ...deliveryOptionsQuery(null), enabled: here });
  // Unknown until the messaging service answers, and nothing to connect where it has none.
  const shown = here && !!data?.enabled && Object.keys(data.apps ?? {}).length === 0;
  const pin = prompt?.pin;
  const unpinned = prompt?.threadId === null;
  useEffect(() => {
    if (shown && unpinned) pin?.(threadId);
  }, [shown, unpinned, pin, threadId]);
  if (!shown || !prompt) return null;
  return (
    <div
      data-testid="chat-apps-prompt"
      className="mt-6 flex flex-wrap items-center gap-3 rounded-lg border px-4 py-3"
      style={{ borderColor: 'var(--color-border-muted)', backgroundColor: 'var(--color-bg-card)' }}
    >
      <span
        aria-hidden="true"
        className="flex h-8 w-8 shrink-0 items-center justify-center rounded-md border"
        style={{ borderColor: 'var(--color-border-muted)', color: 'var(--color-text-secondary)' }}
      >
        <Send className="h-4 w-4" />
      </span>
      <div className="flex min-w-0 flex-1 flex-col gap-0.5">
        <span className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>{t('onboarding.chatApps.title')}</span>
        <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{t('onboarding.chatApps.body')}</span>
      </div>
      <div className="flex shrink-0 items-center gap-2">
        <Button type="button" variant="ghost" size="sm" onClick={prompt.dismiss}>{t('onboarding.chatApps.notNow')}</Button>
        <Button type="button" variant="outline" size="sm" onClick={prompt.connect}>{t('onboarding.chatApps.connect')}</Button>
      </div>
    </div>
  );
}
