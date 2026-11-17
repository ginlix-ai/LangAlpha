import type React from 'react';
import { useTranslation } from 'react-i18next';
import { Loader } from '@/components/ui/loader';
import { ErrorLink, NoticeLine } from '@/components/ui/error-banner';
import { claudeCodeClisLink } from '@/lib/claudeCode';
import { canRunMore, type CliQueueWait, type CliWait } from '@/lib/claudeCodeQueue';
import { formatTook } from '@/lib/elapsed';
import { StatusLine } from './chatView/StatusLine';

/** To the setting that lets the computer run more at once, where the user
 *  sets it and has not set it to the most. */
const runMore = (wait: CliQueueWait | CliWait): React.ReactElement | undefined =>
  (canRunMore(wait) ? <ErrorLink {...claudeCodeClisLink(wait.computerId)} /> : undefined);

/**
 * A Claude Code turn waiting for one of its computer's CLIs, while it waits:
 * without it the turn reads as hung. A computer already running the most it
 * can says so, since the setting would offer nothing more.
 */
export function CliQueueLine({ wait }: { wait: CliQueueWait }): React.ReactElement {
  const { t } = useTranslation();
  const atMost = wait.settable && wait.cap >= wait.max;
  const { name, running: count } = wait;
  const text = name
    ? atMost ? t('chat.cliQueue.waitingAtMost', { name, count }) : t('chat.cliQueue.waiting', { name, count })
    : atMost ? t('chat.cliQueue.waitingHereAtMost', { count }) : t('chat.cliQueue.waitingHere', { count });
  return (
    <StatusLine glyph={<Loader size={12} className="text-(--color-accent-primary)" />} action={runMore(wait)}>
      {text}
    </StatusLine>
  );
}

/** How long a turn waited for a CLI, where in its reply the wait happened. */
export function CliWaitNotice({ wait }: { wait: CliWait }): React.ReactElement {
  const { t } = useTranslation();
  const duration = formatTook(wait.waitedS * 1000, t);
  const message = wait.name
    ? t('chat.cliQueue.waited', { name: wait.name, duration })
    : t('chat.cliQueue.waitedHere', { duration });
  return <NoticeLine message={message} action={runMore(wait)} className="py-1.5" />;
}
