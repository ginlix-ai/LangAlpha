import React from 'react';
import { useTranslation } from 'react-i18next';
import { PauseCircle } from 'lucide-react';
import { ErrorLink } from '@/components/ui/error-banner';
import { CREDIT_STOP_ERROR_TYPE } from '@/types/sse';
import { buildRateLimitError } from '@/utils/rateLimitError';
import { useSubagentTelemetries } from './SubagentTelemetryContext';
import { clampReason } from './messageList/subagentRun/text';

function accountLinks(reason: string) {
  // Same builder the pause card uses, so the two surfaces send the user to the
  // same pages with the same wording rather than growing a second answer.
  return buildRateLimitError(
    { message: reason },
    (import.meta.env.VITE_PLATFORM_URL as string | undefined) || '/account',
  ).links;
}

/**
 * A credit stop, as a notice at the foot of the message in the MAIN transcript.
 *
 * A background task can outlive the turn that spawned it, and when the credit
 * gate stops one the turn is already finished: there is no model boundary left
 * to interrupt, so no pause card is ever raised in the thread. The denial then
 * reaches only the task's own transcript, behind a click, while the thread
 * shows a "Stopped" chip indistinguishable from a task the user ended.
 *
 * Deliberately outside the card rather than a line within it. Inside, it reads
 * as a footnote on one task; the thing that actually happened is that the
 * account ran out of money, which is the turn's news and the user's to act on.
 * It borrows the pause card's layout for that reason - the same event should
 * not look like two different kinds of event depending on when it landed. The
 * placement is MessageContentSegments' call, and the reason it is last is
 * written there.
 *
 * One notice per distinct reason, not per task: one denial stops every task
 * running under it, and the same notice six times over says nothing more.
 *
 * Memoized on the ids because that parent re-creates the list on every
 * streamed chunk, and the telemetry read counts the tasks' tool calls: only
 * the context moves it.
 */
export const SubagentStopNotices = React.memo(function SubagentStopNotices({
  subagentIds,
}: {
  subagentIds: readonly string[];
}): React.ReactElement | null {
  const telemetries = useSubagentTelemetries(subagentIds);
  const reasons: string[] = [];
  for (const telemetry of telemetries) {
    const reason = telemetry?.stopReason;
    if (reason && telemetry.stopReasonType === CREDIT_STOP_ERROR_TYPE && !reasons.includes(reason)) {
      reasons.push(reason);
    }
  }
  if (reasons.length === 0) return null;
  return (
    <>
      {reasons.map((reason) => (
        <CreditStopNotice key={reason} reason={reason} />
      ))}
    </>
  );
}, (prev, next) =>
  prev.subagentIds.length === next.subagentIds.length
  && prev.subagentIds.every((id, i) => id === next.subagentIds[i]));

function CreditStopNotice({ reason }: { reason: string }): React.ReactElement {
  const { t } = useTranslation();
  const links = accountLinks(reason);
  return (
    <div
      data-testid="subagent-credit-stop-notice"
      className="mt-2 rounded-lg px-4 py-3 space-y-2"
      style={{ border: '1px solid var(--color-border-muted)' }}
    >
      <div className="flex items-center gap-2">
        <PauseCircle className="h-4 w-4 shrink-0" style={{ color: 'var(--color-accent-light)' }} />
        <span className="text-[0.9375rem] font-medium" style={{ color: 'var(--color-text-primary)' }}>
          {t('chat.creditStop.title')}
        </span>
      </div>
      <div className="text-sm wrap-break-word" style={{ color: 'var(--color-text-secondary)' }}>
        {clampReason(reason)}
      </div>
      {links && links.length > 0 && (
        <div className="flex items-center gap-4 text-sm" style={{ color: 'var(--color-accent-light)' }}>
          {links.map((l) => (
            <ErrorLink key={`${l.url}|${l.label}`} {...l} />
          ))}
        </div>
      )}
    </div>
  );
}
