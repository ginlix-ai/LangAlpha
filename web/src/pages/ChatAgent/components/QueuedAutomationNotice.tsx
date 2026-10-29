import React from 'react';
import { useTranslation } from 'react-i18next';
import { Clock } from 'lucide-react';
import { useAutomationMutations } from '@/pages/Automations/hooks/useAutomationMutations';
import type { AutomationRun } from '@/types/automation';
import { StatusLine } from './chatView/StatusLine';

/**
 * Automations that came due on this thread while a turn runs here
 * (useWaitingAutomations, read by the status lines that show this). They wait
 * for the turn to end rather than steering into it; the run each one starts
 * reaches this view like any run started elsewhere (ChatView's feed effect).
 */
export function QueuedAutomationNotice({ runs }: { runs: AutomationRun[] }) {
  const { t } = useTranslation();
  const { skip, busy } = useAutomationMutations();
  return (
    <div role="status" aria-live="polite">
      {runs.map((run) => {
        // Truncated beside the Skip button, so the whole line is its title.
        const line = t('chat.automationWaiting', { name: run.automation_name });
        return (
          <StatusLine
            key={run.automation_execution_id}
            live={false}
            glyph={<Clock className="h-3.5 w-3.5" style={{ color: 'var(--color-accent-primary)' }} />}
          >
            <span className="min-w-0 truncate" title={line}>
              {line}
            </span>
            <button
              type="button"
              className="ml-auto shrink-0 text-xs underline-offset-2 hover:underline"
              style={{ color: 'var(--color-text-secondary)' }}
              disabled={busy}
              onClick={() => skip.mutate({ automationId: run.automation_id, executionId: run.automation_execution_id })}
            >
              {t('chat.automationWaitingSkip')}
            </button>
          </StatusLine>
        );
      })}
    </div>
  );
}
