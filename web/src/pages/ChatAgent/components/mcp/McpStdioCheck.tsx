import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Loader } from '@/components/ui/loader';
import { useNow } from '@/hooks/useNow';

/**
 * What a local command's save is doing while it waits. The save starts the
 * command on the user's computer and lists its tools before anything is
 * stored, which takes up to 90 seconds (`CHECK_TIMEOUT_S` in
 * mcp_command_check.py), and for all of that the form used to sit on a
 * spinner in the Add button.
 *
 * The host reports no progress, so the steps are paced by the clock: a guide
 * to where the time usually goes, with the real elapsed time beside it so the
 * wait is never presented as further along than it is.
 *
 * Cancel stays off for the whole wait, and the panel says why. The save route
 * does not stop when the page stops listening, so a form closed mid-check
 * would still add the connector if the command passed, with nothing left on
 * screen to say so.
 */

/** When each step is assumed to start, in seconds since the save began. */
const STEPS = [
  { key: 'mcp.modal.stdioStepWake', from: 0 },
  { key: 'mcp.modal.stdioStepRun', from: 8 },
  { key: 'mcp.modal.stdioStepTools', from: 30 },
] as const;

export function McpStdioCheck() {
  const { t } = useTranslation();
  const [startedAt] = useState(Date.now);
  const now = useNow(1000);
  const elapsed = Math.max(0, Math.floor((now - startedAt) / 1000));
  let current = 0;
  STEPS.forEach((step, i) => {
    if (elapsed >= step.from) current = i;
  });

  return (
    <div
      className="flex flex-col gap-2 p-2.5 rounded-md text-xs"
      style={{ backgroundColor: 'var(--color-bg-card)' }}
      data-testid="mcp-stdio-check"
      role="status"
    >
      <div className="flex items-center justify-between gap-2">
        <span style={{ color: 'var(--color-text-primary)' }}>{t('mcp.modal.stdioChecking')}</span>
        <span className="tabular-nums shrink-0" style={{ color: 'var(--color-text-tertiary)' }}>
          {t('mcp.modal.stdioElapsed', { seconds: elapsed })}
        </span>
      </div>
      <ol className="flex flex-col gap-1">
        {STEPS.map((step, i) => (
          <li
            key={step.key}
            className="flex items-center gap-2"
            style={{
              color:
                i === current
                  ? 'var(--color-text-primary)'
                  : i < current
                    ? 'var(--color-text-secondary)'
                    : 'var(--color-text-tertiary)',
            }}
            aria-current={i === current ? 'step' : undefined}
          >
            <span className="inline-flex w-3.5 justify-center shrink-0">
              {i === current && <Loader size={12} className="text-current" />}
            </span>
            {t(step.key)}
          </li>
        ))}
      </ol>
      <p className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
        {t('mcp.modal.stdioCannotCancel')}
      </p>
    </div>
  );
}
