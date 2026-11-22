import React from 'react';
import { useTranslation } from 'react-i18next';
import { Check, Circle } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { cn } from '@/lib/utils';
import { RunningTitle } from './ActivityRowParts';
import type { PlanStep } from '@/types/chat';
import { planHeading, stepTitle, type PlanState } from './requestPlans';

interface AgentPlanProps {
  steps: PlanStep[];
  /** Live, the running step shimmers and its glyph turns. Waiting, it says
   *  so, since work the agent handed off will bring it back. Ended, every
   *  step left says it was not done. */
  state: PlanState;
  /** Heads the plan with its name and progress, for the transcript; the
   *  Status panel and a tray chip's list head it themselves. */
  heading?: boolean;
  /** Smaller rows with a list's inset, for the tray chip's list. */
  dense?: boolean;
}

/** What a step's glyph says, for a reader who does not see it. */
const STEP_STATUS: Record<PlanStep['status'], string> = {
  completed: 'chat.plan.stepDone',
  in_progress: 'chat.plan.stepRunning',
  pending: 'chat.plan.stepPending',
};

/** The plan the agent shows the reader, as the heading of its work in the
 *  turn. It speaks the status line's glyphs (DESIGN.md § Status & liveness):
 *  the running step takes the loader and the shimmer, a finished one a quiet
 *  check, one not reached a hollow ring, and a step the request ended without
 *  reaching says so rather than taking a color of its own. */
export function AgentPlan({ steps, state, heading = false, dense = false }: AgentPlanProps): React.ReactElement | null {
  const { t } = useTranslation();
  if (steps.length === 0) return null;
  const live = state === 'live';

  return (
    <section aria-label={t('chat.plan.label')} data-agent-plan="" className="flex flex-col gap-0.5">
      {heading && (
        <div className="text-xs font-medium text-(--color-text-tertiary) tabular-nums">
          {t('chat.plan.heading', { progress: planHeading(steps, state, t) })}
        </div>
      )}
      <ol className="flex flex-col">
        {steps.map((step, i) => {
          const title = stepTitle(step, i);
          const running = step.status === 'in_progress';
          const note = step.status === 'completed' || state === 'live' ? null
            : state === 'ended' ? t('chat.plan.notDone')
              : running ? t('chat.plan.waiting') : null;
          return (
            <li
              key={`${i}-${title}`}
              className={cn('grid grid-cols-[16px_minmax(0,1fr)] items-center gap-2', dense ? 'px-2 py-1 text-[0.8125rem]' : 'py-0.5 text-sm')}
            >
              <span aria-hidden className="inline-flex h-5 items-center justify-center">
                {running && live
                  ? <Loader size={13} className="text-(--color-accent-primary)" />
                  : step.status === 'completed'
                    ? <Check size={13} className="text-(--color-text-tertiary)" />
                    : <Circle size={9} strokeWidth={2} className="text-(--color-text-quaternary)" />}
              </span>
              <span className="flex min-w-0 items-baseline gap-2">
                {!note && <span className="sr-only">{t(STEP_STATUS[step.status])}</span>}
                {running && live
                  ? <RunningTitle label={title} />
                  : (
                    <span
                      className={cn(
                        'truncate',
                        running ? 'text-(--color-text-primary)'
                          : step.status === 'completed' ? 'text-(--color-text-secondary)'
                            : 'text-(--color-text-tertiary)',
                      )}
                    >
                      {title}
                    </span>
                  )}
                {note && <span className="shrink-0 text-xs text-(--color-text-quaternary)">{note}</span>}
              </span>
            </li>
          );
        })}
      </ol>
    </section>
  );
}
