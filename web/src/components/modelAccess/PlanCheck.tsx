import type { KeyboardEvent } from 'react';
import { RefreshCw } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { DropdownMenuItem } from '@/components/ui/dropdown-menu';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';
import { usePlanClock } from '@/hooks/useClaudeCodePlan';
import { planTime } from '@/lib/claudeCodePlan';
import { cn } from '@/lib/utils';
import { usePlanCheck, type CheckTarget, type PlanCheck } from './usePlanCheck';

/** What the control says on hover: what a check costs, or while held when the next can go. */
function useCheckTip(check: PlanCheck): string {
  const { t } = useTranslation();
  const clock = usePlanClock();
  return check.state === 'held' && check.until
    ? t('claudeCodePlan.check.held', { time: planTime(new Date(check.until).toISOString(), clock) })
    : t('claudeCodePlan.check.tip');
}

const ownKeys = (event: KeyboardEvent<HTMLButtonElement>) => {
  if (event.key === 'Enter' || event.key === ' ') event.stopPropagation();
};

/**
 * Check now: quiet words in a usage table's header, or an icon beside the
 * picker's meter. Held rather than disabled while it runs or the 5 minutes
 * run (`aria-disabled`), so its tooltip still says when it can go again.
 * Its own keys stay its own: a picker row it sits in would take Enter and
 * Space as a press on the row, and the button would never hear them.
 */
export function CheckNowButton({ check, icon = false }: { check: PlanCheck; icon?: boolean }) {
  const { t } = useTranslation();
  const tip = useCheckTip(check);
  const label = t(check.state === 'running' ? 'claudeCodePlan.check.running' : 'claudeCodePlan.check.now');
  const held = check.state !== 'idle';
  return (
    <TooltipProvider delayDuration={300}>
      <Tooltip>
        <TooltipTrigger asChild>
          <button
            type="button"
            data-check-now
            aria-label={icon ? label : undefined}
            aria-disabled={held || undefined}
            onClick={check.run}
            onKeyDown={ownKeys}
            onKeyUp={ownKeys}
            className={cn(
              'tap-target inline-flex flex-none items-center rounded-sm text-xs whitespace-nowrap outline-none focus-visible:outline-2 focus-visible:outline-offset-2',
              icon ? 'h-5 w-5 justify-center' : 'px-1 py-0.5',
              held ? 'cursor-default opacity-60' : 'hover:text-(--color-text-primary)',
            )}
            style={{ color: 'var(--color-text-secondary)', outlineColor: 'var(--color-text-primary)' }}
          >
            {icon ? <RefreshCw aria-hidden="true" className={cn('h-3.5 w-3.5', check.state === 'running' && 'animate-spin')} /> : label}
          </button>
        </TooltipTrigger>
        <TooltipContent side="top" align="end" className="max-w-64">{tip}</TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}

/** Check now on one credential, where a model its plan holds is offered:
 *  a held model's row in the picker, or the composer's row. */
export function CheckNowFor({ target, icon = false }: { target: CheckTarget; icon?: boolean }) {
  return <CheckNowButton check={usePlanCheck([target])} icon={icon} />;
}

/** Check now as an item of a row's More actions menu, where the usage
 *  table is too narrow for its header row: the same words, states and
 *  tooltip, and a held item keeps the menu open. */
export function CheckNowMenuItem({ check }: { check: PlanCheck }) {
  const { t } = useTranslation();
  const tip = useCheckTip(check);
  const held = check.state !== 'idle';
  return (
    <TooltipProvider delayDuration={300}>
      <Tooltip>
        <TooltipTrigger asChild>
          <DropdownMenuItem
            data-check-now
            aria-disabled={held || undefined}
            className={cn(held && 'opacity-60')}
            onSelect={(event) => {
              if (held) event.preventDefault();
              else check.run();
            }}
          >
            {t(check.state === 'running' ? 'claudeCodePlan.check.running' : 'claudeCodePlan.check.now')}
          </DropdownMenuItem>
        </TooltipTrigger>
        <TooltipContent side="left" className="max-w-64">{tip}</TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}
