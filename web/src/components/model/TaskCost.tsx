/**
 * What a typical task costs, two ways. Within one model, a level as a
 * multiple of its cost at its default level, so a user can weigh a level
 * against the one they would otherwise get. Across models, marks out of five
 * from the model's figure against the catalog's baseline.
 *
 * Multiples and marks rather than dollars: a dollar figure ages with every
 * price change and means little without something to hold it against.
 */
import { Info } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import type { TaskCost } from '@/hooks/useFilteredModels';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';
import { useLocale } from '@/hooks/useLocale';
import { COST_SLOTS, formatCostMultiple, type CostMarks } from '@/lib/taskCost';
import { cn } from '@/lib/utils';

export function TaskCostFigure({ cost, className }: { cost: TaskCost | null; className?: string }) {
  const { t } = useTranslation();
  const locale = useLocale();
  if (!cost) return null;
  const value = formatCostMultiple(cost.value, locale);
  return (
    <span
      title={cost.measured ? undefined : t('chat.modelSelector.taskCostUnmeasured')}
      className={cn(
        'flex-none text-xs tabular-nums',
        cost.measured ? 'text-(--color-text-tertiary)' : 'text-(--color-text-quaternary)',
        className,
      )}
    >
      {t(cost.measured ? 'chat.modelSelector.taskCost' : 'chat.modelSelector.taskCostAtMost', { value })}
    </span>
  );
}

/**
 * A model's cost as dollar signs out of five. A figure that is a bound or an
 * estimate from the price is drawn lighter, since it says less than a
 * measured one. Hidden from a screen reader: the caller says what the marks
 * mean, in their tooltip and the row's name. Null keeps the marks' room
 * empty, so a column of them stays aligned past a row without any.
 */
export function CostMarksGlyph({ marks, className }: { marks: CostMarks | null; className?: string }) {
  const firm = marks?.kind === 'measured';
  return (
    <span aria-hidden className={cn('inline-flex flex-none text-xs font-semibold tracking-[-0.04em]', !marks && 'invisible', className)}>
      {Array.from({ length: COST_SLOTS }, (_, i) => (
        <span
          key={i}
          className={
            !marks || i >= marks.band ? 'text-(--color-border-elevated)'
              : firm ? 'text-(--color-text-primary)' : 'text-(--color-text-quaternary)'
          }
        >
          $
        </span>
      ))}
    </span>
  );
}

/** A task-cost column's heading, whose tooltip says what the figures mean. */
export function TaskCostHeading({ note, className }: { note: string; className?: string }) {
  const { t } = useTranslation();
  return (
    <TooltipProvider delayDuration={300}>
      <Tooltip>
        <TooltipTrigger asChild>
          {/* Focusable, so a keyboard can open the note too. */}
          <span
            tabIndex={0}
            className={cn('inline-flex flex-none cursor-help items-center gap-1 rounded-sm outline-hidden focus-visible:ring-2 focus-visible:ring-ring', className)}
          >
            {t('chat.modelSelector.taskCostHeader')}
            <Info aria-hidden className="h-3 w-3" />
          </span>
        </TooltipTrigger>
        <TooltipContent side="top" className="max-w-56">{note}</TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}
