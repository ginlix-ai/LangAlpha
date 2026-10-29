/**
 * What a typical task costs at one of a model's levels, as a multiple of its
 * cost at its default level, so a user can weigh a level against the one
 * they would otherwise get.
 *
 * A multiple rather than dollars: a dollar figure ages with every price change
 * and means little without something to hold it against.
 */
import { useTranslation } from 'react-i18next';
import type { TaskCost } from '@/hooks/useFilteredModels';
import { useLocale } from '@/hooks/useLocale';
import { createFormatter } from '@/lib/format';
import { cn } from '@/lib/utils';

// One significant figure under the reference, two from it up: 0.04×, 0.4×,
// 1.8×, 11×. Enough to rank by, without reading more into a benchmark average
// than it holds.
const underOne = createFormatter({ maximumSignificantDigits: 1 });
const fromOne = createFormatter({ maximumSignificantDigits: 2 });

export function TaskCostFigure({ cost, className }: { cost: TaskCost | null; className?: string }) {
  const { t } = useTranslation();
  const locale = useLocale();
  if (!cost) return null;
  const value = (cost.value < 1 ? underOne : fromOne)(cost.value, locale);
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
