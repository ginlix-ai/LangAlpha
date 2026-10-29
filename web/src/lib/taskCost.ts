import type { TaskCost } from '@/hooks/useFilteredModels';

/**
 * A model's task cost at each level as a multiple of its cost at `reference`,
 * so its levels are weighed against each other rather than against another
 * model. Empty when the reference has no measured figure: a bound divided
 * into the others gives ratios that mean nothing.
 */
export function costsRelativeTo(
  costs: Record<string, TaskCost> | undefined,
  reference: string | null,
): Record<string, TaskCost> {
  const base = reference ? costs?.[reference] : undefined;
  if (!costs || !base?.measured || base.value <= 0) return {};
  return Object.fromEntries(
    Object.entries(costs).map(([level, cost]) => [level, { value: cost.value / base.value, measured: cost.measured }]),
  );
}
