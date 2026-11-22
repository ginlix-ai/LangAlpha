import type { ModelMetadataEntry, TaskCost } from '@/hooks/useFilteredModels';
import { isSubscription, type Payer } from '@/lib/modelPayer';
import { createFormatter } from '@/lib/format';

// One significant figure under the reference, two from it up: 0.04×, 0.4×,
// 1.8×, 11×. Enough to rank by, without reading more into a benchmark average
// than it holds.
const underOne = createFormatter({ maximumSignificantDigits: 1 });
const fromOne = createFormatter({ maximumSignificantDigits: 2 });

/** A cost multiple as the picker prints it, without its "×". */
export const formatCostMultiple = (value: number, locale: string) => (value < 1 ? underOne : fromOne)(value, locale);

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

/** The level a model measured without levels is filed under. */
const ANY_LEVEL = 'any';

/** How many marks a model's cost fills, out of `COST_SLOTS`. */
export const COST_SLOTS = 5;

/**
 * The marks for a cross-model figure. Banded on a rough log scale, since the
 * catalog spans two orders of magnitude and a linear band would put every
 * model but the dearest in the first slot.
 */
export function costBand(value: number): number {
  if (value < 0.15) return 1;
  if (value < 0.5) return 2;
  if (value < 1.5) return 3;
  if (value < 5) return 4;
  return 5;
}

/**
 * A model's cost marks, from what a task costs at the manifest's default
 * level for it, not the one Settings may give, so every model is weighed at
 * its own standard level. `measured` is a benchmark figure; `bound` one
 * carried down from the level above, so no more than it; `estimate` the
 * model's list price against the baseline's, for a model never benchmarked.
 * All three are multiples of the baseline and band alike.
 */
export type CostMarks =
  | { kind: 'measured' | 'bound'; band: number; value: number; level: string }
  | { kind: 'estimate'; band: number };

/**
 * Whether what a model costs per token is what the user pays: not on a
 * subscription, which draws on a plan and whose badge already says so. Its
 * list price and its marks are hidden; the multiples between its levels are
 * not, since they still help pick a level.
 */
export const paysPerToken = (payer: Payer | undefined): boolean => !isSubscription(payer);

/** None where the user does not pay per token, and none for a model with
 *  neither a figure nor a price. */
export function costMarks(meta: ModelMetadataEntry | undefined, payer: Payer | undefined): CostMarks | null {
  if (!meta || !paysPerToken(payer)) return null;
  const level = meta.reasoning_effort_default && meta.task_cost?.[meta.reasoning_effort_default]
    ? meta.reasoning_effort_default
    : ANY_LEVEL;
  const cost = meta.task_cost?.[level];
  if (cost && cost.value > 0) {
    return { kind: cost.measured ? 'measured' : 'bound', band: costBand(cost.value), value: cost.value, level };
  }
  const estimate = meta.task_cost_estimate;
  if (typeof estimate === 'number' && estimate > 0) return { kind: 'estimate', band: costBand(estimate) };
  return null;
}

/** The smallest and largest figure across the catalog, every level of every
 *  model, which a level's bar is drawn against. Null with fewer than two. */
export function catalogCostRange(metadata: Record<string, ModelMetadataEntry> | undefined): [number, number] | null {
  let lo = Infinity;
  let hi = 0;
  for (const meta of Object.values(metadata ?? {})) {
    for (const cost of Object.values(meta.task_cost ?? {})) {
      if (!(cost.value > 0)) continue;
      lo = Math.min(lo, cost.value);
      hi = Math.max(hi, cost.value);
    }
  }
  return hi > lo ? [lo, hi] : null;
}

/** The least a bar shows, so the cheapest level still reads as a bar. */
const BAR_FLOOR = 0.06;

/**
 * Where `value` sits in `range` on a log scale, 0 to 1. A log scale because
 * the figures are multiples: a level twice the cost of another is the same
 * step anywhere on the catalog.
 */
export function costBarFraction(value: number, range: [number, number] | null): number {
  if (!range || !(value > 0)) return 1;
  const [lo, hi] = range;
  const at = (Math.log(value) - Math.log(lo)) / (Math.log(hi) - Math.log(lo));
  return Math.min(1, Math.max(BAR_FLOOR, at));
}
