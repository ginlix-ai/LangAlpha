/**
 * How a running spec change reads wherever the computer shows. The change
 * runs on the server after the request that asked for it has answered, so
 * every surface reads its progress from the row's `spec_change`, never from a
 * mutation still being pending.
 */
import { useTranslation } from 'react-i18next';

import { Loader } from '@/components/ui/loader';
import { cn } from '@/lib/utils';
import type { Computer, ComputerSpecChange, ResourceTier } from '@/types/api';

import { TIER_ORDER, knownTier, normalizeTier, tierLabel } from './tierUi';

type Translate = ReturnType<typeof useTranslation>['t'];

/** The change still running on this computer, if any. */
export function activeSpecChange(
  computer: Pick<Computer, 'spec_change'> | null | undefined,
): ComputerSpecChange | null {
  const change = computer?.spec_change;
  return change?.state === 'in_progress' ? change : null;
}

/**
 * The tier to show. A change that died mid-way leaves the row's tier on its
 * target, and only the next spec change clears it. The machine may be at
 * either size by then: the old one if the change died before the teardown,
 * the new one if the recreate had finished. Showing the tier it came from
 * keeps retrying the target reachable, and that retry always runs, so it
 * settles the machine either way.
 */
export function effectiveTier(
  computer: Pick<Computer, 'resource_tier' | 'spec_change'> | null | undefined,
): ResourceTier {
  return normalizeTier(shownTierValue(computer));
}

/** As `effectiveTier`, but null where the row names no tier this build knows. */
export function knownEffectiveTier(
  computer: Pick<Computer, 'resource_tier' | 'spec_change'> | null | undefined,
): ResourceTier | null {
  return knownTier(shownTierValue(computer));
}

function shownTierValue(computer: Pick<Computer, 'resource_tier' | 'spec_change'> | null | undefined): unknown {
  const change = computer?.spec_change;
  if (change?.state === 'failed' && change.error?.code === 'interrupted' && change.from_tier) {
    return change.from_tier;
  }
  return computer?.resource_tier;
}

/** "Upgrading to Performance…" or, for a move down, "Changing spec to Standard…". */
export function specChangeLabel(t: Translate, change: ComputerSpecChange): string {
  const target = normalizeTier(change.target_tier);
  const tier = tierLabel(t, target);
  return TIER_ORDER.indexOf(target) > TIER_ORDER.indexOf(normalizeTier(change.from_tier))
    ? t('computer.spec.upgrading', 'Upgrading to {{tier}}…', { tier })
    : t('computer.spec.changing', 'Changing spec to {{tier}}…', { tier });
}

/**
 * The inline "in progress" line: a spinner and the label, announced politely.
 * `asLabel` drops the live region where the line names a control, which
 * carries its own accessible name instead.
 */
export function SpecChangeProgress({
  change,
  className,
  asLabel = false,
}: {
  change: ComputerSpecChange;
  className?: string;
  asLabel?: boolean;
}) {
  const { t } = useTranslation();
  return (
    <span
      role={asLabel ? undefined : 'status'}
      aria-live={asLabel ? undefined : 'polite'}
      className={cn('inline-flex items-center gap-1.5', className)}
      style={{ color: 'var(--color-text-secondary)' }}
    >
      <span aria-hidden="true" className="inline-flex">
        <Loader size={12} className="text-(--color-accent-primary)" />
      </span>
      {specChangeLabel(t, change)}
    </span>
  );
}
