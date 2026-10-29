/**
 * The resource-tier vocabulary and its picker.
 *
 * Two surfaces choose a tier (changing a computer's spec, and creating
 * one), and a tier means the same thing in both, so the ladder, the copy
 * and the keyboard behaviour live here rather than once per dialog.
 */
import { useTranslation } from 'react-i18next';

import { isPlatformMode } from '@/config/hostMode';
import type {
  ComputerProviderOption,
  ComputerTierSpec,
  ResourceTier,
  WorkspaceCapacity,
  WorkspaceQuota,
} from '@/types/api';

import { RadioCardGroup, type RadioCardOption } from './radioCardGroup';

type Translate = ReturnType<typeof useTranslation>['t'];

/** Every tier, smallest first. */
export const TIER_ORDER: ResourceTier[] = ['standard', 'performance', 'max', 'xlarge'];

/**
 * What a picker offers when the provider's own list is unknown: still
 * loading, an older server, or a computer on a provider no longer offered.
 * The sizes every hosted provider runs, so xlarge (E2B's alone) is not one.
 */
const BASE_TIERS: readonly ResourceTier[] = ['standard', 'performance', 'max'];

/** The tier a value names, or null for a missing or unknown one. */
export function knownTier(tier: unknown): ResourceTier | null {
  return TIER_ORDER.find((id) => id === tier) ?? null;
}

/** Coerce an unknown/legacy tier value to a known tier (defaults to standard). */
export function normalizeTier(tier: unknown): ResourceTier {
  return knownTier(tier) ?? 'standard';
}

/** Localized display name for a tier. */
export function tierLabel(t: Translate, tier: ResourceTier): string {
  return t(`workspace.tier.${tier}`);
}

/** The sizes a picker shows, and what each one is when the server said. */
export interface TierOffer {
  tiers: ResourceTier[];
  /** Empty when unknown, and a card then shows its name alone. */
  specs: readonly ComputerTierSpec[];
}

/**
 * The tiers `provider` offers, smallest first, with `current` kept in so the
 * tier a computer is on always renders. Unknown falls back to the base ladder.
 */
export function offeredTiers(
  provider: ComputerProviderOption | null | undefined,
  current: ResourceTier | null = null,
): TierOffer {
  const specs = provider?.tiers;
  return {
    tiers: TIER_ORDER.filter((id) =>
      id === current || (specs ? specs.some((s) => s.tier === id) : BASE_TIERS.includes(id))),
    specs: specs ?? [],
  };
}

/** A tier's count quota. Only elevated tiers carry one, and only in platform mode. */
export function tierCapacity(
  quota: WorkspaceQuota | null | undefined,
  tier: ResourceTier,
): WorkspaceCapacity | null {
  if (tier === 'performance') return quota?.performance ?? null;
  if (tier === 'max') return quota?.max ?? null;
  if (tier === 'xlarge') return quota?.xlarge ?? null;
  return null;
}

/**
 * The quota as a spec change on a computer at `current` reads it. A plan has
 * one slot for a computer bigger than Small, open at any size up to its
 * largest, and every larger size counts the computers held in it. A computer
 * already there gives its slot up as it moves, so each limited size reads one
 * fewer used, as the server's gate counts it. A size the plan leaves out
 * stays out.
 */
export function quotaForSpecChange(
  quota: WorkspaceQuota | null | undefined,
  current: ResourceTier,
): WorkspaceQuota | null | undefined {
  if (!quota || current === 'standard') return quota;
  const returned = (capacity: WorkspaceCapacity | null) =>
    capacity && capacity.limit > 0 ? { ...capacity, used: Math.max(0, capacity.used - 1) } : capacity;
  return {
    ...quota,
    performance: returned(quota.performance),
    max: returned(quota.max),
    xlarge: quota.xlarge && returned(quota.xlarge),
  };
}

/**
 * Why a tier cannot be picked: the plan excludes it, or its quota is spent.
 * Null when it can. A negative limit is unlimited.
 */
export function tierBlockReason(capacity: WorkspaceCapacity | null | undefined): 'not_on_plan' | 'used_up' | null {
  if (!isPlatformMode || !capacity) return null;
  if (capacity.limit === 0) return 'not_on_plan';
  return capacity.limit > 0 && capacity.used >= capacity.limit ? 'used_up' : null;
}

export function isTierBlocked(capacity: WorkspaceCapacity | null | undefined): boolean {
  return tierBlockReason(capacity) !== null;
}

/**
 * Whether only a bigger plan opens `tier`: the plan leaves it out and every
 * size above it too. A plan opens every size up to its largest, so a size it
 * leaves out sits above that, and a Max plan is never told to upgrade.
 */
export function needsBiggerPlan(quota: WorkspaceQuota | null | undefined, tier: ResourceTier): boolean {
  if (tierBlockReason(tierCapacity(quota, tier)) !== 'not_on_plan') return false;
  return TIER_ORDER.slice(TIER_ORDER.indexOf(tier)).every((id) => (tierCapacity(quota, id)?.limit ?? 0) === 0);
}

/**
 * The plan's one slot for computers bigger than Small, which every larger
 * size counts against: what each of them reports, as one count. Null where
 * there is nothing to say of it: an unknown quota, an unlimited plan, or a
 * plan with no larger size at all.
 */
export function largerSlot(quota: WorkspaceQuota | null | undefined): WorkspaceCapacity | null {
  const capacities = TIER_ORDER.slice(1)
    .map((id) => tierCapacity(quota, id))
    .filter((c): c is WorkspaceCapacity => !!c);
  if (capacities.some((c) => c.limit < 0)) return null;
  const limited = capacities.filter((c) => c.limit > 0);
  if (!limited.length) return null;
  return {
    used: Math.max(...limited.map((c) => c.used)),
    limit: Math.max(...limited.map((c) => c.limit)),
  };
}

interface TierRadioGroupProps {
  /** The sizes on offer, from {@link offeredTiers}. */
  offer: TierOffer;
  value: ResourceTier;
  onChange: (tier: ResourceTier) => void;
  /** Shown above the rows, and the group's accessible name. */
  label: string;
  /** Per-tier count quotas (platform mode only); null/undefined hides every quota note. */
  quota?: WorkspaceQuota | null;
  /**
   * The tier the computer is on now. It reads "Current" in place of a quota
   * note, and is never disabled: re-selecting it consumes no new slot,
   * mirroring the backend's skip.
   */
  currentTier?: ResourceTier | null;
}

/**
 * The tier picker, one row per size. In platform mode a size the plan
 * excludes or whose slot is spent goes unselectable and says why, and one
 * line under the rows says what is left of the slot every larger size shares,
 * which a note on each would count three times.
 */
export function TierRadioGroup({
  offer,
  value,
  onChange,
  label,
  quota,
  currentTier = null,
}: TierRadioGroupProps) {
  const { t } = useTranslation();
  const options: RadioCardOption<ResourceTier>[] = offer.tiers.map((id) => {
    const capacity = tierCapacity(quota, id);
    return {
      value: id,
      title: tierLabel(t, id),
      facts: tierFacts(t, offer.specs.find((s) => s.tier === id)),
      // The current tier takes no new slot, so what is left of the quota
      // says nothing about it.
      note: id === currentTier ? t('computer.spec.current', 'Current') : blockNote(t, capacity),
      disabled: id !== currentTier && isTierBlocked(capacity),
    };
  });
  // A computer already bigger than Small holds the slot itself, and the rows
  // say what it can move to.
  const slot = isPlatformMode && (currentTier ?? 'standard') === 'standard' ? largerSlot(quota) : null;
  return (
    <div className="flex flex-col gap-1.5">
      <RadioCardGroup options={options} value={value} onChange={onChange} label={label} layout="rows" />
      {slot && (
        <p className="text-xs" data-testid="tier-slot" style={{ color: 'var(--color-text-tertiary)' }}>
          {slotLine(t, slot)}
        </p>
      )}
    </div>
  );
}

function blockNote(t: Translate, capacity: WorkspaceCapacity | null): string | null {
  const reason = tierBlockReason(capacity);
  if (reason === 'not_on_plan') return t('workspace.quotaNotOnPlan', 'Not on your plan');
  if (reason === 'used_up') return t('workspace.quotaNoneLeft', 'None left');
  return null;
}

function slotLine(t: Translate, slot: WorkspaceCapacity): string {
  const count = slot.limit;
  const remaining = Math.max(0, slot.limit - slot.used);
  const small = tierLabel(t, 'standard');
  if (remaining === 0) {
    return t('computer.largerSlot.inUse', "Your plan's computer larger than {{small}} is in use.", { count, small });
  }
  if (slot.used > 0) {
    return t('computer.largerSlot.partly', 'Your plan includes {{count}} computers larger than {{small}}, {{remaining}} left.', {
      count,
      remaining,
      small,
    });
  }
  return t('computer.largerSlot.open', 'Your plan includes {{count}} computer larger than {{small}}.', { count, small });
}

/**
 * vCPU, RAM and disk, from the provider's config rather than the catalogs,
 * so the row says what the machine will be. None while that is unknown:
 * a name with no numbers beats numbers that may be wrong.
 */
export function tierFacts(t: Translate, spec: ComputerTierSpec | undefined): string[] | undefined {
  if (!spec) return undefined;
  return [
    t('workspace.tierFact.cpu', '{{count}} vCPU', { count: spec.cpu }),
    t('workspace.tierFact.memory', '{{count}} GB RAM', { count: spec.memory_gib }),
    t('workspace.tierFact.disk', '{{count}} GB disk', { count: spec.disk_gib }),
  ];
}
