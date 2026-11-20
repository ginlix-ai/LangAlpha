import { useState } from 'react';
import type { Computer, ComputerProviderOption, ResourceTier, WorkspaceQuota } from '@/types/api';
import { TIER_ORDER, isTierBlocked, normalizeTier, offeredTiers, quotaForSpecChange, tierCapacity, type TierOffer } from './tierUi';
import { diskUpgradeTier, isDiskAlertLevel } from './computerDiskUi';
import { effectiveTier } from './specChangeUi';

type SpecTarget = Pick<Computer, 'resource_tier' | 'spec_change' | 'disk'>;

/**
 * The sizes a change-spec form offers `target` and the one it is on: the
 * user's pick, else `wanted` where it is larger and open, else the size a
 * full disk calls for, else the current one. For the dialog and for any
 * form that changes a computer's spec in its own surface.
 */
export function useTierPick(
  target: SpecTarget,
  provider: ComputerProviderOption | null | undefined,
  planQuota: WorkspaceQuota | null | undefined,
  wanted: ResourceTier | null,
) {
  const currentTier = effectiveTier(target);
  const offer = offeredTiers(provider, currentTier);
  // A computer bigger than Small moves in the slot it holds, so the other
  // larger sizes the plan allows stay open to it. The slot is the size the row
  // holds, which the server counts, even when an interrupted change shows the
  // size it came from.
  const quota = quotaForSpecChange(planQuota, normalizeTier(target.resource_tier));
  // Derived rather than seeded: the quota and the provider's sizes land after
  // the form opens and can refetch while it is open, and the row behind
  // `target` is replaced on every list refetch. A pick holds while the
  // provider offers it and the plan allows it (the current tier always does);
  // otherwise the seed stands.
  const [picked, setPicked] = useState<ResourceTier | null>(null);
  const tier =
    picked && offer.tiers.includes(picked) && (picked === currentTier || !isTierBlocked(tierCapacity(quota, picked)))
      ? picked
      : seedTier(currentTier, target, offer, quota, wanted);
  return { currentTier, offer, quota, tier, setPicked };
}

function seedTier(
  current: ResourceTier,
  target: SpecTarget,
  offer: TierOffer,
  quota: WorkspaceQuota | null | undefined,
  wanted: ResourceTier | null,
): ResourceTier {
  // Only above the size it is now: the row the request was read off can be
  // replaced by one already at or past it, which no request takes down.
  if (
    wanted &&
    TIER_ORDER.indexOf(wanted) > TIER_ORDER.indexOf(current) &&
    offer.tiers.includes(wanted) &&
    !isTierBlocked(tierCapacity(quota, wanted))
  ) {
    return wanted;
  }
  return (isDiskAlertLevel(target.disk?.level) && diskUpgradeTier(current, offer, quota)) || current;
}
