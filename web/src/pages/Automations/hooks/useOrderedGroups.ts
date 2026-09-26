import { useMemo } from 'react';
import type { Automation } from '@/types/automation';
import { automationGroup, GROUP_ORDER, lastRunAt, type AutomationGroup } from '../utils/status';

function lastRunMs(a: Automation): number {
  const at = lastRunAt(a);
  return at ? Date.parse(at) : 0;
}

function sortGroup(group: AutomationGroup, list: Automation[]): Automation[] {
  const byName = (a: Automation, b: Automation) => a.name.localeCompare(b.name);
  // A switched-off automation reaches these groups only once its failure is
  // dismissed, and then sorts after the live ones, as a paused one does.
  const stopped = (a: Automation) => (a.status === 'paused' || a.status === 'disabled' ? 1 : 0);
  switch (group) {
    case 'scheduled':
      return [...list].sort(
        (a, b) =>
          stopped(a) - stopped(b) ||
          (a.next_run_at ? Date.parse(a.next_run_at) : Infinity) - (b.next_run_at ? Date.parse(b.next_run_at) : Infinity) ||
          byName(a, b),
      );
    case 'watching':
      return [...list].sort((a, b) => stopped(a) - stopped(b) || byName(a, b));
    default:
      return [...list].sort((a, b) => lastRunMs(b) - lastRunMs(a) || byName(a, b));
  }
}

export interface OrderedGroup {
  group: AutomationGroup;
  items: Automation[];
}

export function useOrderedGroups(automations: Automation[]): OrderedGroup[] {
  return useMemo(() => {
    const buckets = new Map<AutomationGroup, Automation[]>();
    for (const a of automations) {
      const g = automationGroup(a);
      buckets.set(g, [...(buckets.get(g) ?? []), a]);
    }
    return GROUP_ORDER.filter((g) => buckets.has(g)).map((g) => ({ group: g, items: sortGroup(g, buckets.get(g)!) }));
  }, [automations]);
}
