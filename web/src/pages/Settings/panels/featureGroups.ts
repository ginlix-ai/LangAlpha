import type { FeatureCategory, FeatureState } from '@/types/api';

/** A feature the user sets in Experiments: its gate takes their override. */
export type ExperimentFeature = FeatureState & { gate: 'opt_in' | 'opt_out' };

export type GroupKey = FeatureCategory | 'other';

/** The order the groups are listed in. A category this build does not know
 *  yet lands in `other`, at the end, rather than being dropped. */
const GROUP_ORDER: readonly GroupKey[] = ['chat', 'tools', 'autonomy', 'long_threads', 'dashboard', 'other'];

export interface ExperimentRow {
  feature: ExperimentFeature;
  /** Listed under a feature it requires, in the same group. */
  nested: boolean;
  /** A feature it requires that is off, which holds this one off too. */
  blockedBy: FeatureState | null;
}

export interface ExperimentGroup {
  key: GroupKey;
  rows: ExperimentRow[];
}

/** Experiments' list from every feature the API serves: the ones the user
 *  sets here, in their groups, each group in catalog order with a feature
 *  that requires another in its group listed right under it. */
export function experimentGroups(features: readonly FeatureState[]): ExperimentGroup[] {
  const byKey = new Map(features.map((f) => [f.key, f]));
  const members = new Map<GroupKey, ExperimentFeature[]>();
  for (const feature of features) {
    if (!isExperiment(feature)) continue;
    const key = GROUP_ORDER.find((g) => g === feature.category) ?? 'other';
    members.set(key, [...(members.get(key) ?? []), feature]);
  }
  return GROUP_ORDER.flatMap((key) => {
    const group = members.get(key);
    if (!group) return [];
    const keys = new Set(group.map((f) => f.key));
    const parentOf = (f: FeatureState) => f.requires?.find((parent) => keys.has(parent));
    const rows: ExperimentRow[] = [];
    const place = (feature: ExperimentFeature, nested: boolean) => {
      const blockedBy = feature.requires
        ?.map((k) => byKey.get(k))
        .find((parent): parent is FeatureState => parent !== undefined && !parent.enabled);
      rows.push({ feature, nested, blockedBy: blockedBy ?? null });
      for (const child of group) if (parentOf(child) === feature.key) place(child, true);
    };
    for (const feature of group) if (!parentOf(feature)) place(feature, false);
    return [{ key, rows }];
  });
}

/** A feature turned on elsewhere (`home`) is not offered here as well. */
function isExperiment(feature: FeatureState): feature is ExperimentFeature {
  return (feature.gate === 'opt_in' || feature.gate === 'opt_out') && !feature.home;
}
