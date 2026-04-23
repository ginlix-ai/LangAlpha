import { useMemo } from 'react';
import type { ProviderModelsData } from '@/components/model/types';

/**
 * Filter ``models`` down to the user's configured (starred) set for the
 * primary/flash picker. When the starred set is empty (fresh account),
 * passes through the full map so the picker isn't blank. Current picks are
 * always kept so a stale selection doesn't vanish mid-edit.
 *
 * Shared between DefaultsStep (setup wizard) and Settings — both render the
 * same picker with the same semantics.
 */
export function useStarredPickerModels(
  models: Record<string, ProviderModelsData>,
  starred: Iterable<string>,
  currentPicks: ReadonlyArray<string | null | undefined>,
): Record<string, ProviderModelsData> {
  // Stringify picks so identity changes in caller arrays don't re-fire the
  // memo when the actual pick values are equal.
  const picksKey = currentPicks.join('\0');
  return useMemo(() => {
    const starredArr = Array.from(starred);
    if (starredArr.length === 0) return models;
    const keep = new Set<string>(starredArr);
    for (const p of currentPicks) {
      if (p) keep.add(p);
    }
    const out: Record<string, ProviderModelsData> = {};
    for (const [key, data] of Object.entries(models)) {
      const filtered = (data.models ?? []).filter((m) => keep.has(m));
      if (filtered.length > 0) {
        out[key] = { ...data, models: filtered };
      }
    }
    return out;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [models, starred, picksKey]);
}
