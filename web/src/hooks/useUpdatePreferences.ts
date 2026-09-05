import { useMutation, useQueryClient } from '@tanstack/react-query';
import { MOVED_MODEL_KEYS } from '../lib/modelPreferences';
import { queryKeys } from '../lib/queryKeys';
import { updatePreferences } from '../pages/Dashboard/utils/api';
import type { UserPreferences } from '../types/api';

/** Every preference write shares one mutation identity, so a save indicator can
 *  watch the write rather than the component that issued it. */
export const PREFERENCE_MUTATION_KEY = ['user-preferences'] as const;

/** Columns the server merges recursively (`jsonb_deep_merge`). Only
 *  `model_preference` does: its `profiles` bag is keyed by model name, so a
 *  patch for one model has to leave its siblings standing. */
const DEEP_COLUMNS = new Set(['model_preference']);

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/** Mirrors `jsonb_deep_merge`: a null deletes its key at whatever depth it
 *  appears, two objects merge, anything else replaces. */
function deepMerge(
  base: Record<string, unknown>,
  patch: Record<string, unknown>,
): Record<string, unknown> {
  const merged = { ...base };
  for (const [key, value] of Object.entries(patch)) {
    if (value === null) delete merged[key];
    else if (isObject(merged[key]) && isObject(value)) merged[key] = deepMerge(merged[key], value);
    else merged[key] = value;
  }
  return merged;
}

/** Mirrors `(existing - nulls) || updates` for the columns whose callers
 *  rewrite nested bags whole: `other_preference.feature_overrides` shrinks by
 *  exactly that shallow write. */
function shallowMerge(
  base: Record<string, unknown>,
  patch: Record<string, unknown>,
): Record<string, unknown> {
  const merged = { ...base };
  for (const [key, value] of Object.entries(patch)) {
    if (value === null) delete merged[key];
    else merged[key] = value;
  }
  return merged;
}

/** The clear the server performs alongside ours: a moved key dropped from
 *  `model_preference` is dropped from its pre-move copy too, or the reader
 *  answers the next question with the value we just cleared. */
function withMovedDeletes(patch: Partial<UserPreferences>): Partial<UserPreferences> {
  const model = patch.model_preference;
  if (!isObject(model)) return patch;
  const cleared = Object.keys(model).filter((k) => model[k] === null && MOVED_MODEL_KEYS.has(k));
  if (cleared.length === 0) return patch;
  const legacy = isObject(patch.other_preference) ? patch.other_preference : {};
  return {
    ...patch,
    other_preference: {
      ...Object.fromEntries(cleared.map((k) => [k, null])),
      ...legacy,
    } as UserPreferences['other_preference'],
  };
}

/** What the row will look like once the server has applied this patch. */
function mergePreferences(
  previous: UserPreferences,
  patch: Partial<UserPreferences>,
): UserPreferences {
  const next = { ...previous } as Record<string, unknown>;
  for (const [column, value] of Object.entries(withMovedDeletes(patch))) {
    if (!isObject(value)) {
      next[column] = value;
      continue;
    }
    const base = isObject(next[column]) ? next[column] : {};
    next[column] = DEEP_COLUMNS.has(column) ? deepMerge(base, value) : shallowMerge(base, value);
  }
  return next as UserPreferences;
}

/**
 * Mutation hook for updating user preferences.
 *
 * The cache moves on `onMutate`, not on the response: every control that writes
 * a preference reads it back from this one entry, so a control that mirrored
 * the click in its own state would keep showing a value a failed write never
 * stored, and a scheduled turn would run the stored one. Rolling back here is
 * what lets those controls stay derived.
 *
 * `scope` serializes writes against the single entry. Two in flight at once
 * finish in either order, and the earlier-issued one landing last would replace
 * the whole entry with a snapshot that predates the newer write.
 */
export function useUpdatePreferences() {
  const queryClient = useQueryClient();
  const key = queryKeys.user.preferences();
  return useMutation({
    mutationKey: PREFERENCE_MUTATION_KEY,
    scope: { id: 'user-preferences' },
    mutationFn: updatePreferences as (prefs: Partial<UserPreferences>) => Promise<UserPreferences>,
    onMutate: async (patch: Partial<UserPreferences>) => {
      await queryClient.cancelQueries({ queryKey: key });
      const previous = queryClient.getQueryData<UserPreferences>(key);
      if (previous) queryClient.setQueryData(key, mergePreferences(previous, patch));
      return { previous };
    },
    // Deliberately no onSuccess write. `scope` serializes the requests but not
    // `onMutate`, so every queued write has already moved the cache by the time
    // the first response lands; writing that response over the entry drops the
    // later patches until each one lands in turn. A list-valued control that
    // recomputes from the cache inside that window (fallback models, starred)
    // would send a list with the newer edit missing, which is a permanent loss
    // rather than a flicker.
    onSettled: () => {
      // Only the last write in flight can know the cache and the row agree.
      // Inside onSettled this mutation is still counted, so 1 means it is last.
      if (queryClient.isMutating({ mutationKey: PREFERENCE_MUTATION_KEY }) === 1) {
        queryClient.invalidateQueries({ queryKey: key });
      }
    },
    onError: (_error, _patch, context) => {
      // Same guard, same reason: `previous` is this mutation's snapshot, so
      // restoring it over a queued write's optimistic patch undoes an edit the
      // user just made and the server never rejected. With something newer in
      // flight, leave the cache standing and let the last settle reconcile it.
      const last = queryClient.isMutating({ mutationKey: PREFERENCE_MUTATION_KEY }) === 1;
      if (context?.previous && last) queryClient.setQueryData(key, context.previous);
    },
  });
}
