/**
 * Reading and writing the model-configuration bag.
 *
 * Model configuration has its own `model_preference` column. The legacy
 * `other_preference` is still read underneath so a tab open across the deploy,
 * or a rollback to before that column, keeps resolving; the server does the
 * same on its side. Drop the fallback with the server's.
 */

import type { CustomModelEntry } from '@/components/model/types';
import type { ModelProfile } from './modelTuning';

export interface PreferencesLike {
  other_preference?: Record<string, unknown> | null;
  model_preference?: Record<string, unknown> | null;
}

/** A sub-provider the user defined, as stored. */
export interface CustomProviderEntry {
  name: string;
  parent_provider?: string;
  use_response_api?: boolean;
}

/**
 * The stored model bag: catalog, routing, and the account-wide tuning a model
 * inherits when its own profile is silent.
 *
 * Extends `ModelProfile` because the four tuning fields are the same settings
 * at the account layer that a profile overrides per model.
 */
export interface ModelPreferences extends ModelProfile {
  preferred_model?: string | null;
  preferred_flash_model?: string | null;
  compaction_model?: string | null;
  summarization_model?: string | null;
  fetch_model?: string | null;
  fallback_models?: string[] | null;
  custom_models?: CustomModelEntry[] | null;
  custom_providers?: CustomProviderEntry[] | null;
  // A per-key `null` is how one model's overrides are dropped, the same delete
  // the server applies (`profiles[<model>]: None`), so the value side is
  // nullable as well as the whole map.
  profiles?: Record<string, ModelProfile | null> | null;
}

/**
 * The keys migration 034 moved out of `other_preference`, and so the only ones
 * that may still be found there on a row written before the move.
 *
 * The twin of `MOVED_MODEL_KEYS` in `src/llms/preferences.py`. Narrower than
 * `MODEL_PREF_KEYS` below on purpose: that one routes a write, this one names
 * what has a pre-move copy at all.
 */
const MOVED_KEYS: readonly (keyof ModelPreferences)[] = [
  'preferred_model',
  'preferred_flash_model',
  'compaction_model',
  'summarization_model',
  'fetch_model',
  'fallback_models',
  'custom_models',
  'custom_providers',
  'compaction_profile',
  'reasoning_effort',
  'fast_mode',
];

export const MOVED_MODEL_KEYS: ReadonlySet<string> = new Set<string>(MOVED_KEYS);

/**
 * Which column each preference key is written to.
 *
 * One table rather than a comment repeated at every write site: a key put in
 * the wrong column still reads back today, because the reader merges both, and
 * only vanishes the day the legacy fallback above is dropped. `starred_models`,
 * `search_provider` and `search_depth` are the model-adjacent keys that stay in
 * `other_preference` by design, so they are absent here on purpose.
 */
export const MODEL_PREF_KEYS: ReadonlySet<string> = new Set<string>([
  ...MOVED_KEYS,
  // Neither ever lived in `other_preference`: `profiles` is new, and
  // `prompt_guidance` appears there only if 034's downgrade puts it back. They
  // route to the model column without having a copy to read underneath.
  'profiles',
  'prompt_guidance',
]);

/**
 * The model bag, new column over legacy. Never null, so call sites can index it.
 *
 * The twin of `get_model_preference` in
 * `src/server/services/llm/user_models.py`: the legacy column is read
 * underneath, but only for the keys that moved out of it. Spreading the two
 * bags whole pulled in keys other services write to that column. Clearing a
 * key clears both columns at the write, so absence in the new column is the
 * only thing that reaches the copy underneath. The server's bag also carries
 * the three keys that never moved, because its resolver reads them; here every
 * reader of those takes them off `other_preference` itself.
 */
export function modelPrefs(prefs: unknown): ModelPreferences {
  const p = (prefs ?? {}) as PreferencesLike;
  const current = p.model_preference ?? {};
  const merged: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(p.other_preference ?? {})) {
    if (MOVED_MODEL_KEYS.has(key) && !(key in current)) merged[key] = value;
  }
  for (const [key, value] of Object.entries(current)) {
    if (value !== null && value !== undefined) merged[key] = value;
  }
  return merged as ModelPreferences;
}

/**
 * One model's tuning overrides (`model_preference.profiles[<model>]`).
 *
 * Only ever in the new column: `profiles` never lived in the legacy one, so
 * there is nothing to fall back to. An absent or malformed entry reads as no
 * overrides.
 */
export function modelProfile(prefs: unknown, model: string | null | undefined): ModelProfile {
  if (!model) return {};
  const profiles = (prefs as PreferencesLike | null)?.model_preference?.profiles;
  if (typeof profiles !== 'object' || profiles === null) return {};
  const entry = (profiles as Record<string, unknown>)[model];
  return typeof entry === 'object' && entry !== null ? (entry as ModelProfile) : {};
}

/** A flat patch of preference keys, whichever column they end up in. */
export type PreferencePatch = ModelPreferences & Record<string, unknown>;

/**
 * Route a flat patch to the column each key belongs to.
 *
 * Callers write what they mean (`{ preferred_model, starred_models }`) and this
 * decides where it lands, so the split is stated once instead of hand-rolled
 * differently at each write site. A column with nothing in it is omitted rather
 * than sent empty: the server merges what it receives, and an empty object is
 * still a write.
 */
export function splitPreferenceWrite(patch: PreferencePatch): {
  model_preference?: Record<string, unknown>;
  other_preference?: Record<string, unknown>;
} {
  const model: Record<string, unknown> = {};
  const other: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(patch)) {
    (MODEL_PREF_KEYS.has(key) ? model : other)[key] = value;
  }
  return {
    ...(Object.keys(model).length > 0 ? { model_preference: model } : {}),
    ...(Object.keys(other).length > 0 ? { other_preference: other } : {}),
  };
}
