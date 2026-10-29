import type { UserPreferences } from '../types/api';

/** One key of `other_preference`; undefined when the bag is missing or not an object. */
export function readOtherPreference(prefs: UserPreferences | null | undefined, key: string): unknown {
  const other = prefs?.other_preference;
  return typeof other === 'object' && other !== null && !Array.isArray(other)
    ? (other as Record<string, unknown>)[key]
    : undefined;
}

/** A patch holding the one key. The server merges `other_preference`
 *  shallowly, so sending the key alone never writes a stale cached sibling
 *  back over another tab's newer value. */
export function otherPreferencePatch(key: string, value: unknown): Partial<UserPreferences> {
  return { other_preference: { [key]: value } };
}
