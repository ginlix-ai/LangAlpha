import type { UserPreferences } from '../types/api';
import { otherPreferencePatch, readOtherPreference } from './otherPreference';

const SUBAGENTS_DEFAULT_KEY = 'subagents_default';

/** `other_preference.subagents_default`: what every thread follows until its
 *  own switch is set. Absent means on, as the server reads it, so only an
 *  explicit false is off. */
export function readSubagentsDefault(prefs: UserPreferences | null | undefined): boolean {
  return readOtherPreference(prefs, SUBAGENTS_DEFAULT_KEY) !== false;
}

export function subagentsDefaultPatch(next: boolean): Partial<UserPreferences> {
  return otherPreferencePatch(SUBAGENTS_DEFAULT_KEY, next);
}
