import type { UserPreferences } from '../types/api';
import { otherPreferencePatch, readOtherPreference } from './otherPreference';

const OWN_CONTEXT_KEY = 'own_context_in_shared_workspaces';

/** `other_preference.own_context_in_shared_workspaces`: whether the user's
 *  answer preferences and memory ride their own messages in a shared
 *  workspace. Absent means on, as the server reads it, so only an explicit
 *  false is off. */
export function readOwnContext(prefs: UserPreferences | null | undefined): boolean {
  return readOtherPreference(prefs, OWN_CONTEXT_KEY) !== false;
}

/** On deletes the key (null), since absent already means on. */
export function ownContextPatch(next: boolean): Partial<UserPreferences> {
  return otherPreferencePatch(OWN_CONTEXT_KEY, next ? null : false);
}
