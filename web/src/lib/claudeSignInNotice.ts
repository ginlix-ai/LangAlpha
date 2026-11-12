import type { UserPreferences } from '../types/api';
import { otherPreferencePatch, readOtherPreference } from './otherPreference';

/** `other_preference.claude_sign_in_notice_dismissed`: the end date the user
 *  dismissed the Claude Code announcement for, so a later date asks again. */
const DISMISSED_KEY = 'claude_sign_in_notice_dismissed';

export function readSignInNoticeDismissed(prefs: UserPreferences | null | undefined): string | null {
  const raw = readOtherPreference(prefs, DISMISSED_KEY);
  return typeof raw === 'string' ? raw : null;
}

export function signInNoticeDismissedPatch(endsOn: string): Partial<UserPreferences> {
  return otherPreferencePatch(DISMISSED_KEY, endsOn);
}
