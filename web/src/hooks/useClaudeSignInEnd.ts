import { useOAuthStatus } from '@/hooks/useConfiguredProviders';
import { useLocale } from '@/hooks/useLocale';
import { formatCalendarDay } from '@/lib/format';
import type { UserPreferences } from '@/types/api';
import { otherPreferencePatch, readOtherPreference } from '@/lib/otherPreference';

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

/**
 * When the Claude sign-in the user is signed in with stops working here: the
 * day the deployment set (`endsOn`) and that day as the user's locale
 * writes it. Null where the user is not signed in with it or the deployment
 * set no day.
 */
export function useClaudeSignInEnd(): { endsOn: string; date: string } | null {
  const { data } = useOAuthStatus('claude-oauth');
  const locale = useLocale();
  const endsOn = data?.connected ? data.ends_on : null;
  return endsOn ? { endsOn, date: formatCalendarDay(endsOn, locale) } : null;
}
