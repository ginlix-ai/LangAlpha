import { useLocale } from '@/hooks/useLocale';
import { useUser } from '@/hooks/useUser';
import { useFeatureEnabled, useFeatures } from '@/hooks/useFeatures';

/**
 * CN news eligibility: the `a_share_pack` feature enabled AND a zh locale.
 * Mirrors the server-side guard in app/news.py — the widget targets
 * provider=tushare only when both signals hold; the backend re-checks and
 * falls back regardless.
 */
export function isCnNewsEligible(
  locale: string | null | undefined,
  packEnabled: boolean,
): boolean {
  if (!packEnabled) return false;
  return (locale ?? '').toLowerCase().startsWith('zh');
}

/**
 * Null while the answer waits on the user record (its stored locale
 * outranks the UI language) or, for a zh user, on the feature flags. Reading
 * either pending state as false sent every cold load to the default feed first
 * and then to the CN one; a settled non-zh user is ineligible whatever the
 * flags say.
 */
export function useCnNewsEligibility(): boolean | null {
  const { user, isLoading: userLoading } = useUser();
  const locale = useLocale();
  const packEnabled = useFeatureEnabled('a_share_pack');
  const { isPending: flagsPending } = useFeatures();
  // `users.locale` is only ever written by the Settings dropdown, so a null
  // means "never chosen", not "not Chinese" — the active UI language stands in.
  const effectiveLocale = user?.locale || locale;
  if (userLoading) return null;
  if (flagsPending && isCnNewsEligible(effectiveLocale, true)) return null;
  return isCnNewsEligible(effectiveLocale, packEnabled);
}
