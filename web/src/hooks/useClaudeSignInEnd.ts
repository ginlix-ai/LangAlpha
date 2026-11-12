import type { SignInLifecycle } from '@/api/oauth';
import { useOAuthStatus } from '@/hooks/useConfiguredProviders';
import { useLocale } from '@/hooks/useLocale';
import { formatCalendarDay } from '@/lib/format';

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

/** Where the Claude sign-in stands here, whether or not the user is signed in
 *  with it: once the deployment set an end day, every place that shows it
 *  marks it legacy. */
export function useClaudeSignInLifecycle(): SignInLifecycle {
  return useOAuthStatus('claude-oauth').data?.lifecycle ?? 'active';
}
