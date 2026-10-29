import { useQueryClient, type QueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { authSessionCheck } from '@/lib/authResets';
import { getLocaleCookie, isSupported, setLocaleCookie, switchLocale, type Locale } from '@/lib/locale';
import { queryKeys } from '@/lib/queryKeys';
import { updateCurrentUser } from '@/pages/Dashboard/utils/api';
import { useUser } from './useUser';

/**
 * Put the app in a language the user picked, on this page and the next load:
 * the strings switch once the catalog is in (see switchLocale), and the cookie
 * is what the next load starts from. Every place a language is picked goes
 * through here, so the two never happen in a different order.
 */
export function applyLocale(i18n: Parameters<typeof switchLocale>[0], locale: Locale): Promise<void> {
  const switched = switchLocale(i18n, locale);
  setLocaleCookie(locale);
  return switched;
}

// The last save asked for, which the next one waits on.
let saving: Promise<void> = Promise.resolve();

/**
 * The account's copy of the language, which is what the agent is told to
 * write in. Best-effort: a failed save changes nothing on screen, and the next
 * pick (or Settings) writes it again. Saves go out one at a time, in the order
 * they were asked for: two in flight at once can land in either order, and the
 * account would keep an earlier pick than the one on screen. A save still
 * waiting its turn at a sign-out or account switch is dropped: sent after it,
 * it would carry the next account's token.
 */
function saveAccountLocale(queryClient: QueryClient, locale: Locale): Promise<void> {
  const sameSession = authSessionCheck();
  saving = saving.then(async () => {
    if (!sameSession()) return;
    try {
      await updateCurrentUser({ locale });
      void queryClient.invalidateQueries({ queryKey: queryKeys.user.me() });
    } catch {
      // The switch stands without it.
    }
  });
  return saving;
}

/**
 * A language picked from a quick control such as the onboarding bar's: the
 * app switches at once and the account keeps it. Settings saves the language
 * with the rest of its form instead, so it calls `applyLocale` alone.
 */
export function useChooseLocale(): (locale: Locale) => void {
  const { i18n } = useTranslation();
  const queryClient = useQueryClient();
  return (locale) => {
    void applyLocale(i18n, locale);
    void saveAccountLocale(queryClient, locale);
  };
}

/**
 * Saves the language the app is in to the account when the account holds a
 * different one, or none: a user who never picks one is still in the language
 * their browser or system asked for, and the agent should write in it too.
 * Does nothing until the account has loaded, since an unknown stored value is
 * not a different one.
 */
export function useKeepAccountLocale(): () => void {
  const { user } = useUser();
  const { i18n } = useTranslation();
  const queryClient = useQueryClient();
  return () => {
    if (!user) return;
    // The cookie first: a pick sets it at once, while the language itself
    // changes only once its catalog is in.
    const active = getLocaleCookie() ?? i18n.language;
    if (!isSupported(active) || user.locale === active) return;
    void saveAccountLocale(queryClient, active);
  };
}
