import { useEffect, useRef } from 'react';
import { useTranslation } from 'react-i18next';
import { useUser } from './useUser';
import { getLocaleCookie, isSupported, setLocaleCookie } from '../lib/locale';

/**
 * Seed the locale from the user's DB value on first load — but only when there's
 * no `locale` cookie yet (a browser-level cookie choice wins, and the cookie is
 * what servers can read). Latched so a later /users/me refetch can't clobber a
 * value the user just picked. Writing the cookie makes the DB preference
 * server-readable on a fresh device.
 */
export function useSyncUserLocale() {
  const { user } = useUser();
  const { i18n } = useTranslation();
  const synced = useRef(false);

  useEffect(() => {
    if (synced.current) return;
    const stored = user?.locale as string | undefined;
    if (!isSupported(stored)) return;
    synced.current = true; // latch even if no-op, so a later refetch can't trigger sync
    if (getLocaleCookie()) return; // browser cookie wins; already applied at init
    if (i18n.language !== stored) i18n.changeLanguage(stored);
    setLocaleCookie(stored);
  }, [user?.locale, i18n]);
}
