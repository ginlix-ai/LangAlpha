import { useEffect } from 'react';
import { useTranslation } from 'react-i18next';
import { useUser } from './useUser';

const SUPPORTED = new Set(['en-US', 'zh-CN']);

/**
 * Apply the user's DB-stored locale on app load so cross-device language
 * preference sticks (i18n's detector only sees localStorage + navigator).
 */
export function useSyncUserLocale() {
  const { user } = useUser();
  const { i18n } = useTranslation();

  useEffect(() => {
    const stored = user?.locale as string | undefined;
    if (!stored || !SUPPORTED.has(stored)) return;
    if (i18n.language === stored) return;
    i18n.changeLanguage(stored);
    if (typeof localStorage !== 'undefined') {
      localStorage.setItem('locale', stored);
    }
  }, [user?.locale, i18n]);
}
