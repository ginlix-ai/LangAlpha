import { useEffect, useEffectEvent, useRef } from 'react';
import { useTheme } from '../contexts/ThemeContext';
import { readAccountCardStyle, readAccountTheme } from '../lib/appearancePreference';
import { getCardStyle, hasStoredCardStyle, setCardStyle } from '../lib/cardStyle';
import { usePreferences } from './usePreferences';
import { useUpdatePreferences } from './useUpdatePreferences';

/**
 * Brings this browser's look in line with the account's once preferences load,
 * and again whenever the account's value changes (another device, another
 * tab). Settings writes both sides itself, so its own save arrives here as a
 * value already in place. An account that has saved neither yet is given this
 * browser's, once, so a look picked before the setting moved to the account
 * is not lost.
 */
export function useSyncAppearance() {
  const { preferences, isLoaded } = usePreferences();
  const { preference: theme, setTheme } = useTheme();
  const { mutate } = useUpdatePreferences();
  const accountStyle = readAccountCardStyle(preferences);
  const accountTheme = readAccountTheme(preferences);
  const seeded = useRef(false);

  useEffect(() => {
    if (accountStyle && accountStyle !== getCardStyle()) setCardStyle(accountStyle);
  }, [accountStyle]);

  // Only the account's value moves the theme here, so the current one is read
  // without being a reason to run.
  const followAccountTheme = useEffectEvent((next: typeof accountTheme) => {
    if (next && next !== theme) setTheme(next);
  });
  useEffect(() => followAccountTheme(accountTheme), [accountTheme]);

  const seedAccount = useEffectEvent(() => {
    const other: Record<string, unknown> = {};
    if (!accountStyle && hasStoredCardStyle()) other.card_style = getCardStyle();
    if (!accountTheme) other.theme = theme;
    if (Object.keys(other).length > 0) mutate({ other_preference: other });
  });
  useEffect(() => {
    if (!isLoaded || seeded.current) return;
    seeded.current = true;
    seedAccount();
  }, [isLoaded]);
}
