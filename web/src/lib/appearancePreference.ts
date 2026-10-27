import type { ThemePreference } from '../contexts/ThemeContext';
import type { UserPreferences } from '../types/api';
import { isCardStyle, type CardStyle } from './cardStyle';
import { otherPreferencePatch, readOtherPreference } from './otherPreference';

/** The look a user picks, style and light or dark, follows them to every
 *  device: `other_preference.card_style` and `other_preference.theme`. Each
 *  browser keeps a copy for its first paint (lib/cardStyle, ThemeContext). */

const CARD_STYLE_KEY = 'card_style';
const THEME_KEY = 'theme';

function isThemePreference(value: unknown): value is ThemePreference {
  return value === 'light' || value === 'dark' || value === 'auto';
}

/** Undefined when the account has not saved one, so the browser's copy stands. */
export function readAccountCardStyle(prefs: UserPreferences | null | undefined): CardStyle | undefined {
  const value = readOtherPreference(prefs, CARD_STYLE_KEY);
  return isCardStyle(value) ? value : undefined;
}

export function readAccountTheme(prefs: UserPreferences | null | undefined): ThemePreference | undefined {
  const value = readOtherPreference(prefs, THEME_KEY);
  return isThemePreference(value) ? value : undefined;
}

export function cardStylePatch(style: CardStyle): Partial<UserPreferences> {
  return otherPreferencePatch(CARD_STYLE_KEY, style);
}

export function themePatch(theme: ThemePreference): Partial<UserPreferences> {
  return otherPreferencePatch(THEME_KEY, theme);
}
