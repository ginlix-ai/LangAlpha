import type { ThemePreference } from '../contexts/ThemeContext';
import type { UserPreferences } from '../types/api';
import { isCardStyle, type CardStyle } from './cardStyle';
import { readOtherPreference } from './otherPreference';
import { safeLocalStorage } from './utils';

/** The look a user picks, style and light or dark, follows them to every
 *  device: `other_preference.card_style` and `other_preference.theme`. Each
 *  browser keeps a copy for its first paint (lib/cardStyle, ThemeContext). */

const CARD_STYLE_KEY = 'card_style';
const THEME_KEY = 'theme';

export interface Appearance {
  cardStyle?: CardStyle;
  theme?: ThemePreference;
}

export function isThemePreference(value: unknown): value is ThemePreference {
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

/** A patch holding only the keys given. The server merges `other_preference`
 *  shallowly, so a key left out keeps whatever another device saved. */
export function appearancePatch(look: Appearance): Partial<UserPreferences> | null {
  const other: Record<string, unknown> = {};
  if (look.cardStyle) other[CARD_STYLE_KEY] = look.cardStyle;
  if (look.theme) other[THEME_KEY] = look.theme;
  return Object.keys(other).length > 0 ? { other_preference: other } : null;
}

/** The row with the look's keys set to the given values, and a key given none
 *  removed, as an account that never saved it reads. */
export function withAccountLook(row: UserPreferences, look: Appearance): UserPreferences {
  const other = { ...(row.other_preference as Record<string, unknown> | null | undefined) };
  for (const [key, value] of [
    [CARD_STYLE_KEY, look.cardStyle],
    [THEME_KEY, look.theme],
  ] as const) {
    if (value) other[key] = value;
    else delete other[key];
  }
  return { ...row, other_preference: other as UserPreferences['other_preference'] };
}

/**
 * Which of this browser's stored values an account put there. The stored copy
 * is written the same way whether someone picked it here or the page followed
 * an account, and signing out keeps it, so without this mark the next account
 * signed in here would be given the last one's look as if picked here. Kept in
 * localStorage beside the values, so every tab of this browser reads the same.
 */
const FROM_ACCOUNT_KEY = 'appearanceFromAccount';

export function keysFromAccount(): Set<keyof Appearance> {
  try {
    const parsed: unknown = JSON.parse(safeLocalStorage.getItem(FROM_ACCOUNT_KEY) ?? '[]');
    return new Set(
      Array.isArray(parsed) ? parsed.filter((k): k is keyof Appearance => k === 'cardStyle' || k === 'theme') : [],
    );
  } catch {
    return new Set();
  }
}

/** Records that the keys given now hold an account's value here. */
export function markFromAccount(look: Appearance): void {
  const keys = keysFromAccount();
  const before = keys.size;
  if (look.cardStyle) keys.add('cardStyle');
  if (look.theme) keys.add('theme');
  if (keys.size !== before) safeLocalStorage.setItem(FROM_ACCOUNT_KEY, JSON.stringify([...keys]));
}

/** Records that a key no longer holds an account's value here. */
export function unmarkFromAccount(key: keyof Appearance): void {
  const keys = keysFromAccount();
  if (keys.delete(key)) safeLocalStorage.setItem(FROM_ACCOUNT_KEY, JSON.stringify([...keys]));
}

/** The values given, less any an account put in this browser. */
export function ownPicks(look: Appearance): Appearance {
  const fromAccount = keysFromAccount();
  return {
    cardStyle: fromAccount.has('cardStyle') ? undefined : look.cardStyle,
    theme: fromAccount.has('theme') ? undefined : look.theme,
  };
}

/** What this browser gives an account that has not saved its look yet: each
 *  key the account lacks and the browser has a pick of its own for (ownPicks),
 *  so a look picked before the setting moved to the account is not lost. */
export function seedAppearancePatch(
  prefs: UserPreferences | null | undefined,
  browserPick: Appearance,
): Partial<UserPreferences> | null {
  return appearancePatch({
    cardStyle: readAccountCardStyle(prefs) ? undefined : browserPick.cardStyle,
    theme: readAccountTheme(prefs) ? undefined : browserPick.theme,
  });
}
