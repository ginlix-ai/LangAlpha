import { useEffect, useEffectEvent, useRef } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { useTheme, type ThemePreference } from '../contexts/ThemeContext';
import {
  appearancePatch,
  isThemePreference,
  keysFromAccount,
  markFromAccount,
  ownPicks,
  readAccountCardStyle,
  readAccountTheme,
  seedAppearancePatch,
  unmarkFromAccount,
  withAccountLook,
  type Appearance,
} from '../lib/appearancePreference';
import {
  CARD_STYLE_STORAGE_KEY,
  getCardStyle,
  isCardStyle,
  restoreCardStyle,
  setCardStyle,
  storedCardStyle,
  useCardStyle,
  type CardStyle,
} from '../lib/cardStyle';
import { queryKeys } from '../lib/queryKeys';
import type { UserPreferences } from '../types/api';
import { usePreferences } from './usePreferences';
import { useUpdatePreferences } from './useUpdatePreferences';

/** Picks saved back to back, from the first until the last has settled. */
interface PickRun {
  pending: number;
  failed: boolean;
  /** What the account holds: its values when the run began, then each pick
   *  the server took. */
  account: Appearance;
  /** What the page showed when the run began, for a key the account lacks. */
  shown: Required<Appearance>;
  /** What this browser stored when the run began: its own style, if any, and
   *  which keys an account put there. */
  stored: { cardStyle: CardStyle | null; fromAccount: Set<keyof Appearance> };
}

/**
 * The look as Settings shows and picks it: the account's value, else this
 * browser's. A pick restyles the page at once and writes the account, whose
 * write moves the cached row itself (useUpdatePreferences), so the control
 * reads the cache. The returned promise rejects with the save, for the caller
 * to report.
 *
 * A failed save is put right here, once the last pick of a run has settled,
 * from what the server took rather than from the cache: a rollback restores
 * its own snapshot, which holds the optimistic value of an earlier pick still
 * saving then, and the refetch that would correct it can fail too. The page
 * is put back here as well, rather than left to useSyncAppearance: a save that
 * fails before the optimistic row is ever rendered changes nothing that hook
 * can see. So is what the browser stores, for a key the account lacks: a pick
 * writes the browser's copy and the optimistic row marks it as the account's,
 * and left so, the next load would either offer the account a style nobody
 * picked here or drop one somebody did.
 */
export function useAppearance() {
  const { preferences } = usePreferences();
  const { preference: browserTheme, setTheme } = useTheme();
  const browserStyle = useCardStyle();
  const { mutateAsync } = useUpdatePreferences();
  const queryClient = useQueryClient();
  const accountStyle = readAccountCardStyle(preferences);
  const accountTheme = readAccountTheme(preferences);
  const cardStyle = accountStyle ?? browserStyle;
  const theme = accountTheme ?? browserTheme;
  const run = useRef<PickRun | null>(null);

  const pick = async (look: Appearance): Promise<void> => {
    const patch = appearancePatch(look);
    if (!patch) return;
    const current = (run.current ??= {
      pending: 0,
      failed: false,
      account: { cardStyle: accountStyle, theme: accountTheme },
      shown: { cardStyle, theme },
      stored: { cardStyle: storedCardStyle(), fromAccount: keysFromAccount() },
    });
    current.pending += 1;
    if (look.cardStyle) setCardStyle(look.cardStyle);
    if (look.theme) setTheme(look.theme);
    try {
      await mutateAsync(patch);
      if (look.cardStyle) current.account.cardStyle = look.cardStyle;
      if (look.theme) current.account.theme = look.theme;
    } catch (error) {
      current.failed = true;
      throw error;
    } finally {
      current.pending -= 1;
      if (current.pending === 0) {
        run.current = null;
        if (current.failed) {
          const { account, shown, stored } = current;
          if (account.cardStyle) setCardStyle(account.cardStyle);
          else restoreCardStyle(shown.cardStyle, stored.cardStyle);
          // ThemeContext stores whatever it shows, so the theme needs no more:
          // for a key the account lacks, what it showed is what it stored.
          setTheme(account.theme ?? shown.theme);
          // Marked as the account's only where it was at the start or the
          // server took it; the optimistic row marked every key picked.
          const keep = (key: keyof Appearance) => Boolean(account[key]) || stored.fromAccount.has(key);
          markFromAccount({
            cardStyle: keep('cardStyle') ? shown.cardStyle : undefined,
            theme: keep('theme') ? shown.theme : undefined,
          });
          if (!keep('cardStyle')) unmarkFromAccount('cardStyle');
          if (!keep('theme')) unmarkFromAccount('theme');
          queryClient.setQueryData<UserPreferences>(
            queryKeys.user.preferences(),
            (row) => row && withAccountLook(row, account),
          );
        }
      }
    }
  };

  return {
    cardStyle,
    theme,
    pickStyle: (style: CardStyle) => pick({ cardStyle: style === cardStyle ? undefined : style }),
    pickTheme: (mode: ThemePreference) => pick({ theme: mode === theme ? undefined : mode }),
  };
}

/**
 * Mounted once while signed in (AppearanceSync in App, which every signed-in
 * page renders under, setup and welcome included): brings this browser's look
 * in line with the account's once preferences load, and again whenever the
 * account's value changes (another device, a save rolled back, a reset). An
 * account that has saved neither yet is given this browser's picks, once.
 *
 * Only a value picked here counts as a pick. A value the page followed from
 * an account is marked so (markFromAccount): signing out keeps the look on the
 * page until the next account's loads, and without the mark that account
 * would be given the last one's look for good. An account with no value for a
 * key takes back a followed one, which returns to the default, so a reset (or
 * someone else signing in) shows here what every other device shows. A value
 * picked here and never followed stays, and is offered.
 *
 * A theme counts as picked only when it is light or dark. ThemeContext stores
 * the preference on every load, so a browser nobody ever picked on holds
 * `auto` too, and giving that to the account would replace the dark or light
 * another device picked with the default.
 */
export function useSyncAppearance() {
  const { preferences, isLoaded } = usePreferences();
  const { preference: theme, setTheme } = useTheme();
  const { mutate } = useUpdatePreferences();
  const accountStyle = readAccountCardStyle(preferences);
  const accountTheme = readAccountTheme(preferences);
  const seeded = useRef(false);
  const queryClient = useQueryClient();

  // Another tab's pick reaches this one through storage, where the card style
  // store and ThemeContext apply it. The cached account row moves with it for
  // a key the account holds, or Settings here would show, and compare picks
  // against, the old value until a refetch. A save that fails there writes
  // storage again, which brings this row back with it.
  useEffect(() => {
    const onStorage = (e: StorageEvent) => {
      let key: keyof Appearance;
      let value: CardStyle | ThemePreference | undefined;
      if (e.key === CARD_STYLE_STORAGE_KEY) {
        key = 'cardStyle';
        value = isCardStyle(e.newValue) ? e.newValue : undefined;
      } else if (e.key === 'theme') {
        key = 'theme';
        value = isThemePreference(e.newValue) ? e.newValue : undefined;
      } else return;
      queryClient.setQueryData<UserPreferences>(queryKeys.user.preferences(), (row) => {
        if (!row) return row;
        const held: Appearance = { cardStyle: readAccountCardStyle(row), theme: readAccountTheme(row) };
        if (!held[key]) return row;
        return withAccountLook(row, { ...held, [key]: value });
      });
    };
    window.addEventListener('storage', onStorage);
    return () => window.removeEventListener('storage', onStorage);
  }, [queryClient]);

  // Ahead of the follow effects, which run in order in the same commit: the
  // seed reads the theme this render shows, so it has to read the marks before
  // a followed theme is taken back and unmarked.
  // A seed that fails leaves its picks this browser's own: the optimistic row
  // marked them as the account's, and the rollback would then take them back.
  const seedAccount = useEffectEvent(() => {
    const picks = ownPicks({
      cardStyle: storedCardStyle() ?? undefined,
      theme: theme === 'auto' ? undefined : theme,
    });
    const patch = seedAppearancePatch(preferences, picks);
    if (!patch) return;
    mutate(patch, {
      onError: () => {
        if (picks.cardStyle && !accountStyle) unmarkFromAccount('cardStyle');
        if (picks.theme && !accountTheme) unmarkFromAccount('theme');
      },
    });
  });
  useEffect(() => {
    if (!isLoaded || seeded.current) return;
    seeded.current = true;
    seedAccount();
  }, [isLoaded]);

  useEffect(() => {
    if (!isLoaded) return;
    if (accountStyle) {
      markFromAccount({ cardStyle: accountStyle });
      if (accountStyle !== getCardStyle()) setCardStyle(accountStyle);
    } else if (keysFromAccount().has('cardStyle')) {
      restoreCardStyle('balanced', null);
      unmarkFromAccount('cardStyle');
    }
  }, [accountStyle, isLoaded]);

  // Only the account's value moves the theme here, so the current one is read
  // without being a reason to run.
  const followAccountTheme = useEffectEvent((next: typeof accountTheme) => {
    if (next) {
      markFromAccount({ theme: next });
      if (next !== theme) setTheme(next);
    } else if (keysFromAccount().has('theme')) {
      setTheme('auto');
      unmarkFromAccount('theme');
    }
  });
  useEffect(() => {
    if (isLoaded) followAccountTheme(accountTheme);
  }, [accountTheme, isLoaded]);
}
