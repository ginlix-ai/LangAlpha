import { useEffect, useEffectEvent, useRef } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { useTheme, type ThemePreference } from '../contexts/ThemeContext';
import {
  appearancePatch,
  markFromAccount,
  ownPicks,
  readAccountCardStyle,
  readAccountTheme,
  seedAppearancePatch,
  withAccountLook,
  type Appearance,
} from '../lib/appearancePreference';
import { getCardStyle, hasStoredCardStyle, setCardStyle, useCardStyle, type CardStyle } from '../lib/cardStyle';
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
 * can see.
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
          const { account, shown } = current;
          setCardStyle(account.cardStyle ?? shown.cardStyle);
          setTheme(account.theme ?? shown.theme);
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
 * Mounted once for the app: brings this browser's look in line with the
 * account's once preferences load, and again whenever the account's value
 * changes (another device, another tab, a save rolled back). An account that
 * has saved neither yet is given this browser's picks, once.
 *
 * Only a value picked here counts as a pick. A value the page followed from
 * an account is marked so (markFromAccount) and stays marked: signing out
 * keeps the look on the page until the next account's loads, and without the
 * mark that account would be given the last one's look for good.
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

  useEffect(() => {
    if (!accountStyle) return;
    markFromAccount({ cardStyle: accountStyle });
    if (accountStyle !== getCardStyle()) setCardStyle(accountStyle);
  }, [accountStyle]);

  // Only the account's value moves the theme here, so the current one is read
  // without being a reason to run.
  const followAccountTheme = useEffectEvent((next: typeof accountTheme) => {
    if (!next) return;
    markFromAccount({ theme: next });
    if (next !== theme) setTheme(next);
  });
  useEffect(() => followAccountTheme(accountTheme), [accountTheme]);

  const seedAccount = useEffectEvent(() => {
    const patch = seedAppearancePatch(preferences, ownPicks({
      cardStyle: hasStoredCardStyle() ? getCardStyle() : undefined,
      theme: theme === 'auto' ? undefined : theme,
    }));
    if (patch) mutate(patch);
  });
  useEffect(() => {
    if (!isLoaded || seeded.current) return;
    seeded.current = true;
    seedAccount();
  }, [isLoaded]);
}
