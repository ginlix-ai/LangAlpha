/**
 * Whether the desktop app's first run has a question to ask: where the
 * user's agents work, this Mac or the cloud.
 *
 * Asked once per account and Mac, in the desktop app on a Mac that can pair
 * itself: while the account has never answered, and on a Mac where it has
 * not been through the first run, so a second Mac is asked too. The server
 * keeps one answer per account, which decides whether a cloud computer is
 * built, so which Macs have been asked is kept on each Mac. Everything before
 * the answer is held, since warming Home builds a cloud computer for a user
 * who has none and the answer may be that they want none. A browser, another
 * platform, a shell too old to pair, and a server that cannot say (an error)
 * are never held. Whether this Mac is already one of the account's computers
 * is the first run's own first read, so this module stays light on first load.
 */
import { useQuery } from '@tanstack/react-query';

import { getComputerChoice, type ComputerChoiceResponse } from '@/api/computerChoice';
import { canRemoteControl, isMacDesktop } from '@/lib/desktop';
import { queryKeys } from '@/lib/queryKeys';
import { useStorageUser, userLocalStorage } from '@/lib/userStorage';

/** `pending` while the answer is read; `unasked` while the account, or this Mac, has none. */
export type ComputerChoiceGate = 'none' | 'pending' | 'unasked';

const ASKED_HERE_KEY = 'thisMacFirstRun.asked';

/** Whether `user` has been through the first run in this app on this Mac. */
export function askedOnThisMac(user: string | null): boolean {
  return !!user && userLocalStorage.getItem(ASKED_HERE_KEY) === '1';
}

/** The first run is over for the signed-in user on this Mac, answered or with nothing to ask. */
export function markAskedOnThisMac(): void {
  userLocalStorage.setItem(ASKED_HERE_KEY, '1');
}

export function computerChoiceGate(
  eligible: boolean,
  read: { isPending: boolean; isError: boolean; data: ComputerChoiceResponse | undefined },
  askedHere: boolean,
): ComputerChoiceGate {
  if (!eligible || read.isError) return 'none';
  if (read.isPending || !read.data) return 'pending';
  return read.data.choice === null || !askedHere ? 'unasked' : 'none';
}

export function useComputerChoiceGate(): ComputerChoiceGate {
  const eligible = isMacDesktop() && canRemoteControl();
  const read = useQuery({
    queryKey: queryKeys.user.computerChoice(),
    queryFn: getComputerChoice,
    enabled: eligible,
    staleTime: Infinity,
    retry: false,
  });
  const askedHere = askedOnThisMac(useStorageUser());
  return computerChoiceGate(eligible, read, askedHere);
}
