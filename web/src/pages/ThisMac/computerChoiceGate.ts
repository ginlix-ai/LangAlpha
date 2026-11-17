/**
 * Whether the desktop app's first run has a question to ask: where the
 * user's agents work, this Mac or the cloud.
 *
 * Asked once per account and Mac, in the desktop app on a Mac that can pair
 * itself, so a second Mac is asked too. Which Macs have been through it is
 * kept on each Mac, and that is all the gate reads: a server read would hold
 * every launch on a round trip. Everything before the answer is held, since
 * warming Home builds a cloud computer for a user who has none and the
 * answer may be that they want none. Whether this Mac is already one of the
 * account's computers is the first run's own first read, which marks it
 * asked when it is, so this module stays light on first load.
 */
import { canAddThisMac } from '@/lib/desktop';
import { useStorageUser, userLocalStorage } from '@/lib/userStorage';

const ASKED_HERE_KEY = 'thisMacFirstRun.asked';

/** Whether `user` has been through the first run in this app on this Mac. */
export function askedOnThisMac(user: string | null): boolean {
  return !!user && userLocalStorage.getItem(ASKED_HERE_KEY) === '1';
}

/** The first run is over for the signed-in user on this Mac, answered or with nothing to ask. */
export function markAskedOnThisMac(): void {
  userLocalStorage.setItem(ASKED_HERE_KEY, '1');
}

/** Whether the first run is still to be shown to the signed-in user on this Mac. */
export function useComputerChoiceUnasked(): boolean {
  const asked = askedOnThisMac(useStorageUser());
  return canAddThisMac() && !asked;
}
