import { useEffect, useMemo } from 'react';
import { isFeatureEnabled, useFeatures } from '@/hooks/useFeatures';
import { storageUser, userLocalStorage, useStorageUser } from '@/lib/userStorage';
import { PAGE_TABS_FEATURE } from './pageTabsContext';

const LAST_READ_KEY = 'la_page_tabs_on';

/**
 * Whether the page tabs' bar holds its row, taken from the last read this
 * device kept while the flags are on their way. The flags land after the first
 * paint, and the bar sits above the page, so reading them as off until then
 * brought it in a beat late and pushed the page down on every load. Off with
 * nothing kept or once the read has failed, as a flag fails closed. Only for
 * what the real read corrects when it lands: the tabs themselves, which are
 * saved as they change, and where the app's entry redirects wait for the flags.
 */
export function usePageTabsOn(): boolean {
  const { data, isPending } = useFeatures();
  const user = useStorageUser();
  const read = data ? isFeatureEnabled(data, PAGE_TABS_FEATURE) : undefined;
  const lastRead = useMemo(() => (user ? userLocalStorage.getItem(LAST_READ_KEY) : null), [user]);
  useEffect(() => {
    // The storage follows a switch of account at once, ahead of this effect
    // and the cleared flags, so a read made for the account that left is not
    // kept for the one that came in.
    if (read === undefined || !user || storageUser() !== user) return;
    userLocalStorage.setItem(LAST_READ_KEY, read ? '1' : '0');
  }, [read, user]);
  if (read !== undefined) return read;
  return isPending && lastRead === '1';
}
