import { useMemo } from 'react';
import { useOrdersVisible } from '@/hooks/useOrdersVisible';
import { isFeatureEnabled, useFeatures } from '@/hooks/useFeatures';
import { NAV_ITEMS, NEW_CHAT_ITEM, type NavItem } from './navItems';

/**
 * The primary nav items this user can see, in NAV_ITEMS order.
 *
 * An item that names a feature (the Drive, the organization library) is
 * offered where that feature is on, read off the flags, which fail closed
 * while they load.
 *
 * Orders is offered when a broker is connected or the ledger holds an order
 * from before; someone with neither is spared an entry into an empty list.
 * A gated item is drawn only on a settled `true`, so the loading answer hides
 * it: an entry that appears a beat after the shell has painted reads as the
 * app changing its mind, and the route behind it is reachable by URL anyway.
 */
export function useNavItems(): NavItem[] {
  const ordersVisible = useOrdersVisible();
  const { data: features } = useFeatures();
  return useMemo(
    () =>
      NAV_ITEMS.filter((item) => {
        if (!item.requires) return true;
        if (item.requires === 'orders') return ordersVisible === true;
        return isFeatureEnabled(features, item.requires);
      }),
    [ordersVisible, features],
  );
}

/**
 * The places the desktop offers, in one order for the sidebar and the new-tab
 * menu: a new chat first, where the chat item would be, then the other pages.
 */
export function useDesktopNavItems(): NavItem[] {
  const items = useNavItems();
  return useMemo(() => [NEW_CHAT_ITEM, ...items.filter((item) => item.key !== NEW_CHAT_ITEM.key)], [items]);
}
