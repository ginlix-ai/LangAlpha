import { useMemo } from 'react';
import { useOrdersVisible } from '@/hooks/useOrdersVisible';
import { NAV_ITEMS, type NavItem } from './navItems';

/**
 * The primary nav items this user can see, in NAV_ITEMS order.
 *
 * Orders is offered when a broker is connected or the ledger holds an order
 * from before; someone with neither is spared an entry into an empty list.
 * A gated item is drawn only on a settled `true`, so the loading answer hides
 * it: an entry that appears a beat after the shell has painted reads as the
 * app changing its mind, and the route behind it is reachable by URL anyway.
 */
export function useNavItems(): NavItem[] {
  const ordersVisible = useOrdersVisible();
  return useMemo(
    () =>
      NAV_ITEMS.filter((item) =>
        item.requires === 'orders' ? ordersVisible === true : true,
      ),
    [ordersVisible],
  );
}
