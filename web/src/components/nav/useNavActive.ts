import { useLocation } from 'react-router';
import type { NavItem } from './navItems';

/** Returns a matcher that reports whether a nav item is active for the current pathname. */
export function useNavActive() {
  const { pathname } = useLocation();
  return (item: NavItem): boolean => {
    if (item.match === 'prefix') return pathname.startsWith(item.key);
    return pathname === item.key || pathname.startsWith(item.key + '/');
  };
}
