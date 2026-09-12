import { Blocks, ChartCandlestick, LayoutDashboard, MessagesSquare, Receipt, Settings, Timer } from 'lucide-react';
import type { LucideIcon } from 'lucide-react';

export interface NavItem {
  /** Route path. */
  key: string;
  icon: LucideIcon;
  /** i18n key resolved via t(). */
  labelKey: string;
  /** Active-state strategy: 'prefix' = pathname.startsWith(key); 'exact-or-sub' = exact match or key + '/' sub-path. */
  match: 'prefix' | 'exact-or-sub';
  /**
   * What has to be true before the item is drawn at all; absent = always.
   * Declared here rather than in a nav surface because three of them draw this
   * list, and a gate that lives in one is a gate the other two do not have.
   * Never a route guard: the route stays reachable by URL, and the page it
   * leads to is the one that explains an empty answer.
   */
  requires?: 'orders';
}

// Single source of truth for primary navigation (rail + mobile tab bar).
export const NAV_ITEMS: NavItem[] = [
  { key: '/dashboard', icon: LayoutDashboard, labelKey: 'sidebar.dashboard', match: 'exact-or-sub' },
  { key: '/chat', icon: MessagesSquare, labelKey: 'sidebar.chatAgent', match: 'prefix' },
  { key: '/market', icon: ChartCandlestick, labelKey: 'sidebar.marketView', match: 'exact-or-sub' },
  { key: '/automations', icon: Timer, labelKey: 'sidebar.automations', match: 'exact-or-sub' },
  { key: '/orders', icon: Receipt, labelKey: 'sidebar.orders', match: 'exact-or-sub', requires: 'orders' },
  { key: '/plugins', icon: Blocks, labelKey: 'sidebar.plugins', match: 'exact-or-sub' },
];

// Settings only appears in the mobile tab bar (desktop reaches it via AccountMenu).
export const SETTINGS_ITEM: NavItem = {
  key: '/settings',
  icon: Settings,
  labelKey: 'sidebar.settings',
  match: 'exact-or-sub',
};
