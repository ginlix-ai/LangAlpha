import { useState } from 'react';
import {
  Activity,
  AppWindow,
  BarChart3,
  Building2,
  Cable,
  Circle,
  CreditCard,
  FolderOpen,
  LayoutDashboard,
  Library,
  Network,
  Plug,
  ShieldCheck,
  UserRoundGroup,
  Users,
  UsersRound,
  type LucideIcon,
} from 'lucide-react';
import { readNavGroups, type PortalNavGroup } from './portalMessages';
import { useStorageUser, userLocalStorage } from '@/lib/userStorage';

// The account portal's navigation as it last listed it, so the rail draws its
// pages at once on the next visit rather than when the portal answers. Per
// user, since an organization's pages name it.
const STORAGE_KEY = 'account-portal-nav';

function readSaved(): PortalNavGroup[] | null {
  try {
    const raw = userLocalStorage.getItem(STORAGE_KEY);
    return raw ? readNavGroups(JSON.parse(raw)) : null;
  } catch {
    return null;
  }
}

/** The portal's navigation, last known first, and the setter its messages call. */
export function usePortalNav(): [PortalNavGroup[] | null, (groups: PortalNavGroup[]) => void] {
  const user = useStorageUser();
  const [state, setState] = useState(() => ({ user, groups: readSaved() }));
  if (state.user !== user) setState({ user, groups: readSaved() });
  const update = (groups: PortalNavGroup[]) => {
    setState({ user, groups });
    try {
      userLocalStorage.setItem(STORAGE_KEY, JSON.stringify(groups));
    } catch {
      // Storage unavailable: the rail still has the pages for this visit.
    }
  };
  return [state.user === user ? state.groups : null, update];
}

// The icons the portal may name. A name is drawn only from this list, since
// the app cannot load an icon it was not built with.
const ICONS: Record<string, LucideIcon> = {
  Activity,
  AppWindow,
  BarChart3,
  Building2,
  Cable,
  CreditCard,
  FolderOpen,
  LayoutDashboard,
  Library,
  Network,
  Plug,
  ShieldCheck,
  UserRoundGroup,
  Users,
  UsersRound,
};

export function portalNavIcon(name: string): LucideIcon {
  return Object.hasOwn(ICONS, name) ? ICONS[name] : Circle;
}
