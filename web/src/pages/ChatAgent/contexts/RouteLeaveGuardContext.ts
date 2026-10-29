import { createContext, useContext } from 'react';

/**
 * Lets a control that changes the route ask its host first.
 *
 * A route change fires no beforeunload, so a panel holding unsaved drafts is
 * unmounted with no question asked. The host that owns the drafts provides
 * the guard; a link inside it wraps its navigation in the guard and lets the
 * host decide whether to run it. Without a provider the navigation runs at
 * once, so the same link works in a transcript or on a page with nothing to
 * lose.
 */
export type RouteLeaveGuard = (go: () => void) => void;

export const RouteLeaveGuardContext = createContext<RouteLeaveGuard>((go) => go());

export function useRouteLeaveGuard(): RouteLeaveGuard {
  return useContext(RouteLeaveGuardContext);
}
