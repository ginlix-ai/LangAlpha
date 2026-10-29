import React from 'react';
import { Link, useNavigate, type LinkProps } from 'react-router';
import { useRouteLeaveGuard } from '@/pages/ChatAgent/contexts/RouteLeaveGuardContext';

/**
 * A link to a page of this app whose route change goes through the host's
 * leave guard first, since a route change fires no beforeunload and a panel
 * holding unsaved drafts is otherwise unmounted with no question asked.
 * Modified clicks open a new tab and leave nothing behind, so the Link keeps
 * those. Needs a router; a caller outside one draws a plain anchor.
 */
export function GuardedLink({
  to,
  onClick,
  ...props
}: Omit<LinkProps, 'to'> & { to: string }): React.ReactElement {
  const navigate = useNavigate();
  const guardLeave = useRouteLeaveGuard();
  return (
    <Link
      to={to}
      {...props}
      onClick={(e) => {
        onClick?.(e);
        if (e.defaultPrevented || e.button !== 0 || e.metaKey || e.altKey || e.ctrlKey || e.shiftKey) return;
        e.preventDefault();
        guardLeave(() => navigate(to));
      }}
    />
  );
}
