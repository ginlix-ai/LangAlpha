import type { ReactNode } from 'react';
import { cn } from '@/lib/utils';
import './surface.css';

/**
 * An Accounts section (Brokerages, Secrets): what the user acts on, and beside
 * it what they read once rather than scroll past under every visit to the
 * list. Below 1024px the side column stacks under the main one.
 */
export function AccountsLayout({
  lead,
  aside,
  className,
  hidden,
  children,
}: {
  /** What the section is: the side column's first line. */
  lead: ReactNode;
  /** The side column's blocks after the lead. */
  aside: ReactNode;
  className?: string;
  hidden?: boolean;
  children: ReactNode;
}) {
  return (
    <div className={cn('accounts', className)} hidden={hidden}>
      <div className="accounts-main">{children}</div>
      <aside className="accounts-aside">
        <p className="accounts-aside-lead">{lead}</p>
        {aside}
      </aside>
    </div>
  );
}
