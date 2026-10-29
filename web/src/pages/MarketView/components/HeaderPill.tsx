import React from 'react';
import { cn } from '@/lib/utils';
import './HeaderPill.css';

/** The header's small outlined action, so a host's buttons match Company Overview. */
export function HeaderPill({ className, ...props }: React.ButtonHTMLAttributes<HTMLButtonElement>): React.ReactElement {
  return <button type="button" className={cn('stock-overview-toggle', className)} {...props} />;
}
