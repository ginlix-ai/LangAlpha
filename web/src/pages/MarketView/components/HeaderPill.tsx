import React from 'react';
import { doorSlot } from '@/components/ui/button-class';
import { cn } from '@/lib/utils';
import './HeaderPill.css';

/** The header's small outlined action, so a host's buttons match Company Overview. */
export function HeaderPill({ className, ...props }: React.ButtonHTMLAttributes<HTMLButtonElement>): React.ReactElement {
  return <button type="button" {...doorSlot('outline')} className={cn('stock-overview-toggle', className)} {...props} />;
}
