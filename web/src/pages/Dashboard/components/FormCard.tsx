import type React from 'react';
import { TermCard } from '@/pages/ChatAgent/components/charts/TermCard';
import { TermPanelContext } from '@/pages/ChatAgent/components/charts/termContext';
import { cn } from '@/lib/utils';
import './dashForms.css';

/**
 * A group of a dashboard window's fields, framed as a section of the data
 * cards' detail panel: the card style's frame and title, a size up from a
 * transcript card. The window around it carries `dash-form`, so the title
 * cuts the edge in the window's own ground.
 */
export function FormCard({
  title,
  sub,
  meta,
  className,
  children,
}: {
  title: React.ReactNode;
  sub?: React.ReactNode;
  meta?: React.ReactNode;
  className?: string;
  children: React.ReactNode;
}): React.ReactElement {
  return (
    <TermPanelContext value={true}>
      <TermCard title={title} sub={sub} meta={meta}>
        <div className={cn('dash-form-body', className)}>{children}</div>
      </TermCard>
    </TermPanelContext>
  );
}
