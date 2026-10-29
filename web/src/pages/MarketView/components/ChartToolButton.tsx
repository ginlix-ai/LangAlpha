import React from 'react';
import { cn } from '@/lib/utils';

/**
 * A toolbar-sized icon button in the chart's own button style, so a host's
 * actions placed in the toolbar row look like the chart's tools. `title`
 * doubles as the accessible name because the button shows only its icon.
 */
export function ChartToolButton({ className, title, children, ...props }: React.ButtonHTMLAttributes<HTMLButtonElement> & { title: string }): React.ReactElement {
  return (
    <button type="button" className={cn('chart-tool-btn', className)} title={title} aria-label={title} {...props}>
      {children}
    </button>
  );
}
