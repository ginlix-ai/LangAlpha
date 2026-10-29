import type React from 'react';
import { cn } from '@/lib/utils';

/* One line of a thread's status: a glyph and a short note in the tertiary
   ink, announced politely unless live={false}, which a row inside a live
   region of its own needs. Unpadded on the sides, to align with the
   transcript's text; a host among padded rows passes its padding. */
export function StatusLine({
  glyph,
  live = true,
  title,
  className,
  children,
}: {
  glyph: React.ReactNode;
  live?: boolean;
  title?: string;
  className?: string;
  children: React.ReactNode;
}): React.ReactElement {
  return (
    <div
      className={cn('flex items-center gap-2 py-1.5 text-xs', className)}
      role={live ? 'status' : undefined}
      aria-live={live ? 'polite' : undefined}
      title={title}
      style={{ color: 'var(--color-text-tertiary)' }}
    >
      <span aria-hidden="true" className="shrink-0">
        {glyph}
      </span>
      {children}
    </div>
  );
}
