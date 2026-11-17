import type React from 'react';
import { cn } from '@/lib/utils';

/* One line of a thread's status: a glyph and a short note in the tertiary
   ink, announced politely unless live={false}, which a row inside a live
   region of its own needs. Unpadded on the sides, to align with the
   transcript's text; a host among padded rows passes its padding.
   An `action` follows the note in its flow, so a narrow line wraps it with
   the words, and stays out of what is announced: only the note is read out
   when the line changes. */
export function StatusLine({
  glyph,
  live = true,
  title,
  className,
  action,
  children,
}: {
  glyph: React.ReactNode;
  live?: boolean;
  title?: string;
  className?: string;
  action?: React.ReactNode;
  children: React.ReactNode;
}): React.ReactElement {
  const liveProps = live ? { role: 'status', 'aria-live': 'polite' as const } : {};
  return (
    <div
      className={cn('flex items-center gap-2 py-1.5 text-xs', className)}
      {...(action ? {} : liveProps)}
      title={title}
      style={{ color: 'var(--color-text-tertiary)' }}
    >
      <span aria-hidden="true" className="shrink-0">
        {glyph}
      </span>
      {action ? (
        <span className="min-w-0 wrap-break-word">
          <span {...liveProps}>{children}</span> {action}
        </span>
      ) : children}
    </div>
  );
}
