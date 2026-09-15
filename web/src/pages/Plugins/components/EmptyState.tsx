import React from 'react';

/**
 * An empty list as an invitation: the dot grid (the design system's reserved
 * empty-canvas texture) with the message and the one action that fills it.
 *
 * `compact` is the same invitation as one row, for a section that is empty
 * on a tab that is not: the decks around it are the content, and a hero
 * box there says "nothing here" louder than thirty rows say "here".
 */
export function EmptyState({
  message,
  action,
  compact = false,
}: {
  message: React.ReactNode;
  /** The inline primary action (usually a `HeaderButton`). */
  action?: React.ReactNode;
  compact?: boolean;
}) {
  if (compact) {
    return (
      <div
        className="flex items-center justify-between gap-4 rounded-lg border px-4 py-3"
        style={{ borderColor: 'var(--color-border-muted)' }}
      >
        <p className="min-w-0 flex-1 text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
          {message}
        </p>
        {action}
      </div>
    );
  }
  return (
    <div
      className="flex flex-col items-center gap-3 rounded-lg border px-6 py-10 text-center"
      style={{
        borderColor: 'var(--color-border-muted)',
        backgroundImage:
          'radial-gradient(circle at center, var(--color-dot-grid) 0.75px, transparent 0.75px)',
        backgroundSize: '18px 18px',
        backgroundPosition: '0 0',
      }}
    >
      <p className="text-sm max-w-md" style={{ color: 'var(--color-text-tertiary)' }}>
        {message}
      </p>
      {action}
    </div>
  );
}
