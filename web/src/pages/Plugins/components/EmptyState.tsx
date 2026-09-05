import React from 'react';

/**
 * An empty list as an invitation: the dot grid (the design system's reserved
 * empty-canvas texture) with the message and the one action that fills it.
 */
export function EmptyState({
  message,
  action,
}: {
  message: React.ReactNode;
  /** The inline primary action (usually a `HeaderButton`). */
  action?: React.ReactNode;
}) {
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
