import type { ReactNode } from 'react';

/**
 * The gallery's small mono label over a section or a list: a kicker, in the
 * data face, quiet enough to read as a heading without competing with one.
 */
export function Kicker({ children, className = '' }: { children: ReactNode; className?: string }) {
  return (
    <div
      className={`text-[11px] font-medium uppercase tracking-[0.06em] tabular-nums ${className}`}
      style={{ fontFamily: "'JetBrains Mono', 'Menlo', monospace", color: 'var(--color-text-tertiary)' }}
    >
      {children}
    </div>
  );
}

export default Kicker;
