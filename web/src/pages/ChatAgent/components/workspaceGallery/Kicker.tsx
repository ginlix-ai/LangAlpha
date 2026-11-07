import type { ReactNode } from 'react';

/**
 * The gallery's small label over a section or a list: a kicker, in the card
 * style's label voice, quiet enough to read as a heading without competing
 * with one.
 */
export function Kicker({ children, className = '' }: { children: ReactNode; className?: string }) {
  return (
    <div
      className={`pf-kicker label-voice text-[length:var(--cs-text-label,0.75rem)] font-medium tabular-nums ${className}`}
      style={{ color: 'var(--color-text-tertiary)' }}
    >
      {children}
    </div>
  );
}

export default Kicker;
