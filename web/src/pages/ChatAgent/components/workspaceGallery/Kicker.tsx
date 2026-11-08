import type { ReactNode } from 'react';

/**
 * The gallery's small mono label over a section or a list: a kicker, in the
 * data face (the open card styles' label voice), quiet enough to read as a
 * heading without competing with one.
 */
export function Kicker({ children, className = '' }: { children: ReactNode; className?: string }) {
  return (
    <div
      className={`pf-kicker font-['JetBrains_Mono','Menlo',monospace] text-[11px] font-medium uppercase tracking-[0.06em] tabular-nums open-style:label-voice open-style:text-[length:var(--cs-text-label,0.75rem)] ${className}`}
      style={{ color: 'var(--color-text-tertiary)' }}
    >
      {children}
    </div>
  );
}

export default Kicker;
