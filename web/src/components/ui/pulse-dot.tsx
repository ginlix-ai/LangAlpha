import type { ReactElement } from 'react';
import { cn } from '@/lib/utils';

interface PulseDotProps {
  /** Background of both spans (the static dot + the expanding ping). Any CSS
   *  color; defaults to the accent used by the market-watch chip/status header. */
  color?: string;
  /** Extra classes for the wrapper (e.g. layout tweaks at a call site). */
  className?: string;
}

/**
 * The two-span `animate-ping` accent dot — an expanding, fading ring behind a
 * solid center dot — used as a "live" marker on the market-watch chip, the
 * Status panel section header, and the background-tasks tail notice.
 */
export default function PulseDot({
  color = 'var(--color-accent-primary)',
  className,
}: PulseDotProps): ReactElement {
  return (
    <span className={cn('relative flex h-2 w-2', className)} aria-hidden="true">
      <span
        className="animate-ping absolute inline-flex h-full w-full rounded-full opacity-75"
        style={{ background: color }}
      />
      <span
        className="relative inline-flex h-2 w-2 rounded-full"
        style={{ background: color }}
      />
    </span>
  );
}
