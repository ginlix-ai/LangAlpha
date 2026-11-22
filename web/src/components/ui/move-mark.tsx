import type { ChangeDirection } from '@/lib/priceMove';
import { cn } from '@/lib/utils';

/**
 * A move's direction as a drawn triangle, ▲ up or ▼ down, in the text's
 * color. Drawn rather than typed, so no fallback font can widen it. A screen
 * reader hears the sign or words beside it, never the mark.
 */
export function MoveMark({ dir, className }: { dir: Exclude<ChangeDirection, 'flat'>; className?: string }) {
  return (
    <svg className={cn('h-[0.54em] w-[0.62em] shrink-0 self-center fill-current', className)} viewBox="0 0 10 9" aria-hidden="true">
      <path d={dir === 'up' ? 'M5 0 10 9H0Z' : 'M5 9 0 0h10Z'} />
    </svg>
  );
}
