import type React from 'react';
import { Info } from 'lucide-react';

/* The shell of a row above a composer: the glyph, the sentence and the row's
   actions after it. Neutral on purpose: nothing is wrong, so it does not
   borrow the warning treatment the fallback pill uses. Pointer and focus
   handlers pass through for a row that times out. */
export function ComposerRow({
  text,
  children,
  ...handlers
}: {
  text: React.ReactNode;
  children?: React.ReactNode;
} & Pick<React.HTMLAttributes<HTMLDivElement>, 'onMouseEnter' | 'onMouseLeave' | 'onFocus' | 'onBlur'>): React.ReactElement {
  return (
    <div className="flex flex-wrap items-center gap-x-2 gap-y-1.5 px-3 py-2 rounded-md text-sm"
      role="status" aria-live="polite"
      {...handlers}
      style={{
        backgroundColor: 'var(--color-bg-elevated)',
        color: 'var(--color-text-secondary)',
        border: '1px solid var(--color-border-default)',
      }}>
      <Info aria-hidden="true" className="h-4 w-4 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
      {/* The floor is what lets the row wrap: with none, a narrow composer (the
          market panel) squeezes the sentence to a word per line beside the
          actions instead of moving them below it. */}
      <span className="flex-1 min-w-48">{text}</span>
      {children}
    </div>
  );
}
