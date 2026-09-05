import type { LucideIcon } from 'lucide-react';

/**
 * A short sentence on a row's status line, led by a glyph.
 *
 * Distinct from the pills beside it on purpose: a pill reports the row's
 * state, this reports something the user has to do about it, and the glyph is
 * what keeps the two from reading as one more state field.
 *
 * `tone` exists because this page places real trades. A note saying a flow
 * needs the desktop app and a note saying the click is about to take an
 * account's only AI connection away from wherever it is now are not the same
 * sentence, and rendering both at tertiary weight let the consequential one
 * read as filler. Warning borrows the pill's colour and not its fill: a tinted
 * background here would make it the pill it is deliberately not.
 */
export function RowNote({
  icon: Icon,
  id,
  tone = 'muted',
  children,
}: {
  icon: LucideIcon;
  /** Set when a control points here for the reason it is unavailable. */
  id?: string;
  /** `warning` for a consequence of acting; `muted` for a fact about the row. */
  tone?: 'muted' | 'warning';
  children: React.ReactNode;
}) {
  return (
    <span
      id={id}
      className="inline-flex items-center gap-1 text-[0.6875rem]"
      style={{
        color:
          tone === 'warning' ? 'var(--color-warning)' : 'var(--color-text-tertiary)',
      }}
    >
      <Icon className="h-3 w-3 flex-shrink-0" />
      {children}
    </span>
  );
}
