import React, { useId, useState } from 'react';
import { Check, Circle, X } from 'lucide-react';
import { Disclosure } from '@/components/ui/Disclosure';
import { FoldChevron } from './decisionCardMotion';
import { announceAnchoredToggle } from '../utils/anchoredToggle';

/** `none` is a question nobody answered, which only an older thread's plan
 *  can be now. */
export type SettledVerdict = 'approved' | 'rejected' | 'none';

/**
 * What is left of a review once it has been answered: one line in the
 * timeline's own voice, opening onto what was reviewed.
 *
 * The verdict is the same fact whether the review was of a tool call, an order
 * or a plan, so the row is one component and only what it says differs. A
 * rejected step reads quieter than an approved one, which is the whole reason
 * the opacity and the text colour follow the verdict.
 */
export function SettledStep({
  verdict,
  label,
  reason,
  badge,
  icon,
  order = false,
  defaultOpen = false,
  testId = 'tool-approval-settled',
  children,
}: {
  verdict: SettledVerdict;
  label: React.ReactNode;
  /** The reason typed on Reject. Drawn only on a rejection, where it is the
   *  answer to what the card asked. */
  reason?: string | null;
  /** Anything the verdict line carries between the label and the reason, such
   *  as the mode an order was placed in. */
  badge?: React.ReactNode;
  /** Drawn in place of the verdict's glyph, for an answer the verdict only
   *  approximates, such as a question skipped. */
  icon?: React.ReactNode;
  /** Marks the step as an order's, which is what a transcript test keys on to
   *  tell the two settled cards apart. */
  order?: boolean;
  defaultOpen?: boolean;
  testId?: string;
  children: React.ReactNode;
}): React.ReactElement {
  const [collapsed, setCollapsed] = useState(!defaultOpen);
  const bodyId = useId();
  const approved = verdict === 'approved';
  const rejected = verdict === 'rejected';
  return (
    <div data-testid={testId} {...(order ? { 'data-order': 'true' } : {})}>
      <button
        type="button"
        onClick={(e) => { announceAnchoredToggle(e.currentTarget); setCollapsed((v) => !v); }}
        aria-expanded={!collapsed}
        aria-controls={bodyId}
        className="flex items-center gap-2 py-1 cursor-pointer w-full text-left"
      >
        <FoldChevron open={!collapsed} />
        {icon ?? (approved ? (
          <Check className="h-4 w-4 shrink-0" style={{ color: 'var(--color-accent-light)' }} />
        ) : rejected ? (
          <X className="h-4 w-4 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
        ) : (
          <Circle className="h-3.5 w-3.5 shrink-0" style={{ color: 'var(--color-icon-muted)' }} />
        ))}
        <span
          className="text-sm truncate"
          style={{ color: approved ? 'var(--color-text-tertiary)' : 'var(--color-text-quaternary)' }}
        >
          {label}
        </span>
        {badge}
        {rejected && reason && (
          <span className="text-xs truncate" style={{ color: 'var(--color-icon-muted)' }}>
            {reason}
          </span>
        )}
      </button>
      <Disclosure open={!collapsed} id={bodyId}>
        <div className="pt-2 pb-1 pl-6" style={{ opacity: rejected ? 0.6 : 0.8 }}>
          {children}
        </div>
      </Disclosure>
    </div>
  );
}
