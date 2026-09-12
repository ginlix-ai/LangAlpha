import React, { useState } from 'react';
import { AnimatePresence, motion } from 'framer-motion';
import { Check, ChevronRight, X } from 'lucide-react';

/**
 * What is left of an approval once it has been answered: one line in the
 * timeline's own voice, opening onto the frame that was sent.
 *
 * The verdict is the same fact whether the call placed an order or not, so the
 * row is one component and only what it says differs. A rejected step reads
 * quieter than an approved one, which is the whole reason the opacity and the
 * text colour follow the verdict.
 */
export function SettledToolStep({
  approved,
  label,
  reason,
  badge,
  order = false,
  children,
}: {
  approved: boolean;
  label: React.ReactNode;
  /** The reason typed on Reject. Drawn only on a rejection, where it is the
   *  answer to what the card asked. */
  reason?: string | null;
  /** Anything the verdict line carries between the label and the reason, such
   *  as the mode an order was placed in. */
  badge?: React.ReactNode;
  /** Marks the step as an order's, which is what a transcript test keys on to
   *  tell the two settled cards apart. */
  order?: boolean;
  children: React.ReactNode;
}): React.ReactElement {
  const [collapsed, setCollapsed] = useState(true);
  return (
    <div data-testid="tool-approval-settled" {...(order ? { 'data-order': 'true' } : {})}>
      <button
        type="button"
        onClick={() => setCollapsed((v) => !v)}
        className="flex items-center gap-2 py-1 cursor-pointer w-full text-left"
      >
        <motion.div animate={{ rotate: collapsed ? 0 : 90 }} transition={{ duration: 0.2 }}>
          <ChevronRight className="h-3.5 w-3.5 flex-shrink-0" style={{ color: 'var(--color-icon-muted)' }} />
        </motion.div>
        {approved ? (
          <Check className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-accent-light)' }} />
        ) : (
          <X className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
        )}
        <span
          className="text-sm truncate"
          style={{ color: approved ? 'var(--color-text-tertiary)' : 'var(--color-text-quaternary)' }}
        >
          {label}
        </span>
        {badge}
        {!approved && reason && (
          <span className="text-xs truncate" style={{ color: 'var(--color-icon-muted)' }}>
            {reason}
          </span>
        )}
      </button>
      <AnimatePresence initial={false}>
        {!collapsed && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: 'auto', opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.25, ease: [0.22, 1, 0.36, 1] }}
            className="overflow-hidden"
          >
            <div className="pt-2 pb-1 pl-6" style={{ opacity: approved ? 0.8 : 0.6 }}>
              {children}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}
