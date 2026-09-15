import React from 'react';
import { AnimatePresence, motion, useReducedMotion } from 'framer-motion';
import { DURATION, EASE_OUT } from '@/lib/motion';

/**
 * A section that folds open and closed, animating its height instead of
 * popping. The one shape for every "show more" on a form: the Advanced group
 * on the MCP form, a tool list under a check result, an offer under a field.
 * Respects reduced motion by folding instantly.
 */
export function Disclosure({
  open,
  children,
  className,
  id,
}: {
  open: boolean;
  children: React.ReactNode;
  /** Applied to the inner wrapper, where padding and gaps belong. */
  className?: string;
  id?: string;
}) {
  const reducedMotion = useReducedMotion();
  const transition = reducedMotion
    ? { duration: 0 }
    : { height: { duration: DURATION.fold, ease: EASE_OUT }, opacity: { duration: DURATION.quick } };
  return (
    <AnimatePresence initial={false}>
      {open && (
        <motion.div
          key="disclosure"
          id={id}
          initial={{ height: 0, opacity: 0 }}
          animate={{ height: 'auto', opacity: 1 }}
          exit={{ height: 0, opacity: 0, transition: reducedMotion ? { duration: 0 } : { duration: DURATION.exit } }}
          transition={transition}
          style={{ overflow: 'hidden' }}
        >
          <div className={className}>{children}</div>
        </motion.div>
      )}
    </AnimatePresence>
  );
}
