import { useEffect, type ReactNode } from 'react';
import { motion } from '@/lib/framer';
import { TWEEN } from '@/lib/motion';
import { useDialogA11y } from '@/hooks/useDialogA11y';

/**
 * A dashboard window's panel on the desktop, over its backdrop. It takes focus
 * as it opens, holds it while open and hands it back on close, as a modal
 * owes the keyboard. Its own component so the focus hook runs as the panel
 * mounts, not once with the page.
 */
export function DashWindowDialog({
  label,
  onClose,
  children,
}: {
  label: string;
  onClose: () => void;
  children: ReactNode;
}) {
  const ref = useDialogA11y<HTMLDivElement>(onClose);
  // Focus starts on the panel, not its first control: the window swaps its
  // loading view for the story, and the control focused in the first is gone
  // with it, which would drop focus to the page. The panel stays.
  useEffect(() => {
    ref.current?.focus({ preventScroll: true });
  }, [ref]);
  return (
    <motion.div
      ref={ref}
      initial={{ opacity: 0, scale: 0.95, y: 20 }}
      animate={{ opacity: 1, scale: 1, y: 0 }}
      exit={{ opacity: 0, scale: 0.95, y: 20, transition: TWEEN.exit }}
      transition={TWEEN.enter}
      role="dialog"
      aria-modal="true"
      aria-label={label}
      tabIndex={-1}
      className="dash-window dash-window--dialog w-full max-w-5xl max-h-[90vh] overflow-y-auto"
    >
      {children}
    </motion.div>
  );
}
