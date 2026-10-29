import React, { useRef } from 'react';
import { motion, AnimatePresence } from '@/lib/framer';
import { TWEEN } from '@/lib/motion';
import LangAlphaFab from '@/components/ui/langalpha-fab';
import { useOnClickOutside } from '@/hooks/useOnClickOutside';

interface MobileFabChatProps {
  expanded: boolean;
  onExpand: () => void;
  onCollapse: () => void;
  className?: string;
  style?: React.CSSProperties;
  children: React.ReactNode;
}

function MobileFabChat({
  expanded,
  onExpand,
  onCollapse,
  className,
  style,
  children,
}: MobileFabChatProps) {
  const expandedRef = useRef<HTMLDivElement>(null);
  useOnClickOutside(expandedRef, onCollapse, expanded);

  return (
    // Both layers are `position: fixed`, so they already share the corner and
    // the swap crossfades with no cell to stack them in.
    <AnimatePresence>
      {!expanded ? (
        <LangAlphaFab key="fab" onClick={onExpand} />
      ) : (
        <motion.div
          key="fab-chat-input"
          ref={expandedRef}
          initial={{ scale: 0.9, opacity: 0, y: 20 }}
          animate={{ scale: 1, opacity: 1, y: 0 }}
          exit={{ scale: 0.9, opacity: 0, y: 20, transition: TWEEN.exit }}
          transition={TWEEN.quick}
          className={className}
          style={style}
        >
          {children}
        </motion.div>
      )}
    </AnimatePresence>
  );
}

export { MobileFabChat };
export type { MobileFabChatProps };
