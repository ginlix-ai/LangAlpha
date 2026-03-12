import React from 'react';
import { motion, AnimatePresence } from 'framer-motion';

interface MobileBottomSheetProps {
  open: boolean;
  onClose: () => void;
  /** 'fixed' uses height + flex layout (content fills via flex-1). Default 'auto' uses maxHeight. */
  sizing?: 'auto' | 'fixed';
  height?: string;
  className?: string;
  style?: React.CSSProperties;
  children: React.ReactNode;
}

function MobileBottomSheet({
  open,
  onClose,
  sizing = 'auto',
  height = '80vh',
  className,
  style,
  children,
}: MobileBottomSheetProps) {
  return (
    <AnimatePresence>
      {open && (
        <>
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.2 }}
            className="fixed inset-0 z-40"
            style={{ backgroundColor: 'var(--color-bg-overlay)' }}
            onClick={onClose}
          />
          <motion.div
            initial={{ y: '100%' }}
            animate={{ y: 0 }}
            exit={{ y: '100%' }}
            transition={{ type: 'spring', damping: 30, stiffness: 300 }}
            className={`fixed bottom-0 left-0 right-0 z-50 rounded-t-2xl border-t${sizing === 'fixed' ? ' flex flex-col' : ''}`}
            style={{
              backgroundColor: 'var(--color-bg-card)',
              borderColor: 'var(--color-border-muted)',
              ...(sizing === 'fixed' ? { height } : { maxHeight: height }),
            }}
          >
            {/* Drag handle */}
            <div className="flex justify-center pt-3 pb-2">
              <div
                className="w-10 h-1 rounded-full"
                style={{ backgroundColor: 'var(--color-border-default)' }}
              />
            </div>
            <div
              className={`overflow-y-auto overflow-x-hidden px-4${sizing === 'fixed' ? ' flex-1' : ''}${className ? ` ${className}` : ''}`}
              style={sizing === 'auto' ? { maxHeight: `calc(${height} - 36px)`, ...style } : style}
            >
              {children}
            </div>
          </motion.div>
        </>
      )}
    </AnimatePresence>
  );
}

export { MobileBottomSheet };
export type { MobileBottomSheetProps };
