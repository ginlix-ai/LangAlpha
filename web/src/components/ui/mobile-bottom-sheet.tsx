import React from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { useSwipeToDismiss } from '@/hooks/useSwipeToDismiss';

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

/**
 * Inner component that owns all drag/touch state. Unmounts when the sheet
 * closes, so dragY and other state are always fresh on open — no stale
 * drag offsets conflicting with the enter animation.
 */
function SheetPanel({
  onClose,
  sizing,
  height,
  className,
  style,
  children,
}: Omit<MobileBottomSheetProps, 'open'>) {
  const { contentRef, handleRef, dragY } = useSwipeToDismiss({ onDismiss: onClose });

  return (
    <>
      {/* Backdrop */}
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        exit={{ opacity: 0 }}
        transition={{ duration: 0.2 }}
        className="fixed inset-0 z-[1010]"
        style={{ backgroundColor: 'var(--color-bg-overlay)' }}
        onClick={onClose}
      />
      {/* Sheet */}
      <motion.div
        initial={{ y: '100%' }}
        animate={{ y: 0 }}
        exit={{ y: '100%' }}
        transition={{ type: 'spring', damping: 28, stiffness: 280 }}
        style={{
          y: dragY,
          backgroundColor: 'var(--color-bg-card)',
          borderColor: 'var(--color-border-muted)',
          ...(sizing === 'fixed' ? { height } : { maxHeight: height }),
        }}
        className={`fixed bottom-0 left-0 right-0 z-[1020] rounded-t-3xl border-t${sizing === 'fixed' ? ' flex flex-col' : ''}`}
      >
        {/* Drag handle */}
        <div
          ref={handleRef}
          className="flex justify-center pt-3 pb-2 cursor-grab active:cursor-grabbing"
          style={{ touchAction: 'none' }}
        >
          <div
            className="w-10 h-1 rounded-full"
            style={{ backgroundColor: 'var(--color-border-default)' }}
          />
        </div>
        <div
          ref={contentRef}
          className={`overflow-y-auto overflow-x-hidden px-4 mobile-scroll-contain${sizing === 'fixed' ? ' flex-1' : ''}${className ? ` ${className}` : ''}`}
          style={{
            ...(sizing === 'auto' ? { maxHeight: `calc(${height} - 36px)` } : {}),
            paddingBottom: 14,
            overscrollBehaviorY: 'contain',
            ...style,
          }}
        >
          {children}
        </div>
      </motion.div>
    </>
  );
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
        <SheetPanel
          onClose={onClose}
          sizing={sizing}
          height={height}
          className={className}
          style={style}
        >
          {children}
        </SheetPanel>
      )}
    </AnimatePresence>
  );
}

export { MobileBottomSheet };
export type { MobileBottomSheetProps };
