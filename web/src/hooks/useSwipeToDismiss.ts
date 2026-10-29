import { useRef, useEffect, useEffectEvent, useState, useCallback } from 'react';
import { useMotionValue, animate, type MotionValue } from '@/lib/framer';
import { SPRING_DRAG } from '@/lib/motion';

interface UseSwipeToDismissOptions {
  onDismiss: () => void;
  enabled?: boolean;
}

interface UseSwipeToDismissReturn {
  /** Callback ref — attach to the scrollable content container */
  contentRef: (node: HTMLDivElement | null) => void;
  /** Callback ref — attach to the drag handle element */
  handleRef: (node: HTMLDivElement | null) => void;
  /** Drag offset — apply as transform on the sheet container */
  dragY: MotionValue<number>;
}

/**
 * Shared swipe-to-dismiss gesture hook.
 *
 * Uses native browser scrolling for smooth iOS performance.
 * Only takes over touch handling when transitioning to drag-to-dismiss:
 * - When content is at scrollTop=0 and user pulls down → drag mode
 * - When touch starts on the drag handle → immediate drag mode
 * - Dismisses when velocity > 300px/s or offset > 120px
 * - Springs back on cancel
 *
 * Works whether the handle is a child of content (Dialog) or a sibling (MobileBottomSheet).
 *
 * Uses callback refs + state so effects re-run when the DOM nodes mount/unmount
 * (critical for Radix Dialog portals that remount content between open/close cycles).
 */
export function useSwipeToDismiss({
  onDismiss,
  enabled = true,
}: UseSwipeToDismissOptions): UseSwipeToDismissReturn {
  const [contentNode, setContentNode] = useState<HTMLDivElement | null>(null);
  const [handleNode, setHandleNode] = useState<HTMLDivElement | null>(null);
  const handleElRef = useRef<HTMLDivElement | null>(null);

  const dragY = useMotionValue(0);

  const dismiss = useEffectEvent(onDismiss);

  const contentRef = useCallback((node: HTMLDivElement | null) => {
    setContentNode(node);
  }, []);

  const handleRefCb = useCallback((node: HTMLDivElement | null) => {
    handleElRef.current = node;
    setHandleNode(node);
  }, []);

  // Content area: native scroll + drag-to-dismiss when at top
  useEffect(() => {
    const el = contentNode;
    if (!el || !enabled) return;

    dragY.set(0);

    let startY = 0;
    let lastY = 0;
    let lastTime = 0;
    let velocityY = 0;
    let mode: 'idle' | 'scroll' | 'drag' = 'idle';

    const onTouchStart = (e: TouchEvent) => {
      const y = e.touches[0].clientY;
      startY = y;
      lastY = y;
      lastTime = performance.now();
      velocityY = 0;

      // Touch on drag handle (child of content) → immediate drag
      if (handleElRef.current?.contains(e.target as Node)) {
        mode = 'drag';
      } else {
        mode = 'idle';
      }
    };

    const onTouchMove = (e: TouchEvent) => {
      const y = e.touches[0].clientY;
      const now = performance.now();
      const dt = now - lastTime;
      if (dt > 0) velocityY = ((y - lastY) / dt) * 1000;
      const dy = y - lastY;
      lastY = y;
      lastTime = now;

      // Determine intent from initial movement
      if (mode === 'idle') {
        const totalDelta = y - startY;
        if (Math.abs(totalDelta) < 5) return;
        if (el.scrollTop <= 0 && totalDelta > 0) {
          mode = 'drag';
          startY = y;
        } else {
          mode = 'scroll';
          return; // let browser handle natively
        }
      }

      if (mode === 'drag') {
        e.preventDefault();
        const pull = y - startY;
        if (pull < 0) {
          // Reversed direction — stop drag, let browser scroll
          dragY.set(0);
          mode = 'scroll';
        } else {
          dragY.set(pull);
        }
      } else if (mode === 'scroll') {
        // Browser handles scroll natively — check for transition to drag
        if (el.scrollTop <= 0 && dy > 0) {
          mode = 'drag';
          startY = y;
          e.preventDefault();
        }
      }
    };

    const onTouchEnd = () => {
      if (mode === 'drag') {
        const dy = dragY.get();
        if (velocityY > 300 || dy > 120) {
          dismiss();
        } else {
          animate(dragY, 0, SPRING_DRAG);
        }
      }
      mode = 'idle';
    };

    el.addEventListener('touchstart', onTouchStart, { passive: true });
    el.addEventListener('touchmove', onTouchMove, { passive: false });
    el.addEventListener('touchend', onTouchEnd, { passive: true });
    el.addEventListener('touchcancel', onTouchEnd, { passive: true });

    return () => {
      el.removeEventListener('touchstart', onTouchStart);
      el.removeEventListener('touchmove', onTouchMove);
      el.removeEventListener('touchend', onTouchEnd);
      el.removeEventListener('touchcancel', onTouchEnd);
    };
  }, [contentNode, dragY, enabled]);

  // Handle element: separate listeners for when handle is a sibling of content (MobileBottomSheet)
  // When handle is a child of content, the content listeners detect it via contains() check above
  useEffect(() => {
    if (!handleNode || !enabled) return;
    if (contentNode?.contains(handleNode)) return;

    let startY = 0;
    let lastY = 0;
    let lastTime = 0;
    let velocityY = 0;

    const onTouchStart = (e: TouchEvent) => {
      const y = e.touches[0].clientY;
      startY = y;
      lastY = y;
      lastTime = performance.now();
      velocityY = 0;
    };

    const onTouchMove = (e: TouchEvent) => {
      e.preventDefault();
      const y = e.touches[0].clientY;
      const now = performance.now();
      const dt = now - lastTime;
      if (dt > 0) velocityY = ((y - lastY) / dt) * 1000;
      lastY = y;
      lastTime = now;
      const pull = y - startY;
      dragY.set(Math.max(0, pull));
    };

    const onTouchEnd = () => {
      const dy = dragY.get();
      if (velocityY > 300 || dy > 120) {
        dismiss();
      } else {
        animate(dragY, 0, SPRING_DRAG);
      }
    };

    handleNode.addEventListener('touchstart', onTouchStart, { passive: true });
    handleNode.addEventListener('touchmove', onTouchMove, { passive: false });
    handleNode.addEventListener('touchend', onTouchEnd, { passive: true });
    handleNode.addEventListener('touchcancel', onTouchEnd, { passive: true });

    return () => {
      handleNode.removeEventListener('touchstart', onTouchStart);
      handleNode.removeEventListener('touchmove', onTouchMove);
      handleNode.removeEventListener('touchend', onTouchEnd);
      handleNode.removeEventListener('touchcancel', onTouchEnd);
    };
  }, [handleNode, contentNode, dragY, enabled]);

  return { contentRef, handleRef: handleRefCb, dragY };
}
