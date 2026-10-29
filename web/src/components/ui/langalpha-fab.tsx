import React, { useRef, useCallback, useState, useEffect } from 'react';
import { motion, useMotionValue, useAnimationControls, type PanInfo } from '@/lib/framer';
import { SPRING_DRAG, TWEEN } from '@/lib/motion';

interface LangAlphaFabProps {
  onClick: () => void;
}

const FAB_SIZE = 52;
const EDGE_MARGIN = 12;

/**
 * Draggable floating action button with the LangAlpha logo.
 * Constrained to the lower half of the screen, snaps to nearest edge on release.
 * Tap (not drag) opens the chat input.
 */
function LangAlphaFab({ onClick }: LangAlphaFabProps) {
  const x = useMotionValue(0);
  const y = useMotionValue(0);
  const controls = useAnimationControls();
  const isDragging = useRef(false);
  const [bounds, setBounds] = useState({ top: 0, left: 0, right: 0, bottom: 0 });

  // Appear animation on mount. A tween: framer's default for scale is an
  // underdamped spring, which overshoots.
  useEffect(() => {
    controls.start({ scale: 1, opacity: 1, transition: TWEEN.enter });
  }, [controls]);

  // Compute drag bounds: lower half of viewport, above bottom tab bar
  useEffect(() => {
    const update = () => {
      const vw = window.innerWidth;
      const vh = window.innerHeight;
      const tabHeight = parseInt(getComputedStyle(document.documentElement).getPropertyValue('--bottom-tab-height') || '78', 10);

      setBounds({
        top: -(vh * 0.4),  // can go up to ~40% from default position
        left: -(vw - FAB_SIZE - EDGE_MARGIN * 2),
        right: 0,
        bottom: -tabHeight,
      });
    };
    update();
    window.addEventListener('resize', update);
    return () => window.removeEventListener('resize', update);
  }, []);

  const handleDragStart = useCallback(() => {
    isDragging.current = true;
  }, []);

  const handleDragEnd = useCallback((_: any, _info: PanInfo) => {
    // Snap to nearest horizontal edge
    const vw = window.innerWidth;
    const currentX = x.get();
    // Default position is right: 16px, so x=0 means right edge
    // Negative x means moved left
    const fabCenterX = vw - 16 - FAB_SIZE / 2 + currentX;
    const snapToLeft = fabCenterX < vw / 2;

    const targetX = snapToLeft
      ? -(vw - FAB_SIZE - EDGE_MARGIN * 2)  // snap to left edge
      : 0; // snap back to right edge (default)

    controls.start({
      x: targetX,
      transition: SPRING_DRAG,
    });

    // Small delay so the click handler can check isDragging
    requestAnimationFrame(() => {
      isDragging.current = false;
    });
  }, [x, controls]);

  const handleTap = useCallback(() => {
    if (!isDragging.current) {
      onClick();
    }
  }, [onClick]);

  return (
    <motion.button
      drag
      dragConstraints={bounds}
      dragElastic={0.1}
      dragMomentum={false}
      onDragStart={handleDragStart}
      onDragEnd={handleDragEnd}
      onTap={handleTap}
      animate={controls}
      style={{ x, y }}
      initial={{ scale: 0.6, opacity: 0 }}
      exit={{ scale: 0.6, opacity: 0, transition: TWEEN.exit }}
      className="langalpha-fab"
      aria-label="Open chat"
    >
      <svg
        width="26"
        height="26"
        viewBox="0 0 56 56"
        fill="none"
        xmlns="http://www.w3.org/2000/svg"
      >
        <path
          d="M38.0312 27.6023L47.9852 23.4051C48.5292 23.1758 48.7571 22.5277 48.4765 22.0084L43.6363 13.0496C43.3489 12.5178 42.6591 12.3605 42.1696 12.7153L32.6523 19.6136M38.0312 27.6023L31.933 30.1736C29.7869 31.0785 29.4456 33.9773 31.3229 35.3559L42.168 43.3202C42.6573 43.6795 43.3512 43.5235 43.6397 42.9895L48.5087 33.9776C48.7774 33.4803 48.5808 32.8593 48.0749 32.6072L38.0312 27.6023ZM32.6523 19.6136L28.5854 22.5614C26.7503 23.8916 24.1597 22.7846 23.8525 20.5391L22.1554 8.13556C22.0732 7.53499 22.54 7 23.1461 7H32.7163C33.3048 7 33.766 7.50561 33.7121 8.09159L32.6523 19.6136Z"
          stroke="currentColor"
          strokeWidth="3"
        />
        <path
          d="M33.282 45L33.6587 48.0175C33.7338 48.6188 33.2611 49.1482 32.6551 49.1413L23.1712 49.034C22.5829 49.0273 22.1274 48.5167 22.1878 47.9315L23.1428 38.668C23.2309 37.8127 22.2701 37.2523 21.5691 37.7501L13.853 43.2293C13.3591 43.58 12.6693 43.4146 12.3882 42.8781L7.68991 33.911C7.41644 33.389 7.65127 32.745 8.1965 32.5215L16.1128 29.2775C16.9026 28.9539 16.9499 27.8532 16.1909 27.4629L15.5 27.1076L8.21058 23.3592C7.70888 23.1012 7.51977 22.4795 7.79284 21.9858L12.7222 13.0745C13.0166 12.5423 13.714 12.3939 14.1995 12.7603L16.5 14.4959"
          stroke="currentColor"
          strokeWidth="3"
          strokeLinecap="round"
        />
      </svg>
    </motion.button>
  );
}

export default LangAlphaFab;
