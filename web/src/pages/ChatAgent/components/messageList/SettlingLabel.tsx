import React, { useLayoutEffect, useRef } from 'react';
import { animate, useReducedMotion } from '@/lib/framer';
import { TWEEN } from '@/lib/motion';

const RESIZE = TWEEN.quick;

/**
 * A one-line label whose box eases to the width of new text instead of
 * hopping, so whatever sits after it (the summary's chevron) slides rather
 * than jumps. The text is the box's own inline content, not an inline-block
 * inside it: an atomic inline is clipped whole, never ellipsized, so a label
 * narrower than its text (mid-tween, or at rest on a narrow screen) lost its
 * tail with no "…".
 */
export function SettlingLabel({ text }: { text: string }): React.ReactElement {
  const ref = useRef<HTMLSpanElement>(null);
  // The width on screen, which mid-tween is neither the old text's nor the new.
  const shownRef = useRef<number | null>(null);
  const reduceMotion = useReducedMotion();
  useLayoutEffect(() => {
    const el = ref.current;
    if (!el) return;
    const from = shownRef.current;
    el.style.width = '';
    const to = el.getBoundingClientRect().width;
    shownRef.current = to;
    if (from === null || reduceMotion || Math.abs(to - from) < 0.5) return;
    // Hold the old width in this same layout, before the frame paints: the
    // tween's first write lands a frame later, and one paint at the new
    // width is the hop this exists to remove.
    el.style.width = `${from}px`;
    const controls = animate(from, to, {
      ...RESIZE,
      onUpdate: (v) => { shownRef.current = v; el.style.width = `${v}px`; },
      onComplete: () => { el.style.width = ''; },
    });
    return () => controls.stop();
  }, [text, reduceMotion]);
  return (
    <span ref={ref} className="min-w-0 max-w-full overflow-hidden text-ellipsis whitespace-nowrap">
      {text}
    </span>
  );
}
