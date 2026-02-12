import { useState, useEffect, useRef, useCallback } from 'react';
import { animate } from 'framer-motion';

/**
 * useAnimatedText - Smooth word-based typing animation for streamed text.
 *
 * Uses framer-motion's `animate()` with linear easing and animation chaining
 * to reveal text at a constant speed (~32 words/sec) regardless of chunk size.
 *
 * When new text arrives mid-animation, the current animation finishes and
 * seamlessly chains into a new animation for the remaining text — no
 * stop-and-restart discontinuities.
 *
 * @param {string} text - The full (or partial) text received so far
 * @param {Object}  [options]
 * @param {boolean} [options.enabled=true] - When false, returns `text` as-is (use for history)
 * @returns {string} The portion of `text` to display right now
 */
export function useAnimatedText(text, { enabled = false } = {}) {
  const [displayText, setDisplayText] = useState('');
  const cursorRef = useRef(0);       // characters revealed so far
  const targetRef = useRef('');      // latest full text
  const animatingRef = useRef(false);
  const controlsRef = useRef(null);
  const mountedRef = useRef(false);  // tracks first effect run

  const startChain = useCallback(() => {
    const from = cursorRef.current;
    const target = targetRef.current;
    const to = target.length;

    if (from >= to) {
      animatingRef.current = false;
      return;
    }

    animatingRef.current = true;

    // Count words in the new segment to determine duration
    const segment = target.slice(from, to);
    const wordCount = segment.split(/\s+/).filter(Boolean).length || 1;
    const duration = Math.max(Math.min(wordCount / 32, 2.5), 0.05);

    controlsRef.current = animate(from, to, {
      duration,
      ease: 'linear',
      onUpdate(latest) {
        const idx = Math.round(latest);
        cursorRef.current = idx;
        setDisplayText(target.slice(0, idx));
      },
      onComplete() {
        cursorRef.current = to;
        setDisplayText(target.slice(0, to));

        // Check if more text arrived while we were animating
        if (targetRef.current.length > to) {
          startChain();
        } else {
          animatingRef.current = false;
        }
      },
    });
  }, []);

  useEffect(() => {
    if (!enabled) {
      setDisplayText(text);
      cursorRef.current = text.length;
      targetRef.current = text;
      return;
    }

    // On first effect run, display whatever text already exists instantly.
    // Only text arriving AFTER mount gets the typing animation.
    // This prevents re-animation on tab switches, reconnects, and remounts.
    if (!mountedRef.current) {
      mountedRef.current = true;
      setDisplayText(text);
      cursorRef.current = text.length;
      targetRef.current = text;
      return;
    }

    if (!text) {
      setDisplayText('');
      cursorRef.current = 0;
      targetRef.current = '';
      return;
    }

    // If text was replaced (new message / component remount), reset
    if (!text.startsWith(targetRef.current.slice(0, cursorRef.current))) {
      controlsRef.current?.stop();
      cursorRef.current = 0;
      animatingRef.current = false;
    }

    targetRef.current = text;

    // If an animation is already running, it will pick up the new target on complete
    if (!animatingRef.current) {
      startChain();
    }

    return () => {
      controlsRef.current?.stop();
      animatingRef.current = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [text, enabled]);

  return enabled ? displayText : text;
}
