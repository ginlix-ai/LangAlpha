import { useState, useEffect, useRef, useCallback } from 'react';
import { animate, type AnimationPlaybackControls } from 'framer-motion';

interface UseAnimatedTextOptions {
  enabled?: boolean;
}

// Text that lands in one update by more than CATCH_UP_CHARS is not a token
// stream (a reconnect replay applied in one pass), and text that arrives while
// the tab is hidden is not being watched; both show at once with only the last
// LIVE_TAIL_CHARS left to type.
const CATCH_UP_CHARS = 600;
const LIVE_TAIL_CHARS = 320;

// The reveal paces itself to the model. A slow model is read at BASE_WORDS_PER_SEC,
// chunk by chunk. A fast one is followed at its own measured pace: each chain
// drains the backlog in backlog/pace seconds, so a backlog worth exactly
// TARGET_LAG_S refills to the same size and the reveal settles that far behind
// the newest text: never running dry between chunks, never falling seconds
// behind. The old fixed pace trailed a fast model by 2 to 3 seconds and the
// leftover popped in whole when the stream ended. MIN_PACE and MAX_PACE bound
// the reveal to 0.6x-2.5x the measured arrival rate, so it converges on
// TARGET_LAG_S over a few chains instead of sprinting; past a backlog of
// TARGET_LAG_S * MAX_PACE (~1.1s) it is already at that ceiling and speeds up
// no further. The rate itself is a first-order filter of arrivals (RATE_TAU_S)
// so a lumpy proxy or a batched read does not turn into surges in the reveal.
const BASE_WORDS_PER_SEC = 32;
const TARGET_LAG_S = 0.45;
const RATE_TAU_S = 1.5;
const MIN_PACE = 0.6;
const MAX_PACE = 2.5;
// When the stream ends, whatever is still hidden types out within this long.
const FINISH_S = 0.35;
const MIN_CHAIN_S = 0.05;

/**
 * useAnimatedText - Smooth typing animation for streamed text.
 *
 * Every text update restarts framer-motion's `animate()` from the cursor to the
 * new end, so the reveal is a linear lerp that is continuously re-targeted, not
 * one animation that plays out. What it holds steady is the lag behind the
 * stream (see the pacing constants), not the speed.
 */
export function useAnimatedText(text: string, { enabled = false }: UseAnimatedTextOptions = {}): string {
  const [displayText, setDisplayText] = useState('');
  const cursorRef = useRef(0);       // characters revealed so far
  const targetRef = useRef('');      // latest full text
  const animatingRef = useRef(false);
  const controlsRef = useRef<AnimationPlaybackControls | null>(null);
  const mountedRef = useRef(false);  // tracks first effect run
  const lastUpdateTimeRef = useRef(0); // throttle onUpdate to ~30fps
  const arrivalRef = useRef({ at: 0, cps: 0 }); // filtered arrival rate, chars/s
  const finishingRef = useRef(false);
  const chainRef = useRef(0);        // generation of the chain allowed to write state

  // A superseded chain must never touch state again. framer's stop() runs one
  // last tick at the wall clock before it tears down, so a chain older than
  // its duration (routine in a hidden tab, where timers fire once a second)
  // finishes synchronously inside stop() and its onComplete would start a
  // sibling the effect no longer tracks: two cursors typing the same text.
  const stopChain = useCallback(() => {
    chainRef.current++;
    controlsRef.current?.stop();
    controlsRef.current = null;
    animatingRef.current = false;
  }, []);

  const noteArrival = useCallback((chars: number) => {
    const now = performance.now();
    const a = arrivalRef.current;
    if (chars <= 0) return;
    if (!a.at) { a.at = now; return; }
    const dt = Math.max((now - a.at) / 1000, 0.001);
    a.at = now;
    const inst = chars / dt;
    const k = a.cps ? 1 - Math.exp(-dt / RATE_TAU_S) : 1;
    a.cps += (inst - a.cps) * k;
  }, []);

  const startChain = useCallback(() => {
    const from = cursorRef.current;
    const target = targetRef.current;
    const to = target.length;

    if (from >= to) {
      animatingRef.current = false;
      return;
    }

    animatingRef.current = true;
    const chain = ++chainRef.current;

    const segment = target.slice(from, to);
    const wordCount = segment.split(/\s+/).filter(Boolean).length || 1;
    let duration = wordCount / BASE_WORDS_PER_SEC;
    const cps = arrivalRef.current.cps;
    if (cps > 0) {
      // How many seconds of stream are still hidden. Pace relative to the
      // target lag: behind it, speed up; ahead, ease off.
      const backlogS = segment.length / cps;
      const pace = Math.min(Math.max(backlogS / TARGET_LAG_S, MIN_PACE), MAX_PACE);
      duration = Math.min(duration, backlogS / pace);
    }
    if (finishingRef.current) duration = Math.min(duration, FINISH_S);
    duration = Math.max(duration, MIN_CHAIN_S);

    controlsRef.current = animate(from, to, {
      duration,
      ease: 'linear',
      onUpdate(latest) {
        if (chain !== chainRef.current) return;
        const idx = Math.round(latest);
        cursorRef.current = idx;
        const now = Date.now();
        if (now - lastUpdateTimeRef.current < 32) return;
        lastUpdateTimeRef.current = now;
        setDisplayText(target.slice(0, idx));
      },
      onComplete() {
        if (chain !== chainRef.current) return;
        cursorRef.current = to;
        setDisplayText(target.slice(0, to));

        // Check if more text arrived while we were animating
        if (targetRef.current.length > to) {
          startChain();
        } else {
          animatingRef.current = false;
          finishingRef.current = false;
        }
      },
    });
  }, []);

  useEffect(() => {
    if (!enabled) {
      const behind = mountedRef.current && cursorRef.current > 0 && text.startsWith(targetRef.current.slice(0, cursorRef.current))
        ? text.length - cursorRef.current
        : 0;
      targetRef.current = text;
      if (behind > 0) {
        // The stream ended with text still hidden: type the rest out quickly
        // rather than popping it in whole.
        finishingRef.current = true;
        stopChain();
        startChain();
        return () => {
          stopChain();
        };
      }
      setDisplayText(text);
      cursorRef.current = text.length;
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
      stopChain();
      cursorRef.current = 0;
      arrivalRef.current = { at: 0, cps: 0 };
    }

    const delta = text.length - targetRef.current.length;
    const arrivedAtOnce = delta > CATCH_UP_CHARS;
    targetRef.current = text;
    if (!arrivedAtOnce) noteArrival(delta);
    finishingRef.current = false;

    if ((arrivedAtOnce || document.hidden) && text.length - cursorRef.current > CATCH_UP_CHARS) {
      stopChain();
      cursorRef.current = text.length - LIVE_TAIL_CHARS;
      setDisplayText(text.slice(0, cursorRef.current));
    }

    // The cleanup below stops the running animation before every re-run, so on
    // the streaming path this always starts a fresh chain re-aimed at the new
    // end. The guard still matters for the paths above that return without a
    // cleanup (empty text, a reset), which can leave an animation in flight.
    if (!animatingRef.current) {
      startChain();
    }

    return () => {
      stopChain();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [text, enabled]);

  // Decided during render, not in the effect: the render that turns `enabled`
  // off runs before the effect, and returning the full text there would flash
  // the whole tail for one frame before it types out. A strict prefix of the
  // target is exactly the state that still has something left to type.
  const behindNow = !enabled && displayText.length > 0
    && displayText.length < text.length
    && text.startsWith(displayText);
  return enabled || behindNow ? displayText : text;
}
