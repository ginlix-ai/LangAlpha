import { useState, useEffect, useLayoutEffect, useRef, useCallback } from 'react';
import { animate, type AnimationPlaybackControls } from '@/lib/framer';
import { isPageUnseen } from '@/lib/pageVisibility';

interface UseAnimatedTextOptions {
  enabled?: boolean;
}

// Text that lands in one update by more than CATCH_UP_CHARS is not a token
// stream (a reconnect replay applied in one pass): it shows at once with only
// the last LIVE_TAIL_CHARS left to type.
export const CATCH_UP_CHARS = 600;
export const LIVE_TAIL_CHARS = 320;
// The shown text updates at most once per TICK_MS.
export const TICK_MS = 32;

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
// A stream quiet this long (thinking, a tool call, a stalled link) is treated
// as ended until it speaks again, so the word the hold keeps types out rather
// than staying hidden through the pause. Well past the ~320ms gap of a lumpy
// proxy, so a live stream still never shows a stub.
const QUIET_S = 1;
const MIN_CHAIN_S = 0.05;

// A Latin word wraps as a unit, so a cursor parked inside one shows a stub at
// the line end that hops to the next line once the rest arrives. The cursor
// only rests at word boundaries, and a run still open at the end of the text
// (a token split mid-word, or a word whose following space is the next token)
// stays hidden until something follows it, or the stream ends or goes quiet.
// Scripts that wrap per character (CJK) or are not space-delimited (Thai and
// beyond) are boundaries at every character, so their reveal is unchanged.
// Arrows and math symbols wrap glued to their neighbours, like letters.
const isWordChar = (ch: string) => {
  const c = ch.charCodeAt(0);
  return !/\s/.test(ch) && (c < 0x0e00 || (c >= 0x2190 && c <= 0x27bf));
};
// A run longer than this is not a word that will wrap as a unit but a URL,
// a hash or a minified line, and holding it back until whitespace arrives
// would hide it whole. Inside a longer run the reveal trails the cursor by
// this many characters, so it moves continuously instead of stalling and
// then dumping the run.
const MAX_WORD_CHARS = 24;
// The nearest boundary at or before `idx`. The end of the text counts as
// inside a word: its next character has not arrived yet.
const wordStart = (text: string, idx: number) => {
  if (idx <= 0 || idx > text.length) return idx;
  if (!isWordChar(text[idx - 1]) || (idx < text.length && !isWordChar(text[idx]))) return idx;
  let i = idx;
  while (i > 0 && isWordChar(text[i - 1])) {
    if (idx - i >= MAX_WORD_CHARS) return idx - MAX_WORD_CHARS;
    i--;
  }
  return i;
};
// Where text shown at once stops: before the word still arriving while it
// streams, at the end once it is done.
const holdEnd = (text: string, hold: boolean) => (hold ? wordStart(text, text.length) : text.length);

/**
 * useAnimatedText - Smooth typing animation for streamed text.
 *
 * Every text update restarts framer-motion's `animate()` from the cursor to the
 * new end, so the reveal is a linear lerp that is continuously re-targeted, not
 * one animation that plays out. What it holds steady is the lag behind the
 * stream (see the pacing constants), not the speed.
 */
export function useAnimatedText(text: string, { enabled = false }: UseAnimatedTextOptions = {}): string {
  // Text that exists at mount is on screen in the mount's own commit. Starting
  // empty and snapping from the first effect took two commits, and a bubble
  // that mounts with its reply (a reconnect catch-up) could paint empty in
  // between, then jump to full height as a layout shift. A segment that mounts
  // streaming holds a word still arriving, like every later update does.
  const [displayText, setDisplayText] = useState(() => text.slice(0, holdEnd(text, enabled)));
  const cursorRef = useRef(displayText.length); // characters revealed so far
  // Where the reveal has reached before the word hold: it runs ahead of the
  // cursor while it types through a word the hold keeps off screen.
  const posRef = useRef(displayText.length);
  const targetRef = useRef(text);        // latest full text
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

  // Show `next` at once up to the word the hold keeps, never behind what is
  // already shown of it. Returns whether `next` continues the text shown.
  const snapTo = useCallback((next: string, hold: boolean) => {
    const continues = next.startsWith(targetRef.current.slice(0, cursorRef.current));
    const end = holdEnd(next, hold);
    cursorRef.current = continues ? Math.max(cursorRef.current, end) : end;
    posRef.current = cursorRef.current;
    targetRef.current = next;
    setDisplayText(next.slice(0, cursorRef.current));
    return continues;
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

  const startChain = useCallback(function startChain() {
    // Resume from where the reveal reached, not from what the hold let on
    // screen. Every update restarts the chain, so restarting at the held
    // cursor threw away the progress into the word being held: once text
    // arrived every frame, a word longer than one frame's progress (any
    // link past MAX_WORD_CHARS) stayed hidden until the stream ended.
    const shown = cursorRef.current;
    const from = Math.max(posRef.current, shown);
    const target = targetRef.current;
    const to = finishingRef.current ? target.length : wordStart(target, target.length);

    if (from >= to) {
      animatingRef.current = false;
      return;
    }

    animatingRef.current = true;
    const chain = ++chainRef.current;
    // A finished chain writes nothing more. With its animations skipped (a
    // hidden tab, see lib/framer) framer completes at once but applies the
    // final value on its next frame, after the completion: that late update
    // cut a reply that ended while hidden back to its last word's start.
    let done = false;

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
      // eslint-disable-next-line no-restricted-syntax -- the cursor runs at a constant pace across a chunk, which no curve names
      ease: 'linear',
      onUpdate(latest) {
        if (done || chain !== chainRef.current) return;
        posRef.current = latest;
        // Never behind what is shown: a cursor that already sits inside a
        // word (shown in full before `enabled` turned on) holds there rather
        // than retracting the stub.
        const idx = Math.max(shown, wordStart(target, Math.round(latest)));
        cursorRef.current = idx;
        const now = Date.now();
        if (now - lastUpdateTimeRef.current < TICK_MS) return;
        lastUpdateTimeRef.current = now;
        setDisplayText(target.slice(0, idx));
      },
      onComplete() {
        if (chain !== chainRef.current) return;
        done = true;
        cursorRef.current = to;
        posRef.current = to;
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

  // The effect below arms this for each text it applies, and its cleanup
  // disarms it, so it fires only once the stream has gone quiet.
  const releaseWhenQuiet = useCallback(() => {
    const timer = setTimeout(() => {
      finishingRef.current = true;
      if (!animatingRef.current) startChain();
    }, QUIET_S * 1000);
    return () => clearTimeout(timer);
  }, [startChain]);

  // Text that arrives unseen (a hidden page, or what one held back and applies
  // on its return, see lib/pageVisibility) was never watched arriving: it is on
  // screen at once, up to the word the hold keeps, and the effect below finds
  // nothing left to type. A layout effect, because the return applies in a
  // synchronous commit and only an update from here renders before the first
  // frame back paints.
  useLayoutEffect(() => {
    if (!mountedRef.current || !isPageUnseen()) return;
    const delta = text.length - targetRef.current.length;
    stopChain();
    finishingRef.current = false;
    if (!snapTo(text, enabled)) arrivalRef.current = { at: 0, cps: 0 };
    else if (delta <= CATCH_UP_CHARS) noteArrival(delta);
  }, [text, enabled, stopChain, snapTo, noteArrival]);

  useEffect(() => {
    // If the stretch the reveal ran through changed, resume from the screen.
    if (!text.startsWith(targetRef.current.slice(0, posRef.current))) {
      posRef.current = cursorRef.current;
    }
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
      posRef.current = text.length;
      return;
    }

    // Text that existed at mount shows as is (the initial state holds it),
    // up to the word hold; only text arriving after mount types out. This
    // prevents re-animation on tab switches, reconnects, and remounts. The
    // snap is a no-op on a true mount and catches `enabled` turning on after
    // text changed.
    if (!mountedRef.current) {
      mountedRef.current = true;
      snapTo(text, enabled);
      return releaseWhenQuiet();
    }

    if (!text) {
      setDisplayText('');
      cursorRef.current = 0;
      posRef.current = 0;
      targetRef.current = '';
      return;
    }

    // If text was replaced (new message / component remount), reset
    if (!text.startsWith(targetRef.current.slice(0, cursorRef.current))) {
      stopChain();
      cursorRef.current = 0;
      posRef.current = 0;
      arrivalRef.current = { at: 0, cps: 0 };
    }

    const delta = text.length - targetRef.current.length;
    const arrivedAtOnce = delta > CATCH_UP_CHARS;
    targetRef.current = text;
    if (!arrivedAtOnce) noteArrival(delta);
    finishingRef.current = false;

    if (arrivedAtOnce && text.length - cursorRef.current > CATCH_UP_CHARS) {
      stopChain();
      // Never behind what is already on screen: the boundary can sit before
      // the cursor when the snap point falls inside the run it was typing.
      cursorRef.current = Math.max(cursorRef.current, wordStart(text, text.length - LIVE_TAIL_CHARS));
      posRef.current = Math.max(posRef.current, cursorRef.current);
      setDisplayText(text.slice(0, cursorRef.current));
    }

    // The cleanup below stops the running animation before every re-run, so on
    // the streaming path this always starts a fresh chain re-aimed at the new
    // end. The guard still matters for the paths above that return without a
    // cleanup (empty text, a reset), which can leave an animation in flight.
    if (!animatingRef.current) {
      startChain();
    }

    const disarm = releaseWhenQuiet();
    return () => {
      disarm();
      stopChain();
    };
  }, [text, enabled, stopChain, startChain, snapTo, noteArrival, releaseWhenQuiet]);

  // Decided during render, not in the effect: the render that turns `enabled`
  // off runs before the effect, and returning the full text there would flash
  // the whole tail for one frame before it types out. A strict prefix of the
  // target is exactly the state that still has something left to type.
  const behindNow = !enabled && displayText.length > 0
    && displayText.length < text.length
    && text.startsWith(displayText);
  return enabled || behindNow ? displayText : text;
}
