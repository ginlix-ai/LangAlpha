import React, { useContext, useEffectEvent, useLayoutEffect, useRef, useState } from 'react';
import {
  animate,
  motion,
  PresenceContext,
  useIsPresent,
  useMotionValue,
  useReducedMotion,
  type AnimationPlaybackControls,
} from '@/lib/framer';
import { DURATION, SPRING_SNAPPY, TWEEN } from '@/lib/motion';
import { ROW_FOLD } from './liveZoneTiming';
import { CardCallsContext } from './cardCalls';

/**
 * Enter/exit wrapper for the live-zone rows that never hands framer `auto`.
 *
 * framer cannot tween from `auto`: to resolve the exit keyframes it jumps the
 * row's inline height to 0, forces a layout, measures, then restores. During
 * that one forced layout the transcript is a whole row shorter, Chromium
 * clamps the chat scroll container's scrollTop to the shorter content, and the
 * clamp survives the restore, so the view jumps up at the first frame of every
 * fold of a tall row. framer only guards the window scroll, not an inner
 * scroller. Measuring the content ourselves keeps every keyframe in px.
 *
 * Growth is written to the box at the commit that caused it, from a
 * MutationObserver, which runs in the microtask after React's DOM writes and
 * ahead of the frame's ResizeObserver pass. A height that went through React
 * state and framer's frameloop lands one frame after the content did, and
 * that frame paints the new line clipped to half its glyphs. Writing it from
 * this row's own ResizeObserver instead is also a frame late for the follow:
 * the transcript's observer sits above this one, so a size it is handed from
 * a deeper callback is delivered on the next pass, with a loop error on every
 * growth frame. The ResizeObserver stays for growth that is not a mutation
 * (fonts, images).
 *
 * Only the live zone folds while the follow is pinning the reader to the end,
 * which is where the clamp shows; panels the reader opens and closes by hand
 * elsewhere in the transcript still animate `auto` and are a follow-up.
 *
 * The box opens first and the content then rises into it, a few px as it
 * fades in, once the box is nearly open; leaving, the content fades and lifts
 * while the box holds, and the box closes after. A box that opened over
 * content already showing drew the top of every glyph first and cut them off
 * at the bottom, a blind unrolling down the page, and closed the same way back.
 */

/** How long a row counts as entering. Its enter spring keeps retargeting to
 *  whatever the content becomes in that window, so a tool row whose label
 *  fills in over its first frames still glides in instead of snapping. */
const ENTER_MS = 500;
/** A row opening from rest is launched at the spring's own rate, which makes
 *  the opening an exponential ease-out that never overshoots: nearly open in
 *  160ms rather than 270, and the content, which waits for the box, comes in
 *  that much sooner. */
const OPEN_RATE = Math.sqrt(SPRING_SNAPPY.stiffness);
/** Half the former `space-y-2` on each side, so row-to-row spacing stays
 *  0.5rem and tracks --app-font-scale; the gap closes with the row's height. */
const ROW_GAP = '0.25rem';
const FADE_IN = TWEEN.quick;
/** A gap opening is a small thing arriving; one closing folds, as the row
 *  leaving beside it does. */
const GAP_OPEN = TWEEN.quick;
const GAP_CLOSE = ROW_FOLD;
/** The fade leaves on the exit's clock, and the box closes on the fold's once
 *  the content has gone from it. A live row clearing out takes the curve every
 *  exit takes: a leaving row should not draw the eye. */
const LEAVE = { opacity: TWEEN.exit, height: { ...ROW_FOLD, delay: DURATION.exit } };
/** How open the box is before the content comes in, and how far the content
 *  rises in and lifts out. A card taking a row's place moves the same way. */
const RISE_AT = 0.9;
export const RISE_IN_PX = 4;
export const RISE_OUT_PX = 3;

function paddingOf(el: HTMLElement): { top: number; bottom: number } {
  const style = getComputedStyle(el);
  return { top: parseFloat(style.paddingTop) || 0, bottom: parseFloat(style.paddingBottom) || 0 };
}

interface LiveRowProps {
  /** Target opacity while present; the row exits to 0 on its own. */
  opacity?: number;
  /** Applied to a content box inside the measured one, so a padding class
   *  here is honoured and counted in the row's height. The outer box is
   *  border-box: padding there would desynchronise it from the measurement. */
  className?: string;
  /** Vertical breathing room measured with the row, so it unfolds and folds
   *  with it. `gapBottom` defaults to `gap`, and the two are separate because
   *  the timeline needs an inset above a summary and none anywhere else: the
   *  segment spacing around the block owns the gap on the outside. */
  gap?: string;
  gapBottom?: string;
  /** The tool calls the row draws: the one whose card lands takes over the
   *  row's place (`CardArrivals` finds the row by them). */
  calls?: readonly string[];
  children: React.ReactNode;
}

export function LiveRow({ opacity = 1, className, gap = ROW_GAP, gapBottom = gap, calls, children }: LiveRowProps) {
  const outerRef = useRef<HTMLDivElement>(null);
  const innerRef = useRef<HTMLDivElement>(null);
  const height = useMotionValue(0);
  // The exit owns the height once presence is gone: a settle that touched it
  // then would stop framer's exit animation, whose completion is what
  // AnimatePresence waits on to unmount the row, and the row would stay
  // mounted at opacity 0 for good. Presence is read from the nearest
  // AnimatePresence only: one wrapped around the whole live zone would not
  // reach here (framer propagates an outer exit only on request).
  const isPresent = useIsPresent();
  const readPresent = useEffectEvent(() => isPresent);
  const wasPresentRef = useRef(isPresent);
  // A row already there when its AnimatePresence first rendered (a reload
  // mid-run, a tab return) opens settled, as `initial={false}` asks.
  const skipEnterRef = useRef(useContext(PresenceContext)?.initial === false);
  const reduceMotion = useReducedMotion();
  const readReduceMotion = useEffectEvent(() => reduceMotion);
  // Leaving because its call landed as a card. The card's box takes the
  // row's space in the commit it lands and shows the row fading there, so the
  // row gives its height up at once: folding it as well would move the page
  // by the row twice. Read on the exit's own render, which is the first one
  // to see the card, and marked on the row for the card to find. Decided
  // there once: a row already folding when its card lands keeps folding in
  // its own place, and is not the place the card takes.
  const cardCalls = useContext(CardCallsContext);
  const landedAsCard = !!calls?.some((id) => cardCalls.has(id));
  const [leftForCard, setLeftForCard] = useState<boolean | null>(null);
  if (!isPresent && leftForCard === null) setLeftForCard(landedAsCard);
  if (isPresent && leftForCard !== null) setLeftForCard(null);
  const handedOver = !isPresent && (leftForCard ?? landedAsCard);
  const readHandedOver = useEffectEvent(() => handedOver);
  const settleRef = useRef<(reenter?: boolean) => void>(() => {});
  const liftRef = useRef<() => void>(() => {});
  // The gap on screen, in px, which mid-tween is neither the old prop nor the new.
  const gapPxRef = useRef<{ top: number; bottom: number } | null>(null);

  useLayoutEffect(() => {
    const outer = outerRef.current;
    const inner = innerRef.current;
    if (!outer || !inner) return;
    let mountedAt = skipEnterRef.current ? -Infinity : performance.now();
    let target = 0;
    let running: AnimationPlaybackControls | null = null;
    // The content's own tween, after the box. A stopped tween ticks once more
    // as it stops, so each one checks it is still the latest.
    let rise: 'waiting' | AnimationPlaybackControls | null = null;
    let leg = 0;
    const tweenContent = (transition: typeof TWEEN.quick | typeof TWEEN.exit, paint: (p: number) => void) => {
      const own = ++leg;
      if (rise && rise !== 'waiting') rise.stop();
      rise = animate(0, 1, {
        ...transition,
        onUpdate: (p) => { if (own === leg) paint(p); },
        onComplete: () => { if (own === leg) rise = null; },
      });
    };
    const riseIn = () => tweenContent(TWEEN.quick, (p) => {
      inner.style.opacity = p < 1 ? `${p}` : '';
      inner.style.transform = p < 1 ? `translateY(${(1 - p) * RISE_IN_PX}px)` : '';
    });
    liftRef.current = () => {
      if (readReduceMotion()) return;
      tweenContent(TWEEN.exit, (p) => { inner.style.transform = `translateY(${-p * RISE_OUT_PX}px)`; });
    };
    // The content comes in once the box is nearly open, so no frame clips it.
    const unwatch = height.on('change', (h) => {
      if (rise === 'waiting' && readPresent() && h >= target * RISE_AT) riseIn();
    });
    const settle = (reenter = false, animateShrink = false) => {
      if (!readPresent()) return;
      // No boxes at all: the view holding this row is display:none (a cached
      // background thread that keeps streaming). Its content is still there;
      // measuring it as 0 would fold every row and clamp the scroll on return.
      if (inner.getClientRects().length === 0) return;
      const next = inner.getBoundingClientRect().height;
      if (next === target && !reenter) return;
      const shrink = next < target;
      target = next;
      running?.stop();
      running = null;
      // A re-entry mid-exit starts from wherever the exit tween stopped, so
      // it unfolds like an entry rather than popping open.
      if (reenter) mountedAt = performance.now();
      // Animate discrete DOM changes, but follow measured resizing directly:
      // a nested disclosure already animates its height. Starting another
      // spring on every resize leaves an empty row trailing that collapse.
      const entering = performance.now() - mountedAt < ENTER_MS;
      if (!readReduceMotion() && ((shrink && animateShrink) || entering)) {
        const from = height.get();
        // A retarget keeps the velocity it has; framer carries it over.
        const launch = entering && next > from && height.getVelocity() === 0;
        if (launch) {
          leg++;
          rise = 'waiting';
          inner.style.opacity = '0';
          inner.style.transform = `translateY(${RISE_IN_PX}px)`;
        }
        running = animate(height, next, launch ? { ...SPRING_SNAPPY, velocity: OPEN_RATE * (next - from) } : SPRING_SNAPPY);
        return;
      }
      // Both writes are needed: the style write is what paints this frame
      // (framer applies a jumped value on its own next frame), and the jump
      // is what stops that frame from writing the old value back.
      height.jump(next);
      outer.style.height = `${next}px`;
      if (rise === 'waiting') riseIn();
    };
    settleRef.current = settle;
    settle();
    const mo = new MutationObserver(() => settle(false, true));
    mo.observe(inner, { childList: true, characterData: true, subtree: true });
    const ro = new ResizeObserver(() => settle());
    ro.observe(inner);
    return () => {
      mo.disconnect();
      ro.disconnect();
      running?.stop();
      unwatch();
      leg++;
      if (rise && rise !== 'waiting') rise.stop();
    };
  }, [height]);

  // A gap that changes (the summary arriving above the first row, the first
  // row leaving so the next one is first) tweens in px, and every frame of it
  // re-measures the row and writes its height in the same frame. A CSS
  // transition on the padding was seen by neither observer (a style change is
  // no mutation, and padding is outside the observed content box), so the
  // padding grew under a box that stayed put and clipped the row's bottom.
  useLayoutEffect(() => {
    const inner = innerRef.current;
    if (!inner) return;
    const from = gapPxRef.current;
    const to = paddingOf(inner);
    gapPxRef.current = to;
    if (!from || (from.top === to.top && from.bottom === to.bottom)) return;
    if (readReduceMotion() || !readPresent()) { settleRef.current(); return; }
    inner.style.paddingTop = `${from.top}px`;
    inner.style.paddingBottom = `${from.bottom}px`;
    const grows = to.top + to.bottom > from.top + from.bottom;
    // A stopped tween ticks once more as it stops, after React has written
    // the next gap: that tick, or the landing it may reach, would write this
    // leg's padding over it, and the next leg would start and end there.
    let stopped = false;
    const controls = animate(0, 1, {
      ...(grows ? GAP_OPEN : GAP_CLOSE),
      onUpdate: (p) => {
        if (stopped) return;
        const now = { top: from.top + (to.top - from.top) * p, bottom: from.bottom + (to.bottom - from.bottom) * p };
        gapPxRef.current = now;
        inner.style.paddingTop = `${now.top}px`;
        inner.style.paddingBottom = `${now.bottom}px`;
        settleRef.current();
      },
      // Back to the props' own units, so a rem gap keeps tracking the font scale.
      onComplete: () => {
        if (stopped) return;
        inner.style.paddingTop = gap;
        inner.style.paddingBottom = gapBottom;
        settleRef.current();
      },
    });
    return () => {
      stopped = true;
      controls.stop();
    };
  }, [gap, gapBottom]);

  // A row that re-enters mid-exit keeps whatever height the exit had reached:
  // framer stops the tween where it is and has no px target to return to.
  // Re-measure and reopen it.
  useLayoutEffect(() => {
    if (isPresent && !wasPresentRef.current) settleRef.current(true);
    if (!isPresent && wasPresentRef.current) {
      const outer = outerRef.current;
      // Gone in this commit, so the card measures the list without it. The
      // value as well as the style: framer writes the row's values back from
      // a microtask after every render, and its instant exit only lands on
      // the next frame. A jump, as in a settle, so an opening spring still
      // running stops rather than writing its height back.
      if (readHandedOver() && outer) {
        height.jump(0);
        outer.style.height = '0px';
      } else {
        liftRef.current();
      }
    }
    wasPresentRef.current = isPresent;
  }, [isPresent, height]);

  return (
    <motion.div
      ref={outerRef}
      initial={{ opacity: 0 }}
      animate={{ opacity }}
      exit={{ opacity: 0, height: 0, transition: reduceMotion || handedOver ? { duration: 0 } : LEAVE }}
      transition={FADE_IN}
      style={{ height, overflow: 'hidden' }}
      data-call-row={calls?.length ? calls.join(' ') : undefined}
      data-handed-over={handedOver ? '' : undefined}
    >
      <div ref={innerRef} style={{ paddingTop: gap, paddingBottom: gapBottom }}>
        <div className={className}>{children}</div>
      </div>
    </motion.div>
  );
}
