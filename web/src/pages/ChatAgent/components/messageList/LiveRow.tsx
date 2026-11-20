import React, { useContext, useEffectEvent, useLayoutEffect, useRef, useState } from 'react';
import {
  animate,
  motion,
  PresenceContext,
  useMotionValue,
  usePresence,
  useReducedMotion,
  type AnimationPlaybackControls,
  type MotionValue,
} from '@/lib/framer';
import { DURATION, SPRING_FOLD, SPRING_SNAPPY, TWEEN } from '@/lib/motion';
import { ROW_FOLD } from './liveZoneTiming';
import { CardCallsContext } from './cardCalls';
import { CountToggleContext, type PanelRow } from './countToggle';

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
 *
 * Rows the reader's own toggle of the count brings or sends away are the
 * exception (`countToggle.ts`): they fold together as one panel, which writes
 * each row's height while it runs, and the step on the status line changes
 * rows with no entrance or exit at all.
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

/** Both writes are needed: the style write is what paints this frame (framer
 *  applies a jumped value on its own next frame), and the jump is what stops
 *  that frame from writing the old value back, or a spring still running. */
function writeHeight(value: MotionValue<number>, box: HTMLElement, px: number) {
  value.jump(px);
  box.style.height = `${px}px`;
}

/** How a row comes or goes: on its own entrance and exit, by a step changing
 *  rows, or with the panel the reader's toggle folds. */
type Passage = 'own' | 'row' | 'panel';

/** What the effects ask of the row's height and content, which the first one
 *  owns. */
interface RowMotion {
  settle: (reenter?: boolean) => void;
  /** The content lifting out as the row leaves on its own. */
  lift: () => void;
  /** Joins the panel the toggle folds, opening or shutting. */
  fold: (opening: boolean) => void;
  /** Draws, from this commit, a step another row of `stepHeight` px drew. */
  takeStep: (stepHeight: number | null) => void;
  /** Takes the row back from the panel. */
  unfold: () => void;
}

const AT_REST: RowMotion = { settle: () => {}, lift: () => {}, fold: () => {}, takeStep: () => {}, unfold: () => {} };

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
  /** The steps the row draws, by row key, which makes it a row of the count
   *  (`CountToggleContext`) and names it for the toggle to find. */
  steps?: readonly string[];
  children: React.ReactNode;
}

export function LiveRow({ opacity = 1, className, gap = ROW_GAP, gapBottom = gap, calls, steps, children }: LiveRowProps) {
  const outerRef = useRef<HTMLDivElement>(null);
  const innerRef = useRef<HTMLDivElement>(null);
  const height = useMotionValue(0);
  // The exit owns the height once presence is gone: a settle that touched it
  // then would stop framer's exit animation, whose completion is what
  // AnimatePresence waits on to unmount the row, and the row would stay
  // mounted at opacity 0 for good. Presence is read from the nearest
  // AnimatePresence only: one wrapped around the whole live zone would not
  // reach here (framer propagates an outer exit only on request).
  // The row says when it may be removed as well as the box's own exit: a
  // panel closing takes as long as its height asks, which no exit transition
  // written at render can know.
  const [isPresent, safeToRemove] = usePresence();
  const letGo = useEffectEvent(() => safeToRemove?.());
  const readPresent = useEffectEvent(() => isPresent);
  const wasPresentRef = useRef(isPresent);
  const reduceMotion = useReducedMotion();
  const readReduceMotion = useEffectEvent(() => reduceMotion);
  // What the reader's toggle of the count does with the row, in the commit of
  // the toggle. A step the status line names is on screen open or shut, so
  // the row taking it over is there at once and the one giving it up is gone
  // at once; every other row of the count moves with the panel.
  const toggle = useContext(CountToggleContext);
  const toggled: Passage = !toggle || !steps ? 'own'
    : toggle.step !== null && steps.includes(toggle.step) ? 'row'
    : reduceMotion ? 'own' : 'panel';
  const readToggled = useEffectEvent(() => toggled);
  const readStepHeight = useEffectEvent(() => toggle?.stepHeight ?? null);
  const joinPanel = useEffectEvent((row: PanelRow) => toggle?.panel.join(row));
  // How the row arrived, for its first layout only. A row already there when
  // its AnimatePresence first rendered (a reload mid-run, a tab return) opens
  // settled, as `initial={false}` asks.
  const quietStart = useContext(PresenceContext)?.initial === false;
  const arrivalRef = useRef<{ by: Passage | 'settled'; stepHeight: number | null }>({
    by: toggled !== 'own' ? toggled : quietStart ? 'settled' : 'own',
    stepHeight: toggle?.stepHeight ?? null,
  });
  // Leaving because its call landed as a card. The card's box takes the
  // row's space in the commit it lands and shows the row fading there, so the
  // row gives its height up at once: folding it as well would move the page
  // by the row twice. Read on the exit's own render, which is the first one
  // to see the card, and marked on the row for the card to find. Decided
  // there once: a row already folding when its card lands keeps folding in
  // its own place, and is not the place the card takes. The toggle is read
  // on that render too: the block names it for the one commit.
  const cardCalls = useContext(CardCallsContext);
  const leavingBy: Passage | 'card' = calls?.some((id) => cardCalls.has(id)) ? 'card' : toggled;
  const [leftBy, setLeftBy] = useState<typeof leavingBy | null>(null);
  if (!isPresent && leftBy === null) setLeftBy(leavingBy);
  if (isPresent && leftBy !== null) setLeftBy(null);
  const goneBy = isPresent ? null : (leftBy ?? leavingBy);
  const readGoneBy = useEffectEvent(() => goneBy);
  const motionRef = useRef<RowMotion>(AT_REST);
  // The gap on screen, in px, which mid-tween is neither the old prop nor the new.
  const gapPxRef = useRef<{ top: number; bottom: number } | null>(null);

  useLayoutEffect(() => {
    const outer = outerRef.current;
    const inner = innerRef.current;
    if (!outer || !inner) return;
    const arrival = arrivalRef.current;
    arrivalRef.current = { by: 'settled', stepHeight: null };
    let mountedAt = arrival.by === 'own' ? performance.now() : -Infinity;
    let target = 0;
    let running: AnimationPlaybackControls | null = null;
    // The content's own tween, after the box. A stopped tween ticks once more
    // as it stops, so each one checks it is still the latest.
    let rise: 'waiting' | AnimationPlaybackControls | null = null;
    let leg = 0;
    const stopContent = () => {
      leg++;
      if (rise && rise !== 'waiting') rise.stop();
      rise = null;
    };
    const tweenContent = (transition: typeof TWEEN.quick | typeof TWEEN.exit, paint: (p: number) => void) => {
      stopContent();
      const own = leg;
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
    const lift = () => {
      if (readReduceMotion()) return;
      tweenContent(TWEEN.exit, (p) => { inner.style.transform = `translateY(${-p * RISE_OUT_PX}px)`; });
    };
    // A step taken over from a row of another height: the box is easing from
    // that height to this row's own, the content held at the middle of it,
    // where both rows draw the step.
    let easingIn = false;
    // Whatever the row was doing on its own stops where a toggle takes it from.
    const halt = () => {
      running?.stop();
      running = null;
      stopContent();
      easingIn = false;
      inner.style.transform = '';
    };
    // The content comes in once the box is nearly open, so no frame clips it.
    const unwatch = height.on('change', (h) => {
      if (rise === 'waiting' && readPresent() && h >= target * RISE_AT) riseIn();
      if (easingIn && readPresent()) {
        easingIn = h !== target;
        inner.style.transform = easingIn ? `translateY(${(h - target) / 2}px)` : '';
      }
    });
    // The panel the reader's toggle folds, while it is writing the height.
    let panelRow: PanelRow | null = null;
    const settle = (reenter = false, animateShrink = false) => {
      if (!readPresent() || panelRow) return;
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
          stopContent();
          rise = 'waiting';
          inner.style.opacity = '0';
          inner.style.transform = `translateY(${RISE_IN_PX}px)`;
        }
        running = animate(height, next, launch ? { ...SPRING_SNAPPY, velocity: OPEN_RATE * (next - from) } : SPRING_SNAPPY);
        return;
      }
      writeHeight(height, outer, next);
      if (rise === 'waiting') riseIn();
    };
    const unfold = () => {
      if (!panelRow) return;
      panelRow = null;
      // Whatever the panel had dimmed the content to.
      inner.style.opacity = '';
    };
    const fold = (opening: boolean) => {
      // A row the panel was already moving joins again as it is, as dim as
      // it had got.
      halt();
      const own = inner.getBoundingClientRect().height;
      if (opening) target = own;
      const row: PanelRow = {
        box: outer,
        opening,
        from: height.get(),
        to: opening ? own : 0,
        shown: inner.style.opacity === '' ? 1 : Number(inner.style.opacity),
        paint: (h, shown) => {
          if (panelRow !== row) return;
          writeHeight(height, outer, h);
          inner.style.opacity = shown < 1 ? `${shown}` : '';
        },
        land: () => {
          if (panelRow !== row) return;
          panelRow = null;
          if (opening) settle();
          else letGo();
        },
      };
      panelRow = row;
      joinPanel(row);
    };
    const takeStep = (stepHeight: number | null) => {
      unfold();
      halt();
      inner.style.opacity = '';
      mountedAt = -Infinity;
      // A view that is display:none measures nothing: the row settles when shown.
      if (inner.getClientRects().length === 0) return;
      const own = inner.getBoundingClientRect().height;
      target = own;
      easingIn = stepHeight !== null && Math.abs(stepHeight - own) >= 0.5 && !readReduceMotion();
      if (stepHeight === null || !easingIn) return writeHeight(height, outer, own);
      writeHeight(height, outer, stepHeight);
      inner.style.transform = `translateY(${(stepHeight - own) / 2}px)`;
      // The fold's spring, which the rows the toggle unfolds over it open on:
      // the step moves by the sum of the two, on one curve.
      running = animate(height, own, SPRING_FOLD);
    };
    motionRef.current = { settle, lift, fold, takeStep, unfold };

    if (arrival.by === 'panel') fold(true);
    else if (arrival.by === 'row') takeStep(arrival.stepHeight);
    else settle();
    const mo = new MutationObserver(() => settle(false, true));
    mo.observe(inner, { childList: true, characterData: true, subtree: true });
    const ro = new ResizeObserver(() => settle());
    ro.observe(inner);
    return () => {
      mo.disconnect();
      ro.disconnect();
      running?.stop();
      unwatch();
      unfold();
      stopContent();
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
    if (readReduceMotion() || !readPresent()) { motionRef.current.settle(); return; }
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
        motionRef.current.settle();
      },
      // Back to the props' own units, so a rem gap keeps tracking the font scale.
      onComplete: () => {
        if (stopped) return;
        inner.style.paddingTop = gap;
        inner.style.paddingBottom = gapBottom;
        motionRef.current.settle();
      },
    });
    return () => {
      stopped = true;
      controls.stop();
    };
  }, [gap, gapBottom]);

  useLayoutEffect(() => {
    const outer = outerRef.current;
    const row = motionRef.current;
    if (isPresent && !wasPresentRef.current) {
      // Back by a toggle while the last one's fold still held it in the list:
      // it reopens with the panel from where it had got to, or takes its step
      // back as it would arriving.
      const by = readToggled();
      if (by === 'panel') {
        row.fold(true);
      } else if (by === 'row') {
        row.takeStep(readStepHeight());
      } else {
        // A row that re-enters mid-exit keeps whatever height the exit had
        // reached: framer stops the tween where it is and has no px target to
        // return to. Re-measure and reopen it.
        row.unfold();
        row.settle(true);
      }
    }
    if (!isPresent && wasPresentRef.current && outer) {
      const by = readGoneBy();
      if (by === 'panel') {
        // The panel lets the row go when it lands.
        row.fold(false);
      } else {
        row.unfold();
        // To a card or another row, gone in this commit, so the card measures
        // the list without it: framer writes the row's values back from a
        // microtask after every render, and its instant exit only lands on
        // the next frame.
        if (by === 'own') row.lift();
        else writeHeight(height, outer, 0);
        // The box's own exit is what holds the row from here.
        letGo();
      }
    }
    wasPresentRef.current = isPresent;
  }, [isPresent, height]);

  return (
    <motion.div
      ref={outerRef}
      // Read at mount only: a row the toggle brings has no entrance of its own.
      initial={toggled === 'own' ? { opacity: 0 } : false}
      animate={{ opacity }}
      // A row the toggle sends away keeps its opacity: the panel dims it, and
      // one giving up its step is shut in the same commit and may be back
      // with the next toggle, with no fade to wait on.
      exit={goneBy === 'panel' || goneBy === 'row'
        ? { opacity, transition: { duration: 0 } }
        : { opacity: 0, height: 0, transition: reduceMotion || goneBy === 'card' ? { duration: 0 } : LEAVE }}
      transition={FADE_IN}
      style={{ height, overflow: 'hidden' }}
      data-call-row={calls?.length ? calls.join(' ') : undefined}
      data-step-row={steps?.length ? steps.join(' ') : undefined}
      data-handed-over={goneBy === 'card' ? '' : undefined}
    >
      <div ref={innerRef} style={{ paddingTop: gap, paddingBottom: gapBottom }}>
        <div className={className}>{children}</div>
      </div>
    </motion.div>
  );
}
