import { cubicBezier } from '@/lib/framer';
import { DURATION, EASE_REVEAL } from '@/lib/motion';
import { isPageUnseen } from '@/lib/pageVisibility';
import { prefersReducedMotion } from '@/lib/reducedMotion';
import { AT_BOTTOM_PX, NEAR_BOTTOM_PX, isNearBottom } from '../../utils/scrollHelpers';

/**
 * Following a streaming transcript. A reader near the bottom is carried toward
 * the new bottom by a glide this engine drives itself, one write per frame.
 * Not the browser's smooth scroll: a timer of those lagged a fast stream,
 * absorbed the wheel input of a reader trying to leave, and when a block
 * landed mid-animation it stopped short, read as "left", and never followed
 * again. Not an instant set either, which steps the text up a line at a time
 * and a whole table at once.
 *
 * The glide is a critically damped spring toward a bottom re-read every
 * frame, so growth that lands mid-glide only moves the target. Every position
 * it writes is recorded, and a scroll event at that position is its own and
 * decides nothing; anything else is the reader's, or a fold clamping the view.
 * This record is the one way the scroll code tells its own scrolls from the
 * reader's, so every scroll it makes goes through here (`jump`, `land`).
 * Direction decides intent: an upward scroll leaves unless it ends at the very
 * bottom, so a notch at a time gets away, and a downward one rejoins inside the
 * band. A move up that leaves the end no further away is layout's (a fold
 * clamping, the browser's scroll anchoring) and decides nothing; it is judged
 * as soon as layout makes it (`resized`), since by its scroll event the end may
 * have grown away. One that growth landed with reads the same as a fold
 * under it, so it is the reader's only after their input (`touched`).
 * A reader's input stops a glide in the frame it arrives
 * (`stop`, on INTENT_EVENTS). With reduced motion, or on a page nobody is
 * watching, every move is instant, as the follow always was.
 */
export interface StreamFollow {
  /** Reads a scroll event, and for the reader's own move decides whether they
   *  still ride the end. False for this engine's own, for any move under a
   *  landing, and for a move up that layout made, which decide nothing. */
  scrolled(): boolean;
  /** Reads the view right after a layout that resized the transcript (its
   *  ResizeObserver). A move up that layout made there is known from then on,
   *  as a write is: its scroll event comes a frame later, and growth landing
   *  before it would leave the end further away and pass the clamp off as the
   *  reader leaving. A reader's move up only adds to the distance, so it is
   *  left for its scroll event to judge. */
  resized(): void;
  /** Carries a following reader toward the new bottom, for growth. */
  follow(): void;
  /** Glides to `target`, re-read every frame so it can move while the view
   *  travels, and calls `onEnd` once it is there or was stopped short. Null
   *  from `target` ends it where it is. */
  land(target: () => number | null, onEnd?: () => void): void;
  /** Puts the view at `top` now, as this engine's own scroll. */
  jump(top: number): void;
  /** Ends a glide or a landing where it is. */
  stop(): void;
  /** The reader is moving the view toward the start (`towardStart`). For as
   *  long as one such move takes, a move up is theirs even when the end
   *  shrinks in with it, as it does while a row below folds. */
  pulled(): void;
  /** The reader did something that can move the view (`movesView`). For as
   *  long as one such move takes, a move up that growth landing with it leaves
   *  ambiguous is theirs; without it, that move is layout's. */
  touched(): void;
  /** A pointer or touch went down on the scroller (`down`), or lifted. While
   *  it is held, as on a scrollbar drag however slow, every move is the
   *  reader's. */
  pressed(down: boolean): void;
  /** A landing is under way: it owns the position until it ends. */
  readonly landing: boolean;
}

// The chase's natural frequency, rad/s. Its time constant (1/ω, 83 ms) spreads
// a new line over about a quarter second, so lines arriving at a reading pace
// blend into one slow drift instead of a step per line, and a following reader
// trails a steady stream by 2u/ω (10 px at a typical 60 px/s).
const CHASE_OMEGA = 12;
// Past this gap the spring stiffens with the square root of it, so a burst
// several lines tall still lands within a few hundred ms and a fast stream's
// lag stays under the room the streaming indicator and the bottom padding
// leave below the newest line.
const CHASE_LAG_PX = 64;
// A frame that took longer (a long task, a tab coming back) integrates as this
// long rather than as one leap across the whole gap.
const MAX_STEP_S = 0.05;
const FIRST_STEP_S = 1 / 60;
// About as long as a browser's smooth scroll for one wheel notch or key press.
const PULL_MS = 300;

/** Moves that should land at once: the reader asked for less motion, or the
 *  page applies them unseen (see lib/pageVisibility). */
function instantMoves(): boolean {
  return prefersReducedMotion() || isPageUnseen();
}

const landEase = cubicBezier(...EASE_REVEAL);
const LAND_MS = DURATION.reveal * 1000;

type Motion =
  | { kind: 'chase' }
  | { kind: 'land'; from: number; start: number | null; target: () => number | null; onEnd?: () => void };

export function createStreamFollow(c: HTMLElement, following: { current: boolean }): StreamFollow {
  const bottom = () => Math.max(0, c.scrollHeight - c.clientHeight);
  // Where the view last was by this engine's, layout's or the reader's doing.
  // A write sets it, so a reader who scrolls between a write and its scroll
  // event (a frame later) is measured from where the engine put them, or a
  // notch up from there would read as a move down and be pulled back.
  let lastTop = c.scrollTop;
  // How far the end was from there.
  let lastGap = bottom() - lastTop;
  // Where the view is known to be before its scroll event says so: the last
  // write as the scroller reports it (rounded to its pixel grid), or a move
  // layout made (`resized`). A scroll event there is not the reader's.
  let wrote: number | null = null;
  // The glide's own position, unrounded, and its velocity in px/s.
  let x = c.scrollTop;
  let v = 0;
  let motion: Motion | null = null;
  let raf = 0;
  let lastFrame = 0;
  // Set across a write, for a scroll event dispatched inside it, before
  // `wrote` can say where it landed.
  let writing = false;
  // When the reader last moved the view toward the start.
  let pulledAt = -Infinity;
  // When the reader last did anything that can move the view.
  let touchedAt = -Infinity;
  let pressing = false;
  // How tall the content was at the last known place.
  let lastHeight = c.scrollHeight;

  // Takes where the view is as this engine's own: its scroll event, when it
  // comes, decides nothing.
  const own = (top: number) => {
    wrote = top;
    lastTop = top;
    lastGap = bottom() - top;
    lastHeight = c.scrollHeight;
  };
  const write = (top: number) => {
    x = top;
    writing = true;
    try {
      c.scrollTo({ top });
    } finally {
      writing = false;
    }
    own(c.scrollTop);
  };
  // A move up from the last known place that leaves the end no further away:
  // anchoring holds the view as a row above it closes, and a clamp follows the
  // bottom in. The reader's own move up adds itself to the distance, which
  // growth below only adds to, but a row folding below takes it away again,
  // so while the reader is moving up the move is theirs. A move down that
  // leaves the end exactly as far is anchoring holding the view as a row above
  // it grows; the reader's own move down takes itself off the distance.
  // Growth that landed with a move up leaves the two readings the same: a
  // fold of the move's size under growth at least as tall, or the reader's
  // move under the growth less the fold. Only the reader's input tells them
  // apart there, so with none the move is layout's. A move up with nothing
  // grown (find in page, an anchor) still reads by the distance alone.
  const byLayout = (top: number, gap: number) => {
    const up = lastTop - top;
    const now = performance.now();
    const idle = !pressing && now - touchedAt >= PULL_MS;
    if (up < 0) return Math.abs(gap - lastGap) < 1 && idle;
    if (up <= 0 || pressing || now - pulledAt < PULL_MS) return false;
    if (gap - lastGap < up - 1) return true;
    return Math.abs(c.scrollHeight - lastHeight) >= 1 && idle;
  };
  const schedule = () => {
    if (!raf) raf = requestAnimationFrame(frame);
  };
  // Ends whatever is moving the view, and tells a landing it is over.
  const end = () => {
    if (raf) cancelAnimationFrame(raf);
    raf = 0;
    lastFrame = 0;
    v = 0;
    const was = motion;
    motion = null;
    if (was?.kind === 'land') was.onEnd?.();
  };

  function frame(now: number) {
    raf = 0;
    const m = motion;
    if (!m) return;
    const dt = lastFrame ? Math.min(MAX_STEP_S, Math.max(0, (now - lastFrame) / 1000)) : FIRST_STEP_S;
    lastFrame = now;
    // Moved by something else since the last write (a fold clamping the view,
    // a pin, the browser's scroll anchoring): carry on from there, at rest.
    if (wrote === null || Math.abs(c.scrollTop - wrote) >= 1) {
      x = c.scrollTop;
      v = 0;
    }
    if (m.kind === 'land') {
      const target = m.target();
      if (target == null) return end();
      // Timed from the frame before its first, on the frames' own clock, so
      // the first frame already moves.
      m.start ??= now - FIRST_STEP_S * 1000;
      const p = Math.min(1, Math.max(0, now - m.start) / LAND_MS);
      write(m.from + (Math.min(Math.max(0, target), bottom()) - m.from) * landEase(p));
      if (p >= 1) return end();
    } else {
      if (!following.current) return end();
      const target = bottom();
      const gap = target - x;
      if (gap < 0.5) {
        // There, or the bottom receded under the view, which the browser has
        // already clamped to.
        if (gap > 0) write(target);
        return end();
      }
      // The exact step of a critically damped spring over dt: no overshoot
      // from rest, and stable at any frame time.
      const w = CHASE_OMEGA * Math.max(1, Math.sqrt(gap / CHASE_LAG_PX));
      const e = x - target;
      const k = v + w * e;
      const decay = Math.exp(-w * dt);
      v = (v - w * k * dt) * decay;
      const next = target + (e + k * dt) * decay;
      if (next >= target) v = 0;
      write(Math.min(target, next));
    }
    schedule();
  }

  return {
    scrolled() {
      if (writing) return false;
      const top = c.scrollTop;
      const gap = bottom() - top;
      const known = wrote !== null && Math.abs(top - wrote) < 1;
      const layout = byLayout(top, gap);
      const up = lastTop - top;
      // How far from the end the move alone put the view. Growth that landed
      // with it, as a line can between a clamp and its scroll event, is not
      // the reader leaving.
      const left = lastGap + up;
      lastTop = top;
      lastGap = gap;
      lastHeight = c.scrollHeight;
      if (known) return false;
      // The view has left the last known place, so its scroll event has been
      // and gone: a reader who comes back to that position later is not it.
      wrote = null;
      // A reader's input on the scroller stops a landing before the view moves
      // (INTENT_EVENTS), so a move under one is layout's: the browser's scroll
      // anchoring holding content in place as a row above changes size, or a
      // clamp. Read as the reader's, it took the view from a send's pin halfway
      // there and left the message wherever the landing had got to.
      if (motion?.kind === 'land') return false;
      // Read as the reader's, a fold closing above dropped a glide trailing
      // the end by more than AT_BOTTOM_PX.
      if (layout) return false;
      if (up > 0) {
        following.current = left <= AT_BOTTOM_PX;
        return true;
      }
      const metrics = { scrollTop: top, scrollHeight: c.scrollHeight, clientHeight: c.clientHeight };
      following.current = isNearBottom(metrics, NEAR_BOTTOM_PX);
      return true;
    },
    resized() {
      if (writing || c.clientHeight === 0) return;
      const top = c.scrollTop;
      // Grown or shrunk with the view where it was: the new size is the
      // baseline a later move is measured from, not growth landing with it.
      if (Math.abs(top - lastTop) < 1) {
        lastGap = bottom() - top;
        lastHeight = c.scrollHeight;
        return;
      }
      if (!byLayout(top, bottom() - top)) return;
      // A glide carries on from here, at rest, as from any move not its own.
      x = top;
      v = 0;
      own(top);
    },
    follow() {
      if (!following.current || motion?.kind === 'land') return;
      if (instantMoves()) {
        end();
        const target = bottom();
        if (target > c.scrollTop) write(target);
        return;
      }
      motion ??= { kind: 'chase' };
      schedule();
    },
    land(target, onEnd) {
      end();
      if (instantMoves()) {
        const top = target();
        if (top != null) write(Math.max(0, top));
        onEnd?.();
        return;
      }
      motion = { kind: 'land', from: c.scrollTop, start: null, target, onEnd };
      schedule();
    },
    jump(top) {
      end();
      write(Math.max(0, top));
    },
    stop: end,
    pulled() {
      pulledAt = performance.now();
    },
    touched() {
      touchedAt = performance.now();
    },
    pressed(down) {
      pressing = down;
    },
    get landing() {
      return motion?.kind === 'land';
    },
  };
}

/** How a turn the reader starts moves the transcript: 'send' and 'restart'
 *  (an edit, a regenerate, a retry) open a turn under the reader's message,
 *  'steer' adds to one already running. */
export type TurnStart = 'send' | 'restart' | 'steer';

/** What the follow policy (useTranscriptFollow) asks of a transcript's follow. */
export interface StreamFollowControls {
  /** Takes the reader to the turn they just started, wherever they were. */
  rejoin(start: TurnStart): void;
  /** Brings the start of reply bubble `id` under the viewport top, for a turn
   *  that finished under the 'reply_start' preference. */
  landOnReply(id: string): void;
}

// What a reader does to scroll: a wheel or trackpad, a touch, the scrollbar or
// a click, a key. A move the reader makes is never told from a glide's by
// position alone, so one of these stops the glide before it writes again.
export const INTENT_EVENTS = ['wheel', 'touchstart', 'pointerdown', 'keydown'] as const;

// What ends a pointer or touch held on the scroller (`pressed`).
export const RELEASE_EVENTS = ['pointerup', 'pointercancel', 'touchend', 'touchcancel'] as const;

// Keys that scroll toward the start, and every key that scrolls.
const UP_KEYS = new Set(['ArrowUp', 'PageUp', 'Home']);
const SCROLL_KEYS = new Set([...UP_KEYS, 'ArrowDown', 'PageDown', 'End', ' ']);

// Where every key acts on the control rather than scrolling the view; a
// button takes only Space, which presses it.
const OWN_KEYS = 'input, textarea, select, [contenteditable=""], [contenteditable="true"]';

/** Whether an INTENT_EVENTS event, as the scroller hears it, can move the view:
 *  a wheel, a touch, a pointer on the scroller itself (its scrollbar) or its
 *  middle button, or a key that scrolls and was not taken by the control it
 *  was pressed on. A click on a row inside the transcript is not one. */
export function movesView(e: Event): boolean {
  if (e.defaultPrevented) return false;
  if (e.type === 'pointerdown') return e.target === e.currentTarget || (e as PointerEvent).button === 1;
  if (e.type !== 'keydown') return true;
  const { key, target } = e as KeyboardEvent;
  if (!SCROLL_KEYS.has(key)) return false;
  if (!(target instanceof Element)) return true;
  return !target.closest(OWN_KEYS) && !(key === ' ' && target.closest('button'));
}

/** Whether an INTENT_EVENTS event moves the view toward the start: a wheel up,
 *  or a key that scrolls up. A pointer or a touch does not say which way, and
 *  a pointer is as often a click on a row that folds. */
export function towardStart(e: Event): boolean {
  if (e.type === 'wheel') return (e as WheelEvent).deltaY < 0;
  if (e.type !== 'keydown') return false;
  const { key, shiftKey } = e as KeyboardEvent;
  return UP_KEYS.has(key) || (key === ' ' && shiftKey);
}
