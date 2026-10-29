/**
 * Motion the transcript plays for things that happen in front of the reader:
 * a message they just sent, and a turn they watched finish. Neither plays on a
 * history load, a replay, a reconnect or a tab coming back, which redraw what
 * was already there.
 */
import { createContext } from 'react';
import { cssEase, DURATION, EASE_OUT, STAGGER } from '@/lib/motion';
import { isPageUnseen } from '@/lib/pageVisibility';
import { prefersReducedMotion } from '@/lib/reducedMotion';
import { messageTimeMs } from './turnProjection';

/**
 * The order a turn's ending plays in, in seconds from the commit that settles
 * it, which waits for the reply to finish typing (see `useTurnPresentation`).
 * What leaves goes first and takes the exit duration: the streaming indicator
 * and the running clock. Then the fold row's settled label arrives, then the
 * Sources pill and the deliverables deck, a stagger apart. Those two are
 * floors: they also wait for the turn's work to fold away (`turnEndDelays`).
 */
export const TURN_END_DELAY = {
  label: DURATION.exit,
  sources: DURATION.exit + STAGGER,
  deck: DURATION.exit + 2 * STAGGER,
} as const;

/**
 * When the transcript's running collapses end. A turn folds its work away in
 * the commit that settles it, and a panel's close takes longer the more it
 * folds (FoldPanel), so a fixed delay brought the deck and the Sources pill in
 * under work still closing above them. Each close notes its end as it starts,
 * from a layout effect. What the ending brings reads it from a layout effect
 * of the same commit, which runs after every panel's: those sit in an earlier
 * bubble or inside its own bubble's content, ahead of it in the tree.
 */
export class CollapseClock {
  private until = 0;

  /** A close starting now, lasting `seconds`. */
  note(seconds: number): void {
    this.until = Math.max(this.until, performance.now() + seconds * 1000);
  }

  /** Seconds until every close noted so far is done. */
  remaining(): number {
    return Math.max(0, this.until - performance.now()) / 1000;
  }
}

/** The transcript's clock; a surface without one (no list above it) has no
 *  collapse to wait for. */
export const CollapseClockContext = createContext<CollapseClock | null>(null);

/** When the Sources pill and the deck arrive, in seconds from now: in the
 *  ending's order, and after the last collapse running. Reads the clock, so
 *  only an effect may ask. */
export function turnEndDelays(clock: CollapseClock | null): { sources: number; deck: number } {
  const sources = Math.max(TURN_END_DELAY.sources, clock?.remaining() ?? 0);
  return { sources, deck: sources + STAGGER };
}

/**
 * How long after a send its bubble still counts as just sent. The flag rides
 * the message for good, so a bubble that mounts later is a remount (a subagent
 * tab closed, the view coming back) and arrives with the transcript instead.
 */
const FRESH_SEND_MS = 1000;

/** Whether a message sent at `sentAt` was sent from this view a moment ago
 *  (`sentHere`, see `createUserMessage`). Reads the clock, so only a mount may
 *  ask. */
export function isFreshSend(sentHere: boolean, sentAt: unknown): boolean {
  const at = messageTimeMs(sentAt);
  return sentHere && at !== undefined && Date.now() - at < FRESH_SEND_MS;
}

/**
 * How long after its steps arrived an activity block mounting still counts as
 * new. The prose above a block holds it back while the typewriter catches up,
 * which takes under a second.
 */
const FRESH_STEPS_MS = 2000;

/** Whether every step a block mounts with arrived a moment ago, so the block
 *  is new rather than drawn again (a reload mid-run, the main view coming back
 *  from a subagent tab). A step from history has no arrival, and one a
 *  reconnect replayed arrived at 1. Reads the clock, which a compiled render
 *  would freeze at its first answer: a function component asks once, from a
 *  state initializer, and only a class's render (CardArrivals) asks each time. */
export function isFreshArrival(arrivals: ReadonlyArray<number | undefined>): boolean {
  const now = Date.now();
  return arrivals.length > 0 && arrivals.every((at) => at !== undefined && now - at < FRESH_STEPS_MS);
}

const EASE = cssEase(EASE_OUT);

/**
 * Fades `el` in over quick and, given a rise, lifts it that many px over
 * `riseDuration` (fold unless asked), after `delay` seconds. Opacity and transform only, so the layout is final
 * from the first frame and anything measuring it (the scroll pin) lands at
 * once. Played from script, never a CSS animation: those start over each time
 * their element is drawn again, which a cached thread is on coming back from
 * `display: none`. The only fill holds the first frame through the delay, so
 * nothing is left on the element afterwards: a kept transform would make it
 * the containing block of every fixed-position child. Returns the cancel.
 */
export function playEntrance(el: HTMLElement, rise = 0, delay = 0, riseDuration: number = DURATION.fold): () => void {
  if (typeof el.animate !== 'function' || isPageUnseen()) return () => {};
  const timing = { delay: delay * 1000, fill: delay > 0 ? 'backwards' : 'none', easing: EASE } as const;
  const fade = el.animate([{ opacity: 0 }, { opacity: 1 }], { ...timing, duration: DURATION.quick * 1000 });
  const lift = rise && !prefersReducedMotion()
    ? el.animate([{ transform: `translateY(${rise}px)` }, { transform: 'none' }], { ...timing, duration: riseDuration * 1000 })
    : null;
  return () => {
    fade.cancel();
    lift?.cancel();
  };
}
