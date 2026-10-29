import { animate, type AnimationPlaybackControls, type ValueAnimationTransition } from './framer';

interface FollowHeight {
  /** The box's height when the tween starts, in px. */
  from: number;
  /** How one leg moves, given where it starts and where it is going. */
  leg: (from: number, to: number) => ValueAnimationTransition<number>;
  /** Each frame, after the box's height is written, with where it is going. */
  onFrame?: (height: number, target: number) => void;
  /** The box has reached what it measures. */
  onLand: () => void;
}

/**
 * Tweens `box`'s height, in px, to the height `content` measures, and
 * retargets the running tween whenever `content` resizes before it lands, so a
 * font, a highlighted block or a row still filling in is followed rather than
 * cut off where it was first measured. Every keyframe is a measured number:
 * framer cannot tween from `auto`, and resolving an `auto` keyframe inside the
 * chat scroller forces a layout that clamps the scroll offset (see
 * components/ui/Disclosure). Returns a stop for the tween and the observing.
 */
export function followHeight(box: HTMLElement, content: HTMLElement, { from, leg, onFrame, onLand }: FollowHeight): () => void {
  let height = from;
  let target = -1;
  let controls: AnimationPlaybackControls | null = null;
  let current: object | null = null;
  const run = (next: number) => {
    // The observer reports the size it starts with; a retarget to the same
    // height would restart the spring from rest.
    if (next === target) return;
    target = next;
    // A stopped tween ticks once more as it stops. Here that tick would write
    // the box's height from inside the observer's callback, and a resize
    // there reaches every other observer late, as a loop error. The leg it
    // ends is ignored instead, and the next one starts from the height the
    // last frame drew.
    const token = {};
    const previous = controls;
    current = token;
    previous?.stop();
    controls = animate(height, next, {
      ...leg(height, next),
      onUpdate: (v) => {
        if (current !== token) return;
        height = v;
        box.style.height = `${v}px`;
        onFrame?.(v, next);
      },
      // The stop above can finish a leg whose time had already run out,
      // and a leg framer skips (a hidden page) lands a frame later: either
      // one landing after it was retargeted would let the box go while the
      // next leg still writes its height.
      onComplete: () => {
        if (current !== token) return;
        current = null;
        observer.disconnect();
        onLand();
      },
    });
  };
  const observer = new ResizeObserver(() => run(content.getBoundingClientRect().height));
  run(content.getBoundingClientRect().height);
  if (current) observer.observe(content);
  return () => {
    current = null;
    observer.disconnect();
    controls?.stop();
  };
}
