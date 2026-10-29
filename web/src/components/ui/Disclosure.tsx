import React, { memo, useContext, useEffectEvent, useLayoutEffect, useRef, useState } from 'react';
import { animate, PresenceContext, usePresence } from '@/lib/framer';
import { followHeight } from '@/lib/followHeight';
import { DURATION, EASE_COLLAPSE, SPRING_FOLD } from '@/lib/motion';
import { useMotionSafe } from '@/lib/reducedMotion';
import { cn } from '@/lib/utils';

/**
 * While `held`, the children of the last render before it stay on screen:
 * what is leaving is shown as it was, not re-rendered for the state that sent
 * it away (an accordion told to shut, a card told it has been answered).
 */
export const HeldChildren = memo(
  function HeldChildren({ children }: { held: boolean; children: React.ReactNode }) {
    return <>{children}</>;
  },
  (_prev, next) => next.held,
);

type Phase = 'closed' | 'opening' | 'open' | 'closing';

const OPEN_LEG = () => SPRING_FOLD;
const EXIT_BEAT = () => DURATION.exit;

function gapBelow(box: HTMLElement): number {
  const next = box.nextElementSibling;
  if (!(next instanceof HTMLElement)) return 0;
  const gap = parseFloat(getComputedStyle(next).marginTop);
  return Number.isFinite(gap) ? gap : 0;
}

interface DisclosureProps {
  open: boolean;
  children: React.ReactNode;
  /** Applied to the measured box, where padding and gaps belong: they fold
   *  with the content. */
  className?: string;
  /** For the toggle's `aria-controls`. The box stays in the DOM, hidden,
   *  while shut, so the reference always names something. */
  id?: string;
  /** On the outer box, for what its place in a list asks of it, such as a
   *  sibling margin it must not take. The fold writes the height, overflow,
   *  opacity and bottom margin itself. */
  style?: React.CSSProperties;
  /** Unfolds from shut on mount as well, for a row arriving in a list. */
  appear?: boolean;
  /** A close has finished and the children are gone. */
  onClosed?: () => void;
  /** Seconds a close takes for a box this many px tall: the exit beat unless
   *  the box can be tall enough for that to read as a cut. */
  closeBeat?: (height: number) => number;
  /** A close is starting, and will take this many seconds. */
  onClosing?: (seconds: number) => void;
  /** The next sibling spaces itself off this box with a top margin, which
   *  vanishes the moment the box is hidden and lands again the moment it is
   *  not. A negative bottom margin cancels it in proportion to how far the box
   *  is shut, so the last frame of a close and the first of an open move
   *  nothing. */
  cancelGap?: boolean;
}

/**
 * A section that folds open and shut, the one fold of the app: every "show
 * more", every settled row's body, every block of the transcript.
 *
 * Every keyframe is a number measured from the content. framer cannot tween
 * from `auto`, and resolving an `auto` keyframe inside the chat scroller jumps
 * the box to its target for one forced layout, where Chromium clamps the
 * scroll offset to the shorter transcript and keeps the clamp. So the box
 * opens on the fold spring to what its content measures, following content
 * that grows while it opens, then hands its height back to the content, so
 * whatever opens inside it later is never clipped. A close freezes the current
 * px and eases it down, the content dimming with what is left of it.
 *
 * Children are mounted only while open or moving, so a page holding dozens of
 * shut folds pays nothing for what they hide. Under reduced motion it folds
 * at once.
 */
export function Disclosure({
  open,
  children,
  className,
  id,
  style,
  appear = false,
  onClosed,
  closeBeat = EXIT_BEAT,
  onClosing,
  cancelGap = false,
  ...data
}: DisclosureProps & { [attribute: `data-${string}`]: string | undefined }): React.ReactElement {
  const outerRef = useRef<HTMLDivElement>(null);
  const innerRef = useRef<HTMLDivElement>(null);
  const [mounted, setMounted] = useState(open);
  if (open && !mounted) setMounted(true);
  const phaseRef = useRef<Phase>(open && !appear ? 'open' : 'closed');
  const heightRef = useRef(0);
  const stopRef = useRef<(() => void) | null>(null);
  const motionSafe = useMotionSafe();
  const readMotionSafe = useEffectEvent(() => motionSafe);
  const readGap = useEffectEvent((box: HTMLElement) => (cancelGap ? gapBelow(box) : 0));
  const beatFor = useEffectEvent((height: number) => closeBeat(height));
  const noteClosing = useEffectEvent((seconds: number) => onClosing?.(seconds));
  const noteClosed = useEffectEvent(() => onClosed?.());

  useLayoutEffect(() => {
    const outer = outerRef.current;
    if (!outer) return;
    const phase = phaseRef.current;
    // A leg is in flight only while it can be stopped. Effects replayed
    // (StrictMode, an Activity shown again) stop it without landing it, and
    // the replay starts it again from the height it stopped at.
    const moving = stopRef.current !== null;

    if (open) {
      if (phase === 'open' || (phase === 'opening' && moving)) return;
      const inner = innerRef.current;
      if (!inner) return;
      // Stopped before any write: a stopped close ticks once more, and that
      // tick would write its faded opacity over the one set here.
      stopRef.current?.();
      stopRef.current = null;
      const finish = () => {
        stopRef.current = null;
        phaseRef.current = 'open';
        outer.style.height = '';
        outer.style.overflow = '';
        outer.style.marginBottom = '';
      };
      outer.style.opacity = '';
      if (!readMotionSafe()) {
        finish();
        return;
      }
      // From shut, the children have just mounted into a box nothing has
      // painted yet: close it on them before the first frame.
      if (phase === 'closed') {
        heightRef.current = 0;
        outer.style.height = '0px';
      }
      phaseRef.current = 'opening';
      outer.style.overflow = 'hidden';
      // The neighbour's gap landed with this commit; cancel it in the same
      // layout, before the first frame paints, not from the spring's first
      // update a frame later.
      const gap = readGap(outer);
      outer.style.marginBottom = gap ? `${-gap}px` : '';
      stopRef.current = followHeight(outer, inner, {
        from: heightRef.current,
        leg: OPEN_LEG,
        onFrame: (v, target) => {
          heightRef.current = v;
          if (gap && target > 0) outer.style.marginBottom = `${-gap * Math.max(0, 1 - v / target)}px`;
        },
        onLand: finish,
      });
      return;
    }

    if (phase === 'closed' || (phase === 'closing' && moving)) return;
    // A close that lands mid-open stops the spring before reading, or its
    // last tick would move the box after the height was taken.
    stopRef.current?.();
    stopRef.current = null;
    // The gap compensation stays on the box until the commit that hides it.
    // Cleared here, a frame before React sets `hidden`, the list grew by one
    // gap for that frame and everything below jumped down and back. Hidden,
    // the margin is inert, and the next open writes its own.
    const finish = () => {
      stopRef.current = null;
      phaseRef.current = 'closed';
      heightRef.current = 0;
      setMounted(false);
      noteClosed();
    };
    if (!readMotionSafe()) {
      finish();
      return;
    }
    // Only reads until the first frame: a turn folds every block in one
    // commit, and a write between two boxes' reads would force a layout for
    // each. Fractional, not offsetHeight: a row sits on a half pixel, and
    // eight boxes each rounded up moved the transcript 2px on the first frame.
    const full = outer.getBoundingClientRect().height;
    const gap = readGap(outer);
    heightRef.current = full;
    phaseRef.current = 'closing';
    const duration = beatFor(full);
    noteClosing(duration);
    const controls = animate(full, 0, {
      duration,
      ease: EASE_COLLAPSE,
      onUpdate: (v) => {
        heightRef.current = v;
        outer.style.overflow = 'hidden';
        outer.style.height = `${v}px`;
        // The content is clipped from the bottom as the box comes down, and
        // dims with what is left of it. A fade that ran ahead of the box
        // emptied a tall block in a blink and left a blank hole shrinking for
        // the rest of the close.
        outer.style.opacity = full > 0 ? String(v / full) : '0';
        if (gap && full > 0) outer.style.marginBottom = `${-gap * (1 - v / full)}px`;
      },
      onComplete: finish,
    });
    stopRef.current = () => controls.stop();
  }, [open, mounted]);

  useLayoutEffect(() => () => {
    stopRef.current?.();
    stopRef.current = null;
  }, []);

  return (
    // Inert while it shuts: what is leaving is shown as it was, handlers and
    // all, and a second click on a button already pressed must not land.
    <div ref={outerRef} id={id} style={style} hidden={!open && !mounted} inert={!open || undefined} {...data}>
      {/* flow-root: the measured box contains its children's margins, or the
          box hands back to `auto` taller than the spring reached. */}
      <div ref={innerRef} className={cn('flow-root', className)}>
        {mounted ? <HeldChildren held={!open}>{children}</HeldChildren> : null}
      </div>
    </div>
  );
}

/**
 * A row of an AnimatePresence list that folds shut when the list lets it go,
 * and is removed once it has, so the list closes the gap it made in place.
 * With `appear` it also unfolds on arriving, though not with a first render
 * the list draws without entrances.
 */
export function PresenceFold({
  appear = false,
  ...fold
}: Pick<DisclosureProps, 'appear' | 'className' | 'style' | 'children'>): React.ReactElement {
  const [present, safeToRemove] = usePresence();
  const quietStart = useContext(PresenceContext)?.initial === false;
  return <Disclosure {...fold} open={present} appear={appear && !quietStart} onClosed={safeToRemove ?? undefined} />;
}
