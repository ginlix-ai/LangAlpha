/**
 * Make every framer-motion animation instant while the tab is hidden.
 *
 * A hidden tab runs no animation frames, and framer schedules everything,
 * even an instant animation, on its frame loop. So an animation created while
 * the tab is away sits at its first keyframe until the tab returns, then
 * starts its clock from that frame and plays in full. A streaming transcript
 * accumulates dozens of them (rows folding out of the live zone at height 0,
 * new rows unfolding from it, the accordion growing), and the return becomes
 * a burst of collapses and expansions layered over a scroll pin that already
 * landed. Marking the animations instant at creation makes the first painted
 * frame after the return the settled layout. An animation already in flight
 * at hide time needs nothing: its first tick back reads the wall clock and
 * finishes it in that same frame.
 */
import { MotionGlobalConfig } from 'framer-motion';

function apply(): void {
  MotionGlobalConfig.skipAnimations = document.hidden;
}

if (typeof document !== 'undefined') {
  apply();
  document.addEventListener('visibilitychange', apply);
}
