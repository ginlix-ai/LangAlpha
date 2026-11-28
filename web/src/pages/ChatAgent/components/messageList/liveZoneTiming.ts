/**
 * Live-zone timing: the exposure window, the fold a leaving row gives back
 * and the status line's hold. Its one import is lib/motion, itself a leaf,
 * and it has to stay that way: the e2e live-zone spec reads it from Node,
 * where the rest of the render-block module graph (i18n JSON) cannot load.
 * The path is relative for the same reason, since Node does not know the `@`
 * alias.
 */
import { SPRING_FOLD } from '../../../../lib/motion';

/** Minimum time a just-completed item stays in the live zone before folding. */
export const MIN_LIVE_EXPOSURE_MS = 1800;

/** The height a leaving row gives back. It is a fold, not the exit: the row
 *  has faded out before its box is gone. A spring, because the row arriving
 *  under it opens on one, and two near-identical curves trade the height
 *  between them so the zone barely moves. A fold-length tween closed ahead
 *  of the arrival, and the page moved by the difference. */
export const ROW_FOLD = SPRING_FOLD;

/** How long a newer step runs before it takes the status line from the one
 *  on it, so a call that returns at once never flashes past. The first step
 *  takes an empty line at once. */
export const STATUS_HOLD_MS = 90;

/** The least a subagent row's step stays on its line before a newer one takes
 *  it, so a run of quick calls reads as steps rather than a flicker. */
export const SUBAGENT_STEP_DWELL_MS = 900;

/** How long a subagent row's step stays once its call returns with nothing
 *  to name after it, so the line does not blank between two steps. */
export const SUBAGENT_STEP_LINGER_MS = 1800;

/** How long a subagent's turn between calls runs before the row says so
 *  ("Thinking"): a burst of calls with quick turns between reads as steps,
 *  and only a turn that lasts is named. */
export const SUBAGENT_BETWEEN_GRACE_MS = 1000;
