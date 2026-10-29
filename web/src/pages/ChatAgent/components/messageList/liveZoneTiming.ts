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
