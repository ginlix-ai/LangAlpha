// Pure scroll decision logic for the chat transcript. Kept free of DOM/layout
// so it is unit-testable under jsdom (which has no layout).

/** Distance from the bottom (px) a downward scroll still counts as at the
 *  bottom, rejoining a streaming follow. */
export const NEAR_BOTTOM_PX = 120;
/** "At the bottom" for an upward scroll. Not 0: scrollHeight and clientHeight
 *  are rounded and scrollTop is not, and under browser zoom a container at
 *  its maximum reads a residual of a pixel or two. */
export const AT_BOTTOM_PX = 4;

/** A landing is held through a settle window while async media and folds
 *  keep moving it: released once the transcript has not resized for this long, */
const SETTLE_QUIET_MS = 1500;
/** and after this long in any case. */
const SETTLE_HARD_CAP_MS = 8000;

export interface ScrollMetrics {
  scrollTop: number;
  scrollHeight: number;
  clientHeight: number;
}

/** Distance from the bottom is within `threshold` px (inclusive). */
export function isNearBottom(m: ScrollMetrics, threshold = 120): boolean {
  return m.scrollHeight - m.scrollTop - m.clientHeight <= threshold;
}

export interface SettleWindow {
  /** Opens the window, or restarts its quiet period while it is open. */
  arm(): void;
  /** Closes the window without expiring it. */
  clear(): void;
}

/** The settle window both transcript scrollers hold a landing through.
 *  `onExpire` runs when either limit lapses, and the lapse closes the whole
 *  window: the next landing arms a fresh hard cap rather than inheriting one
 *  already spent, which would give up early. */
export function createSettleWindow(onExpire: () => void): SettleWindow {
  let quiet: ReturnType<typeof setTimeout> | undefined;
  let cap: ReturnType<typeof setTimeout> | undefined;
  const clear = () => {
    clearTimeout(quiet);
    clearTimeout(cap);
    quiet = cap = undefined;
  };
  const expire = () => {
    clear();
    onExpire();
  };
  return {
    arm() {
      clearTimeout(quiet);
      quiet = setTimeout(expire, SETTLE_QUIET_MS);
      cap ??= setTimeout(expire, SETTLE_HARD_CAP_MS);
    },
    clear,
  };
}
