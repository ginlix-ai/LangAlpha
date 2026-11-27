import './driveMotion.css';
import { prefersReducedMotion } from '@/lib/reducedMotion';

/**
 * How the page moves between its two layouts. Opening a file folds the list
 * into a column and brings the file in beside it; closing one gives the list
 * its width back. On a phone the file is pushed over the list and popped off
 * it. Anything else (another file, another folder in the same layout) is no
 * change of layout and moves nothing.
 */
export type DriveMotion = 'open' | 'close' | 'push' | 'pop';

/** The flag on `<html>` the transition's names and timings hang from (driveMotion.css). */
const FLAG = 'data-drive-motion';

/** The longest the old frame is held for the new layout to commit. */
const SETTLE_CAP_MS = 300;

let release: (() => void) | null = null;

export function motionBetween(fileWasOpen: boolean, fileOpens: boolean, phone: boolean): DriveMotion | null {
  if (fileWasOpen === fileOpens) return null;
  if (phone) return fileOpens ? 'push' : 'pop';
  return fileOpens ? 'open' : 'close';
}

function animates(): boolean {
  return typeof document !== 'undefined'
    && typeof document.startViewTransition === 'function'
    && !prefersReducedMotion()
    && document.visibilityState === 'visible';
}

/**
 * Makes `update` (a navigation that changes the layout) inside one view
 * transition, or at once where it is not drawn. The router renders a
 * navigation in a transition of React's own, so the update is done when the
 * page says its new layout is on screen (`markDriveLaidOut`), or at the cap.
 */
export function runDriveMotion(motion: DriveMotion | null, update: () => void): void {
  if (!motion || !animates()) {
    update();
    return;
  }
  const root = document.documentElement;
  // One at a time: a second click mid-flight lands without a second animation.
  if (root.hasAttribute(FLAG)) {
    update();
    return;
  }
  root.setAttribute(FLAG, motion);
  const settle = () => root.removeAttribute(FLAG);
  let transition: ViewTransition;
  try {
    transition = document.startViewTransition(
      () =>
        new Promise<void>((resolve) => {
          const cap = setTimeout(done, SETTLE_CAP_MS);
          function done() {
            clearTimeout(cap);
            if (release === done) release = null;
            resolve();
          }
          release = done;
          update();
        }),
    );
  } catch {
    settle();
    update();
    return;
  }
  // A transition that cannot run (a hidden page, a name taken twice) is
  // skipped with the update still made; each promise rejects to say so.
  transition.ready.catch(() => {});
  transition.updateCallbackDone.catch(() => {});
  transition.finished.then(settle, settle);
}

/** Called by the page once a layout is committed; a no-op when nothing waits on it. */
export function markDriveLaidOut(): void {
  release?.();
}
