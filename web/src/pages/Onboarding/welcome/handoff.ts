import { prefersReducedMotion } from '@/lib/reducedMotion';
import './handoff.css';

/**
 * The welcome flow handing its conversation to the chat, drawn as one view
 * transition rather than a swap of the whole page: the answered turns drift
 * off, the agent's mark, the composer and a task the user wrote travel to
 * where the thread keeps them, and the sidebar and the thread's header come
 * in around them. handoff.css holds the choreography.
 *
 * The router cannot run it. In the declarative router a navigation renders in
 * a React transition and its view-transition option is not read, and the
 * navigation returns long before the chat has drawn anything. So the
 * transition is started here, around the navigation, and its update waits
 * for the chat to say it has drawn what the transition lands on.
 */

/** The page attribute every name in handoff.css is given under, set only
 *  while the transition runs. */
const FLAG = 'data-handoff';

/** How long the answered turn is shown before the page moves: the agent's
 *  pending row rises in under it for about this long, and the transition
 *  takes its picture of the page from the frame it starts on. */
export const HANDOFF_DWELL_MS = 400;

/** The longest the welcome's last frame is held for the chat. The page is a
 *  still picture while the update runs, so a chat slow to mount is shown as
 *  far as it got rather than kept waiting for. */
export const HANDOFF_CAP_MS = 800;

let release: (() => void) | null = null;

/** Whether the hand-off is drawn at all: the browser has view transitions and
 *  the reader has not asked for less motion. Otherwise the chat just opens. */
export function handoffAnimates(): boolean {
  return (
    typeof document !== 'undefined' && typeof document.startViewTransition === 'function' && !prefersReducedMotion()
  );
}

/**
 * Makes `update` (the navigation into the chat) inside the hand-off's
 * transition, or at once where it is not drawn. The update counts as done when
 * the chat calls `markHandoffReady`, or at HANDOFF_CAP_MS.
 */
export function runHandoff(update: () => void): void {
  if (!handoffAnimates()) {
    update();
    return;
  }
  const root = document.documentElement;
  root.setAttribute(FLAG, '');
  const settle = () => root.removeAttribute(FLAG);
  let transition: ViewTransition;
  try {
    transition = document.startViewTransition(
      () =>
        new Promise<void>((resolve) => {
          const cap = setTimeout(done, HANDOFF_CAP_MS);
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
  // A transition that cannot run is skipped with the update still made: two
  // elements under one name, or a page hidden as it starts. Each of these
  // promises rejects to say so, and nothing here needs telling.
  transition.ready.catch(() => {});
  transition.updateCallbackDone.catch(() => {});
  transition.finished.then(settle, settle);
}

/** Called by the chat once it has drawn the agent's turn the hand-off lands
 *  on. Does nothing when no hand-off is waiting. */
export function markHandoffReady(): void {
  release?.();
}
