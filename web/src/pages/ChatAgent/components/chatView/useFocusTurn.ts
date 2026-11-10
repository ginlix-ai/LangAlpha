import { useEffect, useRef, useState } from 'react';
import { useLocation, useNavigate } from 'react-router';
import { findMessageElement } from '../../utils/scrollDom';
import type { OlderHistoryStatus } from '../../session/history/historyWindow';
import type { HoldPlace } from '../../session/history/olderHistory';
import { focusTurnStep } from './focusTurn';
import type { LocationState } from './types';
import './focusTurn.css';

const ENTER_CLASS = 'turn-focus-enter';
// How long a drawn message may take to reach the DOM after it is in the
// transcript's state (a list chunk mounting), before the jump gives up.
const DRAW_BUDGET_MS = 1500;
// How long a jump waits before asking once more for a page that did not
// arrive: a dropped connection or a busy replay mostly answers the second time.
const RETRY_AFTER_MS = 1000;

/** Restarts the arrival tint on `el`, as the file panel's does (useFileFocus). */
function playEnter(el: HTMLElement) {
  el.classList.remove(ENTER_CLASS);
  void el.offsetWidth;
  el.classList.add(ENTER_CLASS);
  const done = () => el.classList.remove(ENTER_CLASS);
  el.addEventListener('animationend', done, { once: true });
  el.addEventListener('animationcancel', done, { once: true });
}

/**
 * Opens the thread at the turn a navigation names (`LocationState.focusTurn`,
 * the command palette's "In conversations" hit): once the thread's entry has
 * landed, older turns are paged in back to it, a bounded page at a time (each
 * landing reruns the step), and its opening bubble is brought under the
 * viewport top and tinted. A page that fails is asked for once more; a turn
 * the thread does not have, or a second failure, leaves the reader at the
 * newest one.
 *
 * Every view the ChatView cache keeps reads the same location, so only the
 * view on screen takes the jump, once per navigation (a cached thread brought
 * back by one jumps too), and the route state loses it at once, so a refresh
 * does not jump again.
 */
export function useFocusTurn({
  isActive,
  ready,
  onMain,
  showMain,
  messages,
  status,
  load,
  place,
  reveal,
  toNewest,
  scrollRoot,
}: {
  isActive: boolean;
  /** The thread's entry restore has landed (useChatScroll's entryRestored). */
  ready: boolean;
  /** The main transcript is the tab shown. */
  onMain: boolean;
  showMain: () => void;
  messages: readonly Record<string, unknown>[];
  status: OlderHistoryStatus;
  load: (place?: HoldPlace, reachTo?: number) => Promise<void>;
  place: HoldPlace;
  reveal: (id: string) => boolean;
  toNewest: () => void;
  scrollRoot: () => HTMLElement | null;
}) {
  const location = useLocation();
  const navigate = useNavigate();
  const [pending, setPending] = useState<{ key: string; turn: number; retried?: boolean } | null>(null);
  const [seenKey, setSeenKey] = useState<string | null>(null);
  // Taken from the navigation as it renders; a view not on screen drops one
  // it was given.
  if (isActive && seenKey !== location.key) {
    setSeenKey(location.key);
    const turn = (location.state as LocationState | null)?.focusTurn;
    if (typeof turn === 'number' && Number.isInteger(turn) && turn >= 0) setPending({ key: location.key, turn });
  }
  if (!isActive && pending) setPending(null);

  // The route state forgets the jump, and keeps everything else it carries.
  useEffect(() => {
    if (!pending || pending.key !== location.key) return;
    const navState = location.state as LocationState | null;
    if (navState?.focusTurn === undefined) return;
    navigate(
      { pathname: location.pathname, search: location.search },
      { replace: true, state: { ...navState, focusTurn: undefined } },
    );
  }, [pending, location.key, location.state, location.pathname, location.search, navigate]);

  // A page that failed is asked for once more, RETRY_AFTER_MS after the
  // failure: keyed on the failure alone, so a streaming thread's messages
  // re-rendering the view do not restart the wait.
  const retryFor =
    pending && !pending.retried && ready && onMain && focusTurnStep(messages, pending.turn, status).kind === 'failed'
      ? pending
      : null;
  const latestRef = useRef({ load, place });
  useEffect(() => {
    latestRef.current = { load, place };
  });
  useEffect(() => {
    if (!retryFor) return;
    const timer = window.setTimeout(() => {
      setPending({ ...retryFor, retried: true });
      void latestRef.current.load(latestRef.current.place, retryFor.turn);
    }, RETRY_AFTER_MS);
    return () => window.clearTimeout(timer);
  }, [retryFor]);

  useEffect(() => {
    if (!pending) return;
    // The jump is to the main transcript, whose entry waits on it being shown.
    if (!onMain) {
      showMain();
      return;
    }
    if (!ready) return;
    const step = focusTurnStep(messages, pending.turn, status);
    if (step.kind === 'wait') return;
    // Asked for again by the retry above.
    if (step.kind === 'failed' && !pending.retried) return;
    let raf = 0;
    const deadline = performance.now() + DRAW_BUDGET_MS;
    const run = () => {
      if (step.kind === 'load') {
        void load(place, pending.turn);
        return;
      }
      if (step.kind === 'missing' || step.kind === 'failed') {
        toNewest();
        setPending(null);
        return;
      }
      if (reveal(step.id)) {
        const root = scrollRoot();
        const el = root && findMessageElement(root, step.id);
        if (el) playEnter(el);
        setPending(null);
      } else if (performance.now() < deadline) {
        raf = requestAnimationFrame(run);
      } else {
        setPending(null);
      }
    };
    // Out of the commit: a page lands with a synchronous render of its own.
    raf = requestAnimationFrame(run);
    return () => cancelAnimationFrame(raf);
  }, [pending, ready, onMain, showMain, messages, status, load, place, reveal, toNewest, scrollRoot]);
}
