import { ANCHOR_OFFSET_PX, composerHeight, resolveScrollContent } from '../../utils/scrollDom';
import type { StreamFollow } from './streamFollow';

// How long a send waits for its message to render before it stops pinning:
// longer than any send takes to draw its bubble, short enough that a send
// refused before it drew one leaves nothing armed.
const PIN_WAIT_MS = 2000;

/** The transcript's own height, without the floor: the padded wrapper. */
function naturalHeight(c: HTMLElement): number {
  const content = resolveScrollContent(c);
  return (content.parentElement ?? content).getBoundingClientRect().height;
}

const USER_BUBBLE = '[data-message-role="user"][data-message-id]';
// What a restarted turn opens with: the reader's message, or the row a wake
// turn opens with in its place (a wakeup's or a price alert's), which pinning
// the last user bubble would read as the turn before's.
const TURN_OPENER = `${USER_BUBBLE}, [data-wakeup], [data-market-alert]`;

function last(c: HTMLElement, selector: string): HTMLElement | null {
  const all = c.querySelectorAll<HTMLElement>(selector);
  return all.length ? all[all.length - 1] : null;
}

const lastUserBubble = (c: HTMLElement) => last(c, USER_BUBBLE);

/** scrollTop that pins the reader's message `el`: its top just under the
 *  viewport top, or, for a message taller than half of what the composer
 *  leaves visible, its bottom at the middle of that, so the reply still
 *  starts on screen. Measured on the bubble's row, which the send entrance
 *  (a transform on the column inside it) does not move. */
function pinnedTop(c: HTMLElement, el: HTMLElement): number {
  const rect = el.getBoundingClientRect();
  const top = c.scrollTop + rect.top - c.getBoundingClientRect().top;
  const visible = c.clientHeight - composerHeight(c);
  return Math.max(0, top - ANCHOR_OFFSET_PX, top + rect.height - visible / 2);
}

/** Room `px` held under the view, lowered only to where it stops mattering: to
 *  the view's bottom while the view is past `under`, what lies beneath the
 *  floor, and away once it is not, so letting it go never clamps the view. */
function lowered(c: HTMLElement, px: number, under: number): number {
  const viewBottom = c.scrollTop + c.clientHeight;
  return viewBottom > under + 1 ? Math.min(px, viewBottom) : 0;
}

interface Turn {
  /** The last user bubble when a send was made, which its own message
   *  replaces as the last; null for a restart, which may re-render the same. */
  before: string | null;
  /** An edit, a regenerate or a retry: the turn's opener is pinned, which the
   *  restart leaves as the transcript's last. */
  restart: boolean;
  /** The reader's message, once it has rendered. */
  el: HTMLElement | null;
  /** The landing on it has started. */
  launched: boolean;
  deadline: number;
}

/** The pinned turn's message as it is now rendered, once it has been. A
 *  render can replace its node (an edit re-renders the message it truncates
 *  at), so one that left is looked for again as the last, and a landing under
 *  way carries on to it rather than stopping short. */
function pinnedMessage(c: HTMLElement, turn: Turn): HTMLElement | null {
  if (turn.el?.isConnected) return turn.el;
  const found = turn.restart ? last(c, TURN_OPENER) : lastUserBubble(c);
  if (found && (turn.el || turn.restart || found.dataset.messageId !== turn.before)) turn.el = found;
  return turn.el?.isConnected ? turn.el : null;
}

/**
 * A turn the reader started, pinned: their message held near the viewport top
 * while the reply fills the room under it, the way Claude and ChatGPT open a
 * turn. The room is a floor, a min-height on the wrapper around the padded
 * transcript (`setFloor`, ChatView's callback ref), which makes the pinned
 * position the transcript's bottom. Pin and follow are then one rule: the view
 * rides the bottom, and the floor decides where that is. While the reply fits,
 * its growth fills the floor and nothing scrolls; a row leaving under it (the
 * streaming indicator, a status line, the work folding at the turn's end)
 * leaves the bottom where it was, since layout holds the min-height before any
 * observer could. The wrapper sits outside what the observer watches, so
 * setting it from the observer is not a resize loop. Once the reply outgrows
 * the floor it is under the transcript's own end, and the glide follows the
 * tail; it stays there, so what the reply loses later can take the view no
 * higher than the pin. The floor lasts until the next send, a scroll of the
 * reader's, a thread switch or a reload, so a reply that never outgrows it
 * keeps the room under it until then.
 */
export interface TurnPin {
  /** A turn's message is pinned, or about to be once it renders. */
  readonly active: boolean;
  setFloor(el: HTMLElement | null): void;
  /** A send, an edit, a regenerate or a retry, before its commit. */
  start(c: HTMLElement, restart: boolean): void;
  /** Finds the pinned message once it has rendered, keeps the floor under it
   *  as the transcript changes, and lands the view on it. Run on every resize
   *  while a pin is set, and once a frame after the send in case the send's
   *  commit did not change the transcript's size. */
  settle(c: HTMLElement, engine: StreamFollow): void;
  /** A reader who scrolls has taken the view from the pin: the room held under
   *  it is theirs to leave, lowered to what they can see and gone once they
   *  cannot see it, so it never moves them. */
  yieldFloor(c: HTMLElement): void;
  /** The pin no longer places the view, though its floor stays as it is. */
  drop(): void;
  /** A thread opening or reloading starts with no floor: it lands at its end
   *  with nothing blank under the last turn. */
  reset(): void;
}

export function createTurnPin(): TurnPin {
  let floorEl: HTMLElement | null = null;
  // `pin` is the pinned turn's room; `hold` keeps the view's bottom from
  // receding under a reader while a send lands (a regenerate takes the old
  // reply away first). Applied as their max.
  let floor = { pin: 0, hold: 0 };
  let turn: Turn | null = null;
  const apply = () => {
    if (!floorEl) return;
    const px = Math.max(floor.pin, floor.hold);
    floorEl.style.minHeight = px > 0 ? `${px}px` : '';
  };
  const releaseHold = (c: HTMLElement) => {
    if (floor.hold <= 0) return;
    const next = lowered(c, floor.hold, Math.max(naturalHeight(c), floor.pin));
    if (next === floor.hold) return;
    floor.hold = next;
    apply();
  };
  return {
    get active() {
      return turn !== null;
    },
    setFloor(el) {
      floorEl = el;
      apply();
    },
    start(c, restart) {
      // Until the new pin lands, the bottom stays at least where the view is:
      // the commit this send makes may take content away first, and a clamp
      // there would drop the view before it has anywhere to go.
      floor.hold = Math.max(floor.pin, floor.hold, c.scrollTop + c.clientHeight);
      floor.pin = 0;
      turn = {
        before: restart ? null : lastUserBubble(c)?.dataset.messageId ?? null,
        restart,
        el: null,
        launched: false,
        deadline: performance.now() + PIN_WAIT_MS,
      };
      apply();
    },
    settle(c, engine) {
      const pin = turn;
      // A hidden view measures nothing.
      if (!pin || c.clientHeight === 0) return;
      // Found in time or not at all: a send refused before it drew its message
      // leaves the pin armed, and a user bubble that renders later (an
      // automation's, a report-back's) is not the reader's to be pulled to.
      const late = performance.now() > pin.deadline;
      const el = pin.el || !late ? pinnedMessage(c, pin) : null;
      if (!el) {
        if (late) {
          turn = null;
          releaseHold(c);
        }
        return;
      }
      // Kept once the reply outgrows it, when it sits under the transcript's
      // own end and does nothing: a row the reply loses later (a live row
      // swapped out, the reasoning closing) would otherwise clamp the view
      // above the pinned position and push the reader's message down the
      // screen.
      floor.pin = pinnedTop(c, el) + c.clientHeight;
      apply();
      if (pin.launched) return;
      pin.launched = true;
      engine.land(
        () => {
          const target = turn === pin ? pinnedMessage(c, pin) : null;
          return target ? pinnedTop(c, target) : null;
        },
        () => releaseHold(c),
      );
    },
    yieldFloor(c) {
      const px = Math.max(floor.pin, floor.hold);
      if (px <= 0) return;
      const next = lowered(c, px, naturalHeight(c));
      if (next === px) return;
      turn = null;
      floor = { pin: 0, hold: next };
      apply();
    },
    drop() {
      turn = null;
    },
    reset() {
      turn = null;
      floor = { pin: 0, hold: 0 };
      apply();
    },
  };
}
