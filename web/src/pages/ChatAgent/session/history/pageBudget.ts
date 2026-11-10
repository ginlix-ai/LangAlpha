/**
 * How much one replay page asks for: its turns, and the bytes that may stop it
 * sooner on the screen it opens on.
 */
import { getIsMobileSnapshot } from '@/hooks/useIsMobile';

/**
 * Turns per replay page, at most: its bytes may stop it sooner (`PageBudget`).
 * A turn of a research thread can run to hundreds of events, so this is about
 * as much as replays in well under a second, and it still fills a tall
 * viewport several times over, which keeps the next page's fetch ahead of a
 * reader scrolling up.
 */
export const HISTORY_PAGE_TURNS = 20;

/**
 * Extra turns a reload asks for beyond the ones on screen. A reload asks for
 * the newest N turns, so turns that land between the status read it counts
 * from and the replay would otherwise push the oldest loaded turn off the top.
 * A return to a thread asks for none: its saved offset is measured from the
 * turn its transcript started at, so the page has to start there again.
 */
export const HISTORY_RELOAD_MARGIN_TURNS = 5;

/**
 * How much one replay page may carry. A turn of a heavy thread runs to megabytes
 * (a subagent's internals ride the turn that ran it), so a page counted only in
 * turns could download tens of them before first paint: the server takes the
 * newest turns until their lines pass `maxBytes`, whole turns only, never fewer
 * than `minTurns` and never more than `limit`. A server that predates the
 * budget ignores it and sends `limit` turns.
 */
export interface PageBudget {
  limit: number;
  minTurns: number;
  maxBytes: number;
}

/** What the screen alone sets of a page's budget. */
export type ScreenBudget = Omit<PageBudget, 'limit'>;

// Bytes a page may carry before it stops short of its limit: a phone, on a
// slower link and a smaller screen, half of what a desktop takes.
const DESKTOP_PAGE_BYTES = 3_000_000;
const PHONE_PAGE_BYTES = 1_500_000;
// About the height of the shortest turn worth counting on, so the floor of
// turns a page carries, however large, still fills the screen it opens on.
const TURN_FLOOR_PX = 400;
const DESKTOP_MIN_TURNS = 5;
// Three, though two fill most phones: a transcript that opens with room above
// it reads as broken, and a slower first open does not.
const PHONE_MIN_TURNS = 3;
const MAX_MIN_TURNS = 10;

/** The bytes and the floor of turns a page takes on a screen this tall, on a
 *  phone (the chat's own mobile breakpoint) or not. */
export function screenPageBudget(phone: boolean, viewportHeight: number): ScreenBudget {
  const fill = Math.ceil(Math.max(0, viewportHeight) / TURN_FLOOR_PX);
  const least = phone ? PHONE_MIN_TURNS : DESKTOP_MIN_TURNS;
  return {
    minTurns: Math.min(MAX_MIN_TURNS, Math.max(least, fill)),
    maxBytes: phone ? PHONE_PAGE_BYTES : DESKTOP_PAGE_BYTES,
  };
}

/** The screen's budget as it is now. Read when a request is built, never in
 *  render: the viewport is not an input render can see. */
export function currentScreenBudget(): ScreenBudget {
  return screenPageBudget(getIsMobileSnapshot(), typeof window === 'undefined' ? 0 : window.innerHeight);
}

/** A page reaching back at least `reach` turns: the screen's floor or the
 *  reach, whichever is more, and room for one page's turns while the bytes
 *  allow. A reach the screen's floor would not cover is delivered whole. */
export function pageBudget(screen: ScreenBudget, reach = 0): PageBudget {
  const minTurns = Math.max(screen.minTurns, reach);
  return { limit: Math.max(HISTORY_PAGE_TURNS, minTurns), minTurns, maxBytes: screen.maxBytes };
}

/**
 * Turns one page of a jump to an older turn (useFocusTurn) may reach back, at
 * most. A jump further back than this takes several pages, each asked once
 * the last has landed, so no one request replays a long thread whole.
 */
export const JUMP_PAGE_TURNS = 100;

/** A page of a jump to a turn `reach` turns above the oldest one loaded: the
 *  turns up to it, at most `JUMP_PAGE_TURNS`, its bytes stopping it sooner,
 *  though never short of a page's turns (or the reach, when that is less) or
 *  the screen's floor. A page that stops short is followed by the next. */
export function jumpPageBudget(screen: ScreenBudget, reach: number): PageBudget {
  const minTurns = Math.max(screen.minTurns, Math.min(reach, HISTORY_PAGE_TURNS));
  return { limit: Math.max(HISTORY_PAGE_TURNS, Math.min(reach, JUMP_PAGE_TURNS)), minTurns, maxBytes: screen.maxBytes };
}

/**
 * How many turns a replay of the newest page must reach back to cover turn
 * `from`: none when nothing reaches further than a first open, otherwise
 * every turn from `from` to the newest the thread has, plus `margin`, so it
 * never takes back what the reader paged in. As a page's floor
 * (`pageBudget`), the bytes never cut it short of that.
 */
export function newestPageReach(from: number | null, newestTurn: number, margin = 0): number {
  if (from === null) return 0;
  return Math.max(0, newestTurn - from + 1 + margin);
}
