import type { ArtifactComment, ArtifactCommentAnchor } from '@/types/api';
import type { FrameRect } from '../pageProtocol';
import type { Draft } from './boxReducer';

export type Point = { x: number; y: number };

/** The anchor id the draft's spot is tracked under, never a comment's uuid. */
export const DRAFT_ID = '__draft__';

/** A pin's size in px: the bubble is this square, its tail one corner of it. */
export const PIN_SIZE = 22;

/** How far a pin sharing its spot with an earlier one is drawn to the right of it: a pin's width and a gap that clears the keylines. */
export const FAN_STEP = PIN_SIZE + 4;

/**
 * Where a pin's tail sits for an anchor found at `rect`. The pin is a speech
 * bubble whose bottom-left corner is the spot, so its body sits up and to the
 * right and the words under the spot stay readable. A clicked element keeps
 * the point it was clicked at, so two comments on one chart do not stack, an
 * area the point its drag was let go at, mostly a corner, and words picked in
 * comment mode where the drag over them was let go or the word was clicked.
 * Without one a selection's pin closes the selected words, at their end, and
 * any other bubble sits inside the top-right corner.
 */
export function pinPoint(anchor: ArtifactCommentAnchor, rect: FrameRect): Point {
  if (anchor.point) return { x: rect.x + anchor.point.x * rect.width, y: rect.y + anchor.point.y * rect.height };
  if (anchor.kind === 'text') return { x: rect.x + rect.width, y: rect.y };
  return {
    x: rect.x + Math.max(0, rect.width - PIN_SIZE - 2),
    y: rect.y + Math.min(rect.height, PIN_SIZE + 2),
  };
}

/**
 * Pins drawn where they can all be seen: one whose box would overlap a pin
 * already drawn steps right, a whole pin at a time, until it clears them, so
 * every number stays readable. Measured against where earlier pins were drawn, not
 * their spots, so a pin whose spot sits where another was stepped to still
 * steps past it. Only the drawing moves; the anchors do not.
 */
export function fanOut<T extends Point>(points: readonly T[]): T[] {
  const drawn: T[] = [];
  for (const p of points) {
    let x = p.x;
    while (drawn.some((d) => Math.abs(d.x - x) < FAN_STEP && Math.abs(d.y - p.y) < PIN_SIZE)) x += FAN_STEP;
    drawn.push(x === p.x ? p : { ...p, x });
  }
  return drawn;
}

/** A pin where it is drawn, its tail at (x, y); the draft's has no comment. */
export interface PlacedPin {
  comment: ArtifactComment | null;
  x: number;
  y: number;
}

interface PinInput {
  comments: readonly ArtifactComment[];
  /** Where the page last said each spot is (`usePageReports`). */
  positions: ReadonlyMap<string, FrameRect | null>;
  /** `positions` is the answer of the document on screen. */
  fresh: boolean;
  /** The document on screen finished building itself. */
  settled: boolean;
  /** The document is the one on screen (`FrameView.shown`). */
  shown: boolean;
  size: { width: number; height: number };
  /** The comment whose card is open, over its pin or in the list. */
  shownId: string | null;
  /** The list is open and shows the comment. */
  lists: (comment: ArtifactComment) => boolean;
  /** The comment being written, in its box or waiting in the list. */
  draft: Draft | null;
  /** The frame shows the file the draft was picked on. */
  draftHere: boolean;
  /** The draft's box is open, or the list it waits in is. */
  draftShown: boolean;
}

export interface PinPlaces {
  pins: PlacedPin[];
  /**
   * Where the document says the draft's spot is, or where it was picked until
   * it has said. A document that does not show it, another file's among
   * them, has no spot to draw.
   */
  draftRect: FrameRect | null;
  /** The draft's spot is marked: its box is open, or the list it waits in. */
  draftPinned: boolean;
  /** Where a comment's pin is drawn: the last document's spot while the next one has not said. */
  rectOf: (comment: ArtifactComment) => FrameRect | null | undefined;
  /**
   * Where the comment is on the document on screen: undefined while it may
   * still say, null once it has finished building and does not show it, which
   * makes the comment outdated. A document that builds itself from data after
   * load reports no spot at first.
   */
  whereNow: (comment: ArtifactComment) => FrameRect | null | undefined;
  /** Where a pin is drawn, the draft's under null. */
  pinAt: (id: string | null) => Point | null;
  /** Where the comment's pin is drawn in view on the document on screen, else null. */
  landedAt: (comment: ArtifactComment) => Point | null;
  /** The draft's pin is drawn in view on the document on screen. */
  draftLanded: () => boolean;
}

/**
 * Where the pins go. A resolved comment's pin shows only while it has a reply
 * not yet read, its card is open or the list shows it. Pins on one spot fan
 * out in number order, the draft's last.
 */
export function placePins({
  comments, positions, fresh, settled, shown, size, shownId, lists, draft, draftHere, draftShown,
}: PinInput): PinPlaces {
  const rectOf = (comment: ArtifactComment) => positions.get(comment.comment_id);
  const draftRect = !draft || !draftHere ? null : positions.has(DRAFT_ID) ? (positions.get(DRAFT_ID) ?? null) : draft.rect;
  const draftPinned = !!draftRect && draftShown;
  const drawn = comments
    .filter((c) => c.status !== 'resolved' || c.unread > 0 || c.comment_id === shownId || lists(c))
    .flatMap((comment) => {
      const rect = rectOf(comment);
      return rect ? [{ comment, ...pinPoint(comment.anchor, rect) }] : [];
    })
    .sort((a, b) => a.comment.number - b.comment.number);
  const pins = fanOut<PlacedPin>(
    draft && draftRect && draftPinned ? [...drawn, { comment: null, ...pinPoint(draft.anchor, draftRect) }] : drawn,
  );
  const at = new Map<string | null, Point>(pins.map((p) => [p.comment?.comment_id ?? null, { x: p.x, y: p.y }]));
  const inView = (p: Point) => p.x >= 0 && p.x <= size.width - PIN_SIZE && p.y >= PIN_SIZE && p.y <= size.height;
  return {
    pins,
    draftRect,
    draftPinned,
    rectOf,
    whereNow: (comment) => {
      if (!fresh) return undefined;
      const rect = positions.get(comment.comment_id);
      return rect === null && !settled ? undefined : rect;
    },
    pinAt: (id) => at.get(id) ?? null,
    landedAt: (comment) => {
      const rect = rectOf(comment);
      if (!rect || !fresh || !shown) return null;
      const point = at.get(comment.comment_id) ?? pinPoint(comment.anchor, rect);
      return inView(point) ? point : null;
    },
    draftLanded: () => {
      const point = fresh && shown ? at.get(null) : undefined;
      return !!point && inView(point);
    },
  };
}
