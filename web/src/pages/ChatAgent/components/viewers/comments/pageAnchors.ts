/**
 * The comments a viewer page is asked to find (`page:anchors`), where each
 * one is now, and the words of the one being written, marked. What finds an
 * anchor is the viewer's strategy; the reporting is the same for every
 * viewer, and is the bridge's own (`comments-anchors.js`), deduped so an
 * unchanged answer is not sent again.
 */
import type { ArtifactCommentAnchor } from '@/types/artifacts';
import { listsEvery } from '../../artifactComments/anchorKinds';
import type { FrameRect, PageAnchorItem, PageToViewerWire, Point, SpotRect } from '../html/pageProtocol';
import type { Edges, Found, SpotStrategy } from './spotStrategy';
import { TextMarks } from './textMarks';
import { pinPoint } from '../html/comments/pinPlaces';
import { type PinMemo, type PinPass, type PinPlaced, keptBlockPin, keptWordsPin, pinPass, union, wordsPin } from './pinRoom';

export interface Tracked {
  id: string;
  anchor: ArtifactCommentAnchor;
  mark: boolean;
  draft: boolean;
  found: Found | null;
  /** Its pin as the last report placed it, kept while its spot only scrolls (`PinMemo`). */
  pin: PinMemo | null;
}

/**
 * Where a parked anchor is said to be: outside any overlay, so it has no pin
 * in view and reads as present, not as gone, while its spot is not drawn.
 */
export const PARKED: FrameRect = { x: -100_000, y: -100_000, width: 0, height: 0 };

/** Whether a reported spot is a parked one (`PARKED`): there, but on a part of the file not drawn yet. */
export function isParked(rect: FrameRect): boolean {
  return rect.x === PARKED.x && rect.y === PARKED.y && !rect.width && !rect.height;
}

export function round(n: number): number {
  return Math.round(n * 10) / 10;
}

function roundPoint(p: Point): Point {
  return { x: round(p.x), y: round(p.y) };
}

/** A client box in the scroll viewport's coordinates, which the layer's overlay covers. */
export function toFrame(scroller: Element, r: Edges): FrameRect {
  const v = scroller.getBoundingClientRect();
  return { x: round(r.left - v.left), y: round(r.top - v.top), width: round(r.right - r.left), height: round(r.bottom - r.top) };
}

/** The scroll viewport's inside, scroll bars left out, in client coordinates: where a pin may go. */
function viewOf(scroller: Element): Edges {
  const v = scroller.getBoundingClientRect();
  return { left: v.left, top: v.top, right: v.left + scroller.clientWidth, bottom: v.top + scroller.clientHeight };
}

function framePoint(scroller: Element, at: Point): Point {
  const v = scroller.getBoundingClientRect();
  return { x: round(at.x - v.left), y: round(at.y - v.top) };
}

/** Where the pin on selected words goes, in the viewport's coordinates, or null when the words show nowhere. */
export function wordsPinIn(scroller: Element, range: Range): Point | null {
  const at = wordsPin(range, scroller, viewOf(scroller));
  return at && framePoint(scroller, at);
}

export class PageAnchors {
  private tracked: Tracked[] = [];
  private stale = true;
  private lastPositions = '';
  /**
   * The content's scroll size at the last report. A change is a resize,
   * which can move text without a mutation (an image or a font loading), so
   * no kept pin outlives it.
   */
  private size = '';
  /** The comment whose card is open (`page:outlined`): besides the draft, the only one whose parts are measured. */
  private outlined: string | null = null;
  /** Words just picked in comment mode, marked until the viewer's anchor marks them. */
  private pending: Range | null = null;
  private readonly marks = new TextMarks();

  constructor(
    private readonly scroller: Element,
    private readonly strategy: SpotStrategy,
    private readonly send: (wire: PageToViewerWire) => void,
  ) {}

  /** The spots' boxes together, in client coordinates. */
  edgesOfSpots(spots: readonly Element[]): Edges | null {
    const picker = this.strategy.spots;
    return picker ? spots.flatMap((s) => picker.rectsOf(s)).reduce<Edges | null>((u, r) => union(u, r), null) : null;
  }

  /** The viewer's list, answered in full on the next report. Returns whether a comment is being written. */
  set(items: PageAnchorItem[]): boolean {
    this.tracked = items.map((item) => ({ id: item.id, anchor: item.anchor, mark: !!item.mark, draft: !!item.draft, found: null, pin: null }));
    // The viewer has opened the words' box, whose anchor marks them now.
    if (items.some((item) => item.mark)) this.pending = null;
    this.stale = true;
    this.lastPositions = '';
    return items.some((item) => item.draft);
  }

  outline(id: string | null): void {
    this.outlined = id;
  }

  /** The content changed: every anchor is looked for afresh. */
  changed(): void {
    this.stale = true;
  }

  /**
   * The text was laid out again with no change to the content or its size
   * (a web font finished loading): the anchors still stand, but every pin is
   * placed afresh, since a kept one checks only its spot's box.
   */
  relaid(): void {
    for (const t of this.tracked) t.pin = null;
  }

  private resolve(): void {
    if (!this.stale) return;
    this.strategy.fresh?.();
    for (const t of this.tracked) {
      // Found again, maybe in another layout: its pin is placed afresh.
      t.pin = null;
      try {
        t.found = this.strategy.find(t.anchor);
      } catch {
        t.found = null;
      }
    }
    this.strategy.settled?.();
    this.stale = false;
    this.paint();
  }

  /**
   * Whether an anchor waits on a part drawn in view, such as a PDF page in
   * view whose words are still being laid out: it will be found in a moment,
   * so the page is not settled yet.
   */
  drawingInView(): boolean {
    this.resolve();
    const v = this.scroller.getBoundingClientRect();
    return this.tracked.some((t) => {
      if (!t.found || !('parked' in t.found) || !t.found.parked.isConnected) return false;
      const r = t.found.parked.getBoundingClientRect();
      return r.bottom > v.top && r.top < v.bottom && r.right > v.left && r.left < v.right;
    });
  }

  draft(): Tracked | null {
    this.resolve();
    return this.tracked.find((t) => t.draft) ?? null;
  }

  /** Where a found anchor shows, a parked one's element included, or undefined when its nodes went with a re-render. */
  private edgesOf(found: Found | null): Edges | null | undefined {
    if (!found) return null;
    if ('parked' in found) return found.parked.isConnected ? found.parked.getBoundingClientRect() : undefined;
    if ('range' in found) {
      if (!found.range.startContainer.isConnected) return undefined;
      const r = found.range.getBoundingClientRect();
      return r.width || r.height ? r : null;
    }
    const spots = found.parts.flatMap((part) => part ?? []);
    if (spots.some((s) => !s.isConnected)) return undefined;
    return this.edgesOfSpots(spots);
  }

  /** Where the comment `id` is now, in client coordinates: a parked one's, where it will be drawn. */
  edgesFor(id: string): Edges | null {
    this.resolve();
    return this.edgesOf(this.tracked.find((t) => t.id === id)?.found ?? null) ?? null;
  }

  /** Where the comment `id` waits for its spot to be drawn, null when it is not parked. */
  parkedIn(id: string): Extract<Found, { parked: Element }> | null {
    this.resolve();
    const found = this.tracked.find((t) => t.id === id)?.found;
    return found && 'parked' in found ? found : null;
  }

  /**
   * Reports where each anchor is, unless that is what was last said. False
   * when a node went with a re-render, so the caller asks again next frame.
   */
  report(): boolean {
    this.resolve();
    const size = `${this.scroller.scrollWidth} ${this.scroller.scrollHeight}`;
    if (size !== this.size) {
      this.size = size;
      for (const t of this.tracked) t.pin = null;
    }
    const pass = pinPass();
    let whole = true;
    const items = this.tracked.map((t) => {
      let edges = this.edgesOf(t.found);
      if (edges === undefined) {
        this.stale = true;
        whole = false;
        edges = null;
      }
      const rect = edges && t.found && 'parked' in t.found ? PARKED : edges ? toFrame(this.scroller, edges) : null;
      // Each part of a list of every part is outlined on its own, on the draft and the open card.
      const outlines = t.draft || t.id === this.outlined;
      const found = outlines && rect && t.found && 'parts' in t.found && listsEvery(t.anchor) ? t.found.parts : [];
      const parts = found.flatMap((part) => {
        const e = part && this.edgesOfSpots(part);
        return e ? [toFrame(this.scroller, e)] : [];
      });
      const spot = rect && edges && t.found && !('parked' in t.found) ? this.withPin(t, t.found, edges, rect, pass) : rect;
      return parts.length ? { id: t.id, rect: spot, parts } : { id: t.id, rect: spot };
    });
    const key = JSON.stringify(items);
    if (key !== this.lastPositions) {
      this.lastPositions = key;
      this.send({ type: 'page:positions', items });
    }
    return whole;
  }

  /**
   * The spot with where its pin goes: clear of the words beside it
   * (`pinRoom`), else where `pinPoint` would put it. Said for every spot, so
   * the layer places none itself.
   */
  private withPin(t: Tracked, found: Exclude<Found, { parked: Element }>, edges: Edges, rect: FrameRect, pass: PinPass): SpotRect {
    const view = viewOf(this.scroller);
    let placed: PinPlaced;
    if ('range' in found) placed = keptWordsPin(found.range, this.scroller, view, edges, pass, t.pin);
    else {
      const at = pinPoint(t.anchor, rect);
      const spots = found.parts.flatMap((part) => part ?? []);
      placed = keptBlockPin(spots, edges, { x: at.x + view.left, y: at.y + view.top }, view, edges, pass, t.pin);
    }
    t.pin = placed.memo;
    const pin = placed.at && framePoint(this.scroller, placed.at);
    return { ...rect, pin: pin ?? roundPoint(pinPoint(t.anchor, rect)) };
  }

  /** Marks words just picked until the viewer answers; null lets the mark go. */
  markPending(range: Range | null): void {
    if (!range && !this.pending) return;
    this.pending = range;
    this.paint();
  }

  private paint(): void {
    const ranges = this.pending ? [this.pending] : [];
    for (const t of this.tracked) if (t.mark && t.found && 'range' in t.found) ranges.push(t.found.range);
    this.marks.paint(ranges);
  }

  dispose(): void {
    this.marks.dispose();
  }
}
