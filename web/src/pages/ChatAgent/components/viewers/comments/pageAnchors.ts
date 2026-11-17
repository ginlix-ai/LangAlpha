/**
 * The comments a viewer page is asked to find (`page:anchors`), where each
 * one is now, and the words of the one being written, marked. What finds an
 * anchor is the viewer's strategy; the reporting is the same for every
 * viewer, and is the bridge's own (`comments-anchors.js`), deduped so an
 * unchanged answer is not sent again.
 */
import type { ArtifactCommentAnchor } from '@/types/artifacts';
import { listsEvery } from '../../artifactComments/anchorKinds';
import type { FrameRect, PageAnchorItem, PageToViewerWire } from '../html/pageProtocol';
import type { Edges, Found, SpotStrategy } from './spotStrategy';
import { TextMarks } from './textMarks';

export interface Tracked {
  id: string;
  anchor: ArtifactCommentAnchor;
  mark: boolean;
  draft: boolean;
  found: Found | null;
}

/**
 * Where a parked anchor is said to be: outside any overlay, so it has no pin
 * in view and reads as present, not as gone, while its spot is not drawn.
 */
export const PARKED: FrameRect = { x: -100_000, y: -100_000, width: 0, height: 0 };

export function round(n: number): number {
  return Math.round(n * 10) / 10;
}

export function union(rects: readonly Edges[]): Edges | null {
  if (!rects.length) return null;
  return {
    left: Math.min(...rects.map((r) => r.left)),
    top: Math.min(...rects.map((r) => r.top)),
    right: Math.max(...rects.map((r) => r.right)),
    bottom: Math.max(...rects.map((r) => r.bottom)),
  };
}

/** A client box in the scroll viewport's coordinates, which the layer's overlay covers. */
export function toFrame(scroller: Element, r: Edges): FrameRect {
  const v = scroller.getBoundingClientRect();
  return { x: round(r.left - v.left), y: round(r.top - v.top), width: round(r.right - r.left), height: round(r.bottom - r.top) };
}

export class PageAnchors {
  private tracked: Tracked[] = [];
  private stale = true;
  private lastPositions = '';
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
    return picker ? union(spots.flatMap((s) => picker.rectsOf(s))) : null;
  }

  /** The viewer's list, answered in full on the next report. Returns whether a comment is being written. */
  set(items: PageAnchorItem[]): boolean {
    this.tracked = items.map((item) => ({ id: item.id, anchor: item.anchor, mark: !!item.mark, draft: !!item.draft, found: null }));
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

  private resolve(): void {
    if (!this.stale) return;
    this.strategy.fresh?.();
    for (const t of this.tracked) {
      try {
        t.found = this.strategy.find(t.anchor);
      } catch {
        t.found = null;
      }
    }
    this.stale = false;
    this.paint();
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
      return parts.length ? { id: t.id, rect, parts } : { id: t.id, rect };
    });
    const key = JSON.stringify(items);
    if (key !== this.lastPositions) {
      this.lastPositions = key;
      this.send({ type: 'page:positions', items });
    }
    return whole;
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
