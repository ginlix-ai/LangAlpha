/**
 * In comment mode, the spot under the pointer: what a click would pick,
 * outlined over the viewer with its name, as a page's bridge reports it.
 */
import type { PageToViewerWire } from '../html/pageProtocol';
import { toFrame, type PageAnchors } from './pageAnchors';
import type { SpotPicker } from './spotStrategy';

/** A pointer crossing the gap between two spots keeps the outline for this long, rather than flashing it off. */
const HOLD_MS = 60;

export class PageHover {
  /** Where the pointer is over the content, while it is. */
  pointer: { x: number; y: number } | null = null;
  private hovered: Element | null = null;
  private hold: ReturnType<typeof setTimeout> | null = null;
  private last = '';

  constructor(
    private readonly scroller: Element,
    private readonly content: Element,
    /** None where the content takes words alone: nothing is outlined. */
    private readonly picker: SpotPicker | undefined,
    private readonly anchors: PageAnchors,
    private readonly send: (wire: PageToViewerWire) => void,
    /** The spot a click takes, which while parts are being added is the part around it. */
    private readonly pickable: (spot: Element) => Element,
  ) {}

  private spotUnder(target: EventTarget | null): Element | null {
    const spot = target instanceof Element && this.content.contains(target) ? (this.picker?.spotAt(target) ?? null) : null;
    return spot && this.pickable(spot);
  }

  /** The pointer is over `target` at (x, y). */
  at(target: EventTarget | null, x: number, y: number): void {
    this.pointer = { x, y };
    const spot = this.spotUnder(target);
    if (spot) {
      this.set(spot);
      return;
    }
    // Crossing the gap between two spots: the next one usually comes within a frame or two.
    if (this.hold) return;
    this.hold = setTimeout(() => {
      this.hold = null;
      const p = this.pointer;
      if (!p || !this.spotUnder(document.elementFromPoint(p.x, p.y))) this.set(null);
    }, HOLD_MS);
  }

  /** What a click under a still pointer takes changed: the spot, or the part around it. */
  again(): void {
    const p = this.pointer;
    if (p) this.at(document.elementFromPoint(p.x, p.y), p.x, p.y);
  }

  /** Nothing is under the pointer: it left, a drag began, or the mode ended. */
  clear(): void {
    this.pointer = null;
    this.set(null);
  }

  set(spot: Element | null): void {
    if (this.hold) clearTimeout(this.hold);
    this.hold = null;
    this.hovered = spot;
    this.report();
  }

  /** Where the spot under the pointer is now, unless that is what was last said. */
  report(): void {
    const spot = this.hovered?.isConnected ? this.hovered : null;
    const edges = spot ? this.anchors.edgesOfSpots([spot]) : null;
    const wire: PageToViewerWire = spot && edges
      ? { type: 'page:hover', rect: toFrame(this.scroller, edges), label: this.picker?.labelOf(spot) ?? null }
      : { type: 'page:hover', clear: true };
    if (!edges) this.hovered = null;
    const key = JSON.stringify(wire);
    if (key === this.last) return;
    this.last = key;
    this.send(wire);
  }

  showing(): boolean {
    return this.hovered !== null;
  }

  dispose(): void {
    if (this.hold) clearTimeout(this.hold);
  }
}
