import { afterEach, describe, expect, it } from 'vitest';
import type { RegionAnchor } from '@/types/artifacts';
import { OWN_ATTR, boxOf, ownChange, regionSpots, type RegionSurfaces } from '../regionSpots';

/** A surface 400 by 200 at (100, 50), as the browser would lay it out. */
function surface(): HTMLElement {
  const el = document.createElement('div');
  el.getBoundingClientRect = () => new DOMRect(100, 50, 400, 200);
  document.body.appendChild(el);
  return el;
}

function surfaces(el: HTMLElement, page?: number): RegionSurfaces {
  return {
    points: page === undefined,
    at: (x, y) => (x >= 100 && x <= 500 && y >= 50 && y <= 250 ? { el, ...(page ? { page } : {}) } : null),
    of: (p) => (p === page ? { el, ...(page ? { page } : {}) } : null),
  };
}

afterEach(() => {
  document.body.innerHTML = '';
});

describe('regionSpots', () => {
  it('draws a box as fractions of its surface, clipped to it, and saves it', () => {
    const el = surface();
    const { spots } = regionSpots(surfaces(el, 3));
    const [drawn] = spots!.draw!({ left: 140, top: 70, right: 900, bottom: 150 }, { x: 140, y: 70 });
    expect(boxOf(drawn!)).toEqual({ page: 3, x: 0.1, y: 0.1, w: 0.9, h: 0.4 });
    expect(spots!.anchorOf([drawn!])).toEqual({ kind: 'region', boxes: [{ page: 3, x: 0.1, y: 0.1, w: 0.9, h: 0.4 }] });
    expect(el.querySelector(`[${OWN_ATTR}] .comment-region`)).toBe(drawn);
  });

  it('makes a slip a point where a click drops one, and draws nothing off every surface', () => {
    const image = regionSpots(surfaces(surface()));
    expect(boxOf(image.spots!.draw!({ left: 140, top: 70, right: 143, bottom: 150 }, { x: 140, y: 70 })[0]!)).toEqual({ x: 0.1, y: 0.1, w: 0, h: 0 });
    expect(image.spots!.draw!({ left: 10, top: 10, right: 200, bottom: 200 }, { x: 10, y: 10 })).toEqual([]);
    const pdf = regionSpots(surfaces(surface(), 1));
    expect(pdf.spots!.draw!({ left: 140, top: 70, right: 143, bottom: 150 }, { x: 140, y: 70 })).toEqual([]);
  });

  it('drops a point where an image takes them, and not on a PDF page', () => {
    const image = regionSpots(surfaces(surface()));
    expect(boxOf(image.spots!.pointAt!(300, 100)!)).toEqual({ x: 0.5, y: 0.25, w: 0, h: 0 });
    const pdf = regionSpots(surfaces(surface(), 1));
    expect(pdf.spots!.pointAt!(300, 100)).toBeNull();
  });

  it('finds a stored box on the element it was drawn as, and lets go of boxes no comment names', () => {
    const el = surface();
    const strategy = regionSpots(surfaces(el));
    const [drawn] = strategy.spots!.draw!({ left: 140, top: 70, right: 300, bottom: 150 }, { x: 140, y: 70 });
    const anchor = strategy.spots!.anchorOf([drawn!]) as RegionAnchor;
    const pass = (...anchors: RegionAnchor[]) => {
      strategy.fresh!();
      const found = anchors.map((a) => strategy.find(a));
      strategy.settled!();
      return found;
    };
    expect(pass(anchor)).toEqual([{ parts: [[drawn]] }]);
    // A box just drawn waits a pass for the layer to name it as the draft.
    const [stray] = strategy.spots!.draw!({ left: 400, top: 200, right: 480, bottom: 240 }, { x: 400, y: 200 });
    pass(anchor);
    expect(stray!.isConnected).toBe(true);
    // Named by the draft, then let go: gone at the end of the first pass without it.
    const strayAnchor = strategy.spots!.anchorOf([stray!]) as RegionAnchor;
    pass(anchor, strayAnchor);
    expect(stray!.isConnected).toBe(true);
    pass(anchor);
    expect(stray!.isConnected).toBe(false);
    expect(drawn!.isConnected).toBe(true);
  });

  it('keeps a thin box a box on a very large surface', () => {
    const el = document.createElement('div');
    el.getBoundingClientRect = () => new DOMRect(0, 0, 20000, 1000);
    document.body.appendChild(el);
    const { spots } = regionSpots({ points: false, at: () => ({ el, page: 1 }), of: () => ({ el, page: 1 }) });
    const [drawn] = spots!.draw!({ left: 100, top: 100, right: 106, bottom: 120 }, { x: 100, y: 100 });
    expect(boxOf(drawn!)).toEqual({ page: 1, x: 0.005, y: 0.1, w: 0.001, h: 0.02 });
    // Flush with the far edge, its start rounded there: kept on the surface.
    const [edge] = spots!.draw!({ left: 19994, top: 980, right: 20000, bottom: 1000 }, { x: 19994, y: 980 });
    expect(boxOf(edge!)).toEqual({ page: 1, x: 0.999, y: 0.98, w: 0.001, h: 0.02 });
  });

  it('leaves other anchors to the strategy it is laid over', () => {
    const strategy = regionSpots(surfaces(surface()), { find: () => ({ parked: document.body }) });
    expect(strategy.find({ kind: 'text', quote: 'net revenue', page: 2 })).toEqual({ parked: document.body });
  });

  it('tells its own boxes from a change to the file', () => {
    const el = surface();
    const strategy = regionSpots(surfaces(el));
    const records: MutationRecord[] = [];
    const watch = new MutationObserver((r) => records.push(...r));
    watch.observe(el, { childList: true, subtree: true });
    strategy.spots!.draw!({ left: 140, top: 70, right: 300, bottom: 150 }, { x: 140, y: 70 });
    el.appendChild(document.createElement('p'));
    const seen = [...records, ...watch.takeRecords()];
    watch.disconnect();
    // The layer laid on the surface, then the box in it; then a paragraph of the file.
    expect(seen.map(ownChange)).toEqual([true, true, false]);
  });
});
