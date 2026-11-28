/**
 * The page says where every spot's pin goes, so the layer places none of
 * its own once the page has reported.
 */
import { afterEach, describe, expect, it } from 'vitest';
import type { PageToViewerWire, SpotRect } from '../../html/pageProtocol';
import { PageAnchors } from '../pageAnchors';
import type { Found, SpotStrategy } from '../spotStrategy';

const rect = (left: number, top: number, width: number, height: number) =>
  ({ left, top, right: left + width, bottom: top + height, width, height, x: left, y: top, toJSON: () => ({}) }) as DOMRect;

afterEach(() => {
  document.body.replaceChildren();
});

function reported(found: (spot: Element) => Found, anchor: Parameters<PageAnchors['set']>[0][number]['anchor']) {
  const scroller = document.createElement('div');
  const spot = document.createElement('p');
  spot.textContent = 'Revenue';
  scroller.append(spot);
  document.body.append(scroller);
  scroller.getBoundingClientRect = () => rect(10, 20, 400, 300);
  const strategy: SpotStrategy = {
    find: () => found(spot),
    spots: {
      spotAt: () => null, spotsIn: () => [], labelOf: () => null, anchorOf: () => null,
      rectsOf: () => [rect(30, 60, 100, 40)],
    },
  };
  const sent: PageToViewerWire[] = [];
  const anchors = new PageAnchors(scroller, strategy, (wire) => sent.push(wire));
  anchors.set([{ id: 'c1', anchor }]);
  anchors.report();
  const positions = sent.find((m) => m.type === 'page:positions') as { items: { rect: SpotRect | null }[] };
  return positions.items[0];
}

describe('the pin a viewer page reports', () => {
  it('is said for a block whose own corner covers none of its words, where that corner is', () => {
    const item = reported((spot) => ({ parts: [[spot]] }), { kind: 'block', lines: '3', quote: 'Revenue' });
    // The block in the viewport: (20, 40), 100 by 40; its pin inside the top-right corner.
    expect(item.rect).toEqual({ x: 20, y: 40, width: 100, height: 40, pin: { x: 20 + 100 - 22 - 2, y: 40 + 22 + 2 } });
  });

  it('is said for a block clicked at a point, at that point', () => {
    const item = reported((spot) => ({ parts: [[spot]] }), { kind: 'block', lines: '3', quote: 'Revenue', point: { x: 0.5, y: 0.5 } });
    expect(item.rect?.pin).toEqual({ x: 70, y: 60 });
  });

  it('is not said for a spot parked on a part not drawn yet', () => {
    const item = reported((spot) => ({ parked: spot }), { kind: 'text', quote: 'Revenue', page: 2 });
    expect(item.rect).not.toHaveProperty('pin');
  });
});
