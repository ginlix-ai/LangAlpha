/**
 * A spot on a part of the file not drawn yet (a PDF page whose words are
 * still being laid out) is parked. One parked on a part in view is found in a
 * moment, so the page does not settle on it: a draft taken up again there
 * would otherwise be read as one that does not show, and sent to the list.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { PARKED, PageAnchors, isParked } from '../pageAnchors';
import type { SpotStrategy } from '../spotStrategy';

const box = (top: number) => ({ top, bottom: top + 100, left: 0, right: 400, width: 400, height: 100, x: 0, y: top, toJSON: () => ({}) }) as DOMRect;

afterEach(() => {
  document.body.replaceChildren();
});

function page(slotTop: number) {
  const scroller = document.createElement('div');
  const slot = document.createElement('div');
  scroller.append(slot);
  document.body.append(scroller);
  scroller.getBoundingClientRect = () => box(0);
  slot.getBoundingClientRect = () => box(slotTop);
  const strategy: SpotStrategy = { find: () => ({ parked: slot }) };
  const anchors = new PageAnchors(scroller, strategy, () => {});
  anchors.set([{ id: '__draft__', anchor: { kind: 'text', quote: 'words', page: 3 }, draft: true }]);
  return anchors;
}

describe('a parked spot', () => {
  it('holds the page unsettled while its part is in view, not once it is out of it', () => {
    expect(page(50).drawingInView()).toBe(true);
    expect(page(5000).drawingInView()).toBe(false);
  });

  it('is told apart from a spot that is there', () => {
    expect(isParked(PARKED)).toBe(true);
    expect(isParked({ x: 10, y: 20, width: 30, height: 4 })).toBe(false);
  });
});
