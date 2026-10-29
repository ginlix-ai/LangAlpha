/**
 * A landing card takes the place its row gave up, and its box starts at
 * whatever height leaves the list as tall as it was drawn.
 */
import { describe, expect, it } from 'vitest';
import { pickSource, splitGrowth } from '../cardArrivalPlan';

const stayed = (row: string) => ({ row, goneBlock: null, handedOver: false });

describe('what a landing card takes the place of', () => {
  it('is the block, when the row was its only item and the block went with it', () => {
    expect(pickSource([{ row: 'row', goneBlock: 'block', handedOver: false }])).toBe('block');
  });

  it('is the row, when it handed its place over in a block that stays', () => {
    expect(pickSource([{ row: 'row', goneBlock: null, handedOver: true }])).toBe('row');
  });

  // A status line names every call in its block: of the rows naming the call,
  // the one that left is the one the card replaces.
  it('is the row that left, among several naming the call', () => {
    expect(pickSource([stayed('line'), { row: 'tool row', goneBlock: null, handedOver: true }])).toBe('tool row');
    expect(pickSource([{ row: 'first', goneBlock: 'its block', handedOver: false }, { row: 'second', goneBlock: null, handedOver: true }]))
      .toBe('its block');
  });

  it('is nothing when every row stayed', () => {
    expect(pickSource([stayed('a'), stayed('b')])).toBeNull();
    expect(pickSource([])).toBeNull();
  });
});

describe('where a landing card box starts', () => {
  it('starts at the space its row gave up', () => {
    // A 40px row became a 120px card: the commit grew the list by 80.
    expect(splitGrowth([{ height: 120, gap: 0, source: { height: 40 } }], 80)).toEqual([40]);
  });

  it('shares the growth by the space each row gave up', () => {
    const boxes = [{ height: 100, gap: 0, source: { height: 30 } }, { height: 100, gap: 0, source: { height: 10 } }];
    expect(splitGrowth(boxes, 120)).toEqual([10, 70]);
  });

  it('shares it evenly when no card took a row', () => {
    const boxes = [{ height: 60, gap: 0, source: null }, { height: 60, gap: 0, source: null }];
    expect(splitGrowth(boxes, 80)).toEqual([20, 20]);
  });

  it('passes a share a box cannot take to the others', () => {
    // The first card's share would close it past its gap; the second takes the rest.
    const boxes = [{ height: 100, gap: 4, source: { height: 30 } }, { height: 100, gap: 4, source: { height: 10 } }];
    expect(splitGrowth(boxes, 160)).toEqual([-4, 44]);
  });

  it('starts short of the gap its block opens, and no shorter', () => {
    // Below zero the box starts closed and its block's gap is that much short.
    expect(splitGrowth([{ height: 50, gap: 12, source: { height: 20 } }], 58)).toEqual([-8]);
    // What not even the gap can take moves the list.
    expect(splitGrowth([{ height: 50, gap: 12, source: { height: 20 } }], 100)).toEqual([-12]);
  });

  it('starts no taller than its row and the gap, unless the commit drew it taller', () => {
    expect(splitGrowth([{ height: 100, gap: 8, source: { height: 20 } }], -50)).toEqual([100]);
    expect(splitGrowth([{ height: 10, gap: 8, source: { height: 20 } }], -50)).toEqual([28]);
  });
});
