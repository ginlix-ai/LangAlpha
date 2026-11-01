// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { FAN_STEP, PIN_SIZE, fanOut, pinPoint } from '../pinPlaces';

describe('pinPoint', () => {
  const rect = { x: 100, y: 50, width: 200, height: 80 };

  it('closes a selection at the end of its words, or stands where words picked in comment mode were let go', () => {
    expect(pinPoint({ kind: 'text', quote: 'Graphics' }, rect)).toEqual({ x: 300, y: 50 });
    expect(pinPoint({ kind: 'text', quote: 'Graphics', point: { x: 0.5, y: 0.75 } }, rect)).toEqual({ x: 200, y: 110 });
  });

  it('keeps the point an element was clicked at', () => {
    expect(pinPoint({ kind: 'element', path: '#a', point: { x: 0.25, y: 0.5 } }, rect)).toEqual({ x: 150, y: 90 });
  });

  it('otherwise draws the bubble inside the top-right corner, and a small element\'s from its own corner', () => {
    expect(pinPoint({ kind: 'element', path: '#a' }, rect)).toEqual({ x: 276, y: 74 });
    expect(pinPoint({ kind: 'element', path: '#a' }, { x: 0, y: 0, width: 8, height: 6 })).toEqual({ x: 0, y: 6 });
  });

  it('stands an area\'s pin where its drag was let go, as an element\'s where it was clicked', () => {
    const area = { kind: 'area' as const, elements: [{ path: '#a' }, { path: '#b' }], count: 2 };
    expect(pinPoint({ ...area, point: { x: 1, y: 1 } }, rect)).toEqual({ x: 300, y: 130 });
    expect(pinPoint({ ...area, point: { x: 0, y: 0 } }, rect)).toEqual({ x: 100, y: 50 });
  });
});

describe('fanOut', () => {
  it('steps pins on one spot right in the order given', () => {
    const out = fanOut([{ x: 100, y: 50 }, { x: 100, y: 50 }, { x: 100, y: 50 }]);
    expect(out.map((p) => p.x)).toEqual([100, 126, 152]);
    expect(FAN_STEP).toBe(PIN_SIZE + 4);
  });

  it('steps a pin whose box would overlap another, keeping its height', () => {
    const out = fanOut([{ x: 100, y: 50 }, { x: 112, y: 52 }, { x: 98, y: 47 }, { x: 109, y: 55 }]);
    expect(out.map((p) => p.y)).toEqual([50, 52, 47, 55]);
    expect(out[1].x).toBe(112 + FAN_STEP);
    for (let i = 0; i < out.length; i++) {
      for (let j = 0; j < i; j++) {
        const apart = Math.abs(out[i].x - out[j].x) >= FAN_STEP || Math.abs(out[i].y - out[j].y) >= PIN_SIZE;
        expect(apart).toBe(true);
      }
    }
  });

  it('steps past a pin drawn where its own spot is', () => {
    // The second is stepped onto the third's spot; the third must not land on it.
    const out = fanOut([{ x: 0, y: 0 }, { x: 5, y: 0 }, { x: 5 + FAN_STEP, y: 0 }]);
    expect(out.map((p) => p.x)).toEqual([0, 5 + FAN_STEP, 5 + 2 * FAN_STEP]);
  });

  it('leaves pins a full pin apart where they are, as the same objects', () => {
    const points = [{ x: 0, y: 0, id: 'a' }, { x: FAN_STEP, y: 0, id: 'b' }, { x: 0, y: PIN_SIZE, id: 'c' }];
    const out = fanOut(points);
    expect(out).toEqual(points);
    out.forEach((p, i) => expect(p).toBe(points[i]));
  });

  it('moves only the drawing, never the points it was given', () => {
    const points = [{ x: 0, y: 0 }, { x: 0, y: 0 }];
    fanOut(points);
    expect(points).toEqual([{ x: 0, y: 0 }, { x: 0, y: 0 }]);
  });
});
