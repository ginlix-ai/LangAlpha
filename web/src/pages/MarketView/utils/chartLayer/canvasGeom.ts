/** Pixel-space geometry for the layer painter and its hit test. */

import type { Extend } from '../../types/chartLayer';

export interface XY {
  x: number;
  y: number;
}

/**
 * Extend a segment to the pane's horizontal bounds per `extend`. A vertical
 * segment cannot extend sideways and comes back unchanged.
 */
export function extendSegment(a: XY, b: XY, extend: Extend, minX: number, maxX: number): [XY, XY] {
  if (extend === 'none' || a.x === b.x) return [a, b];
  const slope = (b.y - a.y) / (b.x - a.x);
  const at = (x: number): XY => ({ x, y: a.y + slope * (x - a.x) });
  const [l, r] = a.x <= b.x ? [a, b] : [b, a];
  const left = extend === 'left' || extend === 'both' ? at(Math.min(minX, l.x)) : l;
  const right = extend === 'right' || extend === 'both' ? at(Math.max(maxX, r.x)) : r;
  return a.x <= b.x ? [left, right] : [right, left];
}

/** Distance from p to segment ab. */
export function distToSegment(p: XY, a: XY, b: XY): number {
  const dx = b.x - a.x;
  const dy = b.y - a.y;
  const len2 = dx * dx + dy * dy;
  let t = len2 > 0 ? ((p.x - a.x) * dx + (p.y - a.y) * dy) / len2 : 0;
  t = Math.max(0, Math.min(1, t));
  const cx = a.x + t * dx;
  const cy = a.y + t * dy;
  return Math.hypot(p.x - cx, p.y - cy);
}

/** Even-odd point-in-polygon. */
export function pointInPolygon(p: XY, poly: readonly XY[]): boolean {
  let inside = false;
  for (let k = 0, j = poly.length - 1; k < poly.length; j = k++) {
    const a = poly[k];
    const b = poly[j];
    if (a.y > p.y !== b.y > p.y && p.x < ((b.x - a.x) * (p.y - a.y)) / (b.y - a.y) + a.x) inside = !inside;
  }
  return inside;
}

/** Arrowhead triangle at `tip`, pointing along from→tip. */
export function arrowHead(from: XY, tip: XY, size: number): [XY, XY, XY] {
  const ang = Math.atan2(tip.y - from.y, tip.x - from.x);
  const spread = Math.PI / 7;
  return [
    tip,
    { x: tip.x - size * Math.cos(ang - spread), y: tip.y - size * Math.sin(ang - spread) },
    { x: tip.x - size * Math.cos(ang + spread), y: tip.y - size * Math.sin(ang + spread) },
  ];
}
