/** The shapes a painted frame leaves for the hover test, and the test itself. */

import { distToSegment, pointInPolygon, type XY } from './canvasGeom';

export type HitShape =
  | { key: string; t: 'seg'; a: XY; b: XY; tol: number }
  | { key: string; t: 'poly'; pts: XY[] }
  | { key: string; t: 'rect'; x0: number; y0: number; x1: number; y1: number; label?: boolean }
  | { key: string; t: 'dot'; c: XY; r: number };

export interface LayerHit {
  key: string;
  distance: number;
  /** 2 point/label, 1 stroke, 0 area: a stroke over a zone wins. */
  priority: number;
}

export function hitShapes(shapes: readonly HitShape[], x: number, y: number): LayerHit | null {
  let best: LayerHit | null = null;
  const consider = (h: LayerHit) => {
    if (!best || h.priority > best.priority || (h.priority === best.priority && h.distance < best.distance)) best = h;
  };
  for (const s of shapes) {
    if (s.t === 'seg') {
      const d = distToSegment({ x, y }, s.a, s.b);
      if (d <= s.tol) consider({ key: s.key, distance: d, priority: 1 });
    } else if (s.t === 'rect') {
      const inX = x >= Math.min(s.x0, s.x1) && x <= Math.max(s.x0, s.x1);
      const inY = y >= Math.min(s.y0, s.y1) && y <= Math.max(s.y0, s.y1);
      if (inX && inY) consider({ key: s.key, distance: 0, priority: s.label ? 2 : 0 });
    } else if (s.t === 'dot') {
      const d = Math.hypot(x - s.c.x, y - s.c.y);
      if (d <= s.r) consider({ key: s.key, distance: d, priority: 2 });
    } else if (pointInPolygon({ x, y }, s.pts)) {
      consider({ key: s.key, distance: 0, priority: 0 });
    }
  }
  return best;
}
