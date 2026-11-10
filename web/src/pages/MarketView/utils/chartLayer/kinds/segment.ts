import type { SegmentDrawing, Extend } from '../../../types/chartLayer';
import { clampWidth, dashFor } from '../format';
import { extentOf, rayExtent, type SceneBase } from '../scene';
import type { IP } from '../timeGrid';
import type { DrawingKind } from './types';

export interface SegmentScene extends SceneBase {
  kind: 'segment';
  a: IP;
  b: IP;
  extend: Extend;
  width: number;
  dash: number[];
}

export const segment: DrawingKind<SegmentDrawing, SegmentScene> = {
  nameKey: 'marketView.layerExplain.types.segment',
  times: (d) => [d.p1.time, d.p2.time],
  anchors: (d) => [d.p1, d.p2],
  resolve(d, { base, P }) {
    const a = P(d.p1);
    const b = P(d.p2);
    if (!a || !b) return null;
    return { ...base, kind: 'segment', a, b, extend: d.extend ?? 'none', width: clampWidth(d.width, 1.5), dash: dashFor(d.style) };
  },
  extent: (s) => rayExtent(extentOf([s.a, s.b], s.extend)),
  paintOver(p, d) {
    const a = p.P(d.a);
    const b = p.P(d.b);
    if (!a || !b) return;
    const [s, e] = p.extended(a, b, d.extend);
    p.line(s, e, d.color, p.strokeWidth(d, d.width), d.dash);
    p.seg(d.key, s, e);
    if (d.label) {
      const end = a.x >= b.x ? a : b;
      const right = end.x > p.w - 80;
      p.nameChip(d, d.label, { x: right ? end.x - 8 : end.x + 8, y: end.y - 12, align: right ? 'right' : 'left' }, 60);
    }
  },
};
