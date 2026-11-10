import type { ArrowDrawing } from '../../../types/chartLayer';
import { clampWidth } from '../format';
import { extentOf, type SceneBase } from '../scene';
import type { IP } from '../timeGrid';
import type { DrawingKind } from './types';

export interface ArrowScene extends SceneBase {
  kind: 'arrow';
  a: IP;
  b: IP;
  width: number;
}

export const arrow: DrawingKind<ArrowDrawing, ArrowScene> = {
  nameKey: 'marketView.layerExplain.types.arrow',
  times: (d) => [d.p1.time, d.p2.time],
  anchors: (d) => [d.p1, d.p2],
  resolve(d, { base, P }) {
    const a = P(d.p1);
    const b = P(d.p2);
    if (!a || !b) return null;
    return { ...base, kind: 'arrow', a, b, width: clampWidth(d.width, 2) };
  },
  extent: (s) => extentOf([s.a, s.b]),
  paintOver(p, d) {
    const a = p.P(d.a);
    const b = p.P(d.b);
    if (!a || !b) return;
    const width = p.strokeWidth(d, d.width);
    if (!p.arrow(a, b, d.color, width, 7 + width * 2)) return;
    p.seg(d.key, a, b, 6);
    if (d.label) {
      const mid = { x: (a.x + b.x) / 2, y: (a.y + b.y) / 2 };
      p.nameChip(d, d.label, { x: mid.x + 10, y: mid.y - 14, align: 'left' }, 58);
    }
  },
};
