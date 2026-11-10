import type { BoxDrawing } from '../../../types/chartLayer';
import { CHIP_H } from '../canvasLabels';
import { fillOf } from '../color';
import { extentOf, type SceneBase } from '../scene';
import type { IP } from '../timeGrid';
import type { DrawingKind } from './types';

export interface BoxScene extends SceneBase {
  kind: 'box';
  a: IP;
  b: IP;
  fill: string;
}

export const box: DrawingKind<BoxDrawing, BoxScene> = {
  nameKey: 'marketView.layerExplain.types.box',
  times: (d) => [d.p1.time, d.p2.time],
  anchors: (d) => [d.p1, d.p2],
  resolve(d, { base, P }) {
    const a = P(d.p1);
    const b = P(d.p2);
    if (!a || !b) return null;
    return { ...base, kind: 'box', a, b, fill: fillOf(d.fill ?? base.color, 0.09, 0.14) };
  },
  extent: (s) => extentOf([s.a, s.b]),
  paintUnder(p, d) {
    const a = p.P(d.a);
    const b = p.P(d.b);
    if (a && b) p.box(a, b, d.fill);
  },
  paintOver(p, d) {
    const a = p.P(d.a);
    const b = p.P(d.b);
    if (!a || !b) return;
    const x0 = Math.min(a.x, b.x);
    const y0 = Math.min(a.y, b.y);
    const bw = Math.abs(b.x - a.x);
    const bh = Math.abs(b.y - a.y);
    p.strokeRect(x0 + 0.5, y0 + 0.5, bw, bh, d.color, 1, p.isHovered(d.key) ? 0.95 : 0.6);
    p.hit({ key: d.key, t: 'rect', x0, y0, x1: x0 + bw, y1: y0 + bh });
    if (d.label) {
      // Over the zone's top edge rather than inside it, where the bars are;
      // its far corner (often the empty future) if the near one is taken.
      const y = y0 - CHIP_H / 2 - 2;
      const left = Math.max(x0, 0) + 4;
      const right = Math.min(x0 + bw, p.w) - 4;
      p.nameChip(d, d.label, { x: left, y, align: 'left' }, 58, {
        alts: [
          { x: right, y, align: 'right' },
          { x: right, y: y0 + bh + CHIP_H / 2 + 2, align: 'right' },
        ],
      });
    }
  },
};
