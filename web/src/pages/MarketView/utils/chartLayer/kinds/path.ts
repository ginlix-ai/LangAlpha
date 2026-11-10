import type { PathDrawing } from '../../../types/chartLayer';
import { withAlpha } from '../../canvasKit';
import type { XY } from '../canvasGeom';
import { fillOf } from '../color';
import { clampWidth, dashFor } from '../format';
import { extentOf, type SceneBase } from '../scene';
import type { IP } from '../timeGrid';
import type { DrawingKind } from './types';

export interface PathScene extends SceneBase {
  kind: 'path';
  pts: IP[];
  pointLabels: (string | null)[];
  labelSides: ('above' | 'below')[];
  closed: boolean;
  width: number;
  dash: number[];
}

/**
 * Which side a path point's label goes: above a local high, below a local low,
 * so H&S shoulders and Elliott pivots read without sitting on the line.
 */
export function pathLabelSides(pts: readonly IP[]): ('above' | 'below')[] {
  return pts.map((pt, k) => {
    const prev = pts[k - 1];
    const next = pts[k + 1];
    const ref = prev && next ? (prev.p + next.p) / 2 : (prev ?? next)?.p ?? pt.p;
    return pt.p >= ref ? 'above' : 'below';
  });
}

export const path: DrawingKind<PathDrawing, PathScene> = {
  nameKey: 'marketView.layerExplain.types.path',
  times: (d) => d.points.map((pt) => pt.time),
  anchors: (d) => d.points,
  resolve(d, { base, P }) {
    const pts: IP[] = [];
    const labels: (string | null)[] = [];
    d.points.forEach((pt, k) => {
      const ip = P(pt);
      if (!ip) return;
      pts.push(ip);
      labels.push(d.point_labels?.[k] ?? null);
    });
    if (pts.length < 2) return null;
    return {
      ...base,
      kind: 'path',
      pts,
      pointLabels: labels,
      labelSides: pathLabelSides(pts),
      closed: !!d.closed,
      width: clampWidth(d.width, 1.5),
      dash: dashFor(d.style),
    };
  },
  extent: (s) => extentOf(s.pts),
  paintOver(p, d) {
    const pts = d.pts.map((pt) => p.P(pt));
    if (pts.some((pt) => !pt)) return;
    const xy = pts as XY[];
    if (p.offscreen(xy.map((pt) => pt.x))) return;
    if (d.closed) p.poly(xy, fillOf(d.color, 0.06));
    p.polyline(d.closed ? [...xy, xy[0]] : xy, d.color, p.strokeWidth(d, d.width), d.dash);
    for (let k = 0; k < xy.length; k++) {
      if (k > 0) p.seg(d.key, xy[k - 1], xy[k]);
      const lbl = d.pointLabels[k];
      if (!lbl) continue;
      p.dot(xy[k], 2.5, d.color);
      const above = d.labelSides[k] === 'above';
      p.label({ el: d, text: lbl, style: 'point', color: withAlpha(d.color, 1), x: xy[k].x, y: xy[k].y + (above ? -11 : 11), align: 'center', priority: 70, movable: false });
    }
    if (d.closed) p.seg(d.key, xy[xy.length - 1], xy[0]);
    if (d.label) {
      let top = xy[0];
      for (const pt of xy) if (pt.y < top.y) top = pt;
      p.nameChip(d, d.label, { x: top.x, y: top.y - 30, align: 'center' }, 62);
    }
  },
};
