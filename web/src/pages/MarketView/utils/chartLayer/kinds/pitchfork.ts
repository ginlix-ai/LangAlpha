import type { Extend, PitchforkDrawing } from '../../../types/chartLayer';
import { fillOf } from '../color';
import type { Painter } from '../painter';
import { extentOf, rayExtent, type SceneBase } from '../scene';
import type { IP } from '../timeGrid';
import type { DrawingKind } from './types';

export interface PitchforkScene extends SceneBase {
  kind: 'pitchfork';
  p1: IP;
  p2: IP;
  p3: IP;
  extend: Extend;
}

/** Andrews pitchfork: midpoint of p2–p3 in grid space, where the median's
 *  target sits in time. Its price is the drawn midpoint only on a linear scale. */
export function pitchforkMid(p2: IP, p3: IP): IP {
  return { i: (p2.i + p3.i) / 2, p: (p2.p + p3.p) / 2 };
}

/** The median aims at the midpoint of the p2–p3 connector as drawn, as the
 *  rails take their direction on screen: on a log scale the midpoint price
 *  projects off the connector. On a linear scale the two agree. */
function geom(p: Painter, d: PitchforkScene) {
  const p1 = p.P(d.p1);
  const p2 = p.P(d.p2);
  const p3 = p.P(d.p3);
  if (!p1 || !p2 || !p3) return null;
  const m = { x: (p2.x + p3.x) / 2, y: (p2.y + p3.y) / 2 };
  const dx = m.x - p1.x;
  const dy = m.y - p1.y;
  const med = p.extended(p1, m, d.extend);
  const up = p.extended(p2, { x: p2.x + dx, y: p2.y + dy }, d.extend);
  const lo = p.extended(p3, { x: p3.x + dx, y: p3.y + dy }, d.extend);
  return { p1, p2, p3, m, med, up, lo };
}

export const pitchfork: DrawingKind<PitchforkDrawing, PitchforkScene> = {
  nameKey: 'marketView.layerExplain.types.pitchfork',
  times: (d) => [d.p1.time, d.p2.time, d.p3.time],
  anchors: (d) => [d.p1, d.p2, d.p3],
  resolve(d, { base, P }) {
    const p1 = P(d.p1);
    const p2 = P(d.p2);
    const p3 = P(d.p3);
    if (!p1 || !p2 || !p3) return null;
    if (pitchforkMid(p2, p3).i === p1.i) return null;
    return { ...base, kind: 'pitchfork', p1, p2, p3, extend: d.extend ?? 'right' };
  },
  extent: (s) => rayExtent(extentOf([s.p1, s.p2, s.p3], s.extend)),
  paintUnder(p, d) {
    const g = geom(p, d);
    if (g) p.poly([g.up[0], g.up[1], g.lo[1], g.lo[0]], fillOf(d.color, 0.05));
  },
  paintOver(p, d) {
    const g = geom(p, d);
    if (!g) return;
    p.line(g.med[0], g.med[1], d.color, p.strokeWidth(d, 1.5));
    p.line(g.up[0], g.up[1], d.color, p.strokeWidth(d, 1.25), [], 0.8);
    p.line(g.lo[0], g.lo[1], d.color, p.strokeWidth(d, 1.25), [], 0.8);
    p.line(g.p2, g.p3, d.color, 1, [3, 3], 0.55);
    for (const pt of [g.p1, g.p2, g.p3]) p.dot(pt, 2.5, d.color);
    p.seg(d.key, g.med[0], g.med[1]);
    p.seg(d.key, g.up[0], g.up[1]);
    p.seg(d.key, g.lo[0], g.lo[1]);
    if (d.label) p.nameChip(d, d.label, { x: g.p1.x - 8, y: g.p1.y, align: 'right' }, 60);
  },
};
