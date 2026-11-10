import type { ConeDrawing } from '../../../types/chartLayer';
import { withAlpha } from '../../canvasKit';
import type { XY } from '../canvasGeom';
import { fillOf, hasAlpha } from '../color';
import { fmtPrice } from '../format';
import type { Painter } from '../painter';
import { extentOf, type SceneBase } from '../scene';
import type { IP } from '../timeGrid';
import type { DrawingKind } from './types';

export interface ConeBandScene {
  label: string;
  upper: IP[];
  lower: IP[];
  fill: string;
  edge: string;
}

export interface ConeScene extends SceneBase {
  kind: 'cone';
  origin: IP;
  bands: ConeBandScene[];
}

/** Cone bands widest first (painted first, so narrower, likelier bands sit on top). */
export function orderConeBands<T extends { upper: IP[]; lower: IP[] }>(bands: readonly T[]): T[] {
  const width = (b: T) => {
    const n = Math.min(b.upper.length, b.lower.length);
    if (n === 0) return 0;
    let sum = 0;
    for (let k = 0; k < n; k++) sum += Math.abs(b.upper[k].p - b.lower[k].p);
    return sum / n;
  };
  return [...bands].sort((x, y) => width(y) - width(x));
}

function polys(p: Painter, d: ConeScene) {
  const o = p.P(d.origin);
  if (!o) return null;
  return {
    o,
    bands: d.bands.map((band) => {
      const up = band.upper.map((pt) => p.P(pt)).filter((pt): pt is XY => !!pt);
      const lo = band.lower.map((pt) => p.P(pt)).filter((pt): pt is XY => !!pt);
      return { band, up: [o, ...up], lo: [o, ...lo] };
    }),
  };
}

function lastPrice(pts: readonly IP[]): number {
  return pts.length ? pts[pts.length - 1].p : NaN;
}

export const cone: DrawingKind<ConeDrawing, ConeScene> = {
  nameKey: 'marketView.layerExplain.types.cone',
  times: (d) => cone.anchors(d).map((pt) => pt.time),
  anchors: (d) => [d.origin, ...d.bands.flatMap((b) => [...b.upper, ...b.lower])],
  resolve(d, { base, P }) {
    const origin = P(d.origin);
    if (!origin) return null;
    const resolved = d.bands
      .map((band) => ({
        label: band.label,
        upper: band.upper.map(P).filter((x): x is IP => !!x),
        lower: band.lower.map(P).filter((x): x is IP => !!x),
        given: band.fill ?? null,
      }))
      .filter((band) => band.upper.length > 0 && band.lower.length > 0);
    const color = base.color;
    const bands: ConeBandScene[] = orderConeBands(resolved).map((band, rank) => ({
      label: band.label,
      upper: band.upper,
      lower: band.lower,
      // Widest is faintest; each narrower band adds a step of tint.
      fill: fillOf(band.given ?? color, 0.045 + 0.04 * rank, 0.12),
      edge: withAlpha(band.given && !hasAlpha(band.given) ? band.given : color, 0.45),
    }));
    if (!bands.length) return null;
    return { ...base, kind: 'cone', origin, bands };
  },
  extent: (s) => extentOf([s.origin, ...s.bands.flatMap((b) => [...b.upper, ...b.lower])]),
  paintUnder(p, d) {
    const g = polys(p, d);
    if (!g) return;
    for (const b of g.bands) p.poly([...b.up, ...[...b.lo].reverse()], b.band.fill);
  },
  paintOver(p, d) {
    const g = polys(p, d);
    if (!g) return;
    for (const b of g.bands) {
      for (const edge of [b.up, b.lo]) {
        p.polyline(edge, b.band.edge, p.strokeWidth(d, 1), [], 1);
        for (let k = 1; k < edge.length; k++) p.seg(d.key, edge[k - 1], edge[k], 4);
      }
      const end = b.up[b.up.length - 1];
      const endLo = b.lo[b.lo.length - 1];
      if (b.band.label) {
        // One tag per end: the band's name rides with its upper price, so the
        // two never contend for one spot and the price is always shown. The
        // bare lower price yields to another element's named label (30).
        const upper = `${b.band.label} ${fmtPrice(lastPrice(b.band.upper))}`;
        p.label({ el: d, text: upper, style: 'tag', color: withAlpha(d.color, 1), x: end.x + 5, y: end.y, align: 'left', priority: 32, movable: false });
        p.label({ el: d, text: fmtPrice(lastPrice(b.band.lower)), style: 'tag', color: withAlpha(d.color, 1), x: endLo.x + 5, y: endLo.y, align: 'left', priority: 28, movable: false });
      }
    }
    p.dot(g.o, 3, d.color);
    const widest = g.bands[0];
    if (widest) p.hit({ key: d.key, t: 'poly', pts: [...widest.up, ...[...widest.lo].reverse()] });
    if (d.label) p.nameChip(d, d.label, { x: g.o.x - 8, y: g.o.y - 16, align: 'right' }, 60);
  },
};
