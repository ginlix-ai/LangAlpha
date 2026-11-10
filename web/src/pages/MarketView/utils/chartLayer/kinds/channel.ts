import type { ChannelDrawing, Extend } from '../../../types/chartLayer';
import { fillOf } from '../color';
import type { Painter } from '../painter';
import { extentOf, rayExtent, type SceneBase } from '../scene';
import type { IP } from '../timeGrid';
import type { DrawingKind } from './types';

export interface ChannelScene extends SceneBase {
  kind: 'channel';
  a: IP;
  b: IP;
  c: IP;
  offset: number;
  extend: Extend;
  fill: string;
}

/** Price on the line through a and b at index i. */
function priceOnLine(a: IP, b: IP, i: number): number {
  if (b.i === a.i) return a.p;
  return a.p + ((b.p - a.p) * (i - a.i)) / (b.i - a.i);
}

/** Channel: the parallel's price offset from line p1–p2, through p3. */
export function channelOffset(a: IP, b: IP, c: IP): number {
  return c.p - priceOnLine(a, b, c.i);
}

/** The parallel is taken on screen, through p3's point, as the pitchfork's
 *  rails are: on a log scale a fixed price offset is neither parallel to p1–p2
 *  nor through p3. On a linear scale the two agree. */
function geom(p: Painter, d: ChannelScene) {
  const a = p.P(d.a);
  const b = p.P(d.b);
  const c = p.P(d.c);
  if (!a || !b || !c || a.x === b.x) return null;
  const dy = c.y - (a.y + ((b.y - a.y) * (c.x - a.x)) / (b.x - a.x));
  const [s1, e1] = p.extended(a, b, d.extend);
  const [s2, e2] = p.extended({ x: a.x, y: a.y + dy }, { x: b.x, y: b.y + dy }, d.extend);
  return { s1, e1, s2, e2 };
}

export const channel: DrawingKind<ChannelDrawing, ChannelScene> = {
  nameKey: 'marketView.layerExplain.types.channel',
  times: (d) => [d.p1.time, d.p2.time, d.p3.time],
  anchors: (d) => [d.p1, d.p2, d.p3],
  resolve(d, { base, P }) {
    const a = P(d.p1);
    const b = P(d.p2);
    const c = P(d.p3);
    if (!a || !b || !c || a.i === b.i) return null;
    return {
      ...base,
      kind: 'channel',
      a,
      b,
      c,
      offset: channelOffset(a, b, c),
      extend: d.extend ?? 'none',
      fill: fillOf(d.fill ?? base.color, 0.07, 0.12),
    };
  },
  extent: (s) => rayExtent(extentOf([s.a, s.b, s.c], s.extend, [s.a.p + s.offset, s.b.p + s.offset])),
  paintUnder(p, d) {
    const g = geom(p, d);
    const x0 = p.X(Math.min(d.a.i, d.b.i, d.c.i));
    const x1 = p.X(Math.max(d.a.i, d.b.i, d.c.i));
    if (g && x0 != null && x1 != null) {
      p.fadedFill(d.fill, x0, x1, d.extend, () => p.poly([g.s1, g.e1, g.e2, g.s2], null));
    }
  },
  paintOver(p, d) {
    const g = geom(p, d);
    if (!g) return;
    const wd = p.strokeWidth(d, 1.5);
    p.line(g.s1, g.e1, d.color, wd);
    p.line(g.s2, g.e2, d.color, wd);
    const ms = { x: (g.s1.x + g.s2.x) / 2, y: (g.s1.y + g.s2.y) / 2 };
    const me = { x: (g.e1.x + g.e2.x) / 2, y: (g.e1.y + g.e2.y) / 2 };
    p.line(ms, me, d.color, 1, [4, 4], 0.4);
    p.seg(d.key, g.s1, g.e1);
    p.seg(d.key, g.s2, g.e2);
    if (d.label) {
      // Above whichever rail is on top at the channel's start.
      const top = g.s1.y <= g.s2.y ? g.s1 : g.s2;
      const x = Math.max(top.x, 8);
      p.nameChip(d, d.label, { x, y: top.y - 14, align: 'left' }, 60);
    }
  },
};
