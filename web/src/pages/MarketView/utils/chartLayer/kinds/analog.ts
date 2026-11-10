import type { AnalogDrawing, LayerTime } from '../../../types/chartLayer';
import { withAlpha } from '../../canvasKit';
import { EDGE, TAG_H } from '../canvasLabels';
import { fmtMove } from '../format';
import type { DrawingExtent, SceneBase } from '../scene';
import { timeToIndex, type IP } from '../timeGrid';
import type { DrawingKind } from './types';

export interface AnalogBarScene {
  i: number;
  open: number;
  high: number;
  low: number;
  close: number;
}

export interface AnalogScene extends SceneBase {
  kind: 'analog';
  origin: IP;
  /** The replayed stretch on the chart's own history, when it is in the grid. */
  source: { i0: number; i1: number } | null;
  /** The source stretch's ends as the agent gave them, worded at paint time in the reader's language. */
  sourceStart: LayerTime;
  sourceEnd: LayerTime;
  bars: AnalogBarScene[];
  /** "212.40  +13.8%": where the replay ends, against the origin. */
  end: string;
}

/** The firm edge along the floor of the source stretch's shading, as a regime has. */
const SOURCE_STRIP = 2;

/**
 * A past stretch replayed from the origin, as TradingView's bars pattern: ghost
 * candles in one colour (hollow when they rose, filled when they fell), so they
 * never read as real bars, with the stretch they copy shaded on the history.
 */
export const analog: DrawingKind<AnalogDrawing, AnalogScene> = {
  nameKey: 'marketView.layerExplain.types.analog',
  times: (d) => [d.source_start, d.origin.time, ...d.bars.map((b) => b.time)],
  // Framed on the replay, not its source: a stretch from two years back would
  // otherwise squash today's candles into a sliver. The source is shaded where
  // it is, and seen when the reader scrolls back to it.
  frameTimes: (d) => [d.origin.time, ...d.bars.map((b) => b.time)],
  anchors: (d) => [d.origin, ...(d.bars.length ? [{ time: d.bars[d.bars.length - 1].time, price: d.bars[d.bars.length - 1].close }] : [])],
  alwaysExplained: true,
  resolve(d, { base, grid, P }) {
    const origin = P(d.origin);
    if (!origin) return null;
    const bars: AnalogBarScene[] = [];
    for (const b of d.bars) {
      const i = timeToIndex(grid, b.time);
      if (i == null || ![b.open, b.high, b.low, b.close].every(Number.isFinite)) continue;
      bars.push({ i, open: b.open, high: b.high, low: b.low, close: b.close });
    }
    if (!bars.length) return null;
    const i0 = timeToIndex(grid, d.source_start);
    const i1 = timeToIndex(grid, d.source_end);
    return {
      ...base,
      kind: 'analog',
      origin,
      source: i0 != null && i1 != null ? { i0: Math.min(i0, i1), i1: Math.max(i0, i1) } : null,
      sourceStart: d.source_start,
      sourceEnd: d.source_end,
      bars,
      end: fmtMove(origin.p, bars[bars.length - 1].close),
    };
  },
  extent(s): DrawingExtent {
    let p0 = s.origin.p;
    let p1 = s.origin.p;
    for (const b of s.bars) {
      p0 = Math.min(p0, b.low);
      p1 = Math.max(p1, b.high);
    }
    return {
      i0: Math.min(s.origin.i, s.source?.i0 ?? s.origin.i),
      i1: s.bars[s.bars.length - 1].i,
      p0,
      p1,
      openLeft: false,
      openRight: false,
      anyPrice: false,
    };
  },
  paintUnder(p, d) {
    if (!d.source) return;
    const x0 = p.X(d.source.i0 - 0.5);
    const x1 = p.X(d.source.i1 + 0.5);
    if (x0 == null || x1 == null) return;
    p.box({ x: x0, y: 0 }, { x: x1, y: p.h }, withAlpha(d.color, 0.07));
    p.box({ x: x0, y: p.h - SOURCE_STRIP }, { x: x1, y: p.h }, withAlpha(d.color, 0.45));
  },
  paintOver(p, d) {
    const first = p.X(d.bars[0].i);
    const second = d.bars.length > 1 ? p.X(d.bars[1].i) : null;
    const spacing = first != null && second != null ? Math.abs(second - first) : 6;
    const half = Math.max(1, Math.min(spacing * 0.32, 6));
    const ink = withAlpha(d.color, 0.75);
    let top = Infinity;
    let leadTop = Infinity;
    let leadBottom = -Infinity;
    const leadCount = Math.max(1, Math.ceil(d.bars.length / 3));
    for (const [n, b] of d.bars.entries()) {
      const x = p.X(b.i);
      const yh = p.Y(b.high);
      const yl = p.Y(b.low);
      const yo = p.Y(b.open);
      const yc = p.Y(b.close);
      if (x == null || yh == null || yl == null || yo == null || yc == null) continue;
      top = Math.min(top, yh);
      if (n < leadCount) {
        leadTop = Math.min(leadTop, yh);
        leadBottom = Math.max(leadBottom, yl);
      }
      p.line({ x, y: yh }, { x, y: Math.min(yo, yc) }, ink, 1, [], 1);
      p.line({ x, y: Math.max(yo, yc) }, { x, y: yl }, ink, 1, [], 1);
      const body = [
        { x: x - half, y: Math.min(yo, yc) },
        { x: x + half, y: Math.min(yo, yc) },
        { x: x + half, y: Math.max(yo, yc) + 0.5 },
        { x: x - half, y: Math.max(yo, yc) + 0.5 },
      ];
      // Hollow when the bar rose, filled when it fell: direction without a second colour.
      p.poly(body, b.close >= b.open ? withAlpha(d.color, 0.08) : withAlpha(d.color, 0.45));
      p.polyline([...body, body[0]], ink, 1, [], 1);
    }
    const o = p.P(d.origin);
    const lastBar = d.bars[d.bars.length - 1];
    const end = p.P({ i: lastBar.i, p: lastBar.close });
    const x0 = p.X(d.bars[0].i);
    if (o) p.dot(o, 3, p.th.ink);
    if (end) p.label({ el: d, text: d.end, style: 'pill', color: d.color, x: end.x + half + 4, y: end.y, align: 'left', priority: 62, movable: true, alts: [{ x: end.x, y: end.y - 18, align: 'right' }] });
    if (x0 != null && end && Number.isFinite(top)) {
      p.hit({ key: d.key, t: 'poly', pts: [{ x: x0 - half, y: top }, { x: end.x + half, y: top }, { x: end.x + half, y: p.h }, { x: x0 - half, y: p.h }] });
      // Named where the replay starts, on the side it moves away from: below a
      // rise, above a fall, so the chip never sits on the bars that follow.
      const rises = lastBar.close >= d.origin.p;
      const y = rises ? leadBottom + 18 : leadTop - 18;
      p.nameChip(d, d.label ?? p.text.replay, { x: x0 - half, y, align: 'left' }, 60);
    }
    if (d.source) {
      const sx0 = p.X(d.source.i0 - 0.5);
      const sx1 = p.X(d.source.i1 + 0.5);
      // Named only while the stretch is on screen, on its floor strip under the
      // part in view: the top-left corner belongs to the layer legend.
      const left = sx0 == null ? null : Math.max(0, sx0);
      const right = sx1 == null ? null : Math.min(p.w, sx1);
      if (left != null && right != null && right > left) {
        p.label({ el: d, text: p.text.replayed(d.sourceStart, d.sourceEnd), style: 'tag', color: withAlpha(d.color, 1), x: (left + right) / 2, y: p.h - SOURCE_STRIP - TAG_H / 2 - EDGE, align: 'center', priority: 30, movable: true });
      }
    }
  },
};
