import type { Extend, FibDrawing } from '../../../types/chartLayer';
import { withAlpha } from '../../canvasKit';
import { fmtPrice, trimRatio } from '../format';
import type { Painter } from '../painter';
import { extentOf, type SceneBase } from '../scene';
import type { IP } from '../timeGrid';
import type { DrawingKind } from './types';

/** The standard Fibonacci retracement ratios, when the layer names none. */
const FIB_RATIOS = [0, 0.236, 0.382, 0.5, 0.618, 0.786, 1] as const;

export interface FibLevel {
  ratio: number;
  price: number;
}

export interface FibScene extends SceneBase {
  kind: 'fib';
  a: IP;
  b: IP;
  levels: FibLevel[];
  extend: Extend;
}

/**
 * Fib levels: ratio 0 sits at p2 and 1 at p1 (a retracement measured back from
 * the end of the move), so 1.618 and other extensions fall past p1.
 */
export function fibLevels(p1: number, p2: number, ratios?: readonly number[] | null): FibLevel[] {
  const rs = ratios && ratios.length ? ratios : FIB_RATIOS;
  return rs
    .filter((r) => Number.isFinite(r))
    .map((ratio) => ({ ratio, price: p2 + (p1 - p2) * ratio }));
}

/**
 * Fib levels in the colors traders know them by (TradingView's defaults,
 * drawn from the theme): the swing's ends in the drawing's own color.
 */
function fibColor(p: Painter, ratio: number, own: string): string {
  const th = p.th;
  const r = Number(ratio.toFixed(3));
  if (r === 0 || r === 1) return own;
  if (r === 0.236) return th.loss;
  if (r === 0.382) return th.warning;
  if (r === 0.5) return th.profit;
  if (r === 0.618) return th.profit;
  if (r === 0.786) return th.neutral;
  return r > 1 ? th.neutral : own;
}

function range(p: Painter, d: FibScene): [number, number] | null {
  const xa = p.X(d.a.i);
  const xb = p.X(d.b.i);
  if (xa == null || xb == null) return null;
  const [left, right] = p.extendedSpan(Math.min(xa, xb), Math.max(xa, xb), d.extend);
  if (right < 0 || left > p.w) return null;
  return [left, right];
}

export const fib: DrawingKind<FibDrawing, FibScene> = {
  nameKey: 'marketView.layerExplain.types.fib',
  times: (d) => [d.p1.time, d.p2.time],
  anchors: (d) => [d.p1, d.p2],
  resolve(d, { base, P }) {
    const a = P(d.p1);
    const b = P(d.p2);
    if (!a || !b) return null;
    return { ...base, kind: 'fib', a, b, levels: fibLevels(a.p, b.p, d.levels), extend: d.extend ?? 'none' };
  },
  extent: (s) => extentOf([s.a, s.b], s.extend, s.levels.map((l) => l.price)),
  paintUnder(p, d) {
    const span = range(p, d);
    const xa = p.X(d.a.i);
    const xb = p.X(d.b.i);
    if (!span || xa == null || xb == null) return;
    const ys = d.levels.map((l) => p.Y(l.price));
    const sorted = d.levels
      .map((l, k) => ({ ratio: l.ratio, y: ys[k] }))
      .filter((l): l is { ratio: number; y: number } => l.y != null)
      .sort((a, b) => a.ratio - b.ratio);
    for (let k = 1; k < sorted.length; k++) {
      const y0 = sorted[k - 1].y;
      const y1 = sorted[k].y;
      // Each band in its upper level's color; the golden pocket (to 0.618) a shade deeper.
      const r = sorted[k].ratio;
      const golden = r > 0.5 && r <= 0.618;
      const fill = withAlpha(fibColor(p, r, d.color), golden ? 0.075 : 0.035);
      // The bands shade the swing; an extended fib runs its lines on, not its bands.
      p.fadedFill(fill, xa, xb, d.extend, () => p.rect(span[0], Math.min(y0, y1), span[1] - span[0], Math.abs(y1 - y0), null));
    }
  },
  paintOver(p, d) {
    const span = range(p, d);
    if (!span) return;
    const [left, right] = span;
    const a = p.P(d.a);
    const b = p.P(d.b);
    if (a && b) p.line(a, b, d.color, 1, [2, 4], 0.5);
    let topY = Infinity;
    let botY = -Infinity;
    for (const lvl of d.levels) {
      const y = p.Y(lvl.price);
      if (y == null) continue;
      topY = Math.min(topY, y);
      botY = Math.max(botY, y);
      const key = lvl.ratio === 0 || lvl.ratio === 1;
      const c = fibColor(p, lvl.ratio, d.color);
      p.line({ x: left, y }, { x: right, y }, c, key ? 1.25 : 1, [], key ? 0.9 : p.isHovered(d.key) ? 0.95 : 0.75);
      p.seg(d.key, { x: left, y }, { x: right, y }, 3);
      p.label({ el: d, text: `${trimRatio(lvl.ratio)} (${fmtPrice(lvl.price)})`, style: 'tag', color: withAlpha(c, 1), x: Math.max(left, 0) + 4, y: y - 7, align: 'left', priority: 30, movable: false });
    }
    if (d.label && Number.isFinite(topY)) {
      p.nameChip(d, d.label, { x: Math.min(right, p.w) - 4, y: topY - 14, align: 'right' }, 58);
    }
    if (Number.isFinite(topY)) p.hit({ key: d.key, t: 'rect', x0: left, y0: topY, x1: right, y1: botY });
  },
};
