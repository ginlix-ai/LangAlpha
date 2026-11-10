import type { PositionDrawing } from '../../../types/chartLayer';
import { withAlpha } from '../../canvasKit';
import { PILL_H } from '../canvasLabels';
import { fmtPrice, fmtSignedPct } from '../format';
import type { Painter } from '../painter';
import { extentOf, type SceneBase } from '../scene';
import { timeToIndex } from '../timeGrid';
import type { DrawingKind } from './types';

/** Bars a position box spans when the agent gave no `until`. */
export const DEFAULT_POSITION_BARS = 20;

export interface PositionTarget {
  price: number;
  r: number;
  pct: number;
}

export interface PositionScene extends SceneBase {
  kind: 'position';
  side: 'long' | 'short';
  i0: number;
  i1: number;
  entry: number;
  stop: number;
  stopPct: number;
  targets: PositionTarget[];
}

/** Long/short R multiples: reward over risk per target, plus each target's % move. */
export function positionStats(
  side: 'long' | 'short',
  entry: number,
  stop: number,
  targets: readonly number[],
): { stopPct: number; targets: PositionTarget[] } {
  const risk = Math.abs(entry - stop);
  const dir = side === 'long' ? 1 : -1;
  return {
    stopPct: entry !== 0 ? (stop - entry) / entry : 0,
    targets: targets
      .filter((t) => Number.isFinite(t))
      .map((price) => ({
        price,
        r: risk > 0 ? ((price - entry) * dir) / risk : 0,
        pct: entry !== 0 ? (price - entry) / entry : 0,
      })),
  };
}

function geom(p: Painter, d: PositionScene) {
  const x0 = p.X(d.i0);
  const x1 = p.X(d.i1);
  const yE = p.Y(d.entry);
  const yS = p.Y(d.stop);
  if (x0 == null || x1 == null || yE == null || yS == null) return null;
  const dir = d.side === 'long' ? 1 : -1;
  // Targets ordered outward from entry, each zone stacked on the last.
  const targets = [...d.targets]
    .filter((t) => (t.price - d.entry) * dir > 0)
    .sort((a, b) => (a.price - b.price) * dir)
    .map((t) => ({ ...t, y: p.Y(t.price) }))
    .filter((t): t is typeof t & { y: number } => t.y != null);
  return { x0: Math.min(x0, x1), x1: Math.max(x0, x1), yE, yS, targets };
}

export const position: DrawingKind<PositionDrawing, PositionScene> = {
  nameKey: 'marketView.layerExplain.types.position',
  times: (d) => [d.entry.time, d.until],
  barsAfter: (d) => (d.until ? null : { time: d.entry.time, bars: DEFAULT_POSITION_BARS }),
  anchors: (d) => [d.entry],
  alwaysExplained: true,
  resolve(d, { base, grid }) {
    const i0 = timeToIndex(grid, d.entry.time);
    if (i0 == null || !Number.isFinite(d.entry.price) || !Number.isFinite(d.stop)) return null;
    const until = d.until ? timeToIndex(grid, d.until) : null;
    const i1 = until != null && until > i0 ? until : i0 + DEFAULT_POSITION_BARS;
    const stats = positionStats(d.side, d.entry.price, d.stop, d.targets);
    return {
      ...base,
      kind: 'position',
      side: d.side,
      i0,
      i1,
      entry: d.entry.price,
      stop: d.stop,
      stopPct: stats.stopPct,
      targets: stats.targets,
    };
  },
  extent: (s) =>
    extentOf(
      [
        { i: s.i0, p: s.entry },
        { i: s.i1, p: s.stop },
      ],
      'none',
      s.targets.map((t) => t.price),
    ),
  paintUnder(p, d) {
    const g = geom(p, d);
    if (!g) return;
    const bw = g.x1 - g.x0;
    p.rect(g.x0, Math.min(g.yE, g.yS), bw, Math.abs(g.yS - g.yE), withAlpha(p.th.loss, 0.15));
    let prevY = g.yE;
    g.targets.forEach((t, k) => {
      p.rect(g.x0, Math.min(prevY, t.y), bw, Math.abs(t.y - prevY), withAlpha(p.th.profit, Math.max(0.07, 0.15 - k * 0.035)));
      prevY = t.y;
    });
  },
  /**
   * TradingView's long/short tool: a solid readout on each edge of the box and
   * one on the entry line. The farthest target and the stop sit outside the
   * box, centred; a nearer target is tagged inside at its line.
   */
  paintOver(p, d) {
    const g = geom(p, d);
    if (!g) return;
    const width = p.strokeWidth(d, 1);
    p.line({ x: g.x0, y: g.yS }, { x: g.x1, y: g.yS }, p.th.loss, width, [], 0.85);
    for (const t of g.targets) p.line({ x: g.x0, y: t.y }, { x: g.x1, y: t.y }, p.th.profit, width, [], 0.85);
    p.line({ x: g.x0, y: g.yE }, { x: g.x1, y: g.yE }, d.color, p.strokeWidth(d, 1.5));
    // Centre the readouts on the part of the box in view; a readout that
    // meets other text there slides to either end of the box instead.
    const bl = Math.max(g.x0, 0) + 2;
    const br = Math.min(g.x1, p.w) - 2;
    const cx = (bl + br) / 2;
    const along = (y: number) => [
      { x: br, y, align: 'right' as const },
      { x: bl, y, align: 'left' as const },
    ];
    const away = (y: number) => (y < g.yE ? -1 : 1);
    const last = g.targets.length - 1;
    g.targets.forEach((t, k) => {
      if (k === last) {
        const text = `${p.text.target} ${fmtPrice(t.price)} (${fmtSignedPct(t.pct)}) · ${t.r.toFixed(1)}R`;
        const y = t.y + away(t.y) * (PILL_H / 2 + 3);
        p.label({ el: d, text, style: 'pill', color: p.th.profit, x: cx, y, align: 'center', priority: 64, movable: false, alts: along(y) });
      } else {
        const text = `${p.text.targetN(k + 1)} ${fmtPrice(t.price)} · ${t.r.toFixed(1)}R`;
        const y = t.y - away(t.y) * 8;
        p.label({ el: d, text, style: 'tag', color: withAlpha(p.th.profit, 1), x: br - 2, y, align: 'right', priority: 40, movable: false, alts: [{ x: bl + 2, y, align: 'left' }] });
      }
    });
    const stopText = `${p.text.stop} ${fmtPrice(d.stop)} (${fmtSignedPct(d.stopPct)})`;
    const stopY = g.yS + away(g.yS) * (PILL_H / 2 + 3);
    p.label({ el: d, text: stopText, style: 'pill', color: p.th.loss, x: cx, y: stopY, align: 'center', priority: 63, movable: false, alts: along(stopY) });
    const best = g.targets.length ? g.targets[last].r : null;
    const side = d.label ?? (d.side === 'long' ? p.text.long : p.text.short);
    const head = `${side} ${fmtPrice(d.entry)}${best != null ? ` · ${p.text.riskReward} ${best.toFixed(1)}` : ''}`;
    p.label({ el: d, text: head, style: 'pill', color: p.th.muted, x: cx, y: g.yE, align: 'center', priority: 65, movable: false, alts: along(g.yE) });
    const allY = [g.yE, g.yS, ...g.targets.map((t) => t.y)];
    p.hit({ key: d.key, t: 'rect', x0: g.x0, y0: Math.min(...allY), x1: g.x1, y1: Math.max(...allY) });
  },
};
