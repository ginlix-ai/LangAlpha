import type { ProfileDrawing, ProfileRow } from '../../../types/chartLayer';
import { withAlpha } from '../../canvasKit';
import { fmtPrice } from '../format';
import type { Painter } from '../painter';
import { extentOf, type SceneBase } from '../scene';
import { timeToIndex } from '../timeGrid';
import type { DrawingKind } from './types';

export interface ProfileRowScene {
  low: number;
  high: number;
  /** Volume relative to the largest row, 0..1. */
  frac: number;
  /** Share of the row's volume that traded up, when the agent gave it. */
  upShare: number | null;
  inValueArea: boolean;
}

export interface ProfileScene extends SceneBase {
  kind: 'profile';
  i0: number;
  i1: number;
  rows: ProfileRowScene[];
  poc: number | null;
  valueArea: [number, number] | null;
}

const PROFILE_MAX_PX = 150;

/** Profile histogram: the widest row's length in px, from the range's pixel span. */
function profileMaxWidth(spanPx: number): number {
  // A quarter of the range, held to a band the candles can be read through: a
  // profile over a whole pane would otherwise reach a third of the way across.
  const span = Math.max(0, spanPx);
  return Math.min(span, Math.max(48, Math.min(span * 0.25, PROFILE_MAX_PX)));
}

/** Profile rows sized against the largest, with the value-area flag. */
export function profileRows(
  rows: readonly ProfileRow[],
  valueArea: readonly [number, number] | null | undefined,
): ProfileRowScene[] {
  let max = 0;
  for (const r of rows) if (Number.isFinite(r.volume) && r.volume > max) max = r.volume;
  const vaLo = valueArea ? Math.min(valueArea[0], valueArea[1]) : null;
  const vaHi = valueArea ? Math.max(valueArea[0], valueArea[1]) : null;
  return rows
    .filter((r) => Number.isFinite(r.low) && Number.isFinite(r.high) && Number.isFinite(r.volume))
    .map((r) => {
      const mid = (r.low + r.high) / 2;
      const up = r.up_volume;
      return {
        low: Math.min(r.low, r.high),
        high: Math.max(r.low, r.high),
        frac: max > 0 ? Math.max(0, r.volume) / max : 0,
        upShare: up != null && Number.isFinite(up) && r.volume > 0 ? Math.max(0, Math.min(1, up / r.volume)) : null,
        inValueArea: vaLo != null && vaHi != null && mid >= vaLo && mid <= vaHi,
      };
    });
}

function geom(p: Painter, d: ProfileScene) {
  const x0 = p.X(d.i0);
  const x1 = p.X(d.i1);
  if (x0 == null || x1 == null || x1 < -20 || x0 > p.w + 20) return null;
  // The bars grow from the range's start, or from the pane's edge once that
  // start has scrolled out of view, so a long profile still shows.
  const bx = Math.max(x0, 0);
  return { x0, x1, bx, maxW: profileMaxWidth(Math.min(x1, p.w) - bx) };
}

export const profile: DrawingKind<ProfileDrawing, ProfileScene> = {
  nameKey: 'marketView.layerExplain.types.profile',
  times: (d) => [d.from, d.to],
  anchors: (d) => [
    { time: d.from, price: d.poc ?? NaN },
    { time: d.to, price: d.poc ?? NaN },
  ],
  alwaysExplained: true,
  resolve(d, { base, grid }) {
    const i0 = timeToIndex(grid, d.from);
    const i1 = timeToIndex(grid, d.to);
    if (i0 == null || i1 == null) return null;
    const rows = profileRows(d.rows, d.value_area);
    if (!rows.length) return null;
    const lo = Math.min(i0, i1);
    return {
      ...base,
      kind: 'profile',
      i0: lo,
      // A profile of one bar still gets that bar's slot to lay its rows across.
      i1: Math.max(i0, i1, lo + 1),
      rows,
      poc: d.poc != null && Number.isFinite(d.poc) ? d.poc : null,
      valueArea: d.value_area ?? null,
    };
  },
  extent: (s) =>
    extentOf([
      { i: s.i0, p: Math.min(...s.rows.map((r) => r.low)) },
      { i: s.i1, p: Math.max(...s.rows.map((r) => r.high)) },
    ]),
  paintUnder(p, d) {
    const g = geom(p, d);
    if (!g) return;
    for (const r of d.rows) {
      const yt = p.Y(r.high);
      const yb = p.Y(r.low);
      if (yt == null || yb == null) continue;
      const top = Math.min(yt, yb) + 0.5;
      const hgt = Math.max(1, Math.abs(yb - yt) - 1);
      const width = r.frac * g.maxW;
      // Quiet enough that the candles read through it: the value area a
      // step stronger than the tails.
      const a = r.inValueArea ? 0.16 : 0.07;
      if (r.upShare != null) {
        const upW = width * r.upShare;
        p.rect(g.bx, top, upW, hgt, withAlpha(p.th.profit, a));
        p.rect(g.bx + upW, top, width - upW, hgt, withAlpha(p.th.loss, a));
      } else {
        p.rect(g.bx, top, width, hgt, withAlpha(d.color, a));
      }
    }
  },
  paintOver(p, d) {
    const g = geom(p, d);
    if (!g) return;
    let yTop = Infinity;
    let yBot = -Infinity;
    for (const r of d.rows) {
      const a = p.Y(r.high);
      const b = p.Y(r.low);
      if (a != null) yTop = Math.min(yTop, a);
      if (b != null) yBot = Math.max(yBot, b);
    }
    if (!Number.isFinite(yTop)) return;
    p.line({ x: g.x1, y: yTop }, { x: g.x1, y: yBot }, d.color, 1, [2, 3], 0.3);
    // The value area's edges as faint rules; the agent names them with levels
    // when they matter, so the profile does not caption them a second time.
    // Across the histogram only: ruled over the whole range they cross every
    // candle in it.
    const histR = g.bx + g.maxW;
    if (d.valueArea) {
      for (const price of d.valueArea) {
        const y = p.Y(price);
        if (y != null) p.line({ x: g.bx, y }, { x: histR, y }, d.color, 1, [3, 3], 0.35);
      }
    }
    if (d.poc != null) {
      const y = p.Y(d.poc);
      if (y != null) {
        // The POC stays the profile's one firm mark: solid over the histogram,
        // a faint run on to the range's end where its tag sits.
        p.line({ x: g.bx, y }, { x: histR, y }, d.color, p.strokeWidth(d, 1.5));
        if (g.x1 > histR) p.line({ x: histR, y }, { x: g.x1, y }, d.color, 1, [2, 3], 0.45);
        p.seg(d.key, { x: g.bx, y }, { x: g.x1, y }, 4);
        p.label({ el: d, text: `${p.text.poc} ${fmtPrice(d.poc)}`, style: 'tag', color: withAlpha(d.color, 1), x: Math.min(g.x1, p.w) + 6, y, align: 'left', priority: 34, movable: false });
      }
    }
    p.hit({ key: d.key, t: 'rect', x0: g.bx, y0: yTop, x1: g.bx + g.maxW, y1: yBot });
    if (d.label) p.nameChip(d, d.label, { x: g.bx + 4, y: yTop - 14, align: 'left' }, 58);
  },
};
