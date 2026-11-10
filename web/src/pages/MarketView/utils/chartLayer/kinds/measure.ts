import type { MeasureDrawing } from '../../../types/chartLayer';
import { withAlpha } from '../../canvasKit';
import { arrowHead } from '../canvasGeom';
import { fmtSigned, fmtSignedPct } from '../format';
import { DEFAULT_LAYER_TEXT, type LayerText } from '../layerText';
import type { Painter } from '../painter';
import { extentOf, type SceneBase } from '../scene';
import { DAY, layerTimeToSeconds, type IP } from '../timeGrid';
import type { DrawingKind } from './types';

interface MeasureStats {
  delta: number;
  pct: number;
  bars: number;
  days: number | null;
}

export interface MeasureScene extends SceneBase, MeasureStats {
  kind: 'measure';
  a: IP;
  b: IP;
  /** The agent's own color; absent, the theme's profit or loss by the move's sign, read at paint. */
  given: string | null;
}

/** Ruler stats between two points. `days` from wall-clock seconds when both times parse. */
export function measureStats(a: IP, b: IP, t1: number | null, t2: number | null): MeasureStats {
  const delta = b.p - a.p;
  return {
    delta,
    pct: a.p !== 0 ? delta / a.p : 0,
    bars: Math.round(b.i - a.i),
    days: t1 != null && t2 != null ? Math.round((t2 - t1) / DAY) : null,
  };
}

export function measureReadout(s: MeasureStats, text: LayerText = DEFAULT_LAYER_TEXT): string {
  const parts = [`${fmtSigned(s.delta)} (${fmtSignedPct(s.pct)})`, text.bars(Math.abs(s.bars))];
  if (s.days != null && s.days !== 0) parts.push(text.days(Math.abs(s.days)));
  return parts.join(' · ');
}

function toneOf(p: Painter, d: MeasureScene): string {
  return d.given ?? (d.delta >= 0 ? p.th.profit : p.th.loss);
}

export const measure: DrawingKind<MeasureDrawing, MeasureScene> = {
  nameKey: 'marketView.layerExplain.types.measure',
  times: (d) => [d.p1.time, d.p2.time],
  anchors: (d) => [d.p1, d.p2],
  alwaysExplained: true,
  resolve(d, { base, P }) {
    const a = P(d.p1);
    const b = P(d.p2);
    if (!a || !b) return null;
    const stats = measureStats(a, b, layerTimeToSeconds(d.p1.time), layerTimeToSeconds(d.p2.time));
    return {
      ...base,
      kind: 'measure',
      a,
      b,
      ...stats,
      given: d.color ?? null,
    };
  },
  extent: (s) => extentOf([s.a, s.b]),
  paintUnder(p, d) {
    const a = p.P(d.a);
    const b = p.P(d.b);
    if (a && b) p.box(a, b, withAlpha(toneOf(p, d), 0.1));
  },
  paintOver(p, d) {
    const a = p.P(d.a);
    const b = p.P(d.b);
    if (!a || !b) return;
    const cx = (a.x + b.x) / 2;
    const cy = (a.y + b.y) / 2;
    const tone = toneOf(p, d);
    const width = p.strokeWidth(d, 1);
    p.line({ x: cx, y: a.y }, { x: cx, y: b.y }, tone, width);
    p.line({ x: a.x, y: cy }, { x: b.x, y: cy }, tone, width);
    if (Math.abs(b.y - a.y) > 10) p.poly(arrowHead({ x: cx, y: a.y }, { x: cx, y: b.y }, 7), withAlpha(tone, 1));
    if (Math.abs(b.x - a.x) > 10) p.poly(arrowHead({ x: a.x, y: cy }, { x: b.x, y: cy }, 7), withAlpha(tone, 1));
    const x0 = Math.min(a.x, b.x);
    const y0 = Math.min(a.y, b.y);
    const y1 = Math.max(a.y, b.y);
    p.hit({ key: d.key, t: 'rect', x0, y0, x1: Math.max(a.x, b.x), y1 });
    // TradingView hangs the readout off the far side of the move.
    const up = d.delta >= 0;
    const stats = measureReadout(d, p.text);
    const text = d.label ? `${d.label} · ${stats}` : stats;
    p.nameChip(d, text, { x: cx, y: up ? y0 - 14 : y1 + 14, align: 'center' }, 66, { color: tone });
  },
};
