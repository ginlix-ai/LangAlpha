import type { ForecastDrawing } from '../../../types/chartLayer';
import { withAlpha } from '../../canvasKit';
import { fmtMove } from '../format';
import type { PaintContext } from '../painter';
import { extentOf, type SceneBase } from '../scene';
import type { IP } from '../timeGrid';
import type { DrawingKind } from './types';

export interface ForecastScene extends SceneBase {
  kind: 'forecast';
  origin: IP;
  target: IP;
  /** The agent's own color; absent, the theme's profit or loss by direction, read at paint. */
  given: string | null;
  /** Whether the target is at or above the origin. */
  up: boolean;
  /** "Retest high · 30% · 243.37 +30.4%": the filled target box. */
  box: string;
}

export type ForecastVerdict = { state: 'pending' } | { state: 'missed' } | { state: 'unknown' } | { state: 'hit'; i: number };

export function forecastBox(label: string | undefined, prob: number | null | undefined, origin: number, target: number): string {
  const move = fmtMove(origin, target);
  const odds = prob != null && Number.isFinite(prob) ? `${Math.round(Math.max(0, Math.min(1, prob)) * 100)}%` : null;
  return [label, odds, move].filter(Boolean).join(' · ');
}

/**
 * Judged as TradingView judges a forecast: hit at the first bar after the
 * origin, up to the target time, whose high (low, for a fall) reaches the
 * target; missed once a bar exists at or past the target time without that;
 * pending until then. Unknown while the chart's bars start after the origin
 * (older history not loaded, or none to load): the target may have been
 * reached on a bar the chart does not hold.
 */
export function forecastVerdict(origin: IP, target: IP, bars: Pick<PaintContext, 'highs' | 'lows'>): ForecastVerdict {
  const last = Math.min(bars.highs.length, bars.lows.length) - 1;
  const up = target.p >= origin.p;
  const first = Math.floor(origin.i) + 1;
  if (first < 0 && last >= 0) return { state: 'unknown' };
  const end = Math.min(last, Math.floor(target.i));
  for (let k = first; k <= end; k++) {
    if (up ? bars.highs[k] >= target.p : bars.lows[k] <= target.p) return { state: 'hit', i: k };
  }
  return last >= Math.ceil(target.i) ? { state: 'missed' } : { state: 'pending' };
}

export const forecast: DrawingKind<ForecastDrawing, ForecastScene> = {
  nameKey: 'marketView.layerExplain.types.forecast',
  times: (d) => [d.origin.time, d.target.time],
  anchors: (d) => [d.origin, d.target],
  alwaysExplained: true,
  resolve(d, { base, P }) {
    const origin = P(d.origin);
    const target = P(d.target);
    if (!origin || !target) return null;
    return {
      ...base,
      kind: 'forecast',
      origin,
      target,
      given: d.color ?? null,
      up: target.p >= origin.p,
      box: forecastBox(d.label ?? undefined, d.probability, origin.p, target.p),
    };
  },
  extent: (s) => extentOf([s.origin, s.target]),
  paintOver(p, d) {
    const a = p.P(d.origin);
    const b = p.P(d.target);
    if (!a || !b) return;
    const color = d.given ?? (d.up ? p.th.profit : p.th.loss);
    if (!p.arrow(a, b, color, p.strokeWidth(d, 1.75), 9)) return;
    p.dot(a, 3, p.th.ink);
    p.seg(d.key, a, b, 6);

    // The box on the side the arrow arrives away from: above a rise, below a fall.
    const dir = b.y <= a.y ? -1 : 1;
    p.label({ el: d, text: d.box, style: 'pill', color: color, x: b.x, y: b.y + dir * 16, align: 'center', priority: 66, movable: true });

    const verdict = forecastVerdict(d.origin, d.target, p.bars);
    if (verdict.state === 'hit') {
      const at = p.P({ i: verdict.i, p: d.target.p });
      if (at) {
        p.line({ x: at.x, y: b.y }, b, withAlpha(color, 0.5), 1, [3, 3]);
        p.dot(at, 4, p.th.profit);
        p.label({ el: d, text: p.text.hit, style: 'pill', color: p.th.profit, x: at.x, y: at.y + dir * 16, align: 'center', priority: 67, movable: true });
      }
    } else if (verdict.state === 'missed') {
      p.label({ el: d, text: p.text.missed, style: 'pill', color: p.th.muted, x: b.x, y: b.y + dir * 36, align: 'center', priority: 65, movable: true });
    }
  },
};
