// @vitest-environment node
import { describe, expect, it } from 'vitest';

import type { PitchforkDrawing } from '../../../types/chartLayer';
import { pitchfork, type PitchforkScene } from '../kinds/pitchfork';
import { LAYER_FALLBACK } from '../layerTheme';
import { Painter } from '../painter';
import type { SceneBase } from '../scene';
import type { IP, TimeGrid } from '../timeGrid';
import type { XMapper } from '../xMapper';

type XY = { x: number; y: number };
type Seg = { a: XY; b: XY };

const ctx = new Proxy({}, { get: () => () => ({ width: 0 }) }) as unknown as CanvasRenderingContext2D;
const xm = { x: (i: number) => i * 10 } as unknown as XMapper;
const BASE = { key: 'k', color: '#2962ff' } as unknown as SceneBase;

/** The pitchfork through grid points (time as the index), resolved as the renderer resolves it. */
function sceneOf(p1: IP, p2: IP, p3: IP): PitchforkScene {
  const pt = ({ i, p }: IP) => ({ time: i, price: p });
  const d = { type: 'pitchfork', p1: pt(p1), p2: pt(p2), p3: pt(p3), extend: 'none' } as unknown as PitchforkDrawing;
  const P = (q: { time: unknown; price: number } | null | undefined) => (q ? { i: q.time as number, p: q.price } : null);
  return pitchfork.resolve(d, { grid: {} as TimeGrid, base: BASE, P })!;
}

/** The median, upper and lower rails' hit segments, in paint order. */
function railsOn(y: (price: number) => number, s: PitchforkScene): Seg[] {
  const p = new Painter(ctx, 400, 600, xm, { priceToCoordinate: y }, LAYER_FALLBACK.light, null);
  pitchfork.paintOver(p, s);
  return p.hits.filter((h) => h.t === 'seg') as unknown as Seg[];
}

const slope = (s: Seg) => (s.b.y - s.a.y) / (s.b.x - s.a.x);

describe('pitchfork geometry', () => {
  const p1 = { i: 0, p: 100 };
  const p2 = { i: 10, p: 200 };
  const p3 = { i: 20, p: 50 };

  it('on a log scale the median runs through the drawn midpoint of the p2–p3 connector', () => {
    const y = (price: number) => 600 - 200 * Math.log(price);
    const [med, up, lo] = railsOn(y, sceneOf(p1, p2, p3));
    const mid = { x: 150, y: (y(200) + y(50)) / 2 };
    expect(med.a).toEqual({ x: 0, y: y(100) });
    expect(med.a.y + slope(med) * (mid.x - med.a.x)).toBeCloseTo(mid.y, 9);
    expect(slope(up)).toBeCloseTo(slope(med), 9);
    expect(slope(lo)).toBeCloseTo(slope(med), 9);
    expect(up.a).toEqual({ x: 100, y: y(200) });
    expect(lo.a).toEqual({ x: 200, y: y(50) });
  });

  it('on a linear scale the median aims at the midpoint price of p2 and p3', () => {
    const y = (price: number) => 3 * (230 - price);
    const [med] = railsOn(y, sceneOf(p1, p2, p3));
    expect(med.a).toEqual({ x: 0, y: y(100) });
    expect(med.b).toEqual({ x: 150, y: y(125) });
  });
});
