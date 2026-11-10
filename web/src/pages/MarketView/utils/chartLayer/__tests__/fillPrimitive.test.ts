// @vitest-environment node
import { describe, expect, it } from 'vitest';
import type { IChartApiBase, Time } from 'lightweight-charts';
import type { CanvasRenderingTarget2D } from 'fancy-canvas';

import { LayerFillPrimitive } from '../fillPrimitive';

function drawnPath(prim: LayerFillPrimitive): { x: number; y: number }[] {
  const pts: { x: number; y: number }[] = [];
  const ctx = {
    beginPath() {},
    closePath() {},
    fill() {},
    moveTo: (x: number, y: number) => pts.push({ x, y }),
    lineTo: (x: number, y: number) => pts.push({ x, y }),
  } as unknown as CanvasRenderingContext2D;
  const target = {
    useMediaCoordinateSpace: (fn: (s: { context: CanvasRenderingContext2D; mediaSize: { width: number } }) => void) =>
      fn({ context: ctx, mediaSize: { width: 400 } }),
  } as unknown as CanvasRenderingTarget2D;
  prim.paneViews()[0].renderer()!.draw(target, {} as never);
  return pts;
}

describe('LayerFillPrimitive', () => {
  it('places each boundary on its own series scale', () => {
    // In the price pane a histogram rides a scale of its own; the fill follows it.
    const chart = { timeScale: () => ({ timeToCoordinate: (t: number) => t }) } as unknown as IChartApiBase<Time>;
    const prim = new LayerFillPrimitive();
    prim.attached({ chart, series: { priceToCoordinate: (p) => 1000 - p }, requestUpdate: () => {} });
    prim.setFill([{ time: 10, a: 100, b: 5 }, { time: 20, a: 110, b: 6 }], 'rgba(0,0,0,0.1)', {
      priceToCoordinate: (p) => 500 - 10 * p,
    });
    expect(drawnPath(prim)).toEqual([
      { x: 10, y: 900 },
      { x: 20, y: 890 },
      { x: 20, y: 440 },
      { x: 10, y: 450 },
    ]);
  });
});
