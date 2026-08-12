import { describe, expect, it, vi } from 'vitest';

import { SelectionPrimitive } from '../selectionPrimitive';

interface CtxStub {
  save: ReturnType<typeof vi.fn>;
  restore: ReturnType<typeof vi.fn>;
  fillRect: ReturnType<typeof vi.fn>;
  strokeRect: ReturnType<typeof vi.fn>;
  beginPath: ReturnType<typeof vi.fn>;
  moveTo: ReturnType<typeof vi.fn>;
  lineTo: ReturnType<typeof vi.fn>;
  stroke: ReturnType<typeof vi.fn>;
  setLineDash: ReturnType<typeof vi.fn>;
  fillStyle: string;
  strokeStyle: string;
  lineWidth: number;
}

function makeCtx(): CtxStub {
  return {
    save: vi.fn(),
    restore: vi.fn(),
    fillRect: vi.fn(),
    strokeRect: vi.fn(),
    beginPath: vi.fn(),
    moveTo: vi.fn(),
    lineTo: vi.fn(),
    stroke: vi.fn(),
    setLineDash: vi.fn(),
    fillStyle: '',
    strokeStyle: '',
    lineWidth: 1,
  };
}

function makeTarget(ctx: CtxStub, width = 800, height = 400) {
  return {
    useMediaCoordinateSpace(cb: (scope: { context: CtxStub; mediaSize: { width: number; height: number } }) => void) {
      cb({ context: ctx, mediaSize: { width, height } });
    },
  } as never;
}

function attach(prim: SelectionPrimitive, requestUpdate = vi.fn()) {
  const chart = { timeScale: () => ({ timeToCoordinate: (t: number) => t / 1000 }) };
  const series = { priceToCoordinate: (p: number) => 400 - p };
  prim.attached({ chart: chart as never, series: series as never, requestUpdate });
  return requestUpdate;
}

function draw(prim: SelectionPrimitive, ctx: CtxStub) {
  const renderer = prim.paneViews()[0].renderer();
  if (!renderer) throw new Error('expected a renderer');
  renderer.draw(makeTarget(ctx));
}

describe('SelectionPrimitive', () => {
  it('does nothing with no draft and no committed selection', () => {
    const prim = new SelectionPrimitive();
    attach(prim);
    const ctx = makeCtx();
    draw(prim, ctx);
    expect(ctx.fillRect).not.toHaveBeenCalled();
    expect(ctx.strokeRect).not.toHaveBeenCalled();
    expect(ctx.stroke).not.toHaveBeenCalled();
  });

  it('setDraft/setCommitted/setTheme request a redraw', () => {
    const prim = new SelectionPrimitive();
    const requestUpdate = attach(prim);
    prim.setDraft({ type: 'region', x1: 1, y1: 2, x2: 3, y2: 4 });
    prim.setCommitted([{ type: 'price_level', priceLow: 10, priceHigh: 10 }]);
    expect(requestUpdate).toHaveBeenCalledTimes(2);
    prim.setTheme('light');
    expect(requestUpdate).toHaveBeenCalledTimes(3);
    prim.setTheme('light'); // unchanged → no redraw
    expect(requestUpdate).toHaveBeenCalledTimes(3);
  });

  it('draws a box for a region draft (pixels)', () => {
    const prim = new SelectionPrimitive();
    attach(prim);
    prim.setDraft({ type: 'region', x1: 100, y1: 50, x2: 200, y2: 150 });
    const ctx = makeCtx();
    draw(prim, ctx);
    expect(ctx.fillRect).toHaveBeenCalledWith(100, 50, 100, 100);
    expect(ctx.strokeRect).toHaveBeenCalledWith(100, 50, 100, 100);
  });

  it('draws a full-width line for a price-level draft', () => {
    const prim = new SelectionPrimitive();
    attach(prim);
    prim.setDraft({ type: 'price_level', x1: 0, y1: 0, x2: 800, y2: 120 });
    const ctx = makeCtx();
    draw(prim, ctx);
    expect(ctx.moveTo).toHaveBeenCalledWith(0, 120);
    expect(ctx.lineTo).toHaveBeenCalledWith(800, 120);
    expect(ctx.stroke).toHaveBeenCalled();
    expect(ctx.fillRect).not.toHaveBeenCalled();
  });

  it('converts a committed region via time/price coordinates', () => {
    const prim = new SelectionPrimitive();
    attach(prim);
    // timeToCoordinate(t) = t/1000 → 1000→1, 5000→5; priceToCoordinate(p) = 400 - p
    prim.setCommitted([{ type: 'region', time1: 1000, time2: 5000, priceLow: 100, priceHigh: 300 }]);
    const ctx = makeCtx();
    draw(prim, ctx);
    // left=1, right=5, top=priceToCoordinate(300)=100, bottom=priceToCoordinate(100)=300
    expect(ctx.fillRect).toHaveBeenCalledWith(1, 100, 4, 200);
  });

  it('draws every committed selection in the list', () => {
    const prim = new SelectionPrimitive();
    attach(prim);
    prim.setCommitted([
      { type: 'region', time1: 1000, time2: 5000, priceLow: 100, priceHigh: 300 },
      { type: 'region', time1: 2000, time2: 3000, priceLow: 150, priceHigh: 250, active: true },
    ]);
    const ctx = makeCtx();
    draw(prim, ctx);
    expect(ctx.fillRect).toHaveBeenCalledTimes(2);
    expect(ctx.fillRect).toHaveBeenNthCalledWith(1, 1, 100, 4, 200);
    expect(ctx.fillRect).toHaveBeenNthCalledWith(2, 2, 150, 1, 100);
  });

  it('draws committed selections and the live draft together (draft on top)', () => {
    const prim = new SelectionPrimitive();
    attach(prim);
    prim.setCommitted([{ type: 'region', time1: 1000, time2: 5000, priceLow: 100, priceHigh: 300 }]);
    prim.setDraft({ type: 'region', x1: 10, y1: 10, x2: 20, y2: 20 });
    const ctx = makeCtx();
    draw(prim, ctx);
    expect(ctx.fillRect).toHaveBeenCalledTimes(2);
    expect(ctx.fillRect).toHaveBeenNthCalledWith(1, 1, 100, 4, 200); // committed
    expect(ctx.fillRect).toHaveBeenNthCalledWith(2, 10, 10, 10, 10); // draft last
  });
});
