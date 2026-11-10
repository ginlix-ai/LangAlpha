import { describe, expect, it } from 'vitest';

import { defaultBarsView } from '@/lib/bars/chartDataLoaders';
import type { LayerDrawing } from '../../types/chartLayer';
import { makeLayer } from '../../stores/__tests__/layerFixture';
import { DEFAULT_POSITION_BARS } from '../chartLayer/kinds/position';
import { layerTimeExtent } from '../chartLayer/layerTimes';
import { FRAME_FUTURE_GUTTER, FRAME_PAD, layerFrame } from '../layerFrame';
import { TARGET_BAR_SPACING } from '../chartConstants';

const DAY = 86400;

/** `count` weekday daily bars ending on `last` (chart seconds, UTC midnight). */
function weekdayBars(count: number, last = Date.UTC(2026, 9, 30) / 1000): number[] {
  const out: number[] = [];
  for (let t = last; out.length < count; t -= DAY) {
    const d = new Date(t * 1000).getUTCDay();
    if (d !== 0 && d !== 6) out.unshift(t);
  }
  return out;
}

const day = (secs: number) => new Date(secs * 1000).toISOString().slice(0, 10);

const segment = (a: string, b: string): LayerDrawing => ({
  type: 'segment', p1: { time: a, price: 100 }, p2: { time: b, price: 110 }, extend: 'none',
} as LayerDrawing);

describe('layerTimeExtent', () => {
  it('spans drawings, markers and regimes, not series, levels or alerts', () => {
    const layer = makeLayer({
      // makeLayer's series and level stay in; they must not count.
      drawings: [segment('2026-09-01', '2026-09-10')],
      markers: [{ time: '2026-08-20', position: 'aboveBar', shape: 'arrowDown' }],
      regimes: [{ from: '2026-09-05', to: '2026-09-25', color: '#888' }],
    });
    const ext = layerTimeExtent(layer)!;
    expect(day(ext.min)).toBe('2026-08-20');
    expect(day(ext.max)).toBe('2026-09-25');
  });

  it('reaches a forecast target, the last bar of an analog, a position until and a vline in the future, not an analog source', () => {
    const layer = makeLayer({
      series: [],
      drawings: [
        { type: 'forecast', origin: { time: '2026-10-30', price: 100 }, target: { time: '2026-11-10', price: 120 } },
        { type: 'analog', origin: { time: '2026-10-30', price: 100 }, source_start: '2026-05-01', source_end: '2026-05-05',
          bars: [{ time: '2026-11-02', open: 100, high: 102, low: 99, close: 101 }, { time: '2026-11-20', open: 101, high: 103, low: 100, close: 102 }] },
        { type: 'position', side: 'long', entry: { time: '2026-10-01', price: 100 }, stop: 95, targets: [110], until: '2026-12-01' },
        { type: 'vline', time: '2026-12-15' },
      ] as LayerDrawing[],
    });
    const ext = layerTimeExtent(layer)!;
    expect(day(ext.min)).toBe('2026-10-01');
    expect(day(ext.max)).toBe('2026-12-15');
  });

  it('is null for a layer of series and levels only', () => {
    expect(layerTimeExtent(makeLayer())).toBeNull();
  });
});

describe('layerFrame', () => {
  const bars = weekdayBars(400);
  const n = bars.length;
  const at = (k: number) => day(bars[k]);

  it('pads the extent by FRAME_PAD each side', () => {
    const layer = makeLayer({ drawings: [segment(at(100), at(300))] });
    const frame = layerFrame(layer, bars, 800, '1day')!;
    expect(frame.from).toBeCloseTo(100 - 200 * FRAME_PAD);
    expect(frame.to).toBeCloseTo(300 + 200 * FRAME_PAD);
  });

  it('is never tighter than the default view bar count, centered on the layer', () => {
    const layer = makeLayer({ drawings: [segment(at(200), at(205))] });
    const width = 1400;
    const frame = layerFrame(layer, bars, width, '1day')!;
    const minSpan = width / TARGET_BAR_SPACING['1day'];
    expect(frame.to - frame.from).toBeCloseTo(minSpan);
    expect((frame.from + frame.to) / 2).toBeCloseTo(202.5);
  });

  it('shifts a minimum-width frame into the history at the right edge', () => {
    const layer = makeLayer({ drawings: [segment(at(n - 4), at(n - 1))] });
    const frame = layerFrame(layer, bars, 1400, '1day')!;
    const minSpan = 1400 / TARGET_BAR_SPACING['1day'];
    expect(frame.to).toBeCloseTo(n - 1 + 1 + FRAME_FUTURE_GUTTER);
    expect(frame.to - frame.from).toBeCloseTo(minSpan);
  });

  it('frames a position with no until through the bars its box is drawn over', () => {
    const position = (until?: string) => makeLayer({
      series: [],
      drawings: [{ type: 'position', side: 'long', entry: { time: at(n - 1), price: 100 }, stop: 95, targets: [110], ...(until ? { until } : {}) } as LayerDrawing],
    });
    const open = layerFrame(position(), bars, 800, '1day')!;
    expect(open.to).toBeGreaterThanOrEqual(n - 1 + DEFAULT_POSITION_BARS);
    // An explicit `until` is framed by its time, as before.
    const ended = layerFrame(position(at(n - 1)), bars, 800, '1day')!;
    expect(ended.to).toBeLessThan(n - 1 + DEFAULT_POSITION_BARS);
  });

  it('leaves room past the last bar for the text of a marker on it', () => {
    const marker = (text?: string) => makeLayer({
      series: [], drawings: [],
      markers: [{ time: at(n - 1), position: 'belowBar', shape: 'arrowUp', ...(text ? { text } : {}) }],
    });
    const plain = layerFrame(marker(), bars, 800, '1day')!;
    const named = layerFrame(marker('60-day low on 3.4x volume'), bars, 800, '1day')!;
    expect(plain.to).toBeCloseTo(n - 1 + 1 + FRAME_FUTURE_GUTTER);
    // Half the text hangs right of its bar: the frame ends far enough past it to show that half.
    const barPx = 800 / (named.to - named.from);
    expect((named.to - (n - 1)) * barPx).toBeGreaterThan(('60-day low on 3.4x volume'.length * 6.5) / 2);
    // A marker far from the edge needs none.
    const inside = makeLayer({ series: [], drawings: [], markers: [{ time: at(n - 40), position: 'belowBar', shape: 'arrowUp', text: '60-day low on 3.4x volume' }] });
    expect(layerFrame(inside, bars, 800, '1day')!.to).toBeLessThan(n - 1 + FRAME_FUTURE_GUTTER + 1);
  });

  it('leaves a short history only the room a marker\'s text on the last bar needs', () => {
    // Ten bars are framed whole, so each is drawn far wider than the default view's.
    const short = weekdayBars(10);
    const last = short.length - 1;
    const text = '60-day low on 3.4x volume';
    const layer = makeLayer({
      series: [], drawings: [],
      markers: [{ time: day(short[last]), position: 'belowBar', shape: 'arrowUp', text }],
    });
    const frame = layerFrame(layer, short, 800, '1day')!;
    const barPx = 800 / (frame.to - frame.from);
    const overhangBars = (text.length * 6.5) / 2 / barPx;
    expect(frame.to - last).toBeGreaterThan(overhangBars);
    expect(frame.to - last).toBeLessThanOrEqual(Math.max(FRAME_FUTURE_GUTTER, Math.ceil(overhangBars) + 1));
  });

  it('frames forward drawings on the future slots the painter uses', () => {
    // 10 weekdays past the last bar (Fri 2026-10-30): Fri 2026-11-13 is slot 10.
    const layer = makeLayer({
      drawings: [{ type: 'cone', origin: { time: at(n - 1), price: 100 },
        bands: [{ label: '1σ', upper: [{ time: '2026-11-13', price: 110 }], lower: [{ time: '2026-11-13', price: 90 }] }] }] as LayerDrawing[],
    });
    const frame = layerFrame(layer, bars, 400, '1day')!;
    const i1 = n - 1 + 10;
    expect(frame.to).toBeCloseTo(i1 + 1 + FRAME_FUTURE_GUTTER);
  });

  it('frames a series projected past the last bar out to its end, not over its history', () => {
    // A projection to Fri 2026-11-13, slot 10 past the last bar; its history starts far back.
    const layer = makeLayer({
      drawings: [],
      series: [{ id: 'proj', title: 'Projection', type: 'line', pane: 'price',
        data: [{ time: day(bars[0]), value: 90 }, { time: at(n - 1), value: 100 }, { time: '2026-11-13', value: 110 }] }],
    });
    const frame = layerFrame(layer, bars, 400, '1day')!;
    expect(frame.to).toBeCloseTo(n - 1 + 10 + 1 + FRAME_FUTURE_GUTTER);
    expect(frame.from).toBeGreaterThan(n / 2);
  });

  it('is never wider than the history plus the layer', () => {
    const short = weekdayBars(30);
    const layer = makeLayer({ drawings: [segment(day(short[10]), day(short[12]))] });
    const frame = layerFrame(layer, short, 1400, '1day')!;
    expect(frame.from).toBeCloseTo(-0.5);
    expect(frame.to).toBeCloseTo(short.length - 1 + FRAME_FUTURE_GUTTER);
  });

  it('opens a layer with no time extent on the default view', () => {
    expect(layerFrame(makeLayer(), bars, 1200, '1day', { defaultView: 'fill' })).toEqual(
      defaultBarsView({ defaultView: 'fill', chartWidth: 1200, barSpacing: TARGET_BAR_SPACING['1day'], dataLen: n }),
    );
  });

  it('frames a replay on its origin and bars, with the context any drawing gets, not on its source stretch', () => {
    // Replaying a stretch from over a year back: the frame stays on today.
    const layer = makeLayer({
      series: [],
      drawings: [{ type: 'analog', origin: { time: at(n - 1), price: 100 }, source_start: at(20), source_end: at(30),
        bars: [{ time: '2026-11-02', open: 100, high: 102, low: 99, close: 101 }, { time: '2026-11-13', open: 101, high: 103, low: 100, close: 102 }] }] as LayerDrawing[],
    });
    const width = 1400;
    const frame = layerFrame(layer, bars, width, '1day')!;
    const lastSlot = n - 1 + 10; // Fri 2026-11-13 is the tenth weekday past Fri 2026-10-30
    expect(frame.to).toBeCloseTo(lastSlot + 1 + FRAME_FUTURE_GUTTER);
    expect(frame.to - frame.from).toBeCloseTo(width / TARGET_BAR_SPACING['1day']);
    expect(frame.from).toBeGreaterThan(30);
  });

  it('frames the union of several layers', () => {
    const a = makeLayer({ id: 'a', drawings: [segment(at(100), at(150))] });
    const b = makeLayer({ id: 'b', drawings: [segment(at(250), at(300))] });
    const frame = layerFrame([a, b], bars, 800, '1day')!;
    expect(frame.from).toBeCloseTo(100 - 200 * FRAME_PAD);
    expect(frame.to).toBeCloseTo(300 + 200 * FRAME_PAD);
  });

  it('is null with no bars', () => {
    expect(layerFrame(makeLayer({ drawings: [segment('2026-01-01', '2026-02-01')] }), [], 800, '1day')).toBeNull();
  });
});
