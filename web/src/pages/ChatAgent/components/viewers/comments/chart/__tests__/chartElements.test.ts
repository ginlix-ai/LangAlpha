import { describe, expect, it } from 'vitest';
import type { ChartElementMark } from '@/types/artifacts';
import type { LayerDrawing } from '@/pages/MarketView/types/chartLayer';
import { makeStored } from '@/pages/MarketView/stores/__tests__/layerFixture';
import { compileLayers } from '@/pages/MarketView/utils/chartLayer/compileLayers';
import { LAYER_FALLBACK } from '@/pages/MarketView/utils/chartLayer/layerTheme';
import { makeTimeGrid } from '@/pages/MarketView/utils/chartLayer/timeGrid';
import { findElement } from '../chartElements';
import { markName } from '../chartMarks';

const p = (time: string, price: number) => ({ time, price });
const trend: LayerDrawing = { type: 'segment', id: 'trend', p1: p('2026-10-01', 180), p2: p('2026-10-20', 200) };
const neck: LayerDrawing = { type: 'segment', label: 'Neckline', p1: p('2026-10-05', 190), p2: p('2026-10-25', 191) };
const top: LayerDrawing = { type: 'vline', time: '2026-10-10' };

/** Who each shape of `layers` is, by key, as the compile walk records them. */
function identities(layers: ReturnType<typeof makeStored>[]) {
  const grid = makeTimeGrid([], [], 86400);
  return compileLayers(layers, grid, { rsiPeriod: null, maPeriods: [], volume: false }, LAYER_FALLBACK.dark).elements.identities;
}

/** The mark a click on `key` saves, at a point that does not matter here. */
function markFor(layers: ReturnType<typeof makeStored>[], key: string): ChartElementMark {
  return { type: 'element', ...identities(layers).get(key)!, time: '2026-10-10', price: 190 };
}

describe('chart element marks', () => {
  const before = [makeStored({ drawings: [trend, neck, top] })];
  const after = [makeStored({ drawings: [top, neck, trend] })];

  it('names a shape by its layer, kind, id or name, and place', () => {
    const named = identities(before);
    expect(named.get('nvda-channel|drawing|1')).toEqual({
      layer: 'nvda-channel', layer_title: 'NVDA rising channel', element_kind: 'drawing',
      element_type: 'segment', element_name: 'Neckline', index: 1,
    });
    expect(named.get('nvda-channel|series|ema')).toMatchObject({ element_kind: 'series', element_id: 'ema', index: 0 });
    expect(named.get('nvda-channel|drawing|9')).toBeUndefined();
  });

  it('finds a shape again after the agent reorders its layer: by id, then name, then place', () => {
    expect(findElement(identities(after), markFor(before, 'nvda-channel|drawing|0'))).toBe('nvda-channel|drawing|2');
    expect(findElement(identities(after), markFor(before, 'nvda-channel|drawing|1'))).toBe('nvda-channel|drawing|1');
    // Nameless, it holds its place while a shape of its type is there.
    const other: LayerDrawing = { type: 'vline', time: '2026-10-12' };
    expect(findElement(identities([makeStored({ drawings: [neck, trend, other] })]), markFor(before, 'nvda-channel|drawing|2')))
      .toBe('nvda-channel|drawing|2');
  });

  it('loses a shape whose layer is gone, or whose place is past the end of its list', () => {
    const mark = markFor(before, 'nvda-channel|drawing|2');
    expect(findElement(identities([makeStored({ id: 'other' })]), mark)).toBeNull();
    expect(findElement(identities([makeStored({ drawings: [trend] })]), mark)).toBeNull();
  });

  it('never takes another shape for one that is gone', () => {
    // Its id gone, the shape is gone, though another sits where it was.
    expect(findElement(identities([makeStored({ drawings: [neck, top] })]), markFor(before, 'nvda-channel|drawing|0'))).toBeNull();
    // A segment now in a vline's place is not that vline.
    expect(findElement(identities(after), markFor(before, 'nvda-channel|drawing|2'))).toBeNull();
  });

  it('never takes another named shape for a named one that is gone', () => {
    const resistance: LayerDrawing = { type: 'segment', label: 'Resistance', p1: p('2026-10-05', 210), p2: p('2026-10-25', 211) };
    expect(findElement(identities([makeStored({ drawings: [trend, resistance, top] })]), markFor(before, 'nvda-channel|drawing|1'))).toBeNull();
  });

  it('keeps a long name to the limit a comment takes, and finds the shape by it', () => {
    const long: LayerDrawing = { type: 'label', at: p('2026-10-08', 195), text: 'Breakout '.repeat(20).trim() };
    const layers = [makeStored({ drawings: [top, long] })];
    const shape = identities(layers).get('nvda-channel|drawing|1')!;
    const mark: ChartElementMark = { type: 'element', ...shape, element_name: markName(shape.element_name!), time: '2026-10-08', price: 195 };
    expect(mark.element_name).toHaveLength(120);
    expect(findElement(identities([makeStored({ drawings: [long, top] })]), mark)).toBe('nvda-channel|drawing|0');
  });
});
