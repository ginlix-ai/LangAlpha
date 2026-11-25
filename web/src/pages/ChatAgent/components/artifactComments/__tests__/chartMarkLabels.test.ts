import { describe, expect, it } from 'vitest';
import type { TFunction } from 'i18next';
import type { ChartElementMark } from '@/types/artifacts';
import { chartTimeLabel, markHeading, markLabel } from '../chartMarkLabels';

const t = ((key: string, values?: Record<string, unknown>) => (values ? `${key} ${JSON.stringify(values)}` : key)) as unknown as TFunction;
const year = new Date().getUTCFullYear();

describe('chart mark labels', () => {
  it('reads a layer time as the wall clock it is, with the year only off this one', () => {
    expect(chartTimeLabel(`${year}-10-03`, 'en-US')).toBe('Oct 3');
    expect(chartTimeLabel('2024-10-03', 'en-US')).toBe('Oct 3, 2024');
    expect(chartTimeLabel(`${year}-10-03T09:30`, 'en-US')).toBe('Oct 3, 09:30');
  });

  it('names a region by its dates, one day by itself', () => {
    expect(markLabel(t, { type: 'region', start: '2024-10-03', end: '2024-10-09', low: 1, high: 2 }))
      .toBe('pageViewer:comments.chartRegion {"from":"Oct 3, 2024","to":"Oct 9, 2024"}');
    expect(markLabel(t, { type: 'region', start: '2024-10-03', end: '2024-10-03', low: 1, high: 2 })).toBe('Oct 3, 2024');
  });

  it('names a point by its price and date', () => {
    expect(markLabel(t, { type: 'point', time: '2024-10-03', price: 182.4512345 }))
      .toBe('pageViewer:comments.chartPoint {"price":"182.451","time":"Oct 3, 2024"}');
  });

  it('names a shape by its own name, and in a heading with its layer', () => {
    const shape: ChartElementMark = {
      type: 'element', layer: 'hs', layer_title: 'Head and shoulders', element_kind: 'drawing',
      element_type: 'segment', index: 0, time: '2024-10-03', price: 180,
    };
    expect(markLabel(t, shape)).toBe('segment');
    expect(markLabel(t, { ...shape, element_name: 'Neckline' })).toBe('Neckline');
    expect(markHeading(t, { ...shape, element_name: 'Neckline' }))
      .toBe('pageViewer:comments.chartElement {"layer":"Head and shoulders","name":"Neckline"}');
  });
});
