import { render } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import type { ChartDataPoint } from '@/types/market';
import { AnnotationPreviewChart } from '../AnnotationPreviewChart';

// Five ascending daily bars (time is unix SECONDS, matching ChartDataPoint).
const DAY = 86_400;
const T0 = 1_700_000_000;
const BARS: ChartDataPoint[] = Array.from({ length: 5 }, (_, i) => ({
  time: T0 + i * DAY,
  open: 100 + i,
  high: 104 + i,
  low: 98 + i,
  close: 101 + i,
  volume: 1_000 + i,
}));

describe('AnnotationPreviewChart', () => {
  it('renders the price area + line from the bars', () => {
    const { container } = render(
      <AnnotationPreviewChart bars={BARS} trendColor="var(--color-profit)" />,
    );
    expect(container.querySelector('svg')).toBeInTheDocument();
    // The price series: a filled area path + the close polyline.
    expect(container.querySelector('path')).toBeInTheDocument();
    expect(container.querySelector('polyline')).toBeInTheDocument();
  });

  it('pulses a current-price dot when showLastPrice is set', () => {
    const { container } = render(
      <AnnotationPreviewChart bars={BARS} trendColor="var(--color-profit)" showLastPrice />,
    );
    expect(container.querySelector('.animate-ping')).toBeInTheDocument();
  });

  it('renders nothing without enough bars to draw a line', () => {
    const { container } = render(
      <AnnotationPreviewChart bars={BARS.slice(0, 1)} trendColor="var(--color-profit)" />,
    );
    expect(container.querySelector('svg')).not.toBeInTheDocument();
  });
});
