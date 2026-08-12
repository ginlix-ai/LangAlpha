import { describe, it, expect } from 'vitest';
import { SPX_CLOSES } from '@/pages/Login/spxSeries';

describe('SPX_CLOSES', () => {
  it('carries the documented 501-session snapshot', () => {
    expect(SPX_CLOSES).toHaveLength(501);
  });

  it('is entirely finite, positive numbers (guards against a corrupted paste / NaN)', () => {
    for (const v of SPX_CLOSES) {
      expect(typeof v).toBe('number');
      expect(Number.isFinite(v)).toBe(true);
      expect(v).toBeGreaterThan(0);
    }
  });

  it('stays within a plausible index range for the decorative tape', () => {
    // A stray 0 or an extra digit would break the chart's autoscaling.
    expect(Math.min(...SPX_CLOSES)).toBeGreaterThan(1000);
    expect(Math.max(...SPX_CLOSES)).toBeLessThan(20000);
  });
});
