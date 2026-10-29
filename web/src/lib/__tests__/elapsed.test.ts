// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { formatClock } from '../elapsed';

describe('formatClock', () => {
  it('reads minutes and seconds under an hour, hours past it', () => {
    expect(formatClock(0)).toBe('0:00');
    expect(formatClock(5_999)).toBe('0:05');
    expect(formatClock(258_000)).toBe('4:18');
    expect(formatClock(3_600_000 + 62_000)).toBe('1:01:02');
  });

  it('reads anything that is not a positive time as zero', () => {
    expect(formatClock(-4_000)).toBe('0:00');
    expect(formatClock(Number.NaN)).toBe('0:00');
  });
});
