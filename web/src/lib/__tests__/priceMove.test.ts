// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { changeDirection, signedPercent } from '../priceMove';

describe('signedPercent', () => {
  it('signs with a hyphen-minus', () => {
    expect(signedPercent(-0.5)).toBe('-0.50%');
  });

  it('prints a move too small to count unsigned', () => {
    expect(changeDirection(0.004)).toBe('flat');
    expect(signedPercent(0.004)).toBe('0.00%');
  });
});
