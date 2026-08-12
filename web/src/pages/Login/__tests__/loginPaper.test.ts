import { describe, it, expect } from 'vitest';
import { hash, derivePalette } from '@/pages/Login/loginPaper';

describe('hash', () => {
  it('is deterministic for the same inputs', () => {
    expect(hash(12.5, -3)).toBe(hash(12.5, -3));
    expect(hash(0, 0)).toBe(hash(0, 0));
  });

  it('returns a fraction in [0, 1)', () => {
    for (const [a, b] of [
      [0, 0],
      [1, 2],
      [12.5, -3],
      [100, 200],
      [-7.3, 88.1],
    ] as const) {
      const v = hash(a, b);
      expect(v).toBeGreaterThanOrEqual(0);
      expect(v).toBeLessThan(1);
    }
  });

  it('is order-sensitive (hash(a,b) != hash(b,a))', () => {
    expect(hash(1, 2)).not.toBe(hash(2, 1));
  });
});

describe('derivePalette', () => {
  it('falls back to the night edition for a detached element (no login root)', () => {
    // A detached element has no `.login-page` ancestor and no parent, so every
    // getComputedStyle probe is skipped and the derivation is fully deterministic:
    // black page bg → luminance 0 → night edition, ember + ink use their fallbacks.
    const el = document.createElement('div');
    const p = derivePalette(el);

    expect(p.aBoost).toBe(1);
    expect(p.ember).toEqual([255, 168, 92]); // EMBER_FALLBACK
    expect(p.field).toEqual([10, 10, 10]); // black lifted 4% toward white
    expect(p.ink).toEqual([235, 235, 235]); // white ink (fallback) softened toward field
  });
});
