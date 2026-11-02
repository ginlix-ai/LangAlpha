import { describe, it, expect, afterEach, vi } from 'vitest';

import { resolveThemeVars, rgbaToHex } from '../buildHtmlSrcDoc';

// A report reads these tokens back to paint its canvas charts, and the report
// skills present them as hex, so a report that makes a translucent shade by
// parsing hex got NaN from `hsl(28 63% 51%)` and the canvas painted its bars
// black. The tokens now leave the app as hex.

// What Chromium's canvas makes of each value: sRGB colors serialized as hex,
// translucent ones as rgba(), anything else refused.
const SERIALIZED: Record<string, string> = {
  'hsl(28 63% 51%)': '#d17d33',
  '#1F1F1E': '#1f1f1e',
  'rgba(31, 31, 30, 0.08)': 'rgba(31, 31, 30, 0.08)',
};

function fakePainter() {
  let fill = '#000000';
  return {
    get fillStyle() { return fill; },
    set fillStyle(value: string) { if (value in SERIALIZED) fill = SERIALIZED[value]; else if (/^#[0-9a-f]{6}$/.test(value)) fill = value; },
  };
}

describe('resolveThemeVars', () => {
  const root = document.documentElement;

  afterEach(() => {
    vi.restoreAllMocks();
    for (const name of ['--color-accent-primary', '--color-text-primary', '--color-border-muted', '--color-bg-card']) {
      root.style.removeProperty(name);
    }
  });

  it('hands every color token over as hex', () => {
    vi.spyOn(HTMLCanvasElement.prototype, 'getContext').mockImplementation(
      () => fakePainter() as unknown as CanvasRenderingContext2D,
    );
    root.style.setProperty('--color-accent-primary', 'hsl(28 63% 51%)');
    root.style.setProperty('--color-text-primary', '#1F1F1E');
    root.style.setProperty('--color-border-muted', 'rgba(31, 31, 30, 0.08)');
    root.style.setProperty('--color-bg-card', 'not-a-color');

    const css = resolveThemeVars();

    expect(css).toContain('--color-accent-primary: #d17d33;');
    expect(css).toContain('--color-text-primary: #1f1f1e;');
    expect(css).toContain('--color-border-muted: #1f1f1e14;');
    // A value the canvas refuses passes through as written.
    expect(css).toContain('--color-bg-card: not-a-color;');
  });

  it('passes values through where there is no canvas', () => {
    root.style.setProperty('--color-accent-primary', 'hsl(28 63% 51%)');
    expect(resolveThemeVars()).toContain('--color-accent-primary: hsl(28 63% 51%);');
  });
});

describe('rgbaToHex', () => {
  it('writes the alpha as a fourth byte', () => {
    expect(rgbaToHex('rgba(233, 149, 74, 0.14)')).toBe('#e9954a24');
    expect(rgbaToHex('rgba(0, 0, 0, 0)')).toBe('#00000000');
  });

  it('answers null for any other form', () => {
    expect(rgbaToHex('#d17d33')).toBeNull();
    expect(rgbaToHex('oklch(70% 0.1 50)')).toBeNull();
  });
});
