import { describe, it, expect, afterEach, vi } from 'vitest';

import { paletteCss, readThemePalette, rgbaToHex } from '../frameTheme';

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

describe('readThemePalette', () => {
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

    const { vars } = readThemePalette();

    expect(vars['--color-accent-primary']).toBe('#d17d33');
    expect(vars['--color-text-primary']).toBe('#1f1f1e');
    expect(vars['--color-border-muted']).toBe('#1f1f1e14');
    // A value the canvas refuses passes through as written.
    expect(vars['--color-bg-card']).toBe('not-a-color');
  });

  it('passes values through where there is no canvas', () => {
    root.style.setProperty('--color-accent-primary', 'hsl(28 63% 51%)');
    expect(readThemePalette().vars['--color-accent-primary']).toBe('hsl(28 63% 51%)');
  });

  it('leaves out what the element does not declare, and a normal color-scheme', () => {
    root.style.setProperty('--color-text-primary', '#1F1F1E');
    root.style.setProperty('color-scheme', 'normal');
    expect(readThemePalette()).toEqual({ vars: { '--color-text-primary': '#1F1F1E' } });
    root.style.setProperty('color-scheme', 'light dark');
    expect(readThemePalette().colorScheme).toBe('light dark');
    root.style.removeProperty('color-scheme');
  });
});

describe('paletteCss', () => {
  it('writes the declarations in THEME_VARS order, the scheme first', () => {
    expect(paletteCss({
      colorScheme: 'dark',
      vars: { '--color-text-primary': '#eeeeee', '--color-bg-page': '#000000' },
    })).toBe('color-scheme: dark;\n  --color-bg-page: #000000;\n  --color-text-primary: #eeeeee;');
    expect(paletteCss({ vars: {} })).toBe('');
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
