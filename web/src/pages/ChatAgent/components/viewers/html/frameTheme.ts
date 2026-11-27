/**
 * The theme a widget document wears: the variables it declares on `:root`, the
 * app's light palette for places with no app stylesheet to read, and the one
 * serializer every theme push goes through. Pure apart from `readThemePalette`,
 * which reads the live document only when called, so the headless render page
 * can import this with no app DOM.
 */

/**
 * The theme variables every widget document declares on `:root`, as hex
 * (`#rrggbb`, or `#rrggbbaa` when translucent). Widgets style with them
 * (`var(--color-text-primary)`), and read them to paint canvases, so this list
 * is part of what a widget may rely on: removing a name breaks widgets already
 * written against it. The frame's own defaults read them too: the base
 * stylesheet (frameStyles.ts), and the Chart.js theme (frameScripts/early.js),
 * which colours series from `--color-chart-1`…`8` in order.
 */
export const THEME_VARS = [
  '--color-bg-page',
  '--color-bg-card',
  '--color-bg-elevated',
  '--color-bg-input',
  '--color-bg-surface',
  '--color-bg-hover',
  '--color-bg-subtle',
  '--color-border-muted',
  '--color-border-default',
  '--color-border-elevated',
  '--color-border-subtle',
  '--color-text-primary',
  '--color-text-secondary',
  '--color-text-tertiary',
  '--color-text-quaternary',
  '--color-text-muted',
  '--color-accent-primary',
  '--color-accent-soft',
  '--color-profit',
  '--color-profit-soft',
  '--color-loss',
  '--color-loss-soft',
  '--color-warning',
  '--color-info',
  '--color-success',
  '--color-chart-1',
  '--color-chart-2',
  '--color-chart-3',
  '--color-chart-4',
  '--color-chart-5',
  '--color-chart-6',
  '--color-chart-7',
  '--color-chart-8',
] as const;

export type ThemeVar = (typeof THEME_VARS)[number];

export interface ThemePalette {
  /**
   * The host's used `color-scheme` (`light`, `dark`, `light dark`), mirrored
   * into `:root`: an embedded document whose scheme differs from its
   * embedder's loses transparent compositing, and the browser paints the
   * frame's canvas opaque white, a white sheet behind the widget in dark mode.
   */
  colorScheme?: string;
  vars: Partial<Record<ThemeVar, string>>;
}

/**
 * The app's light theme (`[data-theme="light"]` in styles/tokens.css, which no
 * card style overrides for these names), for a document built where the app's
 * stylesheet is not, or must not be read: the headless render, and the print
 * copies, which go on white paper whatever the app wears. Pinned to tokens.css
 * by __tests__/themePalette.test.ts.
 */
export const LIGHT_PALETTE = {
  colorScheme: 'light',
  vars: {
    '--color-bg-page': '#ffffff',
    '--color-bg-card': '#ffffff',
    '--color-bg-elevated': '#f6f6f6',
    '--color-bg-input': '#f6f6f6',
    '--color-bg-surface': '#f4f4f4',
    '--color-bg-hover': '#0000000a',
    '--color-bg-subtle': '#00000005',
    '--color-border-muted': '#1f1f1e14',
    '--color-border-default': '#e6e6e6',
    '--color-border-elevated': '#e6e6e6',
    '--color-border-subtle': '#efefef',
    '--color-text-primary': '#1f1f1e',
    '--color-text-secondary': '#5f5e5a',
    '--color-text-tertiary': '#716f6b',
    '--color-text-quaternary': '#767571',
    '--color-text-muted': '#6b6a66',
    '--color-accent-primary': '#d17d33',
    '--color-accent-soft': '#d07d331a',
    '--color-profit': '#1a7f37',
    '--color-profit-soft': '#1a7f3714',
    '--color-loss': '#cf222e',
    '--color-loss-soft': '#cf222e12',
    '--color-warning': '#b45309',
    '--color-info': '#4477d4',
    '--color-success': '#1a7f37',
    '--color-chart-1': '#2a78d6',
    '--color-chart-2': '#eb6834',
    '--color-chart-3': '#1baf7a',
    '--color-chart-4': '#eda100',
    '--color-chart-5': '#e87ba4',
    '--color-chart-6': '#008300',
    '--color-chart-7': '#6250d6',
    '--color-chart-8': '#e34948',
  } satisfies Record<ThemeVar, string>,
} satisfies ThemePalette;

/**
 * A palette as the declarations a widget's `:root` block holds, in THEME_VARS
 * order: the template writes it there, and a `widget:themeUpdate` push carries
 * it, so a pushed theme and a built one read the same.
 */
export function paletteCss({ colorScheme, vars }: ThemePalette): string {
  const decls = THEME_VARS.filter((v) => vars[v]).map((v) => `${v}: ${vars[v]};`).join('\n  ');
  return colorScheme ? `color-scheme: ${colorScheme};\n  ${decls}` : decls;
}

/**
 * `rgba(r, g, b, a)` as the canvas serializes a translucent color, as
 * `#rrggbbaa`; null for any other form.
 */
export function rgbaToHex(serialized: string): string | null {
  const m = /^rgba\((\d+), (\d+), (\d+), ([\d.]+)\)$/.exec(serialized);
  if (!m) return null;
  const byte = (n: number) => Math.min(255, Math.max(0, n)).toString(16).padStart(2, '0');
  return `#${byte(+m[1])}${byte(+m[2])}${byte(+m[3])}${byte(Math.round(+m[4] * 255))}`;
}

/**
 * A theme color as hex: `#rrggbb`, or `#rrggbbaa` when translucent. The tokens
 * are written in whatever syntax suits the stylesheet (`hsl(var(--primary))`
 * reads back as `hsl(28 63% 51%)`), but a document reads them to paint a
 * canvas, and the report skills present them as hex. Code that derives a
 * shade by parsing hex gets NaN from any other form, and the canvas paints a
 * color it rejects black. The canvas's own parser does the conversion; with
 * none (jsdom), or for a value that is not an sRGB color, it passes through.
 */
function hexColor(value: string, painter: CanvasRenderingContext2D | null): string {
  if (!painter) return value;
  painter.fillStyle = '#000000';
  painter.fillStyle = value;
  const read = painter.fillStyle;
  painter.fillStyle = '#ffffff';
  painter.fillStyle = value;
  // Not a color: each assignment was refused and left its sentinel.
  if (typeof read !== 'string' || read !== painter.fillStyle) return value;
  return read.startsWith('#') ? read : (rgbaToHex(read) ?? value);
}

/** The theme `el` wears now, as hex; a variable it does not declare is left out. */
export function readThemePalette(el: Element = document.documentElement): ThemePalette {
  const style = getComputedStyle(el);
  const painter = document.createElement('canvas').getContext('2d');
  const vars: ThemePalette['vars'] = {};
  for (const v of THEME_VARS) {
    const val = style.getPropertyValue(v).trim();
    if (val) vars[v] = hexColor(val, painter);
  }
  const scheme = style.colorScheme;
  return scheme && scheme !== 'normal' ? { colorScheme: scheme, vars } : { vars };
}
