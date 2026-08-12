/**
 * loginPaper - the shared paper the split-login canvases print on. Both
 * MarketScanlines (the market tape) and EdgeGrain (the background dot field)
 * derive their palette from the page at runtime, so the art follows the app
 * theme in both light and dark with no baked-in color. This module folds the
 * identical derivation both canvases used to carry inline; it is self-contained
 * (no React) so either effect can import it.
 */

/** Deterministic per-cell hash so textures stay stable across rebuilds/scroll. */
export function hash(a: number, b: number) {
  const s = Math.sin(a * 127.1 + b * 311.7) * 43758.5453;
  return s - Math.floor(s);
}

export interface Palette {
  ink: [number, number, number]; // print ink, the page text softened toward the field
  field: [number, number, number]; // pillow tone one shade off the page background
  ember: [number, number, number]; // the accent, read from CSS --login-ember
  aBoost: number; // dark ink at low alpha carries less weight; light mode compensates
}

// Ember fallback if --login-ember can't be read: the dark-theme value, which
// is the canvases' historical hardcoded ember and matches LoginPage.css.
const EMBER_FALLBACK: [number, number, number] = [255, 168, 92];

// The login root the palette is read against: the .login-page ancestor, or the
// element's own parent when it stands alone.
const paneRoot = (el: HTMLElement): HTMLElement | null =>
  (el.closest('.login-page') as HTMLElement | null) ?? el.parentElement;

// The page background behind the auth pane, whatever the app theme. Climbs from
// the login root until an opaque color answers; falls back to black (the dark
// branch derives the same field from it).
const paneBg = (el: HTMLElement): string => {
  let node: HTMLElement | null = paneRoot(el);
  while (node) {
    const bg = getComputedStyle(node).backgroundColor;
    if (bg && bg !== 'transparent' && bg !== 'rgba(0, 0, 0, 0)') return bg;
    node = node.parentElement;
  }
  return 'rgb(0, 0, 0)';
};

// The page's text ink, read off the login root. Falls back to white.
const paneInk = (el: HTMLElement): [number, number, number] => {
  const root = paneRoot(el);
  const c = root ? getComputedStyle(root).color : '';
  const m = /rgba?\((\d+),\s*(\d+),\s*(\d+)/.exec(c);
  return m ? [+m[1], +m[2], +m[3]] : [255, 255, 255];
};

// The ember accent, read from CSS (--login-ember on .login-page) so it lives in
// one place. Accepts "rgb(r, g, b)" or "#rrggbb"; falls back to the dark value
// if the variable is missing or unparseable.
const readEmber = (el: HTMLElement): [number, number, number] => {
  const root = paneRoot(el);
  const raw = root ? getComputedStyle(root).getPropertyValue('--login-ember').trim() : '';
  const rgb = /rgba?\((\d+),\s*(\d+),\s*(\d+)/.exec(raw);
  if (rgb) return [+rgb[1], +rgb[2], +rgb[3]];
  const hex = /^#([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i.exec(raw);
  if (hex) return [parseInt(hex[1], 16), parseInt(hex[2], 16), parseInt(hex[3], 16)];
  return [...EMBER_FALLBACK];
};

/**
 * Derive the paper palette from the page at `el`. Luminance of the page
 * background picks the edition; the ember is read from --login-ember either way
 * so the canvases and the CSS never drift.
 */
export function derivePalette(el: HTMLElement): Palette {
  const m = /rgba?\((\d+),\s*(\d+),\s*(\d+)/.exec(paneBg(el));
  const [r, g, b] = m ? [+m[1], +m[2], +m[3]] : [0, 0, 0];
  const light = (0.2126 * r + 0.7152 * g + 0.0722 * b) / 255 > 0.5;
  const ember = readEmber(el);
  if (light) {
    // Print edition: near-black ink; the pillow tone sits one shade deeper than
    // the page, matching the top of the page's wash.
    return {
      ink: [30, 32, 37],
      field: [Math.round(r * 0.955), Math.round(g * 0.955), Math.round(b * 0.955)],
      ember,
      aBoost: 1.3,
    };
  }
  // Night edition: ink is the page's own text color softened a shade toward the
  // lifted pillow tone - derived, so the pane carries no temperature the tokens
  // don't.
  const lr = Math.round(r + (255 - r) * 0.04);
  const lg = Math.round(g + (255 - g) * 0.04);
  const lb = Math.round(b + (255 - b) * 0.04);
  const [tr, tg, tb] = paneInk(el);
  return {
    ink: [
      Math.round(tr * 0.92 + lr * 0.08),
      Math.round(tg * 0.92 + lg * 0.08),
      Math.round(tb * 0.92 + lb * 0.08),
    ],
    field: [lr, lg, lb],
    ember,
    aBoost: 1,
  };
}
