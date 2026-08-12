/**
 * App-level text scale, multiplied onto the browser's own font-size preference
 * at the root (tokens.css: `html { font-size: calc(100% * var(--app-font-scale)) }`),
 * so the OS/browser accessibility setting and this option compose.
 *
 * Not a context: the initial stamp has to happen pre-paint or every rem-sized
 * box reflows once, so index.html's inline script owns it. Nothing re-renders
 * on a change — the CSS variable does all the work — leaving a single writer
 * and a single reader (Settings › UserInfoTab).
 */

export const FONT_SCALES = [0.9, 0.95, 1, 1.05, 1.1] as const;
export type FontScale = (typeof FONT_SCALES)[number];

export function getFontScale(): FontScale {
  const stored = Number(localStorage.getItem('fontScale'));
  return FONT_SCALES.find((s) => s === stored) ?? 1;
}

export function setFontScale(value: FontScale): void {
  document.documentElement.style.setProperty('--app-font-scale', String(value));
  localStorage.setItem('fontScale', String(value));
}
