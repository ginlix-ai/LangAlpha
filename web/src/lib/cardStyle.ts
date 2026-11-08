/**
 * How the app is drawn: Balanced (the default, which leaves each component as it
 * draws itself), Soft, or Terminal. Terminal is the data cards' own
 * stylesheets, and the other two take marks away from them
 * (charts/termCardStyles.css); the rest of the app reads the style as
 * variables (styles/cardStyle.css, which declares every style's values).
 * Saved to the account with the theme (lib/appearancePreference) and kept in
 * this browser too, so the first paint is right before preferences load.
 *
 * Stamped on <html> as `data-card-style`, so a change restyles everything
 * already drawn and nothing re-renders. main.tsx stamps it before the app
 * renders; index.html's pre-paint script does not need it, since nothing it
 * paints reads the style.
 */

import { useSyncExternalStore } from 'react';
import { safeLocalStorage } from './utils';

export const CARD_STYLES = ['soft', 'balanced', 'terminal'] as const;
export type CardStyle = (typeof CARD_STYLES)[number];

// The ids were renamed when the styles reached the whole app (main's Quiet,
// the default, is now Balanced; its Balanced is now Soft), so they are kept
// under a new key and a choice under the old one is carried over once.
export const CARD_STYLE_STORAGE_KEY = 'cardStyle.v2';
const KEY = CARD_STYLE_STORAGE_KEY;
const LEGACY_KEY = 'cardStyle';
const LEGACY: Readonly<Record<string, CardStyle>> = { quiet: 'balanced', balanced: 'soft', terminal: 'terminal' };

export function isCardStyle(value: unknown): value is CardStyle {
  return CARD_STYLES.some((s) => s === value);
}

function readStored(): CardStyle | null {
  const stored = safeLocalStorage.getItem(KEY);
  const legacy = safeLocalStorage.getItem(LEGACY_KEY);
  // The old key goes whenever it is seen, even beside a new one (a tab still on
  // the old build can write it again), or a later reset would bring it back.
  if (legacy !== null) safeLocalStorage.removeItem(LEGACY_KEY);
  if (isCardStyle(stored)) return stored;
  const carried = legacy !== null && Object.hasOwn(LEGACY, legacy) ? LEGACY[legacy] : null;
  if (carried) safeLocalStorage.setItem(KEY, carried);
  return carried;
}

// Read on first use rather than at import, so a module that only imports this
// one (the chart tokens) touches no storage; storage a browser refuses
// (blocked site data) falls back to the default rather than stopping the boot.
// `stored` mirrors what this browser holds under KEY, `current` what is shown.
let stored: CardStyle | null | undefined;
let current: CardStyle = 'balanced';
const listeners = new Set<() => void>();

function show(style: CardStyle): void {
  current = style;
  applyCardStyle(style);
  listeners.forEach((listener) => listener());
}

// Another tab of this browser picked or followed a style: shown here too, and
// not written back, which would only echo it. Removed means the default.
function onStorage(event: StorageEvent): void {
  if (event.key !== KEY && event.key !== null) return;
  stored = isCardStyle(event.newValue) ? event.newValue : null;
  show(stored ?? 'balanced');
}

function load(): void {
  if (stored !== undefined) return;
  stored = readStored();
  current = stored ?? 'balanced';
  if (typeof window !== 'undefined') window.addEventListener('storage', onStorage);
}

export function getCardStyle(): CardStyle {
  load();
  return current;
}

/** The style this browser holds for itself, or null for none, in which case
 *  it has nothing to offer an account but the default. */
export function storedCardStyle(): CardStyle | null {
  load();
  return stored ?? null;
}

export function applyCardStyle(style: CardStyle): void {
  if (style === 'terminal') document.documentElement.removeAttribute('data-card-style');
  else document.documentElement.setAttribute('data-card-style', style);
}

export function setCardStyle(style: CardStyle): void {
  load();
  stored = style;
  safeLocalStorage.setItem(KEY, style);
  show(style);
}

/** Shows a style with the given one stored, or none: for putting the page back
 *  as it was without leaving behind a stored style, which would read as one
 *  picked here. */
export function restoreCardStyle(shown: CardStyle, storedStyle: CardStyle | null): void {
  load();
  stored = storedStyle;
  if (storedStyle) safeLocalStorage.setItem(KEY, storedStyle);
  else safeLocalStorage.removeItem(KEY);
  show(shown);
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function useCardStyle(): CardStyle {
  return useSyncExternalStore(subscribe, getCardStyle);
}

/**
 * A length the current style declares (`--cs-radius-pill`), in px, or the
 * reader's own where the style declares none: for a value that has to be a
 * number in script, as framer's radius correction needs. The style is stamped
 * before the store tells its readers, so the value read follows a change.
 */
export function useStyleLength(name: string, own: number): number {
  return readStyleLength(name, own, useCardStyle());
}

// The style is an argument, though unread, so the React Compiler keys the
// read on it and reads again when it changes; the value is the stylesheet's.
function readStyleLength(name: string, own: number, _style: CardStyle): number {
  const value = parseFloat(getComputedStyle(document.documentElement).getPropertyValue(name));
  return Number.isFinite(value) ? value : own;
}
