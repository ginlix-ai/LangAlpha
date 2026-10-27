/**
 * How the data cards, in chat and on the dashboard, and their detail panels
 * are drawn: Quiet, the default and the nearest to the rest of the app,
 * Balanced, or Terminal. Terminal is the cards' own stylesheets; the
 * other two take marks away from them (charts/termCardStyles.css). Saved to
 * the account with the theme (lib/appearancePreference) and kept in this
 * browser too, so the first paint is right before preferences load.
 *
 * Stamped on <html> as `data-card-style`, so a change restyles every card
 * already drawn and nothing re-renders. No card is on the first paint (cards
 * come with the thread), so main.tsx stamps it before the app renders rather
 * than index.html's pre-paint script.
 */

import { useSyncExternalStore } from 'react';
import { safeLocalStorage } from './utils';

export const CARD_STYLES = ['quiet', 'balanced', 'terminal'] as const;
export type CardStyle = (typeof CARD_STYLES)[number];

const KEY = 'cardStyle';

export function isCardStyle(value: unknown): value is CardStyle {
  return CARD_STYLES.some((s) => s === value);
}

// Read before the app boots, so storage a browser refuses (blocked site data)
// falls back to the default rather than stopping the boot.
const stored = safeLocalStorage.getItem(KEY);
let current: CardStyle = isCardStyle(stored) ? stored : 'quiet';
const listeners = new Set<() => void>();

export function getCardStyle(): CardStyle {
  return current;
}

/** Whether this browser has a style of its own to offer an account that has
 *  none yet, rather than the default. */
export function hasStoredCardStyle(): boolean {
  return isCardStyle(stored);
}

export function applyCardStyle(style: CardStyle): void {
  if (style === 'terminal') document.documentElement.removeAttribute('data-card-style');
  else document.documentElement.setAttribute('data-card-style', style);
}

export function setCardStyle(style: CardStyle): void {
  current = style;
  applyCardStyle(style);
  safeLocalStorage.setItem(KEY, style);
  listeners.forEach((listener) => listener());
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function useCardStyle(): CardStyle {
  return useSyncExternalStore(subscribe, getCardStyle);
}
