import type { Kind } from './types';

/**
 * The character that asks for each kind alone, typed as the field's first.
 * A module of its own so the palette's provider, in the entry chunk, can
 * open at a kind without importing the palette.
 */
export const PREFIXES: Readonly<Record<Kind, string>> = {
  tickers: '$',
  files: '/',
  threads: '#',
  turns: '"',
  workspaces: '@',
  actions: '>',
};
