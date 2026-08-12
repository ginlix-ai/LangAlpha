import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import {
  clearThemeTokenCache,
  createThemeResolver,
  readTokens,
  resolveTokenMap,
} from '../themeTokens';

/**
 * jsdom computes every custom property to `''`, so the literal-fallback merge
 * is the ONLY path these tests can exercise — which is exactly why it needs
 * covering: it is also the path production takes on a theme flip, before the
 * stamp lands.
 */

const SOURCES = {
  dark: { bg: '--color-bg-tool-card', up: '#0FEDBE', down: '--color-loss' },
  light: { bg: '--color-bg-tool-card', up: '--color-profit', down: '--color-loss' },
};
const FALLBACKS = {
  dark: { bg: '#232426', up: '#0FEDBE', down: '#F85149' },
  light: { bg: '#F5F4F1', up: '#1A7F37', down: '#CF222E' },
};

beforeEach(() => {
  clearThemeTokenCache();
  document.documentElement.setAttribute('data-theme', 'dark');
});

afterEach(() => {
  clearThemeTokenCache();
  document.documentElement.removeAttribute('data-theme');
});

describe('readTokens', () => {
  it('returns nothing when the DOM resolves no value (jsdom)', () => {
    expect(readTokens(['--color-loss'], 'dark')).toEqual({});
  });

  it('refuses to read while the stamped theme is the other one', () => {
    document.documentElement.setAttribute('data-theme', 'dark');
    expect(readTokens(['--color-loss'], 'light')).toEqual({});
  });
});

describe('resolveTokenMap', () => {
  it('falls back to the literal for every token-backed slot', () => {
    expect(resolveTokenMap(SOURCES.dark, FALLBACKS.dark, 'dark')).toEqual({
      bg: '#232426',
      up: '#0FEDBE',
      down: '#F85149',
    });
  });

  it('passes one-off literals through instead of looking them up', () => {
    const resolved = resolveTokenMap(
      SOURCES.dark,
      { ...FALLBACKS.dark, up: 'SHOULD-NOT-BE-USED' },
      'dark',
    );
    // `up` is a literal source, so the fallback entry is irrelevant to it.
    expect(resolved.up).toBe('#0FEDBE');
  });

  it('keeps each theme on its own literals', () => {
    document.documentElement.setAttribute('data-theme', 'light');
    expect(resolveTokenMap(SOURCES.light, FALLBACKS.light, 'light')).toEqual(FALLBACKS.light);
  });
});

describe('createThemeResolver', () => {
  it('hands back a stable identity per theme so chart effects do not churn', () => {
    const resolve = createThemeResolver(SOURCES, FALLBACKS);
    expect(resolve('dark')).toBe(resolve('dark'));
  });

  it('returns the requested theme, not the stamped one', () => {
    const resolve = createThemeResolver(SOURCES, FALLBACKS);
    expect(resolve('light')).toEqual(FALLBACKS.light);
    expect(resolve('dark')).toEqual(FALLBACKS.dark);
  });
});
