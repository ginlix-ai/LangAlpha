import { useEffect, useState } from 'react';
import type { ResolvedTheme } from '@/contexts/ThemeContext';

/**
 * Design-token access for painters that cannot resolve CSS custom properties
 * themselves — lightweight-charts and raw 2D canvas take color *strings*.
 *
 * ⚠️ Resolution must happen after the DOM theme stamp. ThemeProvider stamps
 * `data-theme` in a layout effect, so every passive effect sees the current
 * theme but a render-phase read on a flip still sees the outgoing one. Reads
 * are therefore refused whenever the stamped theme disagrees with the theme
 * asked for, which turns that race into "fall back to literals" rather than
 * "paint the other theme's colors".
 */

/** Resolved values per theme — tokens never change within a theme at runtime. */
const tokenCache = new Map<ResolvedTheme, Map<string, string>>();
const resolverResets = new Set<() => void>();

let probe: HTMLElement | null = null;

/**
 * A custom property computes to its raw token text, so a token declared as
 * `hsl(var(--card))` reads back as `hsl(220 4% 14.5%)`. Canvas accepts that,
 * but libraries that parse colors themselves may not — bounce anything that
 * is not already a plain color through a real `color` property to get the
 * browser's canonical `rgb()/rgba()` form.
 */
function normalizeColor(value: string): string {
  if (/^(#|rgb)/.test(value)) return value;
  try {
    if (!probe) {
      probe = document.createElement('span');
      probe.setAttribute('aria-hidden', 'true');
      probe.style.cssText =
        'position:absolute;width:0;height:0;visibility:hidden;pointer-events:none';
      (document.body ?? document.documentElement).appendChild(probe);
    }
    probe.style.color = '';
    probe.style.color = value;
    // CSSOM drops a value it cannot parse, leaving the property empty — without
    // this the computed read would hand back inherited black as if it were the
    // token's color.
    if (!probe.style.color) return value;
    return getComputedStyle(probe).color || value;
  } catch {
    return value;
  }
}

/**
 * Resolve CSS custom-property `names` (e.g. `--color-profit`) off `<html>`.
 * Only names that resolved to a non-empty value come back, so callers spread
 * the result over literal fallbacks — under jsdom every custom property
 * computes to `''`, which yields `{}` and the pure-literal path.
 */
export function readTokens(
  names: readonly string[],
  theme: ResolvedTheme,
): Record<string, string> {
  if (typeof document === 'undefined') return {};
  const root = document.documentElement;
  if (root.getAttribute('data-theme') !== theme) return {};

  let resolved = tokenCache.get(theme);
  if (!resolved) {
    resolved = new Map();
    tokenCache.set(theme, resolved);
  }

  const out: Record<string, string> = {};
  let computed: CSSStyleDeclaration | null = null;
  for (const name of names) {
    let value = resolved.get(name);
    if (value === undefined) {
      computed ??= getComputedStyle(root);
      const raw = computed.getPropertyValue(name).trim();
      value = raw ? normalizeColor(raw) : '';
      resolved.set(name, value);
    }
    if (value) out[name] = value;
  }
  return out;
}

/**
 * Resolve a slot→source map. A `--`-prefixed source is a token name; anything
 * else is a literal one-off (a color the system has no token for). Every
 * token-backed slot needs an entry in `fallbacks`, so the result is complete
 * even when nothing resolves.
 */
export function resolveTokenMap<K extends string>(
  sources: Record<K, string>,
  fallbacks: Record<K, string>,
  theme: ResolvedTheme,
): Record<K, string> {
  const names = Object.values<string>(sources).filter((s) => s.startsWith('--'));
  const values = readTokens(names, theme);
  const out = {} as Record<K, string>;
  for (const key of Object.keys(sources) as K[]) {
    const source = sources[key];
    out[key] = source.startsWith('--') ? (values[source] ?? fallbacks[key]) : source;
  }
  return out;
}

/**
 * Build a per-theme resolver with a STABLE object identity per theme — chart
 * effects key off that identity, so a fresh object every call would tear down
 * and rebuild the chart on every render. Until a live read succeeds the
 * fallback object itself is handed back (same identity, correct colors).
 */
export function createThemeResolver<K extends string>(
  sources: Record<ResolvedTheme, Record<K, string>>,
  fallbacks: Record<ResolvedTheme, Record<K, string>>,
): (theme: ResolvedTheme) => Record<K, string> {
  const resolvedByTheme = new Map<ResolvedTheme, Record<K, string>>();
  resolverResets.add(() => resolvedByTheme.clear());
  return (theme) => {
    const cached = resolvedByTheme.get(theme);
    if (cached) return cached;
    const names = Object.values<string>(sources[theme]).filter((s) => s.startsWith('--'));
    if (!Object.keys(readTokens(names, theme)).length) return fallbacks[theme];
    const resolved = resolveTokenMap(sources[theme], fallbacks[theme], theme);
    resolvedByTheme.set(theme, resolved);
    return resolved;
  };
}

/**
 * Best-available value during render (the theme's literals on a flip, since
 * the stamp is still the outgoing theme), re-read in a passive effect where
 * the stamp is guaranteed current. Only a genuinely different resolver result
 * schedules the second render, so a steady-state mount costs nothing extra.
 */
export function useThemeTokens<K extends string>(
  resolve: (theme: ResolvedTheme) => Record<K, string>,
  theme: ResolvedTheme,
): Record<K, string> {
  const [, bump] = useState(0);
  const tokens = resolve(theme);
  useEffect(() => {
    if (resolve(theme) !== tokens) bump((n) => n + 1);
  }, [resolve, theme, tokens]);
  return tokens;
}

/** Test seam — resolved values are cached for the process lifetime otherwise. */
export function clearThemeTokenCache(): void {
  tokenCache.clear();
  resolverResets.forEach((reset) => reset());
  probe?.remove();
  probe = null;
}
