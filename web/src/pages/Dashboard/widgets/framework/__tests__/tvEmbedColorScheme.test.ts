import { describe, it, expect } from 'vitest';
import { readdirSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';

/** Every `.css` file under `dir`, recursively. */
function cssFiles(dir: string): string[] {
  return readdirSync(dir, { withFileTypes: true }).flatMap((e) => {
    const full = resolve(dir, e.name);
    if (e.isDirectory()) return cssFiles(full);
    return e.name.endsWith('.css') ? [full] : [];
  });
}

/**
 * REGRESSION-CRITICAL — protects TradingView embeds from rendering on white.
 *
 * A browser drops transparent compositing when an embedded document's used
 * color-scheme differs from its embedder's, painting the iframe canvas opaque
 * white. Our <html> declares `color-scheme: dark` and TV's embed documents
 * declare none, so without the reset in `tvEmbed.css` every TV widget gets a
 * white sheet behind it — 12 of the 17 showed it; the heatmaps and the economic
 * map only escaped because they paint an opaque canvas of their own over it.
 *
 * jsdom has no cascade for color-scheme, so it cannot observe the rule taking
 * effect — that is checked in real Chromium by e2e/tv-embed-color-scheme.spec.js.
 * What this file locks is that the rule still exists, still says `light`, and is
 * still imported by both hosts. If it fails, do not change the assertions — fix
 * the CSS.
 */
const here = (p: string) => resolve(__dirname, '..', p);
const css = readFileSync(here('tvEmbed.css'), 'utf8');

describe('TradingView embed color-scheme reset (regression)', () => {
  it('resets color-scheme on both embed container classes', () => {
    // Both hosts must be covered: 16 of the 17 TV widgets render through
    // TradingViewEmbed, the economic map through TradingViewWebComponent.
    // Matched as one rule block rather than three independent substrings, so
    // two empty selectors plus an unrelated `color-scheme: light` elsewhere in
    // the file can't satisfy this.
    expect(css).toMatch(
      /\.tv-embed-container\s*,\s*\.tv-wc-container\s*\{[^}]*color-scheme:\s*light[^}]*\}/,
    );
  });

  it('does NOT use `normal`, which resolves from a page-level meta tag', () => {
    // `normal` means "whatever the page color scheme is", and the page color
    // scheme comes from <meta name="color-scheme"> — not from our CSS. It works
    // today only because index.html ships no such meta tag. Adding one (the
    // standard fix for pre-paint flash, which index.html already comments about)
    // would flip `normal` back to dark and silently restore the white sheet,
    // while computed styles still read `normal`. Verified opaque in Chromium,
    // Firefox and WebKit. `light` is explicit and immune.
    expect(css).not.toMatch(/color-scheme:\s*normal/);
  });

  it('is imported by both embed hosts, or the rule never ships', () => {
    // The rule lives in CSS so a new embed host inherits it by reusing the
    // class — but that only holds while the stylesheet is actually in the
    // bundle. Vite drops a stylesheet nothing imports. Anchored to the start of
    // a line so a commented-out import doesn't satisfy it.
    for (const host of ['TradingViewEmbed.tsx', 'TradingViewWebComponent.tsx']) {
      expect(readFileSync(here(host), 'utf8'), host).toMatch(/^import '\.\/tvEmbed\.css';$/m);
    }
  });

  it('is the only stylesheet setting color-scheme on the embed containers', () => {
    // The rule is a single-class selector, so anything more specific in another
    // sheet would silently win and repaint the sheet. The e2e spec injects
    // tvEmbed.css alone, so it cannot see such an override — this can.
    const srcRoot = resolve(__dirname, '..', '..', '..', '..', '..');
    const others = cssFiles(srcRoot)
      .filter((p) => !p.endsWith('tvEmbed.css'))
      .filter((p) =>
        /tv-(embed|wc)-container[^{]*\{[^}]*color-scheme/.test(readFileSync(p, 'utf8')),
      )
      .map((p) => p.slice(srcRoot.length + 1));
    expect(others, 'another stylesheet also sets color-scheme on a TV container').toEqual([]);
  });
});
