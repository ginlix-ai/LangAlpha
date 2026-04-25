import { describe, it, expect } from 'vitest';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

/**
 * REGRESSION-CRITICAL — protects TradingView license compliance.
 *
 * The CSS file `WidgetFrame.css` previously contained this broad rule:
 *
 *     .widget-frame a[href*="tradingview.com"] { display: none !important; }
 *
 * That rule was added to suppress lightweight-charts' own TradingView
 * watermark inside ChartWidget. But it ALSO hides every TradingView
 * attribution link our TV embed widgets intentionally render (class
 * `tv-attribution`, `<a href="https://www.tradingview.com/">`). Hidden
 * attribution is a license violation per TradingView's embed terms.
 *
 * Our fix is to scope the kill selector to the lightweight-charts
 * watermark class only. This test pins that invariant so a well-meaning
 * future edit doesn't accidentally re-broaden the selector and silently
 * strip attribution from all 10 TV-embed widgets.
 *
 * If this test fails, do not change the assertions — fix the CSS.
 */
describe('TradingView attribution visibility (regression)', () => {
  const cssPath = resolve(
    __dirname,
    '..',
    'WidgetFrame.css',
  );
  const css = readFileSync(cssPath, 'utf8');

  it('WidgetFrame.css hides the lightweight-charts watermark', () => {
    // lightweight-charts renders its watermark as an <a> with a utm_medium
    // query param; we target that URL signature (stable across library
    // versions) and the `.tv-lightweight-charts__watermark` class as a
    // secondary safety net.
    expect(css).toMatch(/utm_medium=lwc-link/);
    expect(css).toMatch(/\.tv-lightweight-charts__watermark/);
  });

  it('WidgetFrame.css does NOT hide arbitrary tradingview.com links', () => {
    // This is the over-broad selector that used to live here. It would also
    // match the `Powered by TradingView ↗` attribution our embed widgets
    // render — seeing it back means we're silently stripping attribution.
    expect(css).not.toMatch(/\.widget-frame\s+a\[href\*=["']tradingview\.com["']\]/);
  });
});
