import { describe, it, expect, afterEach } from 'vitest';

import { buildHtmlSrcDoc } from '../buildHtmlSrcDoc';
// Byte-exact srcDoc for the inline variant, captured under jsdom
// (getComputedStyle resolves themeCSS to '' there, so output is deterministic).
// The inline document is the widget as an ARTIFACT: it is what gets downloaded,
// opened in a tab and printed to PDF. So an unintended byte here is a real
// regression in three places at once, and the fixture is the cheapest detector
// there is. Regenerate it deliberately when the document is meant to change.
import widgetInlineFixture from './__fixtures__/widget-inline.srcdoc.html?raw';
import widgetInlineNodataFixture from './__fixtures__/widget-inline-nodata.srcdoc.html?raw';

// The fixtures were captured with these exact inputs.
const WITH_DATA = { html: '<div>hi</div>', data: { 'a.json': '{"x":1}' } };
const NO_DATA = { html: '<p>no data</p>' };

describe('buildHtmlSrcDoc — widget-inline document lock', () => {
  it('emits the locked inline document (with data)', () => {
    expect(buildHtmlSrcDoc('widget-inline', WITH_DATA)).toBe(widgetInlineFixture);
  });

  it('emits the locked inline document (no data)', () => {
    expect(buildHtmlSrcDoc('widget-inline', NO_DATA)).toBe(widgetInlineNodataFixture);
  });

  it('omits the data script when data is an empty object', () => {
    expect(buildHtmlSrcDoc('widget-inline', { html: '<p>no data</p>', data: {} })).toBe(
      widgetInlineNodataFixture,
    );
  });
});

describe('buildHtmlSrcDoc — widget-fullscreen variant', () => {
  const inline = buildHtmlSrcDoc('widget-inline', WITH_DATA);
  const fullscreen = buildHtmlSrcDoc('widget-fullscreen', WITH_DATA);

  it('differs from widget-inline only in the body rule and the layout block', () => {
    expect(fullscreen).not.toBe(inline);
    // The title's claim, proved rather than asserted by name. The variant reaches
    // the template in exactly two adjacent places — the tail of the body rule and
    // the layout block on the line after it — so blanking that one region makes
    // the two documents identical. What this really pins is the inline fixture's
    // reach: it is a byte lock, and it only stays one as long as the fullscreen
    // variant cannot reach any of the bytes it locks.
    const region = /background: transparent;[\s\S]*?\n::-webkit-scrollbar \{/;
    const blank = (doc: string) => doc.replace(region, '\n::-webkit-scrollbar {');
    expect(blank(fullscreen)).toBe(blank(inline));
  });

  it('scrolls rather than clipping, in either direction', () => {
    // The cap in the layout block is what a widget is authored to, not a
    // guarantee it obeys. `html` carries no overflow here, so the body's value
    // propagates to the viewport: `overflow-x: hidden` clipped anything wider
    // than the cap with no scrollbar to reach it by, in the one view whose job
    // is showing the widget bigger.
    expect(fullscreen).toContain('overflow: auto; height: 100%;');
    expect(fullscreen).not.toContain('overflow-x: hidden');
    expect(fullscreen).not.toContain('background: transparent; overflow: hidden; }');
  });

  it('swaps the seamless reset for the column layout', () => {
    // Both variants style the outermost element; they disagree about what for.
    // Inline flattens it into the chat flow, fullscreen centres it.
    expect(inline).toContain('margin: 0 auto !important;');
    expect(inline).not.toContain('max-width: 768px;');
    expect(fullscreen).toContain('max-width: 768px;');
    expect(fullscreen).not.toContain('box-shadow: none !important;');
  });

  it('carries no sizing logic of its own, in either variant', () => {
    // The magnification that fills the dialog is a `zoom` on the host iframe
    // (HtmlFullscreenModal.css), because the host is what knows how much room
    // there is. It lived in here once, measured from the document's own
    // viewport, and the print root — a 680px box the same markup is rendered
    // into — then shrank every exported widget to fit a number that meant
    // nothing on paper. Nothing in the document may measure or scale itself.
    for (const doc of [inline, fullscreen]) {
      expect(doc).not.toContain('zoom');
      expect(doc).not.toContain('clientWidth');
      expect(doc).not.toContain('scrollbar-gutter');
    }
  });

  it('keeps the CSP meta and the early/runtime scripts identical to the inline variant', () => {
    // CSP meta line — unchanged across variants.
    const csp =
      '<meta http-equiv="Content-Security-Policy" content="default-src \'none\';';
    expect(fullscreen).toContain(csp);
    // NaN/Infinity JSON patch — unchanged.
    expect(fullscreen).toContain("JSON.parse=function(t,r){");
    // sendPrompt bridge — unchanged.
    expect(fullscreen).toContain('window.sendPrompt = function(text) {');
    // Resize reporting — unchanged.
    expect(fullscreen).toContain("parent.postMessage({ type: 'widget:resize', height: h }, '*');");
    // Theme-sync listener — unchanged.
    expect(fullscreen).toContain("e.data.type === 'widget:themeUpdate'");
    // Data script still injected for the fullscreen variant.
    expect(fullscreen).toContain('window.__WIDGET_DATA__ =');
  });
});

describe('buildHtmlSrcDoc — host color-scheme mirroring', () => {
  afterEach(() => {
    document.documentElement.style.removeProperty('color-scheme');
  });

  // A widget document whose used color-scheme differs from the host's loses
  // transparent compositing (the browser paints the iframe canvas opaque
  // white), so the host's declared scheme must ride along in the :root block.
  it('mirrors a declared host color-scheme into the injected :root block', () => {
    document.documentElement.style.setProperty('color-scheme', 'dark');
    expect(buildHtmlSrcDoc('widget-inline', NO_DATA)).toContain(':root {\n  color-scheme: dark;');
  });

  it('omits color-scheme when the host resolves to normal', () => {
    document.documentElement.style.setProperty('color-scheme', 'normal');
    expect(buildHtmlSrcDoc('widget-inline', NO_DATA)).toBe(widgetInlineNodataFixture);
  });
});
