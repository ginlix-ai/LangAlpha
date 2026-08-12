import { describe, it, expect, afterEach } from 'vitest';

import { buildHtmlSrcDoc } from '../buildHtmlSrcDoc';
// Byte-exact srcDoc captured from the pre-refactor InlineWidget under jsdom
// (getComputedStyle resolves themeCSS to '' there, so output is deterministic).
import widgetInlineFixture from './__fixtures__/widget-inline.srcdoc.html?raw';
import widgetInlineNodataFixture from './__fixtures__/widget-inline-nodata.srcdoc.html?raw';

// The fixtures were captured with these exact inputs.
const WITH_DATA = { html: '<div>hi</div>', data: { 'a.json': '{"x":1}' } };
const NO_DATA = { html: '<p>no data</p>' };

describe('buildHtmlSrcDoc — widget-inline byte compatibility', () => {
  it('produces byte-identical srcDoc to the pre-refactor InlineWidget (with data)', () => {
    expect(buildHtmlSrcDoc('widget-inline', WITH_DATA)).toBe(widgetInlineFixture);
  });

  it('produces byte-identical srcDoc to the pre-refactor InlineWidget (no data)', () => {
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

  it('differs from widget-inline only in the body rule and the seamless override', () => {
    expect(fullscreen).not.toBe(inline);
  });

  it('uses overflow:auto; height:100% on the body instead of overflow:hidden', () => {
    expect(fullscreen).toContain('overflow: auto; height: 100%;');
    expect(fullscreen).not.toContain('background: transparent; overflow: hidden; }');
  });

  it('drops the seamless first-child override', () => {
    expect(inline).toContain('body > :first-child {');
    expect(fullscreen).not.toContain('body > :first-child {');
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
