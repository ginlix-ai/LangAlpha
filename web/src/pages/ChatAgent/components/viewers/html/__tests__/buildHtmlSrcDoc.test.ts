import { describe, it, expect, afterEach, beforeEach, vi } from 'vitest';

import {
  buildHtmlSrcDoc,
  buildStreamSrcDoc,
  buildWidgetSrcDoc,
  WIDGET_CSP,
} from '../buildHtmlSrcDoc';
import { BRIDGE_SCRIPT, EARLY_SCRIPT, STREAM_RECEIVER_SCRIPT } from '../frameScripts';
import { LIGHT_PALETTE, paletteCss, THEME_VARS } from '../frameTheme';
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

  it('flattens the first element that draws, past a leading <style>', () => {
    // The tool asks for <style> first so the widget streams in styled; a plain
    // :first-child landed the reset on that <style> and left the card's own
    // background and border on the chat.
    const html = '<style>.c{border:1px solid}</style><link rel="stylesheet" href="x.css"><div class="c" id="outer"><div id="inner"></div></div><script></script>';
    const srcDoc = buildHtmlSrcDoc('widget-inline', { html });
    const doc = new DOMParser().parseFromString(srcDoc, 'text/html');
    const selector = /\n([^\n{]+)\{\n\s*background: transparent !important;/.exec(srcDoc)?.[1];
    expect(selector).toBeTruthy();
    expect([...doc.querySelectorAll(selector!)].map((el) => el.id)).toEqual(['outer']);
    const button = new DOMParser().parseFromString(
      buildHtmlSrcDoc('widget-inline', { html: '<style></style><button id="b">Go</button>' }),
      'text/html',
    );
    expect(button.querySelectorAll(selector!)).toHaveLength(0);
  });

  it('carries no sizing logic of its own, in either variant', () => {
    // The magnification that fills the dialog is a `zoom` on the host iframe
    // (HtmlFullscreenModal.css), because the host is what knows how much room
    // there is. It lived in here once, measured from the document's own
    // viewport, and the print root — a 680px box the same markup is rendered
    // into — then shrank every exported widget to fit a number that meant
    // nothing on paper. Nothing in the document may measure or scale itself.
    // The inspect answer measures for the headless render's report, and
    // changes no layout; everything else in the document must not measure.
    const notInspect = (doc: string) => doc.replace(/function inspect\(\)\{[\s\S]*?\n {2}\}\n/, '');
    for (const doc of [inline, fullscreen].map(notInspect)) {
      expect(doc).not.toContain('function inspect');
      // The CSS property, in a rule or set from script; `data-zoom` is the
      // lightbox request, which scales nothing.
      expect(doc).not.toMatch(/(?<![-\w'])zoom\s*:|\.zoom\b/);
      expect(doc).not.toContain('clientWidth');
      expect(doc).not.toContain('scrollbar-gutter');
    }
  });

  it('keeps the CSP meta and the early/runtime scripts identical to the inline variant', () => {
    // The served-report load policy (src/server/utils/html_policy.py).
    expect(fullscreen).toContain(
      `<meta http-equiv="Content-Security-Policy" content="${WIDGET_CSP}">`,
    );
    // Both programs as the files hold them, called with their arguments.
    expect(fullscreen).toContain(`<script>\n${EARLY_SCRIPT}({},"");\n</script>`);
    expect(fullscreen).toContain(`<script>\n${BRIDGE_SCRIPT}(32768);\n</script>`);
    // Data script still injected for the fullscreen variant.
    expect(fullscreen).toContain('window.__WIDGET_DATA__ =');
  });
});

describe('buildHtmlSrcDoc — load policy, base, storage seed, theme', () => {
  it('declares the served-report policy, which limits no load', () => {
    expect(WIDGET_CSP).toBe("default-src * data: blob: 'unsafe-inline' 'unsafe-eval'");
    // Nothing narrower may ride beside it: a second directive list intersects.
    const doc = buildHtmlSrcDoc('widget-inline', NO_DATA);
    expect(doc.match(/Content-Security-Policy/g)).toHaveLength(1);
  });

  it('puts <base> first in <head>, so every URL after it resolves to the workspace', () => {
    const base = 'https://api.example.com/api/v1/wsfiles/g/tok"&/';
    const doc = buildHtmlSrcDoc('widget-inline', { ...NO_DATA, base });
    expect(doc.startsWith('<!DOCTYPE html><html><head>\n<base href="https://api.example.com/api/v1/wsfiles/g/tok&quot;&amp;/">\n<meta'))
      .toBe(true);
    // The click handler keeps links under it out of address bars.
    expect(doc).toContain(`,${JSON.stringify(base)});`);
  });

  it('emits no <base> without one', () => {
    expect(buildHtmlSrcDoc('widget-inline', NO_DATA)).not.toContain('<base');
  });

  it('seeds the storage stand-in without letting a value close the script', () => {
    const doc = buildHtmlSrcDoc('widget-inline', {
      ...NO_DATA,
      storageSeed: { note: '</script><script>alert(1)</script><!--' },
    });
    expect(doc).not.toContain('</script><script>alert(1)');
    expect(doc).toContain('{"note":"\\u003c/script>\\u003cscript>alert(1)\\u003c/script>\\u003c!--"}');
  });

  it('escapes data the same way', () => {
    const doc = buildHtmlSrcDoc('widget-inline', { html: '<p></p>', data: { 'x.txt': '<!--<script>' } });
    expect(doc).toContain('window.__WIDGET_DATA__ = {"x.txt":"\\u003c!--\\u003cscript>"};');
  });

  it('takes a fixed palette without reading the document', () => {
    const doc = buildHtmlSrcDoc('widget-inline', { ...NO_DATA, theme: LIGHT_PALETTE });
    expect(doc).toContain(':root {\n  color-scheme: light;\n  --color-bg-page: #ffffff;');
    expect(doc).toContain('--color-chart-8: #e34948;\n}');
    expect(paletteCss(LIGHT_PALETTE).split('\n')).toHaveLength(THEME_VARS.length + 1);
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

describe('buildHtmlSrcDoc — scrollbar', () => {
  const INSET_TRACK = '::-webkit-scrollbar-track { background: transparent; margin: 4px; }';

  // The widget's WebKit rules would merge with the inset ones one property at
  // a time, and a 6px lane less the 3px inset paints no thumb.
  it('leaves a widget with WebKit scrollbar rules its own scrollbar', () => {
    const html = '<style>::-webkit-scrollbar { width: 6px; }</style><div>hi</div>';
    expect(buildHtmlSrcDoc('widget-inline', { html })).not.toContain(INSET_TRACK);
  });

  // The standard properties cannot merge into a thumbless scrollbar, and a
  // browser without them (Chrome before 121) has only the inset rules.
  it('keeps the inset scrollbar for a widget that sets only the standard properties', () => {
    const html = '<style>html { scrollbar-width: thin; }</style><div>hi</div>';
    expect(buildHtmlSrcDoc('widget-inline', { html })).toContain(INSET_TRACK);
  });
});

type PromptCall = 'sendPrompt' | 'draftPrompt' | 'startThread';

/** The bridge script, run against a stand-in window. */
function runBridge(userActivation?: { isActive: boolean }) {
  const posted: Array<Record<string, unknown>> = [];
  const win = {
    parent: { postMessage: (m: Record<string, unknown>) => posted.push(m) },
    navigator: userActivation ? { userActivation } : {},
    addEventListener: () => {},
    innerHeight: 0,
  } as unknown as Window & Record<PromptCall, (text: unknown, data?: unknown) => void>;
  const stubDoc = { addEventListener: () => {}, body: null, documentElement: {} };
  // Its timers find no body and return.
  new Function('window', 'document', `${BRIDGE_SCRIPT}(32768);`)(win, stubDoc);
  return { win, posted };
}

describe.each(['sendPrompt', 'draftPrompt', 'startThread'] as const)('bridge — %s needs the user', (call) => {
  const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
  afterEach(() => warn.mockClear());

  it('posts from a click or key press', () => {
    const { win, posted } = runBridge({ isActive: true });
    win[call]('more', { a: 1 });
    expect(posted).toEqual([{ type: `widget:${call}`, text: 'more', data: { a: 1 } }]);
  });

  it('drops one with no user gesture behind it, and says so once', () => {
    const { win, posted } = runBridge({ isActive: false });
    win[call]('on load');
    win[call]('on a timer');
    const why = `${call} ignored: call it from a click or key press`;
    expect(posted).toEqual([{ type: 'widget:diag', kind: 'error', message: why }]);
    expect(warn).toHaveBeenCalledTimes(2);
    expect(warn).toHaveBeenCalledWith(why);
  });

  it('lets it through where the browser cannot tell', () => {
    const { win, posted } = runBridge();
    win[call]('old Safari');
    expect(posted).toEqual([{ type: `widget:${call}`, text: 'old Safari' }]);
  });

  it('coerces the text, and sends without data it cannot carry', () => {
    const { win, posted } = runBridge({ isActive: true });
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    win[call](42, cyclic);
    win[call]('big', { blob: 'x'.repeat(40_000) });
    expect(posted).toEqual([{ type: `widget:${call}`, text: '42' }, { type: `widget:${call}`, text: 'big' }]);
    expect(warn).toHaveBeenCalledWith(`${call}: data is not JSON-serializable; sent without it`);
  });

  // The cap is in UTF-8 bytes, as the parent counts it: 12k CJK characters
  // are 36 KB on the wire though well under 32k characters.
  it('counts the data in bytes', () => {
    const { win, posted } = runBridge({ isActive: true });
    win[call]('cjk', { note: '数'.repeat(12_000) });
    win[call]('ascii', { note: 'x'.repeat(12_000) });
    expect(posted).toEqual([
      { type: `widget:${call}`, text: 'cjk' },
      { type: `widget:${call}`, text: 'ascii', data: { note: 'x'.repeat(12_000) } },
    ]);
    expect(warn).toHaveBeenCalledWith(`${call}: data is over 32768 bytes serialized; sent without it`);
  });
});

describe('buildStreamSrcDoc', () => {
  const doc = buildStreamSrcDoc({ base: 'https://api.example.com/g/' });

  it('runs only its own scripts, under the widget load policy', () => {
    const nonce = /script-src 'nonce-([^']+)'/.exec(doc)?.[1];
    expect(nonce).toBeTruthy();
    expect(doc).toContain(`content="${WIDGET_CSP}; script-src 'nonce-${nonce}'"`);
    const scripts = doc.match(/<script[^>]*>/g) ?? [];
    expect(scripts).toEqual([`<script nonce="${nonce}">`, `<script nonce="${nonce}">`]);
    // A fresh nonce per document, so no markup can have learned it.
    expect(buildStreamSrcDoc()).not.toContain(nonce);
  });

  it('carries none of the widget: an empty body, no data, no storage stand-in', () => {
    expect(doc).toContain('<body></body>');
    expect(doc).not.toContain('__WIDGET_DATA__ =');
    expect(doc).not.toContain(EARLY_SCRIPT);
    expect(doc).toContain(`\n${STREAM_RECEIVER_SCRIPT}();\n`);
  });

  it('shares the inline document base, theme, layout and height reports', () => {
    const inline = buildHtmlSrcDoc('widget-inline', { html: '', base: 'https://api.example.com/g/' });
    const head = (d: string) => d.slice(0, d.indexOf('<script'));
    expect(head(doc).replace(/; script-src 'nonce-[^']+'/, '')).toBe(head(inline));
    expect(doc).toContain(`\n${BRIDGE_SCRIPT}(32768);\n`);
  });
});

describe('buildWidgetSrcDoc', () => {
  it("names the line the widget's own html starts on", () => {
    const { doc, htmlLine } = buildWidgetSrcDoc('widget-inline', { html: 'first\nsecond', data: { 'a.json': '{}' } });
    expect(doc.split('\n')[htmlLine - 1].endsWith('<body>first')).toBe(true);
  });
});

/**
 * A frame document holding the bridge, as jsdom can run it: the program is
 * evaluated in the frame's realm, and the frame's parent is this window.
 */
describe('bridge — in a frame', () => {
  let frame: HTMLIFrameElement;
  let win: Window & typeof globalThis;
  let received: Array<Record<string, unknown>>;
  const onMessage = (e: MessageEvent) => received.push(e.data as Record<string, unknown>);

  beforeEach(() => {
    received = [];
    frame = document.createElement('iframe');
    document.body.appendChild(frame);
    win = frame.contentWindow as Window & typeof globalThis;
    window.addEventListener('message', onMessage);
  });

  afterEach(() => {
    window.removeEventListener('message', onMessage);
    frame.remove();
  });

  const run = () => new win.Function(`${BRIDGE_SCRIPT}(32768);`)();
  const fromParent = (data: unknown) =>
    win.dispatchEvent(new win.MessageEvent('message', { data, source: window }));

  it('reports the height once the document is laid out', async () => {
    run();
    Object.defineProperty(win.document.body, 'scrollHeight', { configurable: true, value: 120 });
    Object.defineProperty(win, 'innerHeight', { configurable: true, value: 120 });
    win.document.dispatchEvent(new win.Event('DOMContentLoaded'));
    await vi.waitFor(() => expect(received).toContainEqual({ type: 'widget:resize', height: 120 }));
  });

  // The theme is its own element, written whole: a widget's own <style>
  // first in the head (a CSS-in-JS library puts one there) is left alone.
  it('writes a theme push into its own element, as the builder writes it', () => {
    const built = new DOMParser().parseFromString(
      buildHtmlSrcDoc('widget-inline', { ...NO_DATA, theme: LIGHT_PALETTE }),
      'text/html',
    );
    const builtTheme = built.getElementById('widget-theme')!.textContent;
    const d = win.document;
    d.head.innerHTML = '<style id="widget-theme">\n:root {\n  --color-bg-page: #000000;\n}\n</style>';
    const own = d.createElement('style');
    own.textContent = '.css-in-js { color: red; }';
    d.head.prepend(own);
    const themechange = vi.fn();
    win.addEventListener('themechange', themechange);
    run();

    fromParent({ type: 'widget:themeUpdate', css: paletteCss(LIGHT_PALETTE) });

    expect(d.getElementById('widget-theme')!.textContent).toBe(builtTheme);
    expect(own.textContent).toBe('.css-in-js { color: red; }');
    expect(themechange).toHaveBeenCalledTimes(1);
  });

  it('answers an inspect, and takes it only from its parent', async () => {
    run();
    fromParent({ type: 'widget:inspect' });
    win.dispatchEvent(new win.MessageEvent('message', { data: { type: 'widget:inspect' }, source: win }));
    await vi.waitFor(() => expect(received.filter((m) => m.type === 'widget:inspected')).toHaveLength(1));
  });
});
