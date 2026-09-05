// Regression lock for the white sheet behind every TradingView embed.
//
// The chain: index.html declares an unscoped `color-scheme: dark` on <html>,
// TV's embed document declares none, and an embedded document whose used
// color-scheme differs from its embedder's loses transparent compositing — the
// browser paints the iframe canvas opaque white. TV is cross-origin, so the
// only reachable end of the mismatch is ours; tvEmbed.css clears the scheme on
// the container the iframe grows inside and inheritance carries it down.
//
// jsdom cannot see any of this — it has no cascade for color-scheme and no
// compositing at all, so the unit tests can only check the class and the rule
// text. This loads the REAL stylesheet under index.html's REAL inline style and
// measures the REAL outcome: a lime backdrop behind a transparent-bodied
// iframe, sampled from a screenshot. Lime means the iframe composited
// transparently; white means the canvas was forced opaque and the bug is back.
// Deleting the rule from tvEmbed.css fails these tests.
//
// Two things this deliberately does NOT cover, both verified by hand instead:
//   - The child is a same-origin `srcdoc`, not a cross-origin document. The
//     compositing rule is origin-independent, and the real cross-origin case
//     was checked against live TradingView embeds in a 17-widget before/after
//     sweep (12 fixed, 5 already correct, 0 regressions).
//   - playwright.config.js runs Chromium only, while this behavior is
//     engine-specific. Firefox and WebKit were verified manually; automating
//     them means widening the shared project matrix for every e2e spec.
import { test, expect } from '@playwright/test';
import { readWebFile, indexHtmlInlineStyle } from './helpers/indexHtmlStyle.js';

const TV_EMBED_CSS = 'src/pages/Dashboard/widgets/framework/tvEmbed.css';

// Stands in for TV's embed document: a separate document that declares no
// color-scheme of its own and paints nothing. That combination is the entire
// precondition for the compositing rule.
const EMBEDDED = `<!DOCTYPE html><html><head><meta charset="utf-8"></head><body style="margin:0;background:transparent"></body></html>`;

const LIME = '0,255,0';

/**
 * Mirrors what both hosts render, with the real class names — the reset is
 * keyed on them, so using anything else would test nothing. `#backdrop` is the
 * lime sheet the iframe has to stay transparent over.
 */
function embedDocument({
  container = 'tv-embed-container',
  meta = null,
  theme = null,
  withStylesheet = true,
} = {}) {
  return `<!DOCTYPE html><html${theme ? ` data-theme="${theme}"` : ''}><head><meta charset="utf-8">
    ${meta ? `<meta name="color-scheme" content="${meta}">` : ''}
    <style>${indexHtmlInlineStyle()}</style>
    ${withStylesheet ? `<style>${readWebFile(TV_EMBED_CSS)}</style>` : ''}
    </head><body style="margin:0"><div id="chrome">
      <div id="backdrop" style="background:rgb(${LIME});width:200px;height:200px">
        <div class="${container}" style="width:200px;height:200px">
          <div class="tradingview-widget-container__widget" style="width:200px;height:200px">
            <iframe id="frame" srcdoc='${EMBEDDED}' style="border:0;width:200px;height:200px"></iframe>
          </div>
        </div>
      </div>
      <div id="fallback">Widget unavailable</div>
    </div></body></html>`;
}

// Both hosts, because the rule has to cover both: 16 of the 17 TV widgets
// render through TradingViewEmbed, the economic map through the web component.
const CONTAINERS = ['tv-embed-container', 'tv-wc-container'];

/**
 * Sample the pixel at the centre of the iframe.
 *
 * A fresh context per scenario is load-bearing: shipping browsers don't
 * re-evaluate the compositing decision when color-scheme changes on an existing
 * page (fixed in Blink M152), so reusing one page across scenarios reports the
 * first scenario's result for all of them.
 */
async function sampleFrame(browser, html) {
  const context = await browser.newContext();
  try {
    const page = await context.newPage();
    await page.setContent(html);
    const shot = await page.screenshot({ clip: { x: 99, y: 99, width: 2, height: 2 } });
    const computed = await page.evaluate(() => ({
      frame: getComputedStyle(document.querySelector('#frame')).colorScheme,
      fallback: getComputedStyle(document.querySelector('#fallback')).colorScheme,
    }));
    // Decode the PNG through a canvas — no image library is a dependency here.
    const pixel = await page.evaluate(async (b64) => {
      const img = new Image();
      img.src = 'data:image/png;base64,' + b64;
      await img.decode();
      const canvas = document.createElement('canvas');
      canvas.width = img.width;
      canvas.height = img.height;
      const ctx = canvas.getContext('2d');
      ctx.drawImage(img, 0, 0);
      const [r, g, b] = ctx.getImageData(0, 0, 1, 1).data;
      return `${r},${g},${b}`;
    }, shot.toString('base64'));
    return { pixel, ...computed };
  } finally {
    await context.close();
  }
}

test.describe('TradingView embeds — container color-scheme', () => {
  for (const container of CONTAINERS) {
    test(`${container} composites transparently over our dark page`, async ({ browser }) => {
      const { pixel } = await sampleFrame(browser, embedDocument({ container }));
      expect(pixel, 'iframe should composite transparently, showing the backdrop').toBe(LIME);
    });

    test(`negative control: without the rule ${container} is painted opaque`, async ({ browser }) => {
      // Proves the assertion above discriminates the fix from the bug rather
      // than passing on a document that was never dark to begin with. This is
      // the reported defect, reproduced.
      const { pixel } = await sampleFrame(
        browser,
        embedDocument({ container, withStylesheet: false }),
      );
      expect(pixel).not.toBe(LIME);
    });
  }

  test('survives a page-level color-scheme meta tag', async ({ browser }) => {
    // The landmine that killed the first version of this fix. `color-scheme:
    // normal` means "whatever the page color scheme is", and the page color
    // scheme comes from this meta tag — not from our CSS — so `normal` reverted
    // to dark and repainted the sheet while computed styles still read
    // `normal`. Adding the meta tag is the standard fix for pre-paint flash,
    // which index.html already comments about, so this is a live risk. The
    // shipped rule uses an explicit `light`, which is immune.
    const { pixel } = await sampleFrame(browser, embedDocument({ meta: 'dark' }));
    expect(pixel, 'an explicit scheme should not be resolved from the meta tag').toBe(LIME);
  });

  test('the reset stays inside the embed and leaves our own chrome dark', async ({ browser }) => {
    // Scope matters: hoisting the reset onto the host or the card would hand our
    // own UI — the fallback, scrollbars, any UA-painted control — the light
    // treatment on a dark page.
    const { fallback } = await sampleFrame(browser, embedDocument());
    expect(fallback).toBe('dark');
  });

  test('holds in light theme, where there is no mismatch to fix', async ({ browser }) => {
    // Light mode needs no reset — a light embedder and TV's undeclared scheme
    // already agree, so nothing is forced opaque. The rule is unconditional
    // anyway because browsers don't re-evaluate compositing when color-scheme
    // changes on a live page (fixed only in Blink M152): a value that never
    // transitions never depends on that buggy repaint path.
    const { pixel } = await sampleFrame(browser, embedDocument({ theme: 'light' }));
    expect(pixel).toBe(LIME);
  });
});
