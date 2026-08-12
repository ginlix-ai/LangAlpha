// Regression lock for the black frame around every page of an exported PDF.
//
// The defect was a cascade collision that only exists in print media, inside
// the iframe react-to-print builds — so jsdom cannot see it (it hardcodes
// screen media) and neither can a plain app-level assertion. This rebuilds the
// print document the way react-to-print documents and implements it, sourcing
// every ingredient from the real files rather than copies, and checks the
// cascade in real Chromium under real print emulation.
//
// Chain: color-scheme dark wins -> Paged.js's print-color-adjust:exact inks
// the dark UA canvas over the white background -> black frame. This test locks
// the first link; the pixel end of the chain was verified by hand at authoring
// time (dark scheme rasterized to rgb(18,18,18), light to rgb(255,255,255)).
import { test, expect } from '@playwright/test';
import { PRINT_PAGE_STYLE } from '../src/pages/ChatAgent/components/printPageStyle.ts';
// Shared with tv-embed-color-scheme.spec.js — both rebuild an embedded document
// around the same index.html declaration, and a drifted copy would silently
// guard a collision that no longer exists.
import { readWebFile as read, indexHtmlInlineStyle } from './helpers/indexHtmlStyle.js';

// Composite `color` over `backdrop` before measuring. Alpha has to be applied
// here or a semi-transparent colour scores as its opaque base: several theme
// tokens are rgba, and rgba(0,0,0,.08) on white reads as full black otherwise.
function luminanceOver(color, backdrop) {
  const parse = (c) => c.match(/[\d.]+/g).map(Number);
  const [br, bg, bb] = parse(backdrop);
  const [r, g, b, a = 1] = parse(color);
  const mix = (fg, bgc) => fg * a + bgc * (1 - a);
  return 0.2126 * mix(r, br) + 0.7152 * mix(g, bg) + 0.0722 * mix(b, bb);
}

// `indexHtmlInlineStyle()` (imported above) reads the rule that leaks into the
// print document: react-to-print builds its iframe from srcdoc="<!DOCTYPE
// html>", so it carries no data-theme and index.html's unscoped dark branch
// matches for every user, light-theme ones included.

// Shape of what pagedjs injects into the parent document and react-to-print
// then copies into the iframe. Asserted against the real dist bundle below.
const PAGED_PRINT_STYLES = `@media print {
  html { width: 100%; height: 100%;
    -webkit-print-color-adjust: exact; print-color-adjust: exact; }
}`;

// react-to-print@3.3.0 appends pageStyle to the iframe head FIRST, then copies
// parent <style>/<link> nodes after it. That ordering is the whole reason the
// declaration needs !important, so the fixture must preserve it.
function printDocument({ pageStyle, modalSheet = '' }) {
  return `<!DOCTYPE html><html><head><meta charset="utf-8">
    <style>${pageStyle}</style>
    <style>${indexHtmlInlineStyle()}</style>
    ${modalSheet ? `<style>${modalSheet}</style>` : ''}
    <style>${PAGED_PRINT_STYLES}</style>
    </head><body><h1>Export</h1></body></html>`;
}

async function colorSchemeInPrint(page, html) {
  await page.emulateMedia({ media: 'print' });
  await page.setContent(html);
  return page.evaluate(() => getComputedStyle(document.documentElement).colorScheme);
}

async function rootStylesInPrint(page, html) {
  await page.emulateMedia({ media: 'print' });
  await page.setContent(html);
  return page.evaluate(() => {
    const root = getComputedStyle(document.documentElement);
    return { colorScheme: root.colorScheme, background: root.backgroundColor };
  });
}

test.describe('PDF export — print color-scheme', () => {
  test('pageStyle forces the light scheme over index.html\'s dark rule', async ({ page }) => {
    const scheme = await colorSchemeInPrint(page, printDocument({ pageStyle: PRINT_PAGE_STYLE }));
    expect(scheme).toBe('light');
  });

  test('negative control: without pageStyle the dark scheme wins', async ({ page }) => {
    // Proves the assertion above discriminates the fix from the bug rather than
    // passing on a document that was never dark to begin with.
    const scheme = await colorSchemeInPrint(page, printDocument({ pageStyle: '' }));
    expect(scheme).toBe('dark');
  });

  test('holds when the lazy chunk stylesheet never reaches the iframe', async ({ page }) => {
    // react-to-print re-fetches <link> stylesheets inside the iframe and prints
    // anyway if the fetch fails ("unable to load a resource but will continue").
    // The modal's CSS ships in a lazy chunk under VITE_CDN_BASE, so that miss is
    // reachable in production; index.html's inline dark rule always survives.
    const withSheet = await colorSchemeInPrint(
      page,
      printDocument({ pageStyle: PRINT_PAGE_STYLE, modalSheet: read('src/pages/ChatAgent/components/ExportPreviewModal.css') }),
    );
    const withoutSheet = await colorSchemeInPrint(page, printDocument({ pageStyle: PRINT_PAGE_STYLE }));
    expect(withSheet).toBe('light');
    expect(withoutSheet).toBe('light');
  });

  test('scheme and background travel together, or the fallback is unreadable', async ({ page }) => {
    // index.html's `html { background: #191919 }` is on the un-failable inlined
    // path, so if only the scheme ships here a missing chunk yields the dark
    // background with light-scheme black text — black on near-black. Shipping
    // the white background alongside it keeps the failure legible.
    const { colorScheme, background } = await rootStylesInPrint(
      page,
      printDocument({ pageStyle: PRINT_PAGE_STYLE }),
    );
    expect(colorScheme).toBe('light');
    expect(background).toBe('rgb(255, 255, 255)');
  });

  test('negative control: shipping the scheme without the background is unreadable', async ({ page }) => {
    // The claim above is that the two must travel together. This is the state
    // that claim rejects — scheme pinned here, background left in the lazy chunk
    // — so a future "simplification" that splits them again fails here instead
    // of shipping a page whose text and paper are both the same colour.
    const schemeOnly = '@page { size: A4 !important; margin: 15mm !important; }\n' +
      'html, body { color-scheme: light !important; }';
    const { colorScheme, background } = await rootStylesInPrint(
      page,
      printDocument({ pageStyle: schemeOnly }),
    );
    expect(colorScheme).toBe('light');
    // index.html's dark charcoal (#191919) survives on the un-failable inlined
    // path, and a light scheme paints the UA's default text black on top of it.
    expect(background).toBe('rgb(25, 25, 25)');
  });

  test('the page stays legible when only the lazy chunk is missing', async ({ page }) => {
    // The asymmetric miss: tokens.css ships in the main bundle behind a static
    // <link> that every page load warms, while the modal's chunk is fetched once
    // per session — so "tokens present, chunk absent" is the likely half of the
    // failure, and it is the one a white page makes worse. Markdown.tsx sets
    // color/background inline from var(--color-*), and the iframe has no
    // data-theme, so those resolve to tokens.css's dark :root branch.
    //
    // Foregrounds and backgrounds are checked in pairs on purpose: pinning text
    // without its background turns white-on-black into dark-on-black, which is
    // just as invisible and passes any test that only looks at the text.
    await page.emulateMedia({ media: 'print' });
    await page.setContent(`<!DOCTYPE html><html><head><meta charset="utf-8">
      <style>${PRINT_PAGE_STYLE}</style>
      <style>${indexHtmlInlineStyle()}</style>
      <style>${read('src/styles/tokens.css')}</style>
      <!-- ExportPreviewModal.css never arrives -->
      <style>${PAGED_PRINT_STYLES}</style>
      </head><body><div class="markdown-print-content">
        <p id="para" style="color: var(--color-text-primary)">Paragraph.</p>
        <code id="code" style="background-color: var(--color-bg-code); color: var(--color-text-primary)">inline</code>
        <table><thead id="head" style="background-color: var(--color-bg-input)">
          <tr><th style="color: var(--color-text-primary)">Header</th></tr></thead></table>
        <del id="del" style="color: var(--color-text-tertiary)">struck</del>
      </div></body></html>`);

    const probes = await page.evaluate(() => {
      const pageBg = getComputedStyle(document.documentElement).backgroundColor;
      // An element with no background of its own sits on the page canvas.
      const pair = (id) => {
        const s = getComputedStyle(document.getElementById(id));
        return { fg: s.color, bg: s.backgroundColor === 'rgba(0, 0, 0, 0)' ? pageBg : s.backgroundColor };
      };
      return { para: pair('para'), code: pair('code'), head: pair('head'), del: pair('del') };
    });

    for (const [name, { fg, bg }] of Object.entries(probes)) {
      expect(
        Math.abs(luminanceOver(fg, bg) - luminanceOver(bg, bg)),
        `${name} rendered ${fg} on ${bg} — invisible without the modal stylesheet`,
      ).toBeGreaterThan(40);
    }
  });

  test('table rules and dividers are visible with or without the lazy chunk', async ({ page }) => {
    // Unlike the colour pins, this one also repairs the happy path: the chunk
    // styles `hr` and `td` but never the table wrapper or `tr`, which carry only
    // var(--color-border-muted) — the dark rgba(255,255,255,.08). Those rules
    // were invisible in every export, not just the fallback.
    const chrome = read('src/pages/ChatAgent/components/ExportPreviewModal.css');
    for (const chunkArrives of [true, false]) {
      await page.emulateMedia({ media: 'print' });
      await page.setContent(`<!DOCTYPE html><html><head><meta charset="utf-8">
        <style>${PRINT_PAGE_STYLE}</style>
        <style>${indexHtmlInlineStyle()}</style>
        <style>${read('src/styles/tokens.css')}</style>
        ${chunkArrives ? `<style>${chrome}</style>` : ''}
        <style>${PAGED_PRINT_STYLES}</style>
        </head><body><div class="markdown-print-content">
          <hr id="hr" style="border-color: var(--color-border-muted)">
          <div id="wrap" style="border: 1px solid var(--color-border-muted)">
            <table><tbody><tr id="row" style="border-bottom: 1px solid var(--color-border-muted)">
              <td>cell</td></tr></tbody></table></div>
        </div></body></html>`);

      const borders = await page.evaluate(() => {
        const pageBg = getComputedStyle(document.documentElement).backgroundColor;
        const edge = (id, prop) => getComputedStyle(document.getElementById(id))[prop];
        return {
          pageBg,
          hr: edge('hr', 'borderTopColor'),
          wrapper: edge('wrap', 'borderTopColor'),
          row: edge('row', 'borderBottomColor'),
        };
      });

      const { pageBg, ...edges } = borders;
      for (const [name, color] of Object.entries(edges)) {
        expect(
          Math.abs(luminanceOver(color, pageBg) - luminanceOver(pageBg, pageBg)),
          `${name} border rendered ${color} on ${pageBg} (chunk ${chunkArrives ? 'present' : 'missing'})`,
        ).toBeGreaterThan(15);
      }
    }
  });

  test('every themed var Markdown.tsx renders inline is pinned or known-safe', async () => {
    // The probes above are hand-written, so they only ever cover the elements
    // someone thought to list. This derives the list from Markdown.tsx instead,
    // so an eighth var added later fails here rather than shipping an invisible
    // element nobody probed. Anything not pinned needs a measured reason.
    const SAFE_UNPINNED = {
      '--color-accent-primary': 'dark value #4161A4 — mid blue, legible on white',
      '--color-accent-overlay': 'rgba(75,107,174,.5) — blockquote rule, visible on white',
      '--color-border-elevated': 'dark value #34363A — dark grey border/rule (luminance ~54 vs 255), visible on white',
    };
    const used = new Set(
      [...read('src/pages/ChatAgent/components/Markdown.tsx').matchAll(/var\((--color-[a-z0-9-]+)\)/g)]
        .map((m) => m[1]),
    );
    expect(used.size, 'expected Markdown.tsx to still render themed vars inline').toBeGreaterThan(0);

    const unaccounted = [...used].filter(
      (v) => !PRINT_PAGE_STYLE.includes(`${v}:`) && !(v in SAFE_UNPINNED),
    );
    expect(
      unaccounted,
      `${unaccounted.join(', ')} resolve to tokens.css's dark :root inside the print iframe. ` +
        'Pin in PRINT_PAGE_STYLE (foreground and background together), or add to SAFE_UNPINNED with a measured reason.',
    ).toEqual([]);
  });

  test('the stylesheet does not reintroduce color-scheme outside the print iframe', async () => {
    // Keeping it in pageStyle is what confines it to the export; a declaration
    // in the component stylesheet would also retint the app's own Ctrl+P output,
    // because the lazy chunk stays in the document after the modal closes.
    const css = read('src/pages/ChatAgent/components/ExportPreviewModal.css');
    const withoutComments = css.replace(/\/\*[\s\S]*?\*\//g, '');
    expect(withoutComments).not.toMatch(/color-scheme\s*:/);
  });

  test('pagedjs still sets print-color-adjust:exact, the co-factor being defended against', async () => {
    // If pagedjs stops forcing exact color adjustment the dark canvas would no
    // longer ink, and this whole defense could be reconsidered.
    const paged = read('node_modules/pagedjs/dist/paged.js');
    expect(paged).toMatch(/print-color-adjust:\s*exact/);
  });
});
