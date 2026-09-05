/**
 * The desktop shell hides the macOS titlebar and lets the window buttons float
 * over the top-left of the page. Two things have to stay true for that to work,
 * and neither is visible from inside a single component:
 *
 *   1. This build declares that it reserves. The shell reads that declaration
 *      to decide whether the next window opens frameless, so deleting the meta
 *      silently pins every install to a framed window forever, with no error.
 *   2. Nothing paints under the buttons. Inside the app shell the sidebar
 *      reserves the strip. Outside it — setup, legal, a shared chat — nothing
 *      does, and those screens are clear today only because of where their
 *      layouts happen to put content. That is worth holding still: a logo added
 *      to the top-left of the setup wizard would land under the close button.
 *
 * The bridge is injected rather than mocked at the module level, because the
 * decision is made by the inline script in index.html before the bundle runs.
 * Injecting it is therefore the only way to exercise the real path.
 */
import { test, expect, mockAPI } from './fixtures.js';

// Where the buttons sit in a hiddenInset window: three lights at x=13/33/53,
// plus the padding Electron leaves around them.
const BUTTON_RECT = { w: 78, h: 38 };

const SHELL_BRIDGE = { version: '0.0.0-e2e', platform: 'darwin', windowChrome: 'hidden' };

// Every route that renders no chrome of its own, and so is left with the
// fallback strip as its only drag region. Two suites below sweep it for two
// different properties; a route added to one list and not the other would
// quietly keep half its coverage.
//
// The two legal documents are here even though the shell no longer opens either
// one (every link to them carries a target, so it reaches the system browser
// instead). They stay because the reason they were exposed is the general one:
// a route of prose has nothing in the `no-drag` list, so the strip covers its
// top line. If one is ever linked back into the app window, this is what still
// measures what that costs.
const CHROMELESS_ROUTES = ['/setup/method', '/privacy', '/legal', '/s/no-such-token'];

// Any valid v4 UUID; nothing has to exist behind it for the thread gallery to
// render its back-button row, which is the bar this suite measures.
const E2E_WORKSPACE_ID = '11111111-1111-4111-8111-111111111111';

// And every route that renders INSIDE the app shell. There the sidebar reserves
// and drags its own column and the fallback strip has stood down, which leaves
// the content column beside it entirely to the route: a route with no top bar of
// its own to mark `data-chrome="drag"` owes the window a `.chrome-drag-strip`, or
// nothing right of the sidebar moves the window at all. Four routes shipped
// without one, and the shape of the miss is why this list is swept rather than
// spot-checked -- see the test.
//
// `/chat` appears twice on purpose. One path segment later ChatAgent swaps the
// workspace gallery for the thread gallery, a different component with a
// different top row, so the bare route proves nothing about the one below it.
// Any route whose component branches on a param owes this list both branches.
//
// The id has to be a well-formed UUID. ChatAgent runs the param through
// isValidUuid and treats anything else as absent, so a readable placeholder
// like `ws-e2e` silently lands back on the workspace gallery and the test
// passes while measuring the route above the one it names.
const APP_ROUTES = ['/dashboard', '/chat', `/chat/${E2E_WORKSPACE_ID}`, '/market', '/plugins', '/automations', '/settings', '/news/1'];

async function asDesktopShell(page, bridge = SHELL_BRIDGE) {
  await page.addInitScript((value) => {
    Object.defineProperty(window, 'langalphaDesktop', { value, configurable: true });
  }, bridge);
}

/**
 * What a user would see under the window buttons.
 *
 * Text nodes and controls, not "what element is at that point" — every page has
 * a full-bleed container in the corner and its ground is not a collision.
 * A bare `svg` is counted only at control scale: a page-scale decorative vector
 * is a backdrop, and one drawn deliberately in the corner would be inside a
 * link or a button, which this does catch.
 */
async function cornerOccupants(page) {
  return page.evaluate(({ w, h }) => {
    const overlaps = (r) =>
      r.width > 0 && r.height > 0 && r.left < w && r.top < h && r.right > 0 && r.bottom > 0;
    const found = [];

    const walk = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
    for (let n = walk.nextNode(); n; n = walk.nextNode()) {
      if (!n.nodeValue || !n.nodeValue.trim()) continue;
      const range = document.createRange();
      range.selectNodeContents(n);
      if (overlaps(range.getBoundingClientRect())) {
        found.push(`text "${n.nodeValue.trim().slice(0, 30)}"`);
      }
    }

    const controls = document.querySelectorAll(
      'a, button, input, select, textarea, img, svg, [role="button"], [role="tab"]',
    );
    for (const el of controls) {
      if (el.closest('#window-drag')) continue;
      const r = el.getBoundingClientRect();
      if (!overlaps(r)) continue;
      if (el.tagName.toLowerCase() === 'svg' && (r.width > 240 || r.height > 120)) continue;
      if (getComputedStyle(el).visibility === 'hidden') continue;
      found.push(el.tagName.toLowerCase() + (el.getAttribute('aria-label') ? `[${el.getAttribute('aria-label')}]` : ''));
    }
    return found;
  }, BUTTON_RECT);
}

test.describe('desktop window chrome', () => {
  test('index.html declares that this build reserves the button strip', async ({ page }) => {
    await page.goto('/');
    // Absence is not "no" to the shell, it is "unknown", and unknown keeps the
    // last answer — which on a fresh install is the framed default, forever.
    await expect(page.locator('meta[name="langalpha-window-chrome"]')).toHaveAttribute(
      'content',
      'reserves',
    );
  });

  test('the shell hides the strip until the desktop bridge says the titlebar is gone', async ({ page }) => {
    await mockAPI(page);
    await page.goto('/');
    await page.waitForSelector('.app-main', { timeout: 15_000 });

    // No bridge: a plain browser, and every reservation must collapse. This is
    // the mutation check for the gate — if `desktop-mac` were stamped here,
    // mobile web would carry a 38px band it has no window buttons for.
    expect(await page.evaluate(() => document.documentElement.classList.contains('desktop-mac'))).toBe(false);
    // Both strips are in the DOM unconditionally and gated in CSS, so the
    // property to assert is that neither takes any space, not that neither
    // exists. `boundingBox()` is null for a `display: none` element, which is
    // the same answer for a strip that was never rendered.
    expect(await page.locator('.sidebar-window-drag').boundingBox()).toBe(null);
    expect(
      await page.evaluate(() => getComputedStyle(document.querySelector('#window-drag')).display),
    ).toBe('none');
  });

  test('the app shell reserves the strip when the titlebar is hidden', async ({ page }) => {
    await asDesktopShell(page);
    await mockAPI(page);
    await page.goto('/');
    await page.waitForSelector('.app-main', { timeout: 15_000 });

    const strip = page.locator('.sidebar-window-drag');
    await expect(strip).toHaveCount(1);
    expect((await strip.boundingBox()).height).toBe(BUTTON_RECT.h);
    expect(await cornerOccupants(page)).toEqual([]);
  });

  // The fixed strip in index.html exists for the window whose bundle never ran,
  // and for every route that renders no chrome of its own. It spans the window
  // and paints under #root, so the `no-drag` rule covers the controls inside it.
  // It still has to stand down once the sidebar renders the real one: a page
  // owning its top row should own all of it, and a plain div carrying an onClick
  // is not in that rule's list. That handover is what this pins.
  test('the fallback drag strip stands down once the sidebar renders its own', async ({ page }) => {
    await asDesktopShell(page);
    await mockAPI(page);
    await page.goto('/');
    await page.waitForSelector('.app-main', { timeout: 15_000 });

    await expect(page.locator('.sidebar-window-drag')).toHaveCount(1);
    expect(
      await page.evaluate(() => getComputedStyle(document.querySelector('#window-drag')).display),
    ).toBe('none');

    // And the main column's top-left is genuinely reachable: whatever sits at
    // the first pixel past the sidebar must not be the overlay.
    const covering = await page.evaluate(() => {
      const x = parseInt(getComputedStyle(document.documentElement).getPropertyValue('--sidebar-width'), 10);
      const el = document.elementFromPoint(x + 8, 19);
      return el ? el.closest('#window-drag') !== null : false;
    });
    expect(covering).toBe(false);
  });

  // Everything outside the app shell. None of these reserve anything, so this
  // asserts the property directly rather than the mechanism.
  // The fallback spans the titlebar on every route that renders no chrome of its
  // own, and it is the ONLY drag region those routes have. Two properties, both
  // of which failed in production before this: it has to be wide enough to aim
  // at (it was 120px, most of which the window buttons cover, so a drag landed
  // on the page and selected text instead), and every control under it has to
  // keep its own clicks. That second one takes two assertions, not one, because
  // it takes two mechanisms: the drag region has to be subtracted (`no-drag` in
  // chrome.css, which resolves in layout-tree order, so DOM position decides it
  // and z-index does not), AND the strip has to stay out of the hit test, which
  // it does by declining pointer events. Assert only the first and a strip that
  // eats every click on the route still reports healthy.
  //
  // Not pinned here, because CDP-synthesized mouse events never reach the hit
  // test that owns `-webkit-app-region`: that dragging the strip moves the
  // window. This asserts the geometry and the computed regions that decide it.
  // `/` is deliberately absent: signed in it IS the app shell, so the sidebar's
  // strip retires this one, and the test above pins that handover instead.
  for (const route of CHROMELESS_ROUTES) {
    test(`the titlebar is draggable and its controls still click on ${route}`, async ({ page }) => {
      await asDesktopShell(page);
      await mockAPI(page);
      await page.goto(route);
      // The same precondition the corner test below uses, for the same reason:
      // these routes are lazy, so `.page-loading` is absent before it mounts as
      // well as after it goes, and the trapped-control sweep below has nothing
      // to sweep on a document that never rendered.
      await page.waitForFunction(
        () => !document.querySelector('.page-loading') && document.body?.innerText.trim().length > 0,
      );

      const strip = await page.evaluate(() => {
        const el = document.querySelector('#window-drag');
        if (!el) return null;
        const r = el.getBoundingClientRect();
        const cs = getComputedStyle(el);
        return { w: Math.round(r.width), h: Math.round(r.height), top: Math.round(r.top),
          left: Math.round(r.left), region: cs.webkitAppRegion, display: cs.display };
      });
      expect(strip, `${route} has no #window-drag`).not.toBeNull();
      expect(strip.display).toBe('block');
      expect(strip.region).toBe('drag');
      // Spans the window, flush to the corner the buttons float over.
      expect(strip.top).toBe(0);
      expect(strip.left).toBe(0);
      expect(strip.w).toBe(page.viewportSize().width);
      expect(strip.h).toBe(BUTTON_RECT.h);

      // The strip must not be what the mouse lands on. This is a SEPARATE
      // property from the drag region below, and the one that actually broke: a
      // fixed element beats every in-flow element that is not itself positioned,
      // so a statically positioned control scrolling under the titlebar had its
      // clicks eaten while still computing `no-drag` -- the region was
      // subtracted correctly, and the hit test took the click anyway. Asserting
      // the region alone reports that arrangement as healthy.
      const eaten = await page.evaluate((stripH) => {
        const y = Math.round(stripH / 2);
        const w = document.documentElement.clientWidth;
        return [8, w * 0.25, w * 0.5, w * 0.75, w - 8]
          .map((x) => document.elementFromPoint(Math.round(x), y))
          .filter((el) => el && el.closest('#window-drag'))
          .length;
      }, BUTTON_RECT.h);
      expect(eaten, `#window-drag is taking the mouse on ${route}`).toBe(0);

      // Anything clickable the strip covers must have taken its region back. A
      // control this misses is not merely awkward, it is UNCLICKABLE, and only
      // in the desktop shell — ordinary browser QA never sees it.
      const trapped = await page.evaluate((stripH) => {
        const out = [];
        const sel = 'a,button,input,textarea,select,label,summary,[role="button"],'
          + '[role="tab"],[role="menuitem"],[role="option"],[role="switch"],[role="checkbox"]';
        for (const el of document.querySelectorAll(sel)) {
          const r = el.getBoundingClientRect();
          if (r.width === 0 || r.height === 0) continue;
          if (r.top >= stripH) continue;
          if (getComputedStyle(el).webkitAppRegion !== 'no-drag') {
            out.push(`<${el.tagName.toLowerCase()}> "${(el.innerText || el.getAttribute('aria-label') || '').trim().slice(0, 30)}"`);
          }
        }
        return out;
      }, BUTTON_RECT.h);
      expect(trapped, `controls under the drag strip on ${route} that cannot be clicked`).toEqual([]);
    });
  }

  for (const route of CHROMELESS_ROUTES) {
    test(`nothing paints under the window buttons on ${route}`, async ({ page }) => {
      await asDesktopShell(page);
      await mockAPI(page);
      await page.goto(route);
      // The settled page, and not a sleep. Both assertions below pass on an EMPTY
      // document — `cornerOccupants` finds nothing in a body with nothing in it,
      // and `desktop-mac` is stamped by the head script before the bundle has
      // run — so without a positive precondition these four tests reported that
      // nothing overlaps the window buttons on a page that never rendered. A
      // slow CI runner was the only thing between them and a vacuous green.
      //
      // `.page-loading` has to be gone specifically, not just "some text
      // present": these routes are lazy, and the loader's decorative quote wall
      // is itself full-bleed text, so it satisfies any weaker precondition while
      // standing exactly where the assertion is looking.
      await page.waitForFunction(
        () => !document.querySelector('.page-loading') && document.body?.innerText.trim().length > 0,
      );

      expect(await page.evaluate(() => document.documentElement.classList.contains('desktop-mac'))).toBe(true);
      expect(await cornerOccupants(page)).toEqual([]);
    });
  }

  // Swept across the whole row rather than sampled in the middle, because both
  // ways this fails leave the middle working. A strip dropped inside a page that
  // carries the column's padding on its root starts BELOW the window's top edge
  // and stops short of BOTH sides -- and the left shortfall lands exactly in the
  // gap between this strip and the sidebar's own, which is where a drag aimed at
  // "the empty bit at the top" tends to go. One sample at x=50% passes on all of
  // it. A strip parked inside a scrolling page is the third shape: it rides the
  // content out of view, so the titlebar works until the user scrolls.
  for (const route of APP_ROUTES) {
    test(`the top of the content column moves the window on ${route}`, async ({ page }) => {
      await asDesktopShell(page);
      await mockAPI(page);
      await page.goto(route);
      // The settled CONTENT COLUMN, and not merely a settled document. The
      // sweeps above wait on `document.body`, which inside the app shell is
      // satisfied by the sidebar alone -- and /chat opens behind a full-height
      // curtain at opacity 0, so the row was swept while the route it belongs to
      // had not painted, and every point read as dead. Waiting on `.main` waits
      // for the column the assertion is about.
      await page.waitForFunction(
        () => !document.querySelector('.page-loading')
          && document.querySelector('.app-main .main')?.innerText.trim().length > 0,
      );

      // Still the route this test names. A route whose data 404s can navigate
      // itself somewhere else before the sweep runs -- /chat/<id> does exactly
      // that, its not-found effect replacing the URL with /chat -- and the sweep
      // then measures a page that was never in question and reports it under the
      // name of the one that was. Mocks keep it here; this is what notices when
      // they stop.
      expect(new URL(page.url()).pathname).toBe(route);

      const dead = await page.evaluate((stripH) => {
        const main = document.querySelector('.app-main');
        if (!main) return ['no .app-main'];
        const box = main.getBoundingClientRect();
        const y = Math.round(stripH / 2);
        const out = [];
        // The 24px stride can stop up to a stride short of the right edge, and a
        // page that carries its padding on the root falls short by exactly that
        // sort of distance. Sample the far edge outright rather than hoping the
        // stride lands inside the gap.
        const xs = [];
        for (let x = Math.round(box.left) + 2; x < box.right - 2; x += 24) xs.push(x);
        xs.push(Math.round(box.right) - 3);
        for (const x of xs) {
          const el = document.elementFromPoint(x, y);
          let region = 'none';
          for (let node = el; node; node = node.parentElement) {
            const r = getComputedStyle(node).webkitAppRegion;
            if (r === 'drag' || r === 'no-drag') { region = r; break; }
          }
          // `no-drag` is a control taking its own clicks back, which is the whole
          // point of the rule in chrome.css and is expected wherever a route puts
          // a real bar in the titlebar row. `none` is the row never having been a
          // drag region at all, which is the bug.
          if (region === 'none') {
            out.push(`x=${x} <${el ? el.tagName.toLowerCase() : 'nothing'}` +
              `${el && el.className ? ` class="${String(el.className).slice(0, 40)}"` : ''}>`);
          }
        }
        return out;
      }, BUTTON_RECT.h);

      expect(dead, `dead points in the titlebar row on ${route}`).toEqual([]);
    });
  }

  // The third shape, and the one the sweep above cannot see: a strip parked
  // inside a page that scrolls sits in the right row at load and then rides the
  // content out of view on the first wheel event, so the titlebar works until
  // the user scrolls and the sweep passes the whole time. `/news/:id` is the
  // long route -- an article -- and was exactly that until it grew a scroll port
  // of its own.
  test('the titlebar stays put once a long route is scrolled', async ({ page }) => {
    await asDesktopShell(page);
    await mockAPI(page, {
      'GET /news/*': {
        id: 'news-long',
        title: 'Markets Rally on Strong Earnings',
        published_at: '2025-01-01T12:00:00Z',
        source: { name: 'Reuters', favicon_url: '' },
        tickers: ['AAPL'],
        // Long enough to overflow any window this suite runs in.
        description: 'Lorem ipsum dolor sit amet. '.repeat(2000),
        sentiments: [],
        article_url: 'https://example.com/article',
      },
    });
    await page.goto('/news/1');
    await page.waitForFunction(
      () => !document.querySelector('.page-loading')
        && document.querySelector('.app-main .main')?.innerText.trim().length > 0,
    );

    // Whichever element ended up owning the overflow -- that is the question, so
    // the test must not assume the answer.
    const scrolled = await page.evaluate(() => {
      let moved = 0;
      for (const el of document.querySelectorAll('*')) {
        if (el.scrollHeight > el.clientHeight + 4) {
          el.scrollTop = el.scrollHeight;
          if (el.scrollTop > 0) moved += 1;
        }
      }
      return moved;
    });
    expect(scrolled, 'nothing scrolled; the article is not long enough to test').toBeGreaterThan(0);

    const region = await page.evaluate((stripH) => {
      const box = document.querySelector('.app-main').getBoundingClientRect();
      const el = document.elementFromPoint(Math.round((box.left + box.right) / 2), Math.round(stripH / 2));
      for (let node = el; node; node = node.parentElement) {
        const r = getComputedStyle(node).webkitAppRegion;
        if (r === 'drag' || r === 'no-drag') return r;
      }
      return `none <${el ? el.tagName.toLowerCase() : 'nothing'}>`;
    }, BUTTON_RECT.h);
    expect(region).toBe('drag');
  });

  test('a pre-0.1.1 shell still gets the strip from the platform guess', async ({ page }) => {
    // The bridge without `windowChrome` is what an already-installed older shell
    // injects. It cannot answer per window, so the page falls back to macOS,
    // which is what that shell always meant.
    await asDesktopShell(page, { version: '0.1.0', platform: 'darwin' });
    await mockAPI(page);
    await page.goto('/');
    await page.waitForSelector('.app-main', { timeout: 15_000 });
    expect(await page.evaluate(() => document.documentElement.classList.contains('desktop-mac'))).toBe(true);
  });
});
