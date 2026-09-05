/**
 * A focus ring is the mouse cursor for someone navigating by keyboard, so it
 * has to be there for them and absent for everyone else. Both halves break
 * silently and neither is visible from inside a component.
 *
 * The half that broke: Radix hands focus back to the trigger when an overlay
 * closes, and Chromium propagates :focus-visible across a programmatic focus
 * move. So a menu opened and dismissed entirely with the mouse left its trigger
 * ringed until the next click, on every dropdown in the app. Nothing about that
 * is expressible in CSS, and a screenshot of the ring looks identical whether
 * it was earned by a keystroke or not. This is the only place the difference
 * can be measured.
 *
 * The other half is the one an over-eager fix takes out. Suppressing the
 * restore unconditionally would pass every mouse assertion below and leave
 * keyboard users pressing Escape into nowhere, so the keyboard cases are not
 * balance: they are the thing most at risk.
 */
import { test, expect, mockAPI } from './fixtures.js';
import { keyboardFocus, outlineOn, tabTo, unpainted } from './helpers/focusPaint.js';

// The account button in the sidebar footer, chosen because it is a plain
// DropdownMenu over the shared ui/ wrapper -- whatever is true here is true of
// the other call sites, which is the point of fixing it in the wrapper.
const ROW = '.sidebar-account-row';

// Somewhere in the page body with nothing interactive under it.
const EMPTY = { x: 760, y: 300 };

/** Is a focus indicator actually painted on the element that holds focus? */
async function ringed(page) {
  return page.evaluate((sel) => {
    const row = document.querySelector(sel);
    if (!row) throw new Error('the account row is not on the page');
    return document.activeElement === row && row.matches(':focus-visible');
  }, ROW);
}

test.beforeEach(async ({ page }) => {
  await mockAPI(page);
  await page.goto('/dashboard');
  await expect(page.locator(ROW)).toBeVisible();
});

test.describe('a flow driven entirely by the mouse', () => {
  test('leaves no ring after choosing a menu item', async ({ page }) => {
    await page.click(ROW);
    // The regression lived here: this close returns focus to the trigger.
    await page.locator('[role="menuitem"]').first().click();
    await expect.poll(() => ringed(page)).toBe(false);
  });

  test('leaves no ring after dismissing the menu', async ({ page }) => {
    await page.click(ROW);
    await page.mouse.click(EMPTY.x, EMPTY.y);
    await expect.poll(() => ringed(page)).toBe(false);
  });

  test('leaves no ring on the click that opened it', async ({ page }) => {
    await page.click(ROW);
    await page.click(ROW);
    await expect.poll(() => ringed(page)).toBe(false);
  });
});

test.describe('a keyboard user still gets an indicator', () => {
  test('when focus lands on the row', async ({ page }) => {
    await page.keyboard.press('Escape');
    await page.locator(ROW).evaluate((el) => el.focus());
    await expect.poll(() => ringed(page)).toBe(true);
  });

  test('when they open the menu and press Escape', async ({ page }) => {
    await page.locator(ROW).evaluate((el) => el.focus());
    await page.keyboard.press('Enter');
    await expect(page.locator('[role="menuitem"]').first()).toBeVisible();
    // Escape must put them back where they were, still able to see where.
    await page.keyboard.press('Escape');
    await expect.poll(() => ringed(page)).toBe(true);
  });
});

/**
 * The chart half. Inside an SVG, Chromium paints its own focus ring on plain
 * :focus -- it does not gate that on :focus-visible the way it does for HTML.
 * So a mouse click on a chart draws a ring where the same click on a button
 * draws nothing, and no amount of :focus-visible styling reaches it.
 *
 * Which node the click lands on is the part that is easy to get wrong: recharts
 * 3.8 stacks its z-index layers as `<g tabindex="-1">` inside the surface, so a
 * click on a bar focuses the layer, not the surface. The rule covering that is
 * one space in a selector (`svg :focus...`) and reads like a typo. It shipped
 * missing once and took three rounds of screenshots to find, with the whole
 * unit suite green throughout: jsdom does not paint UA outlines, so this is the
 * only place the difference exists.
 *
 * The clicks below have to be real. A programmatic .focus() matches
 * :focus-visible and is caught by the tabindex="-1" rule instead, which passes
 * whether or not the rule under test is present.
 *
 * The fixture is hand-built rather than a rendered chart on purpose: what is
 * under test is the stylesheet, and coupling it to market-data mocks would let
 * it pass vacuously the day that data stops arriving.
 */
const BAR = '#chart-fixture rect';
const BEFORE = '#before-fixture';
const PLAIN = '#plain-fixture';

// Where the fixture sits, so a click can be aimed at plot space with no bar
// under it -- the other way a chart takes focus from the mouse.
const CHART = { x: 600, y: 200, w: 200, h: 100 };
const BAR_BOX = { x: 10, y: 10, w: 100, h: 40 };
const EMPTY_PLOT = { x: CHART.x + CHART.w - 20, y: CHART.y + CHART.h - 20 };

/** A recharts surface, reduced to the parts the focus rules actually select. */
async function mountChartFixture(page, chart, bar) {
  await page.evaluate(({ chart, bar }) => {
    const NS = 'http://www.w3.org/2000/svg';
    const fixed = (css) => `position:fixed;z-index:9999;${css}`;

    // Tab lands here first, so the next Tab is a real keyboard entry into the
    // chart rather than a programmatic focus that would prove nothing.
    const before = document.createElement('button');
    before.id = 'before-fixture';
    before.type = 'button';
    before.textContent = 'before';
    before.setAttribute('style', fixed(`top:${chart.y - 40}px;left:${chart.x}px`));

    const svg = document.createElementNS(NS, 'svg');
    svg.id = 'chart-fixture';
    svg.setAttribute('class', 'recharts-surface');
    svg.setAttribute('role', 'application');
    svg.setAttribute('tabindex', '0');
    svg.setAttribute('style', fixed(`top:${chart.y}px;left:${chart.x}px;width:${chart.w}px;height:${chart.h}px`));

    const layer = document.createElementNS(NS, 'g');
    layer.setAttribute('class', 'recharts-zIndex-layer_300');
    layer.setAttribute('tabindex', '-1');
    const rect = document.createElementNS(NS, 'rect');
    for (const [k, v] of [['x', bar.x], ['y', bar.y], ['width', bar.w], ['height', bar.h]]) {
      rect.setAttribute(k, String(v));
    }
    rect.setAttribute('fill', 'currentColor');
    layer.appendChild(rect);
    svg.appendChild(layer);

    // The control the chart surface has to agree with: one baseline rule
    // serves both, and this is what proves it still does.
    const plain = document.createElement('button');
    plain.id = 'plain-fixture';
    plain.type = 'button';
    plain.textContent = 'reference';
    plain.setAttribute('style', fixed(`top:${chart.y + chart.h + 20}px;left:${chart.x}px`));

    document.body.append(before, svg, plain);
  }, { chart, bar });
}

/** What is focused right now, and the outline actually computed on it. */
test.describe('a chart clicked with the mouse', () => {
  test.beforeEach(async ({ page }) => {
    await mountChartFixture(page, CHART, BAR_BOX);
  });

  test('leaves no ring on the layer a click on a bar lands in', async ({ page }) => {
    await page.locator(BAR).click();
    const el = await outlineOn(page);
    // If this is the surface, the fixture stopped reproducing the real bug.
    expect({ tag: el.tag, tabindex: el.tabindex }).toEqual({ tag: 'g', tabindex: '-1' });
    // `auto` is the UA ring, in a blue this product uses nowhere.
    expect(el.style).toBe('none');
  });

  test('leaves no ring on the surface a click on empty plot space lands in', async ({ page }) => {
    await page.mouse.click(EMPTY_PLOT.x, EMPTY_PLOT.y);
    const el = await outlineOn(page);
    expect(el.tag).toBe('svg');
    expect(el.style).toBe('none');
  });
});

test.describe('a chart reached by keyboard', () => {
  test.beforeEach(async ({ page }) => {
    await mountChartFixture(page, CHART, BAR_BOX);
  });

  test('is a visible tab stop wearing the same ring as every other control', async ({ page }) => {
    await page.locator(BEFORE).focus();
    await page.keyboard.press('Tab');

    const surface = await outlineOn(page);
    expect(surface.tag).toBe('svg');
    expect(surface.style).toBe('solid');

    await page.locator(PLAIN).focus();
    const plain = await outlineOn(page);
    expect(plain.style).toBe('solid');
    // One baseline rule serves both; drift here means a chart grew its own.
    expect(surface.color).toBe(plain.color);
    expect(surface.width).toBe(plain.width);
  });
});

/**
 * The text-field half. A text field is the one control the browser always
 * matches :focus-visible on, a mouse click included: the heuristic exists so a
 * field about to receive typing looks focused, and it is spec behaviour, not a
 * quirk. So the baseline ring reached all 33 raw inputs the moment their
 * `outline-none` came off, and a click drew a ring the same click on a button
 * would not. No selector can separate those two cases, which is why the fix
 * stamps how focus arrived and why this file is the only place it is visible.
 *
 * The keyboard cases below are again the ones most at risk: suppressing the
 * ring on every text field would satisfy every mouse assertion here and leave
 * a keyboard user with a caret for an indicator.
 */
const SEARCH = '.dashboard-search-input';
const COMPOSER = '.chat-input-container textarea';
const WORKSPACE_SEARCH = 'input[placeholder="Search workspaces..."]';
// The pill around the Workspaces field: the field fills it, so the ring goes on
// the container. See the `rings-within` block at the bottom of this file.
const WORKSPACE_PILL = `.rings-within:has(> ${WORKSPACE_SEARCH})`;
// The name field of the Create Workspace modal: a call site of the shared
// ui/ Input, which drew a ring of its own rather than the baseline.
const SHARED_INPUT = '.cwm-modal .cwm-field input';

/**
 * One row per kind of text field in the app; `open` is how to get to it, and
 * `rings` names the element the indicator lands on when it is not the field.
 */
const CLICKED = [
  { field: 'the dashboard search box', sel: SEARCH },
  { field: 'the chat composer', sel: COMPOSER },
  {
    field: 'the Workspaces search box',
    sel: WORKSPACE_SEARCH,
    rings: WORKSPACE_PILL,
    open: (page) => page.goto('/chat'),
  },
  {
    // The shared primitive is the other half of the app's text fields: it drew
    // its own `focus-visible:ring-2` rather than the baseline, on the same
    // always-on pseudo-class, so it rang on a click for its own reasons.
    field: 'a shared ui/ Input',
    sel: SHARED_INPUT,
    open: async (page) => {
      await page.goto('/chat');
      await page.getByRole('button', { name: /new workspace/i }).first().click();
    },
  },
];

test.describe('a text field clicked with the mouse', () => {
  for (const { field, sel, open } of CLICKED) {
    test(`leaves no ring on ${field}`, async ({ page }) => {
      if (open) await open(page);
      const target = page.locator(sel);
      await expect(target).toBeVisible();

      // The resting paint is the reference, because "no ring" is not the same
      // question as "no outline": half the app draws its ring as a Tailwind
      // box-shadow, and `outline: none` cannot suppress one. Reading the shadow
      // against its own resting value is what makes restoring a `ring-2` on
      // this field fail here instead of passing quietly.
      const resting = await outlineOn(page, sel);

      await target.click();
      const clicked = await outlineOn(page, sel);
      expect(clicked.focused).toBe(true);
      expect(unpainted(clicked.style, clicked.color)).toBe(true);
      expect(clicked.shadow).toBe(resting.shadow);
    });
  }
});

test.describe('a text field reached by keyboard', () => {
  // The mouse cases above are the ones a regression re-opens; these are the
  // ones an over-eager fix closes. Suppressing the ring on every text field
  // satisfies every assertion in the block above and leaves a keyboard user
  // with a caret for an indicator, so each field family has to answer both.
  for (const { field, sel, rings, open } of CLICKED) {
    test(`rings ${field}`, async ({ page }) => {
      if (open) await open(page);
      await expect(page.locator(sel)).toBeVisible();
      await keyboardFocus(page, sel);
      expect(await outlineOn(page, sel)).toMatchObject({ focused: true });
      expect(await outlineOn(page, rings ?? sel)).toMatchObject({ style: 'solid' });
    });
  }

  test('wears the same ring as every other control', async ({ page }) => {
    // The tab order is only complete once the route has mounted; walking it
    // early wraps through a shorter ring and never arrives.
    await expect(page.locator(SEARCH)).toBeVisible();
    await tabTo(page, SEARCH);
    const field = await outlineOn(page, SEARCH);
    expect(field.style).toBe('solid');

    // One baseline rule serves both; drift here means a field grew its own.
    // The reference is a plain button rather than a control off the page:
    // several draw their ring as an inset shadow instead (the account row
    // does), and an outline of `none` would make this pass vacuously.
    await page.evaluate(() => {
      const plain = document.createElement('button');
      plain.id = 'plain-text-reference';
      plain.type = 'button';
      plain.setAttribute('style', 'position:fixed;z-index:9999;top:8px;left:8px');
      document.body.append(plain);
    });
    await page.locator('#plain-text-reference').evaluate((el) => el.focus());
    const reference = await outlineOn(page);
    expect(reference.style).toBe('solid');
    expect(field.color).toBe(reference.color);
    expect(field.width).toBe(reference.width);
  });

  test('still rings after the click that focused it is followed by typing', async ({ page }) => {
    // The trap in gating on "last input device": typing is a keydown, so a
    // global flag would light the ring under the user mid-sentence.
    await page.click(SEARCH);
    await page.keyboard.type('AAPL');
    const typed = await outlineOn(page, SEARCH);
    expect(typed.focused).toBe(true);
    expect(unpainted(typed.style, typed.color)).toBe(true);
  });

  test('leaves an outline box for forced colors to repaint', async ({ page }) => {
    // Forced colors repaints outlines out of the system palette, but it cannot
    // paint one that was never drawn -- and it drops the box-shadow and
    // normalizes the border that would otherwise mark a clicked field, so the
    // outline is the last thing standing. Suppressing with `none` leaves a
    // high-contrast user a caret and nothing else, which is why the rule writes
    // a transparent 2px outline instead. Read from the computed style rather
    // than under emulation: emulating the media query does not perform the
    // colour substitution that makes the outline visible, so the substitution
    // is the browser's half of the contract and this is ours.
    await page.click(SEARCH);
    const clicked = await outlineOn(page, SEARCH);
    expect(clicked.style).toBe('solid');
    expect(clicked.color).toMatch(/,\s*0\)$/);
  });
});

/**
 * A field that fills a bordered container has nowhere to put a ring of its own:
 * an outline on the field alone cuts across the container's border and leaves
 * the icon beside it outside the indicator. `rings-within` moves the ring onto
 * the container, and has to carry the same device gate the field does -- which
 * is the half a container rule is most likely to miss, since :focus-within has
 * no :focus-visible counterpart to inherit it from.
 */
test.describe('a field whose container is the indicator', () => {
  const PILL = WORKSPACE_PILL;

  test.beforeEach(async ({ page }) => {
    await page.goto('/chat');
    await expect(page.getByPlaceholder('Search workspaces...')).toBeVisible();
  });

  test('rings the Workspaces pill when the field inside it is tabbed to', async ({ page }) => {
    await keyboardFocus(page, `${PILL} > input`);
    expect(await outlineOn(page, PILL)).toMatchObject({ style: 'solid' });
  });

  test('leaves the pill unringed on a click', async ({ page }) => {
    const resting = await outlineOn(page, PILL);
    await page.getByPlaceholder('Search workspaces...').click();
    const clicked = await outlineOn(page, PILL);
    expect(unpainted(clicked.style, clicked.color)).toBe(true);
    expect(clicked.shadow).toBe(resting.shadow);
  });

  test('leaves the field itself unringed either way, so the two never stack', async ({ page }) => {
    const field = `${PILL} > input`;
    await keyboardFocus(page, field);
    expect(await outlineOn(page, field)).toMatchObject({ focused: true, style: 'none' });
  });
});

/**
 * The half that survives leaving the browser.
 *
 * Switching to another app does not release element focus, it parks it:
 * `document.activeElement` is unchanged across the whole trip, and the browser
 * fires a `focusout`/`focusin` pair around it as bookkeeping. That restoring
 * `focusin` is indistinguishable from any other, and by the time it arrives the
 * recorded device has been flipped to keyboard by the user's own typing -- so
 * the composer someone clicked, typed into, and left for the length of an agent
 * turn came back wearing the ring. It is the shape of bug that reads as "it
 * still happens sometimes": the trip out and back is what triggers it, and
 * nothing about the app does.
 *
 * The trip is synthesized because Playwright cannot make one. It pins every
 * page it drives to `Emulation.setFocusEmulationEnabled`, so a backgrounded tab
 * still believes it has focus and fires nothing -- verified by driving a real
 * Chrome with `osascript` instead, which is also where the sequence replayed
 * below was measured. Everything else here is real: the app, the stylesheet,
 * the click, the typing, and the paint that gets read back.
 */
const PARKED = [
  { field: 'the chat composer', sel: COMPOSER },
  { field: 'the dashboard search box', sel: SEARCH },
];

/** What the browser fires around a trip out of the window and back. */
async function leaveWindowAndReturn(page) {
  await page.evaluate(() => {
    const el = document.activeElement;
    const pair = { bubbles: true, composed: true, relatedTarget: null };
    el.dispatchEvent(new FocusEvent('focusout', pair));
    window.dispatchEvent(new FocusEvent('blur'));
    window.dispatchEvent(new FocusEvent('focus'));
    el.dispatchEvent(new FocusEvent('focusin', pair));
  });
}

test.describe('a field left focused while the user is in another app', () => {
  for (const { field, sel } of PARKED) {
    test(`comes back unringed on ${field}`, async ({ page }) => {
      await expect(page.locator(sel)).toBeVisible();
      const resting = await outlineOn(page, sel);
      await page.click(sel);
      // Typing is what makes this reachable: it flips the recorded device to
      // keyboard while focus sits still, so the restore has stale evidence to
      // read. Without it the trip is harmless and this passes unfixed.
      await page.keyboard.type('AAPL');
      await leaveWindowAndReturn(page);

      const returned = await outlineOn(page, sel);
      expect(returned.focused).toBe(true);
      expect(unpainted(returned.style, returned.color)).toBe(true);
      expect(returned.shadow).toBe(resting.shadow);
    });
  }

  test('comes back still ringed for someone who tabbed to it', async ({ page }) => {
    // The over-eager fix passes every assertion above by never re-deciding at
    // all. A keyboard user has to leave and return to their own indicator.
    await expect(page.locator(SEARCH)).toBeVisible();
    await keyboardFocus(page, SEARCH);
    expect(await outlineOn(page, SEARCH)).toMatchObject({ style: 'solid' });

    await leaveWindowAndReturn(page);
    expect(await outlineOn(page, SEARCH)).toMatchObject({ focused: true, style: 'solid' });
  });

  test('leaves the next real focus move free to decide for itself', async ({ page }) => {
    // Passing over the restore must consume exactly one focusin. If the flag
    // outlives it, the first control the user reaches after coming back
    // inherits the verdict of the one they left.
    await expect(page.locator(SEARCH)).toBeVisible();
    await page.click(SEARCH);
    await page.keyboard.type('AAPL');
    await leaveWindowAndReturn(page);

    await tabTo(page, ROW);
    expect(await outlineOn(page, ROW)).toMatchObject({ focused: true });
    const ring = await outlineOn(page, ROW);
    expect(ring.style === 'solid' || ring.shadow !== 'none').toBe(true);
  });
});
