/**
 * The auth surface answers focus in its own language: no ring anywhere, an
 * ember halo on a field a keyboard reached, and each button or link lighting in
 * the colour this page reserves for a control about to act.
 *
 * Two things break silently here and neither is visible from a component. A
 * field matches `:focus-visible` for a mouse click as well as a Tab, which is
 * spec behaviour rather than a quirk, so the ember lit on every click for as
 * long as that rule existed. And once the app-wide ring is suppressed on this
 * surface, a control added without a focus treatment has nothing at all, which
 * no unit test can see because jsdom paints no cascade.
 *
 * That second failure is per view, not per page: the surface is four screens
 * behind one class, and a screen the audit never opens is a screen where every
 * control can be dark. So the tab walk runs on each of them, reached the way a
 * user reaches them.
 *
 * Runs against the `auth-surface` project, which boots its own dev server in
 * platform mode. Only the send that opens the inbox screen is answered, by a
 * route handler rather than a server, so nothing here reaches a network.
 */
import { test, expect } from '@playwright/test';

import { outlineOn, tabTo, unpainted } from './helpers/focusPaint.js';

const EMAIL = 'input[type=email]';
const SUBMIT = '.login-page__submit';
const TOGGLE = '.login-page__password-toggle';
/**
 * Everything a Tab can land on, in the view under test. Disabled controls are
 * out: they are unreachable by definition and have no focus state to check, so
 * counting them would report a permanent dark control -- the resend button
 * spends its first minute that way.
 */
const FOCUSABLE = '.login-page button:not(:disabled), .login-page a, .login-page input:not(:disabled)';

/** The paint that carries focus on this surface, once transitions settle. */
async function paintOn(page, sel) {
  return page.evaluate(async (s) => {
    const el = document.querySelector(s);
    if (!el) throw new Error(`${s} is not on the page`);
    await new Promise((r) => setTimeout(r, 300));
    const cs = getComputedStyle(el);
    return {
      focused: document.activeElement === el,
      outline: cs.outlineStyle,
      outlineColor: cs.outlineColor,
      border: cs.borderColor,
      background: cs.backgroundColor,
      color: cs.color,
      shadow: cs.boxShadow,
      underline: cs.textDecorationLine,
    };
  }, sel);
}

/** The screen the surface opens on: a list of sign-in methods. */
async function methodView(page) {
  await page.goto('/app');
  await expect(page.locator('.login-page__method-btn--primary')).toBeVisible();
}

/** Email and password, one click in from the method list. */
async function emailView(page) {
  await methodView(page);
  await page.locator('.login-page__method-btn--primary').click();
  await expect(page.locator(EMAIL)).toBeVisible();
}

/**
 * The screen after a link is sent, which owns the one control no other view
 * has. Reaching it needs a send to succeed, so the send is answered here
 * instead of by a server: this spec measures paint, and an auth backend is not
 * part of what it is measuring.
 */
async function inboxView(page) {
  await page.route('**/auth/v1/otp**', (route) =>
    route.fulfill({ status: 200, contentType: 'application/json', body: '{}' }));
  await methodView(page);
  await page.locator('.login-page__method-btn').last().click();
  await expect(page.locator(EMAIL)).toBeVisible();
  await page.locator(EMAIL).fill('someone@example.com');
  await page.locator(SUBMIT).click();
  await expect(page.locator('.login-page__resend-btn')).toBeVisible();
}

/**
 * Walk the whole tab order of whatever is on screen and return the controls
 * that never changed paint.
 *
 * Walked with real Tab presses. A programmatic focus does not match
 * :focus-visible on a button in a page no key has touched, so a loop calling
 * .focus() reports every button dark and proves nothing.
 */
async function darkAfterTabWalk(page) {
  const resting = await page.evaluate((sel) => {
    const paint = (el) => {
      const c = getComputedStyle(el);
      return [c.outlineStyle, c.borderColor, c.backgroundColor, c.color,
        c.boxShadow, c.textDecorationLine, c.textDecorationThickness].join('|');
    };
    return [...document.querySelectorAll(sel)].map((el, i) => {
      el.dataset.focusProbe = String(i);
      return { i, name: [...el.classList].find((c) => c.startsWith('login-page')) || el.tagName, paint: paint(el) };
    });
  }, FOCUSABLE);

  const lit = new Set();
  for (let press = 0; press < resting.length * 3; press += 1) {
    await page.keyboard.press('Tab');
    const hit = await page.evaluate(async () => {
      const el = document.activeElement;
      if (!(el instanceof HTMLElement) || el.dataset.focusProbe === undefined) return null;
      await new Promise((r) => setTimeout(r, 300));
      const c = getComputedStyle(el);
      return {
        i: Number(el.dataset.focusProbe),
        paint: [c.outlineStyle, c.borderColor, c.backgroundColor, c.color,
          c.boxShadow, c.textDecorationLine, c.textDecorationThickness].join('|'),
      };
    });
    if (hit && hit.paint !== resting[hit.i].paint) lit.add(hit.i);
  }

  return {
    walked: resting.length,
    dark: resting.filter((r) => !lit.has(r.i)).map((r) => r.name),
  };
}

test.describe('a field on the auth surface', () => {
  test('keeps the ember for a keyboard and the muted halo for a mouse', async ({ page }) => {
    await emailView(page);
    await page.locator(EMAIL).click();
    const clicked = await paintOn(page, EMAIL);
    expect(clicked.focused).toBe(true);
    // Unpainted rather than absent: the app-wide suppression outranks this
    // page's own `outline: none` and leaves a transparent outline behind, so
    // forced colors has something to repaint. Nothing shows either way.
    expect(unpainted(clicked.outline, clicked.outlineColor)).toBe(true);

    await emailView(page);
    await tabTo(page, EMAIL);
    const tabbed = await paintOn(page, EMAIL);
    expect(tabbed.focused).toBe(true);
    expect(unpainted(tabbed.outline, tabbed.outlineColor)).toBe(true);

    // The ember is the whole point of the keyboard case, and the click has to
    // land somewhere other than the ember rather than nowhere at all.
    expect(tabbed.border).not.toBe(clicked.border);
    expect(tabbed.shadow).not.toBe(clicked.shadow);
    expect(clicked.shadow).not.toBe('none');
  });
});

test.describe('a button or link on the auth surface', () => {
  test('wears no ring, whichever device brought focus', async ({ page }) => {
    await emailView(page);
    await tabTo(page, SUBMIT);
    expect((await outlineOn(page, SUBMIT)).style).toBe('none');

    // A control the mouse pressed should not light at all. The password toggle
    // is the one button here that can be clicked without leaving the view.
    await page.locator(TOGGLE).click();
    const clicked = await paintOn(page, TOGGLE);
    expect(clicked.focused).toBe(true);
    expect(unpainted(clicked.outline, clicked.outlineColor)).toBe(true);
    expect(clicked.background).toMatch(/^rgba\(0, 0, 0, 0\)$|^transparent$/);
  });
});

/**
 * The rule that makes these pass is a default rather than a list of the
 * controls that opted in, so a control added later is wrong-looking at worst.
 * Without it the failure is silent: no ring, no replacement, and nothing on
 * screen to say where the keyboard is.
 */
test.describe('every view of the auth surface', () => {
  const VIEWS = [
    { name: 'the method list', open: methodView, least: 6 },
    { name: 'the email form', open: emailView, least: 5 },
    { name: 'the check-inbox screen', open: inboxView, least: 3 },
  ];

  for (const { name, open, least } of VIEWS) {
    test(`lights every focusable on ${name}`, async ({ page }) => {
      await open(page);
      const { walked, dark } = await darkAfterTabWalk(page);
      // A view that stopped rendering its controls would otherwise pass with
      // an empty walk.
      expect(walked).toBeGreaterThanOrEqual(least);
      expect(dark).toEqual([]);
    });
  }

  test('lights the resend button, once its cooldown lets it be focused', async ({ page }) => {
    // Resend opens disabled for the minute the send rate limit lasts, so the
    // walk above skips it and it is the one control of this surface no view
    // covers. Run the minute out rather than leave the gap: it is a boxed
    // control, and boxed controls are exactly the ones whose focus treatment
    // turns the default underline off and replaces it.
    await page.clock.install();
    await inboxView(page);

    // One second at a time, not one jump: the countdown re-arms its timer from
    // an effect, so the next tick does not exist yet when the current one
    // fires. A single fastForward finds one timer and lands on 59.
    //
    // `install()` makes the clock steerable, it does not stop it -- `pauseAt`
    // is what freezes time. Measured: a 300ms setTimeout inside the page still
    // resolves in 300ms after installing. So the waits further down this file
    // are unaffected, and real time alone is still far too slow to walk out a
    // 60s cooldown inside a 30s test, which is what runFor is for.
    const resend = page.locator('.login-page__resend-btn');
    for (let tick = 0; tick < 90 && await resend.isDisabled(); tick += 1) {
      await page.clock.runFor(1000);
    }
    await expect(resend).toBeEnabled();

    const { dark } = await darkAfterTabWalk(page);
    expect(dark).toEqual([]);
  });
});

/**
 * With forced colors on, everything this surface focuses with is gone: the OS
 * re-paints backgrounds and borders from its own palette, and box-shadow is
 * dropped outright. The underline survives, but the boxed controls are the
 * ones that switch it off. So the outline the page suppresses everywhere else
 * has to come back here, and only here.
 */
test.describe('the auth surface under forced colors', () => {
  // Emulated per page rather than declared with `test.use`: the context-level
  // option leaves `(forced-colors: active)` unmatched in this browser, so the
  // block under test would never be evaluated and the assertion would fail for
  // a reason that has nothing to do with the CSS.
  test.beforeEach(async ({ page }) => {
    await page.emulateMedia({ forcedColors: 'active' });
  });

  test('gives the boxed controls an outline back', async ({ page }) => {
    await methodView(page);
    await tabTo(page, '.login-page__method-btn--primary');
    expect((await outlineOn(page, '.login-page__method-btn--primary')).style).toBe('solid');

    await emailView(page);
    await tabTo(page, SUBMIT);
    expect((await outlineOn(page, SUBMIT)).style).toBe('solid');
  });
});
