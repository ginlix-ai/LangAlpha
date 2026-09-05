/**
 * The effort and speed lists in the composer's model menu, reached the way
 * someone without a mouse reaches them.
 *
 * These were plain `<div role="menuitemradio">` once. That reads correctly to a
 * screen reader and works under a mouse, which is why it survived: the failure
 * is that a div is not a member of the menu's item collection, so roving focus
 * steps straight over it. Arrowing into the list landed nowhere and Enter did
 * nothing -- the options existed and could not be operated. Nothing about that
 * is visible from inside the component, and jsdom has no roving focus to skip,
 * so a unit test can only assert which component was used. Whether focus
 * actually arrives is a question with an answer in one place, here.
 *
 * The mouse half is the other risk. Menu items deliberately wear no ring --
 * `outline-none` in the primitive, indication via `data-[highlighted]` for
 * pointer and keyboard alike -- so the fix must not have handed these rows the
 * app's baseline `:focus-visible` ring on the way in.
 */
import { test, expect, mockAPI } from './fixtures.js';
import { defaultResponses } from './helpers/mockResponses.js';
import { unpainted } from './helpers/focusPaint.js';

const MODEL = 'claude-sonnet-4-20250514';

/** Mirrors the server's deep merge closely enough for this spec: object into
 *  object, a null deletes its key, anything else replaces. */
function merge(base, patch) {
  const out = { ...base };
  const plain = (v) => v && typeof v === 'object' && !Array.isArray(v);
  for (const [k, v] of Object.entries(patch)) {
    if (v === null) delete out[k];
    else out[k] = plain(v) && plain(out[k]) ? merge(out[k], v) : v;
  }
  return out;
}

/**
 * Preferences that remember what was written.
 *
 * Picking a level writes it and reads it straight back out of the one
 * preference entry, which the mutation replaces with the server's answer. Left
 * unmocked the write falls through and fails, the hook rolls the optimistic
 * value back, and whether that lands before or after the assertion is a coin
 * flip -- which read as flake in a spec about focus.
 */
function preferenceRoutes() {
  let prefs = {
    ...defaultResponses['GET /users/me/preferences'],
    model_preference: { preferred_model: MODEL },
  };
  const send = (route) => route.fulfill({
    status: 200, contentType: 'application/json', body: JSON.stringify(prefs),
  });
  return {
    'GET /users/me/preferences': send,
    'PUT /users/me/preferences': (route) => {
      prefs = merge(prefs, JSON.parse(route.request().postData() ?? '{}'));
      return send(route);
    },
  };
}

// The menu renders nothing without a model to configure, and the effort list
// nothing without a ladder on it. Both come from the manifest in real use.
const overrides = {
  'GET /models': {
    model_metadata: {
      [MODEL]: {
        display_name: 'Claude Sonnet 4',
        provider: 'anthropic',
        reasoning_efforts: ['none', 'low', 'medium', 'high'],
        reasoning_effort_default: 'medium',
      },
    },
  },
};

const TRIGGER = '.model-selector-trigger';
// A setting row is a menu item wearing the primitive's `setting` variant, so it
// carries no class of its own; the value it currently reads is what marks one.
const ROW = '[role="menuitem"]:has(.model-setting-value)';
const VALUE = '.model-setting-value';
const OPTION = '[role="menuitemradio"]';
// A plain item in the same open menu, so the forced-colors reading below is
// compared against the primitive as it stands rather than a memory of it.
const CONTROL = 'Manage models';

/** What Radix marks as highlighted, which is also what holds focus. */
function highlighted(page, sel = '[data-highlighted]') {
  return page.evaluate((s) => {
    const el = document.querySelector(s);
    return el && { text: el.textContent, focused: document.activeElement === el };
  }, sel);
}

/**
 * Arrow down until the Effort row is the highlighted item. A count of presses
 * would pin the menu's current length instead of the reachability under test.
 */
async function arrowToEffortRow(page) {
  const marked = () => page.evaluate(() => document.querySelector('[data-highlighted]')?.textContent ?? null);
  // Roving focus is installed as the menu opens, and a key pressed before that
  // lands nowhere. Waiting for the first highlight is what separates "the menu
  // is visible" from "the menu takes keys".
  await expect.poll(marked).not.toBe(null);

  const items = await page.locator('[role="menuitem"], [role="menuitemradio"]').count();
  for (let i = 0; i <= items; i += 1) {
    if (await page.evaluate((s) => !!document.querySelector(`${s}[data-highlighted]`), ROW)) return;
    const before = await marked();
    await page.keyboard.press('ArrowDown');
    // The highlight *moving* is what proves the press was taken. Polling for
    // any highlight passes on the one already there, so a press swallowed
    // mid-animation still spends an iteration and the walk stops short.
    await expect.poll(marked).not.toBe(before);
  }
  throw new Error('arrowing through the menu never reached the effort row');
}

async function openMenuByKeyboard(page) {
  await page.locator(TRIGGER).evaluate((el) => el.focus());
  await page.keyboard.press('Enter');
  await expect(page.locator(ROW)).toBeVisible();
}

test.beforeEach(async ({ page }) => {
  await mockAPI(page, { ...preferenceRoutes(), ...overrides });
  await page.goto('/dashboard');
  await expect(page.locator(TRIGGER)).toBeVisible();
});

test.describe('the effort list on a keyboard', () => {
  test('takes focus when arrowed into, and picking one leaves the menu open', async ({ page }) => {
    await openMenuByKeyboard(page);
    await arrowToEffortRow(page);
    await page.keyboard.press('ArrowRight');
    await expect(page.locator(OPTION).first()).toBeVisible();

    // The regression: focus never arrived here, so this was an empty list to
    // anyone arrowing. Both halves matter -- highlighted and actually focused.
    await expect.poll(() => highlighted(page, `${OPTION}[data-highlighted]`))
      .toEqual({ text: 'Off', focused: true });

    await page.keyboard.press('ArrowDown');
    await expect.poll(() => highlighted(page, `${OPTION}[data-highlighted]`))
      .toEqual({ text: 'Low', focused: true });

    await page.keyboard.press('Enter');
    // Choosing an effort is a setting, not a send: the value updates in place
    // and the menu stays up for the next choice.
    await expect(page.locator(VALUE)).toHaveText('Low');
    await expect(page.locator(ROW)).toBeVisible();
  });

  test('is reachable when the row expands in place, as it does on a narrow screen', async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await page.click('.langalpha-fab');
    await expect(page.locator(TRIGGER)).toBeVisible();

    await openMenuByKeyboard(page);
    await arrowToEffortRow(page);
    // No flyout here; the row itself is the toggle, which only answers Enter if
    // it is a menu item rather than the div it used to be.
    await page.keyboard.press('Enter');
    await expect(page.locator(OPTION).first()).toBeVisible();

    await page.keyboard.press('ArrowDown');
    await expect.poll(() => highlighted(page, `${OPTION}[data-highlighted]`))
      .toEqual({ text: 'Off', focused: true });
    await page.keyboard.press('Enter');
    await expect(page.locator(VALUE)).toHaveText('Off');
  });
});

/**
 * Forced colors is where a menu item has nothing else to fall back on: the OS
 * repaints background-color from its own palette, so the highlight tint that
 * marks the current row everywhere else is simply not there. What survives is
 * the outline, which is why the primitive spends `outline-none` on a
 * transparent 2px ring rather than `outline-style: none` -- a ring in nothing
 * is a ring the OS can repaint, and no ring at all is nothing to repaint.
 *
 * These rows once carried their own `outline: none`, at equal specificity and
 * later in the sheet, which replaced that transparent ring with no ring on
 * exactly the rows this branch made keyboard-reachable. Nothing on screen said
 * so outside forced colors, and jsdom paints no outlines at all.
 */
test.describe('the setting rows under forced colors', () => {
  test.beforeEach(async ({ page }) => {
    await page.emulateMedia({ forcedColors: 'active' });
  });

  test('keep the ring the untouched items in the same menu still have', async ({ page }) => {
    await page.click(TRIGGER);
    await page.hover(ROW);
    await expect(page.locator(OPTION).first()).toBeVisible();

    const outline = (locator) => locator.evaluate((el) => {
      const cs = getComputedStyle(el);
      return `${cs.outlineStyle} ${cs.outlineWidth}`;
    });

    expect({
      control: await outline(page.getByRole('menuitem', { name: CONTROL })),
      row: await outline(page.locator(ROW)),
      option: await outline(page.locator(OPTION).first()),
    }).toEqual({ control: 'solid 2px', row: 'solid 2px', option: 'solid 2px' });
  });
});

test.describe('the same list under a mouse', () => {
  test('paints no ring on the option that was clicked', async ({ page }) => {
    await page.click(TRIGGER);
    await page.hover(ROW);
    await expect(page.locator(OPTION).first()).toBeVisible();
    await page.locator(OPTION).nth(1).click();
    await expect(page.locator(VALUE)).toHaveText('Low');

    // The click leaves the option holding focus, which is what makes this
    // worth measuring: a rule keyed on :focus rather than :focus-visible would
    // light it up here and nowhere in the unit suite.
    //
    // Invisible is the question, not absent. These rows wear the primitive's
    // `outline-none`, which is a transparent 2px ring rather than no ring, so
    // that forced colors has something to repaint; reading `outlineStyle` alone
    // would call that a ring coming back.
    const rings = await page.evaluate((sel) => [...document.querySelectorAll(sel)]
      .map((el) => {
        const cs = getComputedStyle(el);
        return { text: el.textContent, style: cs.outlineStyle, color: cs.outlineColor, shadow: cs.boxShadow };
      }), `${OPTION}, ${ROW}`);
    const painted = rings
      .filter((r) => !unpainted(r.style, r.color) || r.shadow !== 'none')
      .map((r) => r.text);
    expect(painted).toEqual([]);
  });

  test('still marks the clicked option, so the row is not left silent', async ({ page }) => {
    // The over-eager reading of the test above: drop the indication as well as
    // the ring and every assertion there still passes, with nothing on screen
    // saying which effort is armed.
    await page.click(TRIGGER);
    await page.hover(ROW);
    await page.locator(OPTION).nth(1).click();
    await expect.poll(() => highlighted(page, `${OPTION}[data-highlighted]`))
      .toEqual({ text: 'Low', focused: true });
  });
});
