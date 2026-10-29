/**
 * The effort and speed list in the composer's effort menu, reached the way
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
 * The mouse half is the other risk. Menu items deliberately wear no ring under
 * a pointer -- `outline-hidden`, indication via the `data-focused` tint for
 * pointer and keyboard alike -- so the rows must not pick up the app's
 * baseline `:focus-visible` ring.
 *
 * The menu is react-aria's now, as the model picker beside it is; it opens on
 * the current level, so arrowing starts from what is set.
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

// The pill renders nothing without a model to configure, and nothing for a
// model with no ladder on it. Both come from the manifest in real use.
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

const TRIGGER = 'button[data-pill="effort"]';
const OPTION = '[role="menuitemradio"]';

/** The row react-aria marks as focused, and whether it holds focus. */
function highlighted(page, sel = `${OPTION}[data-focused]`) {
  return page.evaluate((s) => {
    const el = document.querySelector(s);
    return el && { text: el.textContent, focused: document.activeElement === el };
  }, sel);
}

async function openMenuByKeyboard(page) {
  await page.locator(TRIGGER).evaluate((el) => el.focus());
  await page.keyboard.press('Enter');
  await expect(page.locator(OPTION).first()).toBeVisible();
}

test.beforeEach(async ({ page }) => {
  await mockAPI(page, { ...preferenceRoutes(), ...overrides });
  await page.goto('/dashboard');
  await expect(page.locator(TRIGGER)).toBeVisible();
});

test.describe('the effort list on a keyboard', () => {
  test('takes focus when arrowed through, and picking one leaves the menu open', async ({ page }) => {
    await openMenuByKeyboard(page);
    // It opens on the current level, the one Settings gives here.
    await expect.poll(() => highlighted(page)).toEqual({ text: 'MediumDefault', focused: true });

    await page.keyboard.press('ArrowDown');
    await expect.poll(() => highlighted(page)).toEqual({ text: 'High', focused: true });

    await page.keyboard.press('Enter');
    // Choosing an effort is a setting, not a send: the pill updates in place
    // and the menu stays up for the next choice.
    await expect(page.locator(TRIGGER)).toHaveText('High');
    await expect(page.locator(OPTION).first()).toBeVisible();
  });

  test('works the same on a narrow screen', async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await page.click('.langalpha-fab');
    await expect(page.locator(TRIGGER)).toBeVisible();

    await openMenuByKeyboard(page);
    await expect.poll(() => highlighted(page)).toEqual({ text: 'MediumDefault', focused: true });
    await page.keyboard.press('ArrowUp');
    await page.keyboard.press('Enter');
    await expect(page.locator(TRIGGER)).toHaveText('Low');
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
test.describe('the options under forced colors', () => {
  test.beforeEach(async ({ page }) => {
    await page.emulateMedia({ forcedColors: 'active' });
  });

  test("keep the primitive's ring", async ({ page }) => {
    await page.click(TRIGGER);
    await expect(page.locator(OPTION).first()).toBeVisible();

    const outlines = await page.locator(OPTION).evaluateAll((els) => els.map((el) => {
      const cs = getComputedStyle(el);
      return `${cs.outlineStyle} ${cs.outlineWidth}`;
    }));
    expect(new Set(outlines)).toEqual(new Set(['solid 2px']));
  });
});

test.describe('the same list under a mouse', () => {
  test('paints no ring on the option that was clicked', async ({ page }) => {
    await page.click(TRIGGER);
    await expect(page.locator(OPTION).first()).toBeVisible();
    await page.locator(OPTION).nth(1).click();
    await expect(page.locator(TRIGGER)).toHaveText('Low');

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
      }), OPTION);
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
    await page.locator(OPTION).nth(1).click();
    await expect.poll(() => highlighted(page))
      .toEqual({ text: 'Low', focused: true });
  });
});
