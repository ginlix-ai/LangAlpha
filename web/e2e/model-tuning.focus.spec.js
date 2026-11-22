/**
 * The reasoning levels and speeds in the composer's model picker, beside the
 * model list (its pane) or under it (its strip), reached the way someone
 * without a mouse reaches them.
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
 * a pointer -- `outline-hidden`, indication via a tint, the hovered row's under
 * a pointer and the focused one's from the keyboard -- so the rows must not
 * pick up the app's baseline `:focus-visible` ring.
 *
 * The pane is a react-aria menu beside the picker's model grid, each one Tab
 * stop; Tabbing into the pane lands on the current level, so arrowing starts
 * from what is set. Where the composer has no room for the pane, the levels
 * are a segmented control under the list, arrowed through the same way.
 */
import { test, expect, mockAPI } from './fixtures.js';
import { defaultResponses, sampleThread } from './helpers/mockResponses.js';
import { chatViewOverrides, replayNothing, TH } from './helpers/chatScenario.js';
import { unpainted } from './helpers/focusPaint.js';

const MODEL = 'claude-sonnet-4-20250514';

/**
 * The thread row that remembers what was written.
 *
 * A level picked on a thread is saved on its row, and the composer reads it
 * back from the server's answer. Left unmocked the save fails, the hook rolls
 * the optimistic value back, and whether that lands before or after the
 * assertion is a coin flip -- which read as flake in a spec about focus.
 */
function threadRoutes() {
  let thread = sampleThread({ llm_model: MODEL });
  const send = (route) => route.fulfill({
    status: 200, contentType: 'application/json', body: JSON.stringify(thread),
  });
  return {
    ...chatViewOverrides({ threads: [thread] }),
    [`GET /threads/${TH}`]: send,
    [`PATCH /threads/${TH}`]: (route) => {
      thread = { ...thread, ...JSON.parse(route.request().postData() ?? '{}') };
      return send(route);
    },
  };
}

// The picker lists no levels without a model to configure, and none for a
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

const TRIGGER = 'button[data-pill="model"]';
const PANE = '[data-slot="tuning-pane"]';
const OPTION = `${PANE} [role="menuitemradio"]`;
const SEGMENT = '[data-slot="tuning-strip"] [role="radio"]';

/** The row react-aria marks as focused, and whether it holds focus. */
function highlighted(page, sel = `${OPTION}[data-focused]`) {
  return page.evaluate((s) => {
    const el = document.querySelector(s);
    return el && { text: el.textContent, focused: document.activeElement === el };
  }, sel);
}

/** What holds focus, by its text, once it is inside `within`. */
function focusedIn(page, within) {
  return page.evaluate((w) => {
    const el = document.activeElement;
    return el && el.closest(w) ? el.textContent : null;
  }, within);
}

async function openPickerByKeyboard(page) {
  await page.locator(TRIGGER).evaluate((el) => el.focus());
  await page.keyboard.press('Enter');
  await expect(page.getByRole('dialog', { name: 'Select model' })).toBeVisible();
}

/** Tabs from the search field until focus is inside `within`: the list and
 *  the tuning are each a stop of their own, so a few presses reach it. */
async function tabInto(page, within) {
  for (let i = 0; i < 8; i += 1) {
    if (await focusedIn(page, within) !== null) return;
    await page.keyboard.press('Tab');
  }
  throw new Error(`Tab never reached ${within}`);
}

test.beforeEach(async ({ page }) => {
  await mockAPI(page, {
    ...threadRoutes(),
    'GET /users/me/preferences': defaultResponses['GET /users/me/preferences'],
    ...overrides,
  });
  await replayNothing();
});

/** Opens the thread at the viewport the test set, its pill naming the level. */
async function openThread(page) {
  await page.goto(`/chat/t/${TH}`);
  await expect(page.locator(TRIGGER)).toBeVisible({ timeout: 15000 });
  await expect(page.locator(TRIGGER)).toContainText('Medium');
}

test.describe('the levels on a keyboard', () => {
  test('are a Tab stop past the list, and picking one leaves the picker open', async ({ page }) => {
    await openThread(page);
    await openPickerByKeyboard(page);
    await tabInto(page, PANE);
    // It lands on the current level, the one Settings gives here.
    await expect.poll(() => highlighted(page)).toEqual({ text: 'MediumDefault', focused: true });

    await page.keyboard.press('ArrowDown');
    await expect.poll(() => highlighted(page)).toEqual({ text: 'High', focused: true });

    await page.keyboard.press('Enter');
    // Choosing a level is a setting, not a send: the pill names it in place
    // and the picker stays up for the next choice.
    await expect(page.locator(TRIGGER)).toContainText('High');
    await expect(page.locator(OPTION).first()).toBeVisible();
  });

  test('works the same in the strip on a narrow screen', async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await openThread(page);

    await openPickerByKeyboard(page);
    await expect(page.locator(PANE)).toHaveCount(0);
    await tabInto(page, '[data-slot="tuning-strip"]');
    await expect.poll(() => focusedIn(page, '[data-slot="tuning-strip"]')).toBe('Medium');
    await page.keyboard.press('ArrowLeft');
    await page.keyboard.press('Enter');
    await expect(page.locator(TRIGGER)).toContainText('Low');
    await expect(page.locator(SEGMENT).first()).toBeVisible();
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
    await openThread(page);
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
    await openThread(page);
    await page.click(TRIGGER);
    await expect(page.locator(OPTION).first()).toBeVisible();
    await page.locator(OPTION).nth(1).click();
    await expect(page.locator(TRIGGER)).toContainText('Low');

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
    // saying which level is armed.
    await openThread(page);
    await page.click(TRIGGER);
    await page.locator(OPTION).nth(1).click();
    await expect.poll(() => highlighted(page))
      .toEqual({ text: 'Low', focused: true });
  });
});
