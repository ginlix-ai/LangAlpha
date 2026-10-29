/**
 * The account panel is positioned against the account *row*, and that row is
 * inset inside the sidebar's padding. So any width the panel names for itself
 * lines up on one edge and misses on the other, and the miss is invisible in
 * code review: `w-64` next to `align="start"` looks deliberate. It shipped that
 * way, 17px wider than the row and hanging 7px off the sidebar entirely.
 *
 * Taking the width from the trigger fixes it by construction, and this is what
 * keeps it fixed: the assertion is that the two edges agree, so it survives a
 * change to --sidebar-width or to the panel's padding, neither of which a
 * pinned pixel count would.
 */
import { test, expect, mockAPI } from './fixtures.js';

/**
 * How far each edge of the open panel sits from the same edge of the row.
 *
 * Polled rather than read once: the sidebar animates in, so for the first few
 * frames the row is still travelling (left 4.8 on the way to 10) and Radix
 * anchors to wherever it was when the panel mounted, then corrects. Asserting
 * on the first paint measures the animation, not the alignment.
 */
const edgeGaps = (page) =>
  page.evaluate(() => {
    const row = document.querySelector('.sidebar-account-row');
    const menu = document.querySelector('[role="menu"]');
    if (!row || !menu) return null;
    const r = row.getBoundingClientRect();
    const m = menu.getBoundingClientRect();
    return { left: Math.round(m.left - r.left), right: Math.round(m.right - r.right) };
  });

test.beforeEach(async ({ page }) => {
  await mockAPI(page);
  await page.goto('/dashboard');
  await expect(page.locator('.sidebar-account-row')).toBeVisible();
});

// The row spells the account out, so it carries no fixed `aria-label` -- and
// that means every descendant is exposed, the avatar included. The initials are
// a picture of the name sitting beside them, so undecorated the row announces
// the account twice over: "TU Test User".
test('the row announces the account once, not once per rendering of it', async ({ page }) => {
  await expect(page.locator('.sidebar-account-row')).toHaveAccessibleName('Test User');
});

test('the open panel lines up with the row it came from', async ({ page }) => {
  await page.click('.sidebar-account-row');
  await expect(page.locator('[role="menu"]')).toBeVisible();

  // Both edges, not just the one align="start" already guaranteed. The right
  // edge is the one that was wrong: +17 against the row, and 7px past the
  // sidebar's own edge.
  await expect.poll(() => edgeGaps(page)).toEqual({ left: 0, right: 0 });
});

test('the panel is not wider than its own content needs', async ({ page }) => {
  await page.click('.sidebar-account-row');
  await expect(page.locator('[role="menu"]')).toBeVisible();

  // Inheriting a narrower width than before is only correct if nothing had to
  // be cut to fit; a clipped label would make the alignment a bad trade.
  const clipped = await page.evaluate(() => {
    const m = document.querySelector('[role="menu"]');
    return m.scrollWidth > m.clientWidth;
  });
  expect(clipped).toBe(false);
});
