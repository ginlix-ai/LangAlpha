/**
 * A sidebar row's actions are a hover affordance, with keyboard focus as the
 * one other way to raise them. The workspace row is itself a tab stop (it is
 * the drag handle), so a click focuses it, and focus that a click placed used
 * to hold the actions up after the pointer had left. jsdom paints neither
 * :hover nor :focus-within, so only a real browser can tell the two apart.
 */
import { resetMockServer, mockAPI, test, expect } from './fixtures.js';
import { sampleWorkspace } from './helpers/mockResponses.js';

const WORKSPACE = sampleWorkspace({
  workspace_id: 'a0000002-0000-4000-8000-000000000001',
  name: 'Alpha',
});

// Somewhere in the page body away from the sidebar.
const AWAY = { x: 900, y: 600 };

const header = (page) => page.locator('.sidebar-tree [data-ws-id] .nav-panel-row').filter({ hasText: 'Alpha' }).first();

const actionsOpacity = (page) => header(page).evaluate(
  (row) => getComputedStyle(row.querySelector('.nav-panel-row-actions')).opacity,
);

const rowHoldsFocus = (page) => header(page).evaluate((row) => row.contains(document.activeElement));

test.beforeEach(async ({ page }) => {
  await resetMockServer();
  await mockAPI(page, {
    'GET /workspaces': (route) => route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ workspaces: [WORKSPACE], total: 1, limit: 20, offset: 0 }),
    }),
  });
  await page.goto('/chat');
  // The tree paints static and swaps in its drag layer at idle, replacing the
  // rows. Wait for that swap (dnd-kit's handle carries aria-describedby, the
  // static one does not) so the row focused below is the one still mounted.
  await expect(header(page)).toHaveAttribute('aria-describedby', /.+/, { timeout: 15_000 });
});

test('a clicked workspace row drops its actions once the pointer leaves', async ({ page }) => {
  await header(page).hover();
  await expect.poll(() => actionsOpacity(page)).toBe('1');

  await header(page).click();
  await page.mouse.move(AWAY.x, AWAY.y);

  // The click left focus in the row; that alone must not keep the actions up.
  expect(await rowHoldsFocus(page)).toBe(true);
  await expect.poll(() => actionsOpacity(page)).toBe('0');
});

test('a keyboard user tabbing into the row keeps its actions in view', async ({ page }) => {
  await header(page).click();
  await page.mouse.move(AWAY.x, AWAY.y);
  await page.keyboard.press('Tab');

  expect(await rowHoldsFocus(page)).toBe(true);
  await expect.poll(() => actionsOpacity(page)).toBe('1');
});
