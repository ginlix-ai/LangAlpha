/**
 * A sidebar thread row's right-click menu, and the workspace row's. Radix
 * opens a context menu on a real contextmenu event and portals it to the
 * body; jsdom paints neither, so only a browser can show what a user gets.
 */
import { resetMockServer, mockAPI, test, expect } from './fixtures.js';
import { sampleWorkspace, sampleThread } from './helpers/mockResponses.js';

const WS_ID = 'a0000003-0000-4000-8000-000000000001';
const TH_ID = 'b0000003-0000-4000-8000-000000000001';
const OTHER_ID = 'b0000003-0000-4000-8000-000000000002';

const WORKSPACE = sampleWorkspace({ workspace_id: WS_ID, name: 'Dip Opportunities' });
const threads = () => [
  sampleThread({ thread_id: TH_ID, workspace_id: WS_ID, title: 'AMD recovery entry plan' }),
  sampleThread({ thread_id: OTHER_ID, workspace_id: WS_ID, title: 'NVDA intraday dip and rebound' }),
];

const json = (route, body) => route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(body) });

const threadRow = (page, title) => page.locator('.sidebar-tree .nav-panel-row').filter({ hasText: title }).first();
const header = (page) => page.locator('.sidebar-tree [data-ws-id] .nav-panel-row').filter({ hasText: 'Dip Opportunities' }).first();

test.beforeEach(async ({ page, context }) => {
  await page.setViewportSize({ width: 1280, height: 800 });
  await context.grantPermissions(['clipboard-read', 'clipboard-write']);
  await resetMockServer();
  await mockAPI(page, {
    'GET /workspaces': (route) => json(route, { workspaces: [WORKSPACE], total: 1, limit: 20, offset: 0 }),
    'GET /workspaces/*': (route) => json(route, WORKSPACE),
    'GET /threads': (route) => json(route, { threads: threads(), total: 2 }),
    'PATCH /threads/*': (route) => {
      const body = JSON.parse(route.request().postData() || '{}');
      return json(route, { ...threads()[0], ...body, updated_at: new Date().toISOString() });
    },
    'GET /threads/*/share': (route) => json(route, {
      is_shared: false, share_token: '', share_url: '', permissions: { allow_files: false, allow_download: false },
    }),
  });
  await page.goto('/chat');
  // The tree paints static and swaps in its drag layer at idle; wait for it.
  await expect(header(page)).toHaveAttribute('aria-describedby', /.+/, { timeout: 15_000 });
  if (!(await threadRow(page, 'AMD recovery').isVisible())) await header(page).click();
  await expect(threadRow(page, 'AMD recovery')).toBeVisible();
});

test('a thread row offers its actions on right-click', async ({ page }) => {
  await threadRow(page, 'AMD recovery').click({ button: 'right' });
  const menu = page.getByRole('menu');
  await expect(menu).toBeVisible();
  const items = await menu.getByRole('menuitem').allInnerTexts();
  expect(items.map((s) => s.trim())).toEqual([
    'Open in new tab', 'Pin to top', 'Rename', 'Share…', 'Copy link', 'Archive', 'Delete',
  ]);
});

test('the desktop app leaves out Open in new tab', async ({ page }) => {
  // The shell sends a new window of the app's own pages to the system
  // browser, so the item would leave the app rather than open a tab.
  await page.addInitScript(() => { window.langalphaDesktop = {}; });
  await page.reload();
  await expect(header(page)).toHaveAttribute('aria-describedby', /.+/, { timeout: 15_000 });
  if (!(await threadRow(page, 'AMD recovery').isVisible())) await header(page).click();
  await threadRow(page, 'AMD recovery').click({ button: 'right' });
  const items = await page.getByRole('menu').getByRole('menuitem').allInnerTexts();
  expect(items.map((s) => s.trim())).toEqual([
    'Pin to top', 'Rename', 'Share…', 'Copy link', 'Archive', 'Delete',
  ]);
});

test('copy link puts the thread address on the clipboard', async ({ page }) => {
  await threadRow(page, 'AMD recovery').click({ button: 'right' });
  await page.getByRole('menuitem', { name: 'Copy link' }).click();
  await expect(page.getByText('Link copied', { exact: true })).toBeVisible();
  const copied = await page.evaluate(() => navigator.clipboard.readText());
  expect(copied).toBe(new URL(`/chat/t/${TH_ID}`, page.url()).href);
});

test('rename edits the title in place and saves it', async ({ page }) => {
  await threadRow(page, 'AMD recovery').click({ button: 'right' });
  await page.getByRole('menuitem', { name: 'Rename' }).click();
  const input = page.getByLabel('Thread title');
  await expect(input).toBeFocused();
  await expect(input).toHaveValue('AMD recovery entry plan');
  // The pointer leaving across where the menu was must not take the field's
  // focus: a closing menu that still held its items used to, and the edit
  // ended on its own.
  for (const [x, y] of [[200, 330], [300, 300], [500, 400], [900, 700]]) await page.mouse.move(x, y, { steps: 5 });
  await expect(input).toBeFocused();
  const patch = page.waitForRequest((req) => req.method() === 'PATCH' && req.url().endsWith(`/threads/${TH_ID}`));
  await input.fill('AMD entry, revised');
  await input.press('Enter');
  expect(JSON.parse((await patch).postData() || '{}')).toEqual({ title: 'AMD entry, revised' });
  await expect(threadRow(page, 'AMD entry, revised')).toBeVisible();
});

test('escape leaves the title as it was', async ({ page }) => {
  let patched = false;
  page.on('request', (req) => { if (req.method() === 'PATCH') patched = true; });
  await threadRow(page, 'AMD recovery').click({ button: 'right' });
  await page.getByRole('menuitem', { name: 'Rename' }).click();
  const input = page.getByLabel('Thread title');
  await input.fill('Never saved');
  await input.press('Escape');
  await expect(input).toHaveCount(0);
  await expect(threadRow(page, 'AMD recovery entry plan')).toBeVisible();
  expect(patched).toBe(false);
});

test('share opens the share controls for that thread', async ({ page }) => {
  const status = page.waitForRequest((req) => req.method() === 'GET' && req.url().endsWith(`/threads/${OTHER_ID}/share`));
  await threadRow(page, 'NVDA intraday').click({ button: 'right' });
  await page.getByRole('menuitem', { name: 'Share…' }).click();
  await status;
  await expect(page.getByRole('dialog', { name: 'Share conversation' })).toBeVisible();
});

test('a workspace row opens its options menu on right-click', async ({ page }) => {
  await header(page).click({ button: 'right' });
  const items = await page.getByRole('menu').getByRole('menuitem').allInnerTexts();
  expect(items.map((s) => s.trim())).toEqual(['Pin to top', 'Rename', 'Duplicate', 'Delete']);
});
