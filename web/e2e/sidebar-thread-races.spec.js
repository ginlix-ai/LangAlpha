/**
 * What the sidebar's thread actions keep true when the server is slow or a
 * second surface acts on the same thread: a deleted thread stays gone, the
 * keyboard reaches the row's menu, and two writes to one thread land in the
 * order made. Each was a race found in review; these pin the fixes.
 */
import { resetMockServer, mockAPI, test, expect } from './fixtures.js';
import { sampleWorkspace, sampleThread } from './helpers/mockResponses.js';

const WS_ID = 'a0000004-0000-4000-8000-000000000001';
const T1 = 'b0000004-0000-4000-8000-000000000001';
const T2 = 'b0000004-0000-4000-8000-000000000002';
const T3 = 'b0000004-0000-4000-8000-000000000003';

const WORKSPACE = sampleWorkspace({ workspace_id: WS_ID, name: 'Dip Opportunities' });
const ALL = [
  sampleThread({ thread_id: T1, workspace_id: WS_ID, title: 'AMD recovery entry plan' }),
  sampleThread({ thread_id: T2, workspace_id: WS_ID, title: 'NVDA intraday dip and rebound' }),
  sampleThread({ thread_id: T3, workspace_id: WS_ID, title: 'TSLA deliveries preview' }),
];
const NOT_SHARED = { is_shared: false, share_token: '', share_url: '', permissions: { allow_files: false, allow_download: false } };
const SHARED = { is_shared: true, share_token: 'tok', share_url: '/s/tok', permissions: { allow_files: false, allow_download: false } };

const json = (route, body, status = 200) => route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) });
const threadRow = (page, title) => page.locator('.sidebar-tree .nav-panel-row').filter({ hasText: title }).first();
const header = (page) => page.locator('.sidebar-tree [data-ws-id] .nav-panel-row').filter({ hasText: 'Dip Opportunities' }).first();
const byId = (route) => ALL.find((th) => route.request().url().includes(th.thread_id));

async function open(page, path, routes = {}, gone = new Set()) {
  await page.setViewportSize({ width: 1280, height: 800 });
  await resetMockServer();
  await mockAPI(page, {
    'GET /workspaces': (route) => json(route, { workspaces: [WORKSPACE], total: 1, limit: 20, offset: 0 }),
    'GET /workspaces/*': (route) => json(route, WORKSPACE),
    'GET /threads': (route) => {
      const threads = ALL.filter((th) => !gone.has(th.thread_id));
      return json(route, { threads, total: threads.length });
    },
    'GET /threads/*': (route) => {
      const thread = byId(route);
      return thread && !gone.has(thread.thread_id) ? json(route, thread) : json(route, { detail: 'Not found' }, 404);
    },
    'GET /threads/*/share': (route) => json(route, NOT_SHARED),
    ...routes,
  });
  await page.goto(path);
  // The tree paints static and swaps in its drag layer at idle; wait for it.
  await expect(header(page)).toHaveAttribute('aria-describedby', /.+/, { timeout: 15_000 });
  if (!(await threadRow(page, 'AMD recovery').isVisible())) await header(page).click();
  await expect(threadRow(page, 'AMD recovery')).toBeVisible();
}

test('Back to a thread deleted from the sidebar leaves for its workspace', async ({ page }) => {
  const gone = new Set();
  await open(page, `/chat/t/${T2}`, {
    'DELETE /threads/*': (route) => {
      gone.add(byId(route).thread_id);
      return json(route, { ok: true });
    },
  }, gone);
  await threadRow(page, 'AMD recovery').click();
  await expect(page).toHaveURL(new RegExp(T1));

  await threadRow(page, 'NVDA intraday').click({ button: 'right' });
  await page.getByRole('menuitem', { name: 'Delete' }).click();
  await page.getByRole('dialog').getByRole('button', { name: 'Delete' }).click();
  await expect(threadRow(page, 'NVDA intraday')).toHaveCount(0);

  await page.goBack();
  await expect(page).toHaveURL(new RegExp(`/chat/${WS_ID}$`));
});

test('a keyboard reaches a thread and its menu', async ({ page }) => {
  await open(page, '/chat');
  const title = threadRow(page, 'NVDA intraday').getByRole('button', { name: 'NVDA intraday dip and rebound' });
  await title.focus();
  await expect(title).toBeFocused();
  // What a browser sends the focused element on Shift+F10 or the Menu key.
  await title.evaluate((el) => {
    const r = el.getBoundingClientRect();
    el.dispatchEvent(new MouseEvent('contextmenu', { bubbles: true, cancelable: true, clientX: r.left + 8, clientY: r.bottom, button: 2 }));
  });
  await expect(page.getByRole('menuitem', { name: 'Rename' })).toBeVisible();
  await page.keyboard.press('Escape');
  await expect(page.getByRole('menu')).toHaveCount(0);

  // Tab walks title, pin, archive, then the next title; Enter opens it.
  const next = threadRow(page, 'TSLA deliveries').getByRole('button', { name: 'TSLA deliveries preview' });
  for (let i = 0; i < 6 && !(await next.evaluate((el) => el === document.activeElement)); i++) {
    await page.keyboard.press('Tab');
  }
  await expect(next).toBeFocused();
  await page.keyboard.press('Enter');
  await expect(page).toHaveURL(new RegExp(T3));
});

test('a sidebar rename and a gallery rename of one thread reach the server in the order made', async ({ page }) => {
  const applied = [];
  await open(page, `/chat/${WS_ID}`, {
    'PATCH /threads/*': async (route) => {
      const body = JSON.parse(route.request().postData() || '{}');
      // The first answer is slow: the second write must still land after it.
      if (applied.length === 0 && body.title === 'Sidebar title') await new Promise((r) => setTimeout(r, 1500));
      applied.push(body.title);
      return json(route, { ...ALL[0], ...body, updated_at: new Date().toISOString() });
    },
  });

  await threadRow(page, 'AMD recovery').click({ button: 'right' });
  await page.getByRole('menuitem', { name: 'Rename' }).click();
  const field = page.getByRole('textbox', { name: 'Thread title' });
  await expect(field).toBeFocused();
  // Off the menu, so its pointer leave can't pull focus from the field.
  await page.mouse.move(700, 700);
  await field.fill('Sidebar title');
  await field.press('Enter');

  const card = page.locator('main').getByText('Sidebar title').first();
  await expect(card).toBeVisible();
  await card.hover();
  await page.locator('main button[title="Rename thread"]').first().click();
  const input = page.getByPlaceholder('Enter thread title');
  await input.fill('Gallery title');
  await input.press('Enter');

  await expect.poll(() => applied.length, { timeout: 10_000 }).toBe(2);
  expect(applied).toEqual(['Sidebar title', 'Gallery title']);
});

test('a share made while the share status is read again stands', async ({ page }) => {
  let reads = 0;
  let slowReadAnswered = false;
  await open(page, `/chat/t/${T1}`, {
    'GET /threads/*/share': async (route) => {
      reads += 1;
      if (reads === 1) return json(route, NOT_SHARED);
      // The panel's read as it opens: still on the way when the share lands.
      await new Promise((r) => setTimeout(r, 2000));
      slowReadAnswered = true;
      return json(route, NOT_SHARED);
    },
    'POST /threads/*/share': (route) => json(route, SHARED),
  });
  await expect.poll(() => reads).toBeGreaterThanOrEqual(1);

  await threadRow(page, 'AMD recovery').click({ button: 'right' });
  await page.getByRole('menuitem', { name: 'Share…' }).click();
  const toggle = page.getByRole('dialog').getByRole('switch', { name: 'Share conversation' });
  await expect(toggle).toHaveAttribute('aria-checked', 'false');
  await toggle.click();
  await expect(toggle).toHaveAttribute('aria-checked', 'true');

  await expect.poll(() => slowReadAnswered, { timeout: 10_000 }).toBe(true);
  await expect(toggle).toHaveAttribute('aria-checked', 'true');
  await page.keyboard.press('Escape');
  expect(await page.locator('main button[title="Share conversation"]').first().evaluate((el) => el.style.color)).toContain('accent');
});
