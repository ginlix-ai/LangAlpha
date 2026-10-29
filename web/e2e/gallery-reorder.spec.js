/**
 * E2E for arranging the workspace gallery by dragging its cards.
 *
 * The cards drag in the Custom order only, and a drop saves the whole order
 * with Home in its slot. The sidebar shares that order, so it follows the drop.
 */
import { resetMockServer, mockAPI, test, expect } from './fixtures.js';
import { sampleWorkspace } from './helpers/mockResponses.js';

const HOME = {
  workspace_id: 'ws-home',
  name: 'Home',
  status: 'unbound',
  is_home: true,
  is_pinned: true,
  sort_order: 0,
  config: {},
  created_at: '2025-01-01T00:00:00Z',
  updated_at: '2025-01-01T00:00:00Z',
};
const NAMES = ['Alpha', 'Bravo', 'Charlie', 'Delta'];
const WORKSPACES = NAMES.map((name, i) => sampleWorkspace({
  workspace_id: `a0000001-0000-4000-8000-00000000000${i + 1}`,
  name,
  description: '',
  sort_order: i + 1,
}));
const idOf = (name) => WORKSPACES[NAMES.indexOf(name)].workspace_id;

const json = (route, body) => route.fulfill({
  status: 200,
  contentType: 'application/json',
  body: JSON.stringify(body),
});

/**
 * Serves the workspaces in the order the last reorder POST wrote. `posts`
 * collects each POST's body.
 */
async function mockGallery(page) {
  let order = [HOME, ...WORKSPACES].map((ws) => ws.workspace_id);
  const byId = new Map([HOME, ...WORKSPACES].map((ws) => [ws.workspace_id, ws]));
  const posts = [];
  await mockAPI(page, {
    'GET /workspaces': (route) => {
      const params = new URL(route.request().url()).searchParams;
      const includeHome = params.get('include_home') === 'true';
      // Recent: newest first, which these rows reverse from the stored order.
      const ids = params.get('sort_by') === 'custom' ? order : [...order].reverse();
      const workspaces = ids
        .map((id) => ({ ...byId.get(id), sort_order: order.indexOf(id) }))
        .filter((ws) => includeHome || !ws.is_home);
      return json(route, { workspaces, total: workspaces.length, limit: 100, offset: 0 });
    },
    'POST /workspaces/reorder': (route) => {
      const body = JSON.parse(route.request().postData() || '{}');
      posts.push(body);
      order = body.items.map((item) => item.workspace_id);
      return json(route, {});
    },
  });
  return { posts };
}

const card = (page, name) => page.getByTestId('workspace-card').filter({ has: page.getByText(name, { exact: true }) });
const cardNames = (page) => page.getByTestId('workspace-card').locator('[role="button"]').evaluateAll(
  (els) => els.map((el) => el.getAttribute('aria-label')),
);

async function openGallery(page, sort) {
  await page.addInitScript((s) => localStorage.setItem('workspaceGallery.sort', s), sort);
  await page.goto('/chat');
  await expect(card(page, 'Delta')).toBeVisible({ timeout: 15000 });
}

/** Presses `from`, carries it past the activation distance and drops it on `to`'s centre. */
async function drag(page, from, to) {
  const a = await card(page, from).boundingBox();
  const b = await card(page, to).boundingBox();
  await page.mouse.move(a.x + a.width / 2, a.y + a.height / 2);
  await page.mouse.down();
  await page.mouse.move(a.x + a.width / 2 + 12, a.y + a.height / 2, { steps: 3 });
  await page.mouse.move(b.x + b.width / 2, b.y + b.height / 2, { steps: 15 });
  return async () => page.mouse.up();
}

test.describe('gallery drag to arrange', () => {
  test.beforeEach(async () => {
    await resetMockServer();
  });

  test('a card dropped in Custom saves the order, Home first, and the sidebar follows', async ({ page }) => {
    const { posts } = await mockGallery(page);
    await openGallery(page, 'custom');
    expect(await cardNames(page)).toEqual(NAMES);

    const release = await drag(page, 'Delta', 'Alpha');
    // Lifted: a copy follows the pointer, and the slot it left is outlined.
    await expect(page.getByTestId('workspace-card-lifted')).toBeVisible();
    await expect(page.locator('[data-drag-state="placeholder"]')).toHaveCount(1);
    await release();

    await expect.poll(() => posts.length).toBe(1);
    expect(posts[0].items).toEqual(['ws-home', ...['Delta', 'Alpha', 'Bravo', 'Charlie'].map(idOf)]
      .map((workspace_id, sort_order) => ({ workspace_id, sort_order })));
    await expect.poll(() => cardNames(page)).toEqual(['Delta', 'Alpha', 'Bravo', 'Charlie']);
    await expect(page.locator('.sidebar-tree [data-ws-id]').filter({ hasText: /Alpha|Bravo|Charlie|Delta/ }))
      .toHaveText([/Delta/, /Alpha/, /Bravo/, /Charlie/]);
  });

  test('the keyboard arranges a card with Space and the arrows', async ({ page }) => {
    const { posts } = await mockGallery(page);
    await openGallery(page, 'custom');

    const alpha = card(page, 'Alpha').getByRole('button', { name: 'Alpha', exact: true });
    await alpha.focus();
    await page.keyboard.press('Space');
    await expect(page.locator('[data-drag-state="placeholder"]')).toHaveCount(1);
    // What a screen reader hears is what the next key acts on.
    await expect(page.getByRole('status').filter({ hasText: 'Picked up Alpha.' })).toHaveCount(1);
    // dnd-kit measures where the cards are a frame after the lift; an arrow
    // pressed before that has nowhere to go, as no hand is quick enough to find.
    await page.evaluate(() => new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve))));
    await page.keyboard.press('ArrowRight');
    await expect(page.getByRole('status').filter({ hasText: 'Alpha is over Bravo.' })).toHaveCount(1);
    await page.keyboard.press('Space');

    await expect.poll(() => posts.length).toBe(1);
    expect(posts[0].items.map((it) => it.workspace_id)).toEqual(['ws-home', idOf('Bravo'), idOf('Alpha'), idOf('Charlie'), idOf('Delta')]);
  });

  test('cards do not drag in Recent, and a click still opens one', async ({ page }) => {
    const { posts } = await mockGallery(page);
    await openGallery(page, 'activity');

    const release = await drag(page, 'Delta', 'Alpha');
    await expect(page.getByTestId('workspace-card-lifted')).toHaveCount(0);
    await release();
    expect(posts).toHaveLength(0);

    await card(page, 'Bravo').click();
    await expect(page).toHaveURL(new RegExp(idOf('Bravo')));
  });

  test('the menu opens without lifting the card', async ({ page }) => {
    await mockGallery(page);
    await openGallery(page, 'custom');

    const target = card(page, 'Bravo');
    await target.hover();
    await target.getByTestId('workspace-card-menu').click();
    await expect(page.getByRole('menu')).toBeVisible();
    await expect(page.getByTestId('workspace-card-lifted')).toHaveCount(0);
  });
});
