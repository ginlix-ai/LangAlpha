/**
 * E2E for reordering workspaces in the sidebar tree straight after a load.
 *
 * The tree paints before its drag layer arrives (navTreeKit swaps it in at
 * idle), and a drag begun on the tree it painted with is handed to dnd-kit
 * mid-gesture. So one case drags the moment the rows appear, and the others
 * hold the drag layer back until the press has already become a drag, so the
 * handover is certain to run: one keeps moving after it, one holds still over
 * its target.
 */
import { resetMockServer, mockAPI, test, expect } from './fixtures.js';
import { sampleWorkspace } from './helpers/mockResponses.js';

const NAMES = ['Alpha', 'Bravo', 'Charlie'];
const WORKSPACES = NAMES.map((name, i) => sampleWorkspace({
  workspace_id: `a0000001-0000-4000-8000-00000000000${i + 1}`,
  name,
  sort_order: i,
}));
const idOf = (name) => WORKSPACES[NAMES.indexOf(name)].workspace_id;

const json = (route, body) => route.fulfill({
  status: 200,
  contentType: 'application/json',
  body: JSON.stringify(body),
});

/**
 * Serves the three workspaces. `reorder` resolves with the body of the reorder
 * POST; it rides in an object because an async function would adopt it.
 */
async function mockTree(page) {
  let resolveReorder;
  const reorder = new Promise((resolve) => { resolveReorder = resolve; });
  await mockAPI(page, {
    'GET /workspaces': (route) => json(route, { workspaces: WORKSPACES, total: 3, limit: 20, offset: 0 }),
    'POST /workspaces/reorder': (route) => {
      resolveReorder(JSON.parse(route.request().postData() || '{}'));
      return json(route, {});
    },
  });
  return { reorder };
}

const header = (page, name) => page.locator('.sidebar-tree [data-ws-id] .nav-panel-row').filter({ hasText: name }).first();
const chip = (page) => page.locator('.nav-panel-drag-chip');

/** Holds the interactive kit's download until `release()`; `requested` settles once it is asked for. */
async function holdKit(page) {
  let release;
  const held = new Promise((resolve) => { release = resolve; });
  let markRequested;
  const requested = new Promise((resolve) => { markRequested = resolve; });
  await page.route((url) => url.pathname.includes('navTreeKitInteractive'), async (route) => {
    markRequested();
    await held;
    await route.continue();
  });
  return { release, requested };
}

/**
 * The rows are replaced when the drag layer swaps in at idle, and a read that
 * lands on one just detached measures nothing, so read until a box holds.
 */
async function boxOf(page, name) {
  let box = null;
  await expect.poll(async () => (box = await header(page, name).boundingBox())).not.toBeNull();
  return box;
}

/** Presses a header and moves past dnd-kit's 8px activation distance. */
async function pressAndLift(page, name) {
  const box = await boxOf(page, name);
  const x = box.x + 24;
  const y = box.y + box.height / 2;
  await page.mouse.move(x, y);
  await page.mouse.down();
  await page.mouse.move(x, y - 12, { steps: 3 });
}

async function dropOn(page, name) {
  const box = await boxOf(page, name);
  await page.mouse.move(box.x + 24, box.y + box.height / 2 - 4, { steps: 12 });
  await page.mouse.up();
}

async function expectOrder(page, reorder, names) {
  const body = await reorder;
  const known = new Set(WORKSPACES.map((ws) => ws.workspace_id));
  const sent = body.items.map((item) => item.workspace_id).filter((id) => known.has(id));
  expect(sent).toEqual(names.map(idOf));
  await expect(page.locator('.sidebar-tree [data-ws-id]')).toHaveText(names.map((n) => new RegExp(n)));
}

test.describe('sidebar workspace reorder', () => {
  test.beforeEach(async () => {
    await resetMockServer();
  });

  test('a drag started as soon as the rows paint lands', async ({ page }) => {
    const { reorder } = await mockTree(page);
    await page.goto('/chat');
    await expect(header(page, 'Charlie')).toBeVisible({ timeout: 15000 });

    await pressAndLift(page, 'Charlie');
    // A person's drag outlasts the drag layer's download; this one waits for
    // the lift the way their hand would, then drops.
    await expect(page.locator('.nav-panel-drag-chip')).toBeVisible({ timeout: 10000 });
    await dropOn(page, 'Alpha');

    await expectOrder(page, reorder, ['Charlie', 'Alpha', 'Bravo']);
  });

  test('a drag begun before the drag layer loads is handed over mid-gesture', async ({ page }) => {
    const { reorder } = await mockTree(page);
    const kit = await holdKit(page);

    await page.goto('/chat');
    await expect(header(page, 'Charlie')).toBeVisible({ timeout: 15000 });
    await pressAndLift(page, 'Charlie');
    await kit.requested;
    // Still the static tree: nothing is lifted yet.
    await expect(chip(page)).toHaveCount(0);

    kit.release();
    await expect(chip(page)).toBeVisible({ timeout: 10000 });
    await dropOn(page, 'Alpha');

    await expectOrder(page, reorder, ['Charlie', 'Alpha', 'Bravo']);
  });

  test('a drag already at its target when the drag layer loads drops there without another move', async ({ page }) => {
    const { reorder } = await mockTree(page);
    const kit = await holdKit(page);

    await page.goto('/chat');
    await expect(header(page, 'Charlie')).toBeVisible({ timeout: 15000 });
    // The middle row, so the chip is clear of the block's edges it clamps to.
    const target = await boxOf(page, 'Bravo');
    const targetY = target.y + target.height / 2;
    await pressAndLift(page, 'Charlie');
    await page.mouse.move(target.x + 24, targetY - 4, { steps: 12 });
    await kit.requested;
    await expect(chip(page)).toHaveCount(0);

    // The pointer holds still from here on.
    kit.release();
    await expect(chip(page)).toBeVisible({ timeout: 10000 });
    const lifted = await chip(page).boundingBox();
    expect(Math.abs(lifted.y + lifted.height / 2 - targetY)).toBeLessThan(target.height / 2);
    // Bravo making room is dnd-kit settling on it as the drop target.
    await expect.poll(
      () => page.locator(`.sidebar-tree [data-ws-id="${idOf('Bravo')}"]`).evaluate((el) => el.style.transform),
    ).toContain('translate');
    await page.mouse.up();

    await expectOrder(page, reorder, ['Alpha', 'Charlie', 'Bravo']);
  });
});
