/**
 * E2E for renaming a workspace while a chat is open.
 *
 * The chat header used to hold a copy of the name taken when the view mounted,
 * so a rename landed in the sidebar and left the header on the old name for as
 * long as that view stayed cached.
 */
import { configureSSE, resetMockServer, mockAPI, test, expect } from './fixtures.js';
import { sampleWorkspace, sampleThread, sseEvents } from './helpers/mockResponses.js';

const WS_ID = 'a0000001-0000-4000-8000-000000000001';
const TH_ID = 'b0000001-0000-4000-8000-000000000001';
// Not a name any fixture ships: `sampleWorkspace()` and the default
// `GET /workspaces/*` are both 'Research', so renaming to that would let the
// "after" assertion pass on a default response that never saw the PUT.
const NEW_NAME = 'Retitled Desk QA';

const json = (route, body) => route.fulfill({
  status: 200,
  contentType: 'application/json',
  body: JSON.stringify(body),
});

test.describe('workspace rename', () => {
  test.beforeEach(async () => {
    await resetMockServer();
  });

  test('the open chat header follows the new name without a reload', async ({ page }) => {
    // Server-side name, flipped by the PUT the rename issues. The workspace
    // routes are functions rather than literals so they read `name` at request
    // time — mockAPI hands the route to a function response.
    let name = 'Scratch';
    const ws = () => sampleWorkspace({ name });
    const th = sampleThread();

    await mockAPI(page, {
      'GET /threads': { threads: [th], total: 1 },
      [`GET /threads/${TH_ID}`]: th,
      'GET /workspaces': (route) => json(route, { workspaces: [ws()], total: 1, limit: 20, offset: 0 }),
      'GET /workspaces/*': (route) => json(route, ws()),
      'PUT /workspaces/*': (route) => {
        name = JSON.parse(route.request().postData() || '{}').name || name;
        return json(route, ws());
      },
      [`GET /workspaces/${WS_ID}/files`]: { files: [] },
    });

    await configureSSE({
      method: 'GET',
      path: `/api/v1/threads/${TH_ID}/messages/replay`,
      events: [sseEvents.userMessage('hi', 0), sseEvents.finishStop(), sseEvents.replayDone()],
      delay: 10,
    });

    await page.goto(`/chat/t/${TH_ID}`);
    await expect(page.locator('h1').filter({ hasText: 'Scratch' }).first())
      .toBeVisible({ timeout: 15000 });

    // Rename from the sidebar tree — the path the report came from. Anchored
    // on the row that owns the Options button rather than on DOM order: the
    // header carries the same text, so .last() would be a layout accident.
    const row = page.locator('.nav-panel-row').filter({ hasText: 'Scratch' }).first();
    await row.hover();
    await row.getByRole('button', { name: 'Options', exact: true }).click();
    await page.getByRole('menuitem', { name: 'Rename', exact: true }).click();
    const input = page.getByLabel('Rename');
    // Armed before the submit so a rename that never reaches the network fails
    // as "the PUT never fired" instead of as a header that stayed on 'Scratch'.
    const renamePut = page.waitForRequest(
      (req) => req.method() === 'PUT' && new URL(req.url()).pathname === `/api/v1/workspaces/${WS_ID}`,
      { timeout: 10000 },
    );
    await input.fill(NEW_NAME);
    await input.press('Enter');
    await renamePut;

    await expect(page.locator('h1').filter({ hasText: NEW_NAME }).first())
      .toBeVisible({ timeout: 10000 });
    await expect(page.locator('h1').filter({ hasText: 'Scratch' })).toHaveCount(0);
  });
});
