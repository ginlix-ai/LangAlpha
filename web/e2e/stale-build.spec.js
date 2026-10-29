/**
 * A tab left on an old build takes the new one on its next switch between app
 * sections, without asking, and never inside a section, where chat keeps the
 * threads it has open mounted with their unsent messages.
 */
import { test, expect, mockAPI } from './fixtures.js';

const WORKSPACE = '5b0f6c9e-2d1a-4f3b-9c8e-7a6d5e4f3b2a';
const THREAD = '8c1e2f3a-4b5c-4d6e-8f7a-9b0c1d2e3f4a';

async function openOnStaleBuild(page) {
  await page.addInitScript(() => {
    localStorage.setItem('langalpha-onboarding-ignored-at', String(Date.now()));
  });
  await mockAPI(page);
  // The dev server's entry is /src/main.tsx, so any other build id is a newer one.
  await page.route(
    (url) => url.pathname.endsWith('/version.json'),
    (route) => route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ build: 'index-NEWER.js' }),
    }),
  );
}

/** A client-side route change, the way a link in the app makes one, with route state if given. */
function navigate(page, path, state) {
  return page.evaluate(([to, usr]) => {
    history.pushState(usr ? { usr, key: 'e2e', idx: history.length } : {}, '', to);
    window.dispatchEvent(new PopStateEvent('popstate'));
  }, [path, state ?? null]);
}

test('a stale tab reloads on its next section switch, once', async ({ page }) => {
  await openOnStaleBuild(page);
  let loads = 0;
  page.on('load', () => {
    loads++;
  });
  await page.goto('/automations');
  await page.waitForFunction(() => window.__LA_BOOTED__ === true);
  expect(loads).toBe(1);

  // Coming back to the tab is when it learns the server moved on.
  await page.evaluate(() => window.dispatchEvent(new Event('focus')));
  await expect(page.getByText('A new version is available', { exact: true })).toBeVisible();

  await navigate(page, '/automations?view=1');
  await page.waitForTimeout(500);
  expect(loads).toBe(1);

  // A switch that hands something to the next page (a message sent from the
  // dashboard into chat) leaves it to that page.
  await navigate(page, '/market', { draftMessage: 'hello' });
  await page.waitForTimeout(500);
  expect(loads).toBe(1);
  // A new thread learns its workspace from nothing but the route state.
  await navigate(page, '/chat/t/__default__', { workspaceId: WORKSPACE });
  await page.waitForTimeout(500);
  expect(loads).toBe(1);
  await navigate(page, '/automations?view=2', { draftMessage: 'hello' });
  await page.waitForTimeout(500);
  expect(loads).toBe(1);

  // A thread the URL names looks its workspace up again, so the state that
  // only spares it the lookup is no hand-off.
  await navigate(page, `/chat/t/${THREAD}`, { workspaceId: WORKSPACE });
  await expect.poll(() => loads).toBe(2);
  await expect(page).toHaveURL(new RegExp(`/chat/t/${THREAD}$`));
  await page.waitForFunction(() => window.__LA_BOOTED__ === true);

  // The reload came back on the same build here, as it would behind an edge
  // still serving the old shell; switching again must not loop.
  await page.evaluate(() => window.dispatchEvent(new Event('focus')));
  await expect(page.getByText('A new version is available', { exact: true })).toBeVisible();
  await navigate(page, '/automations');
  await page.waitForTimeout(1000);
  expect(loads).toBe(2);
});
