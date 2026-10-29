import { test, expect, mockAPI } from './fixtures.js';

const WORKSPACE = { workspace_id: 'ws-mine', name: 'Mine', status: 'ready', is_home: false, config: {}, created_at: '2025-01-01T00:00:00Z', updated_at: '2025-01-01T00:00:00Z' };

/** A MarketView pref as stored, under whatever user scope the build keys it by. */
const storedPref = (page, name) => page.evaluate((suffix) => {
  const key = Object.keys(localStorage).find((k) => k.endsWith(suffix));
  return key ? localStorage.getItem(key) : null;
}, `market-chart:${name}`);

test.describe('MarketView workspace restore', () => {
  for (const mode of ['fast', 'ptc']) {
    test(`a stored workspace missing from the list is never requested (${mode})`, async ({ page }) => {
      await page.addInitScript((m) => {
        if (sessionStorage.getItem('seeded')) return;
        sessionStorage.setItem('seeded', '1');
        localStorage.setItem('market-chart:mode', JSON.stringify(m));
        localStorage.setItem('market-chart:selectedWorkspaceId', JSON.stringify('ws-gone'));
      }, mode);
      const named = [];
      page.on('request', (req) => {
        const url = new URL(req.url());
        if (url.pathname.startsWith('/api/') && `${url.pathname}${url.search}`.includes('ws-gone')) named.push(url.pathname + url.search);
      });
      await mockAPI(page, {
        'GET /workspaces': (route) => route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({ workspaces: [WORKSPACE], total: 1, limit: 50, offset: 0 }),
        }),
      });

      await page.goto('/market?symbol=AAPL');
      // The reconcile lands on the Chief of Staff and the first listed workspace,
      // after a first paint that can outlast the 5s default on a loaded machine.
      await expect.poll(() => storedPref(page, 'selectedWorkspaceId'), { timeout: 15000 }).toBe('"ws-mine"');
      await expect.poll(() => storedPref(page, 'mode')).toBe('"fast"');
      expect(named).toEqual([]);
    });
  }
});
