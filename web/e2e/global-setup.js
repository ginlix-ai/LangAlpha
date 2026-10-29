import { chromium } from '@playwright/test';

// Routes whose lazy chunks the specs load, between them most of the app.
const ROUTES = ['/dashboard', '/chat', '/chat/t/warmup', '/s/warmup', '/app'];
// No new module request for this long means the route's import graph is in.
const QUIET_MS = 1000;
const ROUTE_CAP_MS = 60_000;

/**
 * Load each route once on every dev server before any worker starts.
 *
 * Vite transforms a module the first time a browser asks for it, so on a fresh
 * server the first test to open a route pays for compiling its whole chunk, and
 * with several workers doing that at once the page can miss a 5-10s assertion
 * before it has rendered at all. One serial pass here compiles each module once.
 *
 * @param {import('@playwright/test').FullConfig} config
 */
export default async function warmDevServers(config) {
  const origins = [...new Set(config.projects.map((p) => p.use.baseURL).filter(Boolean))];
  const browser = await chromium.launch();
  try {
    for (const origin of origins) {
      const page = await browser.newPage();
      let lastModuleRequest = Date.now();
      page.on('request', (req) => {
        if (req.url().startsWith(origin)) lastModuleRequest = Date.now();
      });
      for (const route of ROUTES) {
        const started = Date.now();
        await page.goto(origin + route).catch(() => {});
        while (Date.now() - lastModuleRequest < QUIET_MS && Date.now() - started < ROUTE_CAP_MS) {
          await page.waitForTimeout(100);
        }
      }
      await page.close();
    }
  } finally {
    await browser.close();
  }
}
