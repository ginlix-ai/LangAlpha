/**
 * Test utilities for Playwright E2E tests.
 * Provides helpers for configuring the mock SSE server and mocking REST APIs.
 */
import { test as base } from '@playwright/test';
import { defaultResponses } from './helpers/mockResponses.js';
import { startMockServer } from './mock-sse-server.js';

// The dev servers bake one API origin into the app, E2E_MOCK_PORT, and nothing
// listens there. Each worker runs its own mock server on the ports above it and
// routes the page's API traffic to it, so one worker's reset or one-shot
// scenario never reaches another's page, and a request that slips past the
// route fails to connect instead of reading someone else's scenario.
const APP_API_PORT = Number(process.env.E2E_MOCK_PORT);
const WORKER_MOCK_PORT = APP_API_PORT + 1 + Number(process.env.TEST_PARALLEL_INDEX ?? 0);
const MOCK_SERVER = `http://127.0.0.1:${WORKER_MOCK_PORT}`;

/** Configure a scenario on the mock SSE server */
export async function configureSSE(scenario) {
  await fetch(`${MOCK_SERVER}/__scenario`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(scenario),
  });
}

/** Get captured requests from mock server (for assertion) */
export async function getCapturedRequests() {
  const res = await fetch(`${MOCK_SERVER}/__requests`);
  return res.json();
}

/** Reset mock server state */
export async function resetMockServer() {
  await fetch(`${MOCK_SERVER}/__reset`, { method: 'POST' });
}

/**
 * Mock REST APIs via page.route() (non-SSE endpoints).
 * The app's API client hits VITE_API_BASE_URL, routed to this worker's mock server.
 * We intercept via page.route() for instant JSON responses on REST endpoints,
 * while SSE endpoints pass through to the mock server for real chunked streaming.
 */
export async function mockAPI(page, overrides = {}) {
  const routes = { ...defaultResponses, ...overrides };

  for (const [key, response] of Object.entries(routes)) {
    const [method, pathPattern] = key.split(' ', 2);
    // Convert glob-style * in path to regex [^/]+ for segment matching.
    // Use a URL predicate so query params are ignored (glob patterns
    // match the full URL string, which breaks on ?key=val).
    const pathRegex = new RegExp(
      '^' + pathPattern.replace(/\*/g, '[^/]+') + '$',
    );

    await page.route(
      (url) => pathRegex.test(url.pathname.replace('/api/v1', '')),
      async (route) => {
        // A page navigation is never one of these calls. Some API paths are also
        // app routes -- `/news/:id` is both -- and the predicate above sees only
        // a pathname, so without this the document request for /news/1 is
        // answered with the article JSON and the SPA never loads at all.
        if (route.request().resourceType() === 'document') return route.fallback();
        const reqMethod = route.request().method();
        if (method !== '*' && reqMethod !== method) {
          return route.fallback();
        }
        if (typeof response === 'function') return response(route);
        await route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify(response),
        });
      },
    );
  }
}

export const test = base.extend({
  mockServer: [
    // Playwright reads fixture dependencies from this pattern, so it must be an
    // object pattern even when empty.
    // eslint-disable-next-line no-empty-pattern
    async ({}, provide) => {
      const server = await startMockServer(WORKER_MOCK_PORT);
      await provide(MOCK_SERVER);
      await server.close();
    },
    { scope: 'worker', auto: true },
  ],
  // Context-level so it runs after every page.route() handler has had its turn
  // (mockAPI falls back to it) and covers every page the test opens.
  context: async ({ context }, provide) => {
    await context.route(
      (url) => url.hostname === '127.0.0.1' && url.port === String(APP_API_PORT),
      (route) => {
        const url = new URL(route.request().url());
        url.port = String(WORKER_MOCK_PORT);
        return route.fallback({ url: url.href });
      },
    );
    await provide(context);
  },
});

export { expect } from '@playwright/test';
