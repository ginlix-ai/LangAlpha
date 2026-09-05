/**
 * Test utilities for Playwright E2E tests.
 * Provides helpers for configuring the mock SSE server and mocking REST APIs.
 */
import { defaultResponses } from './helpers/mockResponses.js';

const MOCK_SERVER = 'http://127.0.0.1:4100';

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
 * The app's axios client hits VITE_API_BASE_URL (mock server on :4100).
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

export { test, expect } from '@playwright/test';
