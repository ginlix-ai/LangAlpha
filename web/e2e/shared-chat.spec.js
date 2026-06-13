/**
 * E2E tests for SharedChatView (/s/:shareToken).
 *
 * This page is public (no auth) and uses raw fetch() against
 * VITE_API_BASE_URL (mock server on :4100) for all API calls.
 * SSE replay and JSON endpoints are configured on the mock server
 * via configureSSE().
 */
import { test, expect, configureSSE, resetMockServer } from './fixtures.js';

// ── Shared constants ──

const TOKEN = 'tok-abc';
const THREAD_ID = 'th-s1';

const sharedMetadata = {
  thread_id: THREAD_ID,
  title: 'Shared Analysis',
  workspace_name: 'Research',
  permissions: { allow_files: false, allow_download: false },
};

/** Replay events with turn_index and role fields required by SharedChatView. */
const replayEvents = [
  {
    event: 'user_message',
    data: {
      thread_id: THREAD_ID,
      turn_index: 0,
      content: 'What is AAPL?',
      timestamp: '2025-01-01T00:00:00Z',
      metadata: { msg_type: 'ptc', workspace_id: 'ws-shared' },
    },
  },
  {
    event: 'message_chunk',
    data: {
      thread_id: THREAD_ID,
      agent: 'model:test',
      id: 'lc_run--test',
      role: 'assistant',
      content: 'Apple Inc is a technology company.',
      content_type: 'text',
      turn_index: 0,
    },
  },
  {
    event: 'message_chunk',
    data: {
      thread_id: THREAD_ID,
      agent: 'model:test',
      id: 'lc_run--test',
      role: 'assistant',
      content: '',
      content_type: 'text',
      finish_reason: 'stop',
      turn_index: 0,
    },
  },
  {
    event: 'replay_done',
    data: { thread_id: THREAD_ID },
  },
];

// ── Helpers ──

/** Configure mock server with metadata JSON + replay SSE for the standard token. */
async function setupStandardScenarios(meta = sharedMetadata, events = replayEvents) {
  await configureSSE({
    method: 'GET',
    path: `/api/v1/public/shared/${TOKEN}`,
    json: meta,
    status: 200,
  });
  await configureSSE({
    method: 'GET',
    path: `/api/v1/public/shared/${TOKEN}/replay`,
    events,
    delay: 20,
  });
}

// ── Setup / teardown ──

test.beforeEach(async () => {
  await resetMockServer();
});

// ── Tests ──

test.describe('SharedChat page', () => {
  test('loading spinner before data', async ({ page }) => {
    // Intercept the metadata fetch via page.route() with a delay so the
    // loading spinner is visible before the response arrives.
    await page.route('**/api/v1/public/shared/tok-abc', async (route) => {
      await new Promise((r) => setTimeout(r, 2000));
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify(sharedMetadata),
      });
    });

    // Configure replay on the mock server for after metadata loads
    await configureSSE({
      method: 'GET',
      path: `/api/v1/public/shared/${TOKEN}/replay`,
      events: replayEvents,
      delay: 20,
    });

    await page.goto(`/s/${TOKEN}`);

    // Spinner should be visible while waiting for metadata
    const spinner = page.locator('.animate-spin');
    await expect(spinner).toBeVisible({ timeout: 3000 });
  });

  test('metadata title renders in topbar', async ({ page }) => {
    await setupStandardScenarios();
    await page.goto(`/s/${TOKEN}`);

    // workspace_name takes priority over title
    const heading = page.locator('h1');
    await expect(heading).toHaveText('Research', { timeout: 10000 });
  });

  test('SSE replay builds conversation', async ({ page }) => {
    await setupStandardScenarios();
    await page.goto(`/s/${TOKEN}`);

    // Wait for user message to appear
    await expect(page.getByText('What is AAPL?')).toBeVisible({ timeout: 10000 });

    // Wait for assistant response to appear
    await expect(page.getByText('Apple Inc is a technology company.')).toBeVisible({ timeout: 10000 });
  });

  test('read-only: no input box, disabled send', async ({ page }) => {
    await setupStandardScenarios();
    await page.goto(`/s/${TOKEN}`);

    // Wait for replay to complete so the UI is fully rendered
    await expect(page.getByText('What is AAPL?')).toBeVisible({ timeout: 10000 });

    // Verify read-only message is shown
    await expect(page.getByText('read-only shared conversation')).toBeVisible();

    // Verify the send button is disabled
    const sendButton = page.locator('button[disabled]').last();
    await expect(sendButton).toBeVisible();
    await expect(sendButton).toBeDisabled();
  });

  test('invalid token shows error page', async ({ page }) => {
    const badToken = 'bad-token';

    await configureSSE({
      method: 'GET',
      path: `/api/v1/public/shared/${badToken}`,
      status: 404,
      errorBody: { detail: 'Not found' },
    });

    await page.goto(`/s/${badToken}`);

    // Error message for 404
    await expect(
      page.getByText('This shared conversation is no longer available.')
    ).toBeVisible({ timeout: 10000 });

    // "Go to LangAlpha" link
    const link = page.getByRole('link', { name: 'Go to LangAlpha' });
    await expect(link).toBeVisible();
    await expect(link).toHaveAttribute('href', '/');
  });

  test('file panel toggles when allowed', async ({ page }) => {
    const metaWithFiles = {
      ...sharedMetadata,
      permissions: { allow_files: true, allow_download: true },
    };

    await setupStandardScenarios(metaWithFiles);

    // Configure the files endpoint
    await configureSSE({
      method: 'GET',
      path: `/api/v1/public/shared/${TOKEN}/files`,
      json: { path: '.', files: ['analysis.py'], source: 'workspace' },
      status: 200,
    });

    await page.goto(`/s/${TOKEN}`);

    // Wait for page to load
    await expect(page.locator('h1')).toHaveText('Research', { timeout: 10000 });

    // Verify the folder button is visible
    const folderButton = page.locator('button[title="Workspace Files"]');
    await expect(folderButton).toBeVisible();

    // Click the folder button to open file panel
    await folderButton.click();

    // Verify file name appears in the panel
    await expect(page.getByText('analysis.py')).toBeVisible({ timeout: 10000 });
  });
});
