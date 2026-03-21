/**
 * E2E tests for the ChatAgent page (/chat).
 *
 * Covers: WorkspaceGallery, ThreadGallery, ChatView (SSE streaming),
 * error handling, HITL plan approval, message editing, and file panel.
 */
import {
  configureSSE,
  resetMockServer,
  mockAPI,
  test,
  expect,
} from './fixtures.js';
import {
  sampleWorkspace,
  sampleThread,
  sseEvents,
} from './helpers/mockResponses.js';
import { loadFixture } from './helpers/loadFixture.js';

// -- Shared helpers --

const ws1 = sampleWorkspace();
const ws2 = sampleWorkspace({
  workspace_id: 'ws-2',
  name: 'Alpha Research',
  description: 'Second workspace',
});
const th1 = sampleThread();
const th2 = sampleThread({
  thread_id: 'th-2',
  workspace_id: 'ws-1',
  title: 'Second thread',
});

/** Standard overrides that make the workspace gallery show ws1 + ws2. */
function workspaceOverrides() {
  return {
    'GET /workspaces': { workspaces: [ws1, ws2], total: 2, limit: 20, offset: 0 },
  };
}

/** Overrides that populate a thread list for ws-1. */
function threadOverrides() {
  return {
    ...workspaceOverrides(),
    'GET /workspaces/ws-1': ws1,
    'GET /threads': { threads: [th1, th2], total: 2 },
  };
}

/** Overrides for the chat view with a specific thread. */
function chatViewOverrides() {
  return {
    ...threadOverrides(),
    'GET /threads/th-1': th1,
    'GET /threads/th-1/status': { can_reconnect: false, status: 'idle' },
    'GET /threads/th-1/turns': {
      thread_id: 'th-1',
      turns: [
        {
          turn_index: 0,
          edit_checkpoint_id: 'cp-edit-0',
          regenerate_checkpoint_id: 'cp-regen-0',
        },
      ],
      retry_checkpoint_id: 'cp-retry-0',
    },
    'GET /workspaces/ws-1/files': { files: [] },
  };
}

/** Configure replay SSE to return replay_done immediately (empty history). */
async function configureEmptyReplay() {
  await configureSSE({
    method: 'GET',
    path: '/api/v1/threads/th-1/messages/replay',
    events: [sseEvents.replayDone()],
    delay: 10,
  });
}

// ================================================================
// Workspace Gallery (/chat)
// ================================================================

test.describe('Workspace Gallery', () => {
  test.beforeEach(async () => {
    await resetMockServer();
  });

  test('workspace cards render with names', async ({ page }) => {
    await mockAPI(page, workspaceOverrides());
    await page.goto('/chat');

    // Both workspace names should appear
    await expect(page.getByText('Research', { exact: true })).toBeVisible({ timeout: 10000 });
    await expect(page.getByText('Alpha Research', { exact: true })).toBeVisible();
  });

  test('empty state shows create prompt', async ({ page }) => {
    // Override flash workspace POST to fail so no workspaces exist at all
    await mockAPI(page, {
      'POST /workspaces/flash': (route) =>
        route.fulfill({ status: 500, contentType: 'application/json', body: '{"detail":"error"}' }),
    });
    await page.goto('/chat');

    // The empty state shows a "Create Workspace" button
    await expect(page.locator('button', { hasText: 'Create Workspace' })).toBeVisible({ timeout: 10000 });
  });

  test('create workspace via dialog', async ({ page }) => {
    await mockAPI(page, {
      ...workspaceOverrides(),
      'POST /workspaces': (route) => {
        return route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify(sampleWorkspace({ workspace_id: 'ws-new', name: 'New Project' })),
        });
      },
    });
    await page.goto('/chat');

    // Wait for gallery to load
    await expect(page.getByText('Research', { exact: true })).toBeVisible({ timeout: 10000 });

    // Click "New workspace" header button (use getByRole to avoid matching the hidden mobile duplicate)
    await page.getByRole('button', { name: 'New workspace' }).click();

    // Modal should appear
    await expect(page.locator('h2.cwm-title')).toBeVisible();

    // Fill in the name and submit
    await page.locator('div.cwm-modal input').first().fill('New Project');
    await page.locator('button.cwm-btn-create').click();

    // Progress phase: wait for "done" state (open workspace button appears)
    await expect(page.locator('button.cwm-btn-create', { hasText: /Open Workspace/ })).toBeVisible({ timeout: 10000 });
  });

  test('delete workspace removes card', async ({ page }) => {
    // Use a dynamic mock: after DELETE is called, GET /workspaces returns without ws-1
    let wsDeleted = false;
    await mockAPI(page, {
      'GET /workspaces': (route) => {
        const data = wsDeleted
          ? { workspaces: [ws2], total: 1, limit: 20, offset: 0 }
          : { workspaces: [ws1, ws2], total: 2, limit: 20, offset: 0 };
        return route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify(data),
        });
      },
      'DELETE /workspaces/ws-1': (route) => {
        wsDeleted = true;
        return route.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
      },
    });
    await page.goto('/chat');

    // Wait for card to appear
    const researchCard = page.getByText('Research', { exact: true });
    await expect(researchCard).toBeVisible({ timeout: 10000 });

    // Hover on the workspace card to reveal the 3-dot menu
    const cardContainer = researchCard.locator('xpath=ancestor::div[contains(@class, "group")]').first();
    await cardContainer.hover();

    // Click the 3-dot menu button (MoreHorizontal icon)
    await cardContainer.locator('button:has(svg)').first().click();

    // Click Delete in the dropdown
    await page.locator('button', { hasText: 'Delete' }).first().click();

    // Confirm deletion in the modal
    await expect(page.locator('h2', { hasText: 'Delete Workspace' })).toBeVisible();
    await page.locator('button', { hasText: 'Delete' }).last().click();

    // The Research card should disappear (refetch returns without ws-1)
    await expect(page.getByText('Research', { exact: true })).not.toBeVisible({ timeout: 10000 });
  });

  test('click workspace navigates to thread gallery', async ({ page }) => {
    await mockAPI(page, {
      ...threadOverrides(),
    });
    await page.goto('/chat');

    // Wait for Research card
    const researchCard = page.getByText('Research', { exact: true });
    await expect(researchCard).toBeVisible({ timeout: 10000 });

    // Click the workspace card
    await researchCard.click();

    // Should navigate to thread gallery - workspace name appears as header
    await expect(page.locator('h1', { hasText: 'Research' })).toBeVisible({ timeout: 10000 });
  });
});

// ================================================================
// Thread Gallery (/chat/:wsId)
// ================================================================

test.describe('Thread Gallery', () => {
  test.beforeEach(async () => {
    await resetMockServer();
  });

  test('thread list renders', async ({ page }) => {
    await mockAPI(page, {
      ...threadOverrides(),
      'GET /workspaces/ws-1/files': { files: [] },
    });

    await page.goto('/chat/ws-1');

    // Thread titles should be visible
    await expect(page.locator('h3.text-sm.font-normal.truncate', { hasText: 'Test conversation' })).toBeVisible({ timeout: 10000 });
    await expect(page.locator('h3.text-sm.font-normal.truncate', { hasText: 'Second thread' })).toBeVisible();
  });

  test('click thread navigates to chat', async ({ page }) => {
    await mockAPI(page, {
      ...threadOverrides(),
      ...chatViewOverrides(),
      'GET /workspaces/ws-1/files': { files: [] },
    });
    await configureEmptyReplay();

    await page.goto('/chat/ws-1');

    // Click on first thread
    const threadCard = page.locator('h3.text-sm.font-normal.truncate', { hasText: 'Test conversation' });
    await expect(threadCard).toBeVisible({ timeout: 10000 });
    await threadCard.click();

    // Should navigate to chat view - textarea should appear
    await expect(page.locator('textarea')).toBeVisible({ timeout: 10000 });
  });

  test('delete thread removes from list', async ({ page }) => {
    await mockAPI(page, {
      ...threadOverrides(),
      'DELETE /threads/th-1': { success: true },
      'GET /workspaces/ws-1/files': { files: [] },
    });

    await page.goto('/chat/ws-1');

    // Wait for thread to appear
    const threadTitle = page.locator('h3.text-sm.font-normal.truncate', { hasText: 'Test conversation' });
    await expect(threadTitle).toBeVisible({ timeout: 10000 });

    // Hover on the thread card to reveal the delete button
    const threadRow = threadTitle.locator('xpath=ancestor::div[contains(@class, "group")]').first();
    await threadRow.hover();

    // Click delete button (title="Delete thread")
    await threadRow.locator('button[title="Delete thread"]').click();

    // Confirm in the delete modal
    await expect(page.locator('h2', { hasText: 'Delete Thread' })).toBeVisible();
    // Click the destructive Delete button inside the modal (last one)
    const modal = page.locator('div.fixed');
    await modal.locator('button', { hasText: 'Delete' }).last().click();

    // Thread should be removed (only "Second thread" remains)
    await expect(threadTitle).not.toBeVisible({ timeout: 10000 });
    await expect(page.locator('h3.text-sm.font-normal.truncate', { hasText: 'Second thread' })).toBeVisible();
  });
});

// ================================================================
// Chat View -- SSE Streaming (/chat/t/:tid)
// ================================================================

test.describe('Chat View -- SSE Streaming', () => {
  test.beforeEach(async () => {
    await resetMockServer();
  });

  test('history replay populates messages', async ({ page }) => {
    await mockAPI(page, chatViewOverrides());

    // Configure replay with a user message and assistant response
    await configureSSE({
      method: 'GET',
      path: '/api/v1/threads/th-1/messages/replay',
      events: [
        sseEvents.userMessage('What is AAPL trading at?', 0),
        sseEvents.messageChunk('Apple (AAPL) is currently trading at $185.50.'),
        sseEvents.finishStop(),
        sseEvents.creditUsage(),
        sseEvents.replayDone(),
      ],
      delay: 10,
    });

    await page.goto('/chat/t/th-1');

    // Both user and assistant messages should appear
    await expect(page.getByText('What is AAPL trading at?')).toBeVisible({ timeout: 10000 });
    await expect(page.getByText('Apple (AAPL) is currently trading at $185.50.')).toBeVisible({ timeout: 10000 });
  });

  test('send message streams response chunks', async ({ page }) => {
    await mockAPI(page, chatViewOverrides());
    await configureEmptyReplay();

    // Configure the SSE response for sending a message
    await configureSSE({
      method: 'POST',
      path: '/api/v1/threads/th-1/messages',
      events: [
        sseEvents.messageChunk('The S&P 500 '),
        sseEvents.messageChunk('is up 1.2% today.'),
        sseEvents.finishStop(),
        sseEvents.creditUsage(),
      ],
      delay: 30,
    });

    await page.goto('/chat/t/th-1');
    await page.waitForSelector('textarea', { timeout: 10000 });

    // Type a message and send
    await page.locator('textarea').fill('How is the market today?');
    await page.locator('button[aria-label="Send message"]').click();

    // Streamed response should appear
    await expect(page.getByText('is up 1.2% today.')).toBeVisible({ timeout: 15000 });
  });

  test('tool call renders tool card with result', async ({ page }) => {
    await mockAPI(page, chatViewOverrides());

    const toolCallId = 'toolu_web_search_1';

    // Replay with a tool call sequence
    await configureSSE({
      method: 'GET',
      path: '/api/v1/threads/th-1/messages/replay',
      events: [
        sseEvents.userMessage('Search for NVDA earnings'),
        sseEvents.toolCalls([{ name: 'WebSearch', args: { query: 'NVDA earnings Q4 2025' }, id: toolCallId }]),
        sseEvents.finishToolCalls(),
        sseEvents.toolCallResult(toolCallId, 'NVIDIA reported Q4 2025 revenue of $22.1B, beating estimates.'),
        sseEvents.messageChunk('Based on the search results, NVIDIA reported strong Q4 earnings.'),
        sseEvents.finishStop(),
        sseEvents.creditUsage(),
        sseEvents.replayDone(),
      ],
      delay: 10,
    });

    await page.goto('/chat/t/th-1');

    // The tool call card should appear (collapsed as "N step(s) completed")
    await expect(page.getByText('step completed')).toBeVisible({ timeout: 10000 });
    // The final assistant text should appear
    await expect(page.getByText('NVIDIA reported strong Q4 earnings')).toBeVisible({ timeout: 10000 });
  });

  test('plan mode interrupt shows approval UI', async ({ page }) => {
    await mockAPI(page, chatViewOverrides());

    // Replay that ends with an interrupt
    await configureSSE({
      method: 'GET',
      path: '/api/v1/threads/th-1/messages/replay',
      events: [
        sseEvents.userMessage('Analyze TSLA'),
        sseEvents.interrupt('int-1', 'Here is my plan:\n1. Fetch TSLA financials\n2. Create a chart'),
        sseEvents.replayDone(),
      ],
      delay: 10,
    });

    await page.goto('/chat/t/th-1');

    // Plan approval card should be visible
    await expect(page.getByText('Plan Approval Required')).toBeVisible({ timeout: 10000 });
    // Approve and Reject buttons should be present
    await expect(page.getByText('Approve')).toBeVisible();
    await expect(page.getByText('Reject')).toBeVisible();
  });

  test('approve plan resumes workflow', async ({ page }) => {
    await mockAPI(page, {
      ...chatViewOverrides(),
      'POST /threads/th-1/interrupt': { success: true },
    });

    // Replay with interrupt
    await configureSSE({
      method: 'GET',
      path: '/api/v1/threads/th-1/messages/replay',
      events: [
        sseEvents.userMessage('Analyze TSLA'),
        sseEvents.interrupt('int-1', 'Plan:\n1. Fetch data\n2. Analyze'),
        sseEvents.replayDone(),
      ],
      delay: 10,
    });

    // Configure the POST for plan approval (HITL response sends as message)
    await configureSSE({
      method: 'POST',
      path: '/api/v1/threads/th-1/messages',
      events: [
        sseEvents.messageChunk('Executing the plan...'),
        sseEvents.finishStop(),
        sseEvents.creditUsage(),
      ],
      delay: 30,
    });

    await page.goto('/chat/t/th-1');

    // Wait for approval UI
    await expect(page.getByText('Plan Approval Required')).toBeVisible({ timeout: 10000 });

    // Click Approve
    await page.getByText('Approve').click();

    // After approval, status should change to "Plan Approved"
    await expect(page.getByText('Plan Approved')).toBeVisible({ timeout: 10000 });
  });

  test('stop button interrupts streaming', async ({ page }) => {
    let interruptCalled = false;
    await mockAPI(page, {
      ...chatViewOverrides(),
      'POST /threads/*/interrupt': (route) => {
        interruptCalled = true;
        return route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({ success: true }),
        });
      },
    });
    await configureEmptyReplay();

    // Configure a slow SSE stream
    await configureSSE({
      method: 'POST',
      path: '/api/v1/threads/th-1/messages',
      events: [
        sseEvents.messageChunk('Starting analysis...'),
        sseEvents.messageChunk(' Step 1 complete.'),
        sseEvents.messageChunk(' Step 2 in progress.'),
        sseEvents.messageChunk(' Almost done.'),
        sseEvents.finishStop(),
        sseEvents.creditUsage(),
      ],
      delay: 500, // Slow enough to click stop
    });

    await page.goto('/chat/t/th-1');
    await page.waitForSelector('textarea', { timeout: 10000 });

    // Send a message
    await page.locator('textarea').fill('Run a long analysis');
    await page.locator('button[aria-label="Send message"]').click();

    // Wait for streaming to start, then click Stop
    await expect(page.getByText('Starting analysis...')).toBeVisible({ timeout: 10000 });
    await page.locator('button[title="Stop"]').click();

    // Verify the interrupt was sent (via page.route capture)
    // Wait for the button to change to "Stopping..." (deterministic signal that the click handler ran)
    await expect(page.locator('button[title="Stopping..."]')).toBeVisible({ timeout: 5000 });
    expect(interruptCalled).toBe(true);
  });

  test('403 on thread shows access denied', async ({ page }) => {
    await mockAPI(page, {
      ...workspaceOverrides(),
      'GET /threads/th-forbidden': (route) =>
        route.fulfill({ status: 403, contentType: 'application/json', body: '{"detail":"forbidden"}' }),
    });

    await page.goto('/chat/t/th-forbidden');

    // Access denied page should show
    await expect(page.getByText("You don't have access to this conversation")).toBeVisible({ timeout: 10000 });
    await expect(page.getByText('Go to Chats')).toBeVisible();
  });

  test('429 rate limit shows error message', async ({ page }) => {
    await mockAPI(page, chatViewOverrides());
    await configureEmptyReplay();

    // Configure the send endpoint to return 429
    await configureSSE({
      method: 'POST',
      path: '/api/v1/threads/th-1/messages',
      status: 429,
      errorBody: { detail: 'Rate limit exceeded. Please try again later.' },
      events: [],
    });

    await page.goto('/chat/t/th-1');
    await page.waitForSelector('textarea', { timeout: 10000 });

    // Send a message
    await page.locator('textarea').fill('test query');
    await page.locator('button[aria-label="Send message"]').click();

    // Error message should appear
    await expect(page.getByText(/[Rr]ate limit/)).toBeVisible({ timeout: 10000 });
  });

  test('edit user message forks conversation', async ({ page }) => {
    await mockAPI(page, chatViewOverrides());

    // Replay with a conversation
    await configureSSE({
      method: 'GET',
      path: '/api/v1/threads/th-1/messages/replay',
      events: [
        sseEvents.userMessage('Tell me about AAPL'),
        sseEvents.messageChunk('Apple Inc. is a technology company.'),
        sseEvents.finishStop(),
        sseEvents.creditUsage(),
        sseEvents.replayDone(),
      ],
      delay: 10,
    });

    // Configure the response after editing
    await configureSSE({
      method: 'POST',
      path: '/api/v1/threads/th-1/messages',
      events: [
        sseEvents.messageChunk('Google (GOOGL) is a subsidiary of Alphabet.'),
        sseEvents.finishStop(),
        sseEvents.creditUsage(),
      ],
      delay: 30,
    });

    await page.goto('/chat/t/th-1');

    // Wait for the user message to appear
    await expect(page.getByText('Tell me about AAPL')).toBeVisible({ timeout: 10000 });

    // Hover over the user message bubble to reveal the edit button
    const userMsg = page.getByText('Tell me about AAPL');
    await userMsg.hover();

    // Click the edit button
    await page.locator('button[title="Edit message"]').click();

    // An edit textarea or input should appear -- find it and modify the message
    // The edit mode replaces the message content with an editable area
    const editArea = page.locator('textarea').first();
    await editArea.fill('Tell me about GOOGL');

    // Submit the edit (press Enter or click send)
    await editArea.press('Enter');

    // The new response should stream in
    await expect(page.getByText('Google (GOOGL) is a subsidiary of Alphabet.')).toBeVisible({ timeout: 15000 });
  });

  test('regenerate re-sends from checkpoint', async ({ page }) => {
    await mockAPI(page, chatViewOverrides());

    // Replay with a conversation
    await configureSSE({
      method: 'GET',
      path: '/api/v1/threads/th-1/messages/replay',
      events: [
        sseEvents.userMessage('Explain options trading'),
        sseEvents.messageChunk('Options are financial derivatives.'),
        sseEvents.finishStop(),
        sseEvents.creditUsage(),
        sseEvents.replayDone(),
      ],
      delay: 10,
    });

    // Configure regenerate response
    await configureSSE({
      method: 'POST',
      path: '/api/v1/threads/th-1/messages',
      events: [
        sseEvents.messageChunk('Options trading involves contracts that give the holder the right to buy or sell.'),
        sseEvents.finishStop(),
        sseEvents.creditUsage(),
      ],
      delay: 30,
    });

    await page.goto('/chat/t/th-1');

    // Wait for the assistant message
    await expect(page.getByText('Options are financial derivatives.')).toBeVisible({ timeout: 10000 });

    // Hover over the assistant message to reveal the regenerate button
    const assistantMsg = page.getByText('Options are financial derivatives.');
    await assistantMsg.hover();

    // Click regenerate
    await page.locator('button[title="Regenerate response"]').click();

    // New response should appear
    await expect(page.getByText('Options trading involves contracts')).toBeVisible({ timeout: 15000 });
  });

  test('file panel opens and lists files', async ({ page }) => {
    await mockAPI(page, {
      ...chatViewOverrides(),
      'GET /workspaces/ws-1/files': { files: ['report.pdf', 'analysis.py', 'chart.png'] },
    });
    await configureEmptyReplay();

    await page.goto('/chat/t/th-1');
    await page.waitForSelector('textarea', { timeout: 10000 });

    // Click the file panel toggle button (title="Workspace Files")
    await page.locator('button[title="Workspace Files"]').click();

    // File panel should open and list the files
    await expect(page.getByText('report.pdf')).toBeVisible({ timeout: 10000 });
    await expect(page.getByText('analysis.py')).toBeVisible();
    await expect(page.getByText('chart.png')).toBeVisible();
  });

  test('workspace starting shows warm-up indicator', async ({ page }) => {
    await mockAPI(page, chatViewOverrides());

    // Replay includes workspace_status starting event before content
    await configureSSE({
      method: 'GET',
      path: '/api/v1/threads/th-1/messages/replay',
      events: [sseEvents.replayDone()],
      delay: 10,
    });

    // Configure send to show workspace starting status
    await configureSSE({
      method: 'POST',
      path: '/api/v1/threads/th-1/messages',
      events: [
        sseEvents.workspaceStatus('starting'),
        sseEvents.workspaceStatus('ready'),
        sseEvents.messageChunk('Workspace is ready. Here is your answer.'),
        sseEvents.finishStop(),
        sseEvents.creditUsage(),
      ],
      delay: 100,
    });

    await page.goto('/chat/t/th-1');
    await page.waitForSelector('textarea', { timeout: 10000 });

    // Send a message to trigger workspace starting
    await page.locator('textarea').fill('Run analysis');
    await page.locator('button[aria-label="Send message"]').click();

    // Warm-up indicator should appear
    await expect(page.getByText('Starting workspace...')).toBeVisible({ timeout: 10000 });

    // Eventually the response arrives and the indicator goes away
    await expect(page.getByText('Workspace is ready. Here is your answer.')).toBeVisible({ timeout: 15000 });
  });

  test('reconnect resumes in-progress stream', async ({ page }) => {
    await mockAPI(page, {
      ...chatViewOverrides(),
      'GET /threads/th-1/status': { can_reconnect: true, status: 'streaming' },
    });

    // Replay returns conversation history
    await configureSSE({
      method: 'GET',
      path: '/api/v1/threads/th-1/messages/replay',
      events: [
        sseEvents.userMessage('Analyze the portfolio'),
        sseEvents.messageChunk('Starting portfolio analysis...'),
        sseEvents.replayDone(),
      ],
      delay: 10,
    });

    // Reconnect stream picks up where it left off
    await configureSSE({
      method: 'GET',
      path: '/api/v1/threads/th-1/messages/stream',
      events: [
        sseEvents.messageChunk(' Your portfolio returned 12% YTD.'),
        sseEvents.finishStop(),
        sseEvents.creditUsage(),
      ],
      delay: 30,
    });

    await page.goto('/chat/t/th-1');

    // The reconnected stream content should appear
    await expect(page.getByText('Your portfolio returned 12% YTD.')).toBeVisible({ timeout: 15000 });
  });
});

// ================================================================
// Steering -- History Replay (captured SSE fixtures)
// ================================================================

/** Overrides for multi-turn chat with steering. */
function steeringChatOverrides(turnCount = 1) {
  const turns = Array.from({ length: turnCount }, (_, i) => ({
    turn_index: i,
    edit_checkpoint_id: `cp-edit-${i}`,
    regenerate_checkpoint_id: `cp-regen-${i}`,
  }));
  return {
    ...threadOverrides(),
    'GET /threads/th-1': th1,
    'GET /threads/th-1/status': { can_reconnect: false, status: 'idle' },
    'GET /threads/th-1/turns': {
      thread_id: 'th-1',
      turns,
      retry_checkpoint_id: 'cp-retry-0',
    },
    'GET /workspaces/ws-1/files': { files: [] },
  };
}

test.describe('Steering -- History Replay', () => {
  test.beforeEach(async () => {
    await resetMockServer();
  });

  test('steering_delivered renders user bubble and post-steering content', async ({ page }) => {
    await mockAPI(page, steeringChatOverrides(1));

    // Fixture: single turn where the user steers the agent mid-stream with
    // "focus on speaker list". The agent produces content before and after.
    const turn0Events = loadFixture('steering-single-turn.json', 0);

    await configureSSE({
      method: 'GET',
      path: '/api/v1/threads/th-1/messages/replay',
      events: [
        sseEvents.userMessage('any investment opportunity?', 0),
        ...turn0Events,
        sseEvents.replayDone(),
      ],
      delay: 5,
    });

    await page.goto('/chat/t/th-1');

    // User query should appear
    await expect(page.getByText('any investment opportunity?')).toBeVisible({ timeout: 15000 });

    // Steering user message should appear as a delivered bubble
    await expect(page.getByText('focus on speaker list')).toBeVisible({ timeout: 15000 });

    // Post-steering assistant content should appear (the agent continued after steering)
    await expect(page.getByText('NVIDIA GTC 2026').first()).toBeVisible({ timeout: 15000 });

    // The turn has 1 steering_delivered → 2 assistant messages (pre + post steering).
    // Each assistant message renders exactly one img[alt="Assistant"] avatar.
    const assistantAvatars = page.locator('img[alt="Assistant"]');
    await expect(assistantAvatars).toHaveCount(2);
  });

  test('subagent steering_delivered does not create empty main-chat placeholders', async ({ page }) => {
    await mockAPI(page, steeringChatOverrides(2));

    // Fixture: 2-turn conversation. Turn 1 spawns 3 subagents, user steers
    // mid-stream, and the main agent forwards steering to all 3 subagents.
    // This produces 1 main steering_delivered + 3 subagent steering_delivered events.
    //
    // Regression: before the fix, each subagent steering_delivered was caught by
    // the main-agent history handler, creating 3 empty assistant placeholders
    // (inflating the assistant avatar count from 4 to 7).
    const turn0Events = loadFixture('steering-single-turn.json', 0);
    const turn1Events = loadFixture('steering-with-subagents.json', 1);

    await configureSSE({
      method: 'GET',
      path: '/api/v1/threads/th-1/messages/replay',
      events: [
        sseEvents.userMessage('any investment opportunity?', 0),
        ...turn0Events,
        sseEvents.userMessage('find investment ideas from GTC 2026', 1),
        ...turn1Events,
        sseEvents.replayDone(),
      ],
      delay: 2,
    });

    await page.goto('/chat/t/th-1');

    // Wait for post-steering content to confirm replay completed
    await expect(page.getByText('All three subagents updated')).toBeVisible({ timeout: 30000 });

    // Main steering user message should be visible (from the steering_delivered event)
    await expect(page.getByText('let subagent group its finding by sector')).toBeVisible();

    // Regression gate: count assistant avatars. Each assistant message renders
    // exactly one img[alt="Assistant"]. Expected layout:
    //   Turn 0: 2 assistants (pre-steering + post-steering)
    //   Turn 1: 2 assistants (pre-steering + post-steering)
    //   Total: 4
    // Before fix: 3 subagent steering_delivered events inflated this to 7.
    const assistantAvatars = page.locator('img[alt="Assistant"]');
    await expect(assistantAvatars).toHaveCount(4);
  });
});
