/**
 * E2E for the file panel's editor.
 *
 * The editor opens on the verbatim read, while the viewer's body comes from the
 * paged one, which the server returns as "\n".join(text.splitlines()): no final
 * newline, CRLF as LF. The unsaved marker used to measure the draft against that
 * body, so once it refetched an untouched file read as edited.
 */
import fs from 'node:fs';
import path from 'node:path';
import { configureSSE, resetMockServer, mockAPI, test, expect } from './fixtures.js';
import { sseEvents } from './helpers/mockResponses.js';
import { chatViewOverrides, WS, TH } from './helpers/chatScenario.js';

const FILE = 'notes.md';
const VERBATIM = 'line one\r\nline two\r\n';
const PAGED = 'line one\nline two';

const json = (route, body) => route.fulfill({
  status: 200,
  contentType: 'application/json',
  body: JSON.stringify(body),
});

/** Serves FILE's two reads and records which one each request was. */
async function mockFile(page, writes = []) {
  const reads = [];
  await mockAPI(page, {
    [`PUT /workspaces/${WS}/files/write`]: (route) => {
      writes.push(JSON.parse(route.request().postData()).content);
      return json(route, {});
    },
    ...chatViewOverrides(),
    [`GET /workspaces/${WS}/files`]: { files: [FILE] },
    [`GET /workspaces/${WS}/files/read`]: (route) => {
      const full = new URL(route.request().url()).searchParams.get('unlimited') === 'true';
      reads.push(full ? 'full' : 'paged');
      return json(route, { workspace_id: WS, path: FILE, content: full ? VERBATIM : PAGED, mime: 'text/markdown', truncated: false });
    },
  });
  await configureSSE({
    method: 'GET',
    path: `/api/v1/threads/${TH}/messages/replay`,
    events: [sseEvents.replayDone()],
    delay: 10,
  });
  return reads;
}

// Monaco's AMD build, which the app loads from its CDN, served from the
// installed package instead, so a spec that types into the editor runs offline.
const MONACO_MIN = path.resolve(import.meta.dirname, '../node_modules/monaco-editor/min');
async function serveMonacoLocally(page) {
  await page.route(/cdn\.jsdelivr\.net\/npm\/monaco-editor@[^/]+\/min\//, (route) => {
    const rel = new URL(route.request().url()).pathname.replace(/^.*?\/min\//, '');
    const file = path.join(MONACO_MIN, rel);
    if (!file.startsWith(MONACO_MIN + path.sep) || !fs.existsSync(file)) return route.fulfill({ status: 404 });
    return route.fulfill({ path: file });
  });
}

async function openEditor(page) {
  await page.goto(`/chat/t/${TH}`);
  await page.waitForSelector('textarea', { timeout: 10000 });
  await page.locator('button[title="Workspace Files"]').click();
  // Dispatched rather than clicked: on a cold load the tree column's entry
  // animation can stall with the row still off screen, and the tree is not
  // what this spec is about.
  await page.getByRole('treeitem', { name: FILE }).dispatchEvent('click');
  await page.locator('button[title="Edit file"]').click();
}

test.describe('file editor', () => {
  test.beforeEach(async () => {
    await resetMockServer();
  });

  test('an untouched file stays clean when its body refetches', async ({ page }) => {
    // The marker and the Save button are under test, not Monaco, which comes
    // from its CDN: a loader that never answers keeps the editor on its
    // placeholder without a network fetch.
    await page.route(/cdn\.jsdelivr\.net/, (route) => route.fulfill({
      contentType: 'text/javascript',
      body: 'self.require = Object.assign(function () {}, { config: function () {} });',
    }));
    const reads = await mockFile(page);
    await openEditor(page);

    const name = page.locator('.file-panel-crumb.is-file');
    const save = page.locator('button[title="Save (Cmd+S)"]');
    await expect(save).toBeDisabled();
    await expect(name).toHaveText(FILE);

    // Past the body's fresh window, coming back to the window refetches it.
    await page.clock.install();
    await page.clock.fastForward('01:05');
    await page.evaluate(() => window.dispatchEvent(new Event('visibilitychange')));
    await expect.poll(() => reads).toEqual(['paged', 'full', 'paged']);

    await expect(name).toHaveText(FILE);
    await expect(save).toBeDisabled();
  });

  test('Cmd+S straight after typing saves every keystroke', async ({ page }) => {
    // Monaco reports typing outside any discrete event, so the render that
    // shows the edit can land after the next keypress. Cmd+S in that gap used
    // to find nothing to save and swallow the press.
    await serveMonacoLocally(page);
    const writes = [];
    await mockFile(page, writes);
    await openEditor(page);

    const lines = page.locator('.monaco-editor .view-lines');
    await lines.waitFor({ timeout: 30000 });
    await lines.click();
    await page.keyboard.press('ControlOrMeta+End');
    await page.keyboard.type('print(1)');
    await page.keyboard.press('ControlOrMeta+s');

    await expect(page.getByRole('dialog')).toBeVisible();
    await page.keyboard.press('Enter');
    await expect.poll(() => writes).toEqual([`${VERBATIM}print(1)`]);
  });

  test('a save that lands with another tab in front leaves nothing unsaved', async ({ page }) => {
    // Switching tabs parks the draft, measured against the text it started
    // from. A save that answered while it was parked left it there, so the
    // file held the edit and its tab still asked to discard it.
    const OTHER = 'other.md';
    let land;
    const held = new Promise((resolve) => { land = resolve; });
    const writes = [];
    await serveMonacoLocally(page);
    await mockAPI(page, {
      [`PUT /workspaces/${WS}/files/write`]: async (route) => {
        writes.push(JSON.parse(route.request().postData()).content);
        await held;
        return json(route, {});
      },
      ...chatViewOverrides(),
      [`GET /workspaces/${WS}/files`]: { files: [FILE, OTHER] },
      [`GET /workspaces/${WS}/files/read`]: (route) => {
        const p = new URL(route.request().url()).searchParams.get('path');
        return json(route, { workspace_id: WS, path: p, content: p === FILE ? VERBATIM : 'other', mime: 'text/markdown', truncated: false });
      },
    });
    await configureSSE({
      method: 'GET',
      path: `/api/v1/threads/${TH}/messages/replay`,
      events: [sseEvents.replayDone()],
      delay: 10,
    });
    await openEditor(page);

    const lines = page.locator('.monaco-editor .view-lines');
    await lines.waitFor({ timeout: 30000 });
    await lines.click();
    await page.keyboard.press('ControlOrMeta+End');
    await page.keyboard.type('x');
    await page.keyboard.press('ControlOrMeta+s');
    await page.getByRole('dialog').getByRole('button', { name: 'Save' }).click();
    await expect.poll(() => writes).toEqual([`${VERBATIM}x`]);

    const tab = (name) => page.locator('[role="tab"]', { hasText: name });
    await page.getByRole('button', { name: 'Toggle file tree' }).click();
    await page.getByRole('treeitem', { name: OTHER }).dispatchEvent('click');
    await expect(tab(OTHER)).toHaveAttribute('aria-selected', 'true');
    const answered = page.waitForResponse((r) => r.url().includes('/files/write'));
    land();
    await answered;
    // The write's answer reaches the hook a few microtasks after the response.
    await page.evaluate(() => new Promise((resolve) => setTimeout(resolve, 100)));

    await tab(FILE).click();
    await expect(page.locator('button[title="Edit file"]')).toBeVisible();
    await page.getByRole('button', { name: `Close ${FILE}` }).click();
    await expect(tab(FILE)).toHaveCount(0);
    await expect(page.getByRole('dialog', { name: 'Discard changes' })).toHaveCount(0);
  });

  test('the chat header closes a panel holding a draft only through the app dialog', async ({ page }) => {
    // The chat's own exits asked with a native confirm, which froze the page,
    // the chat's stream included, and read the draft as last rendered.
    const native = [];
    page.on('dialog', (dialog) => { native.push(dialog.type()); dialog.dismiss(); });
    await serveMonacoLocally(page);
    await mockFile(page);
    await openEditor(page);

    const lines = page.locator('.monaco-editor .view-lines');
    await lines.waitFor({ timeout: 30000 });
    await lines.click();
    await page.keyboard.press('ControlOrMeta+End');
    await page.keyboard.type('x');

    const toggle = page.locator('button[title="Workspace Files"]');
    const ask = page.getByRole('dialog', { name: 'Discard changes' });
    await toggle.click();
    await expect(ask).toBeVisible();
    await page.keyboard.press('Escape');
    await expect(ask).toBeHidden();
    await expect(lines).toContainText('x');

    await toggle.click();
    await ask.getByRole('button', { name: 'Discard' }).click();
    await expect(page.locator('.file-panel-crumb')).toHaveCount(0);
    expect(native).toEqual([]);
  });

  test('a keyboard close held by the discard question still hands focus to the header toggle', async ({ page }) => {
    await serveMonacoLocally(page);
    await mockFile(page);
    await openEditor(page);

    const lines = page.locator('.monaco-editor .view-lines');
    await lines.waitFor({ timeout: 30000 });
    await lines.click();
    await page.keyboard.press('ControlOrMeta+End');
    await page.keyboard.type('x');

    await page.locator('[data-panel-toggle="panel"]').focus();
    await page.keyboard.press('Enter');
    const ask = page.getByRole('dialog', { name: 'Discard changes' });
    await expect(ask).toBeVisible();
    // Longer than the hand-off waits for a toggle to render, so a hand-off
    // started by the press rather than the close would have given up.
    await page.waitForTimeout(2500);
    await ask.getByRole('button', { name: 'Discard' }).focus();
    await page.keyboard.press('Enter');

    await expect(page.locator('[data-panel-toggle="header"]')).toBeFocused();
  });
});
