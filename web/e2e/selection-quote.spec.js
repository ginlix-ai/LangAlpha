/**
 * Quoting a selection from the transcript: toolbar over the text, a reply in the
 * popover, a token in the draft that typed words go around, and a chip with the
 * quote and reply in the sent message.
 */
import { configureSSE, getCapturedRequests, mockAPI, resetMockServer, test, expect } from './fixtures.js';
import { sseEvents } from './helpers/mockResponses.js';
import { TH, WS, chatViewOverrides, replayNothing } from './helpers/chatScenario.js';
import fs from 'node:fs';
import path from 'node:path';

const FIRST = "#662's three Codex threads are fixed and resolved. The fix round is pushed at bfd7de0c, on the current main 29ff9cd5.";
const SECOND = 'The summary line no longer disappears when the server cannot read the state at the end of a run.';

async function openWithReply(page, lead = '') {
  await mockAPI(page, chatViewOverrides());
  await configureSSE({
    method: 'GET',
    path: `/api/v1/threads/${TH}/messages/replay`,
    events: [
      sseEvents.userMessage('How did the round go?', 0),
      sseEvents.messageChunk(`${lead}${FIRST}\n\nWhat changed for users:\n\n- ${SECOND}\n- Skip now survives a send that falls back.`),
      sseEvents.finishStop(),
      sseEvents.replayDone(),
    ],
    delay: 5,
  });
  await page.goto(`/chat/t/${TH}`);
  await expect(page.getByText(SECOND)).toBeVisible({ timeout: 15000 });
}

/** Selects the whole of the element holding `text` the way a drag does. */
async function select(page, text) {
  const box = await page.getByText(text, { exact: false }).first().boundingBox();
  await page.evaluate((needle) => {
    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
    while (walker.nextNode()) {
      const node = walker.currentNode;
      const at = node.data.indexOf(needle);
      if (at < 0) continue;
      const range = document.createRange();
      range.setStart(node, at);
      range.setEnd(node, at + needle.length);
      const selection = window.getSelection();
      selection.removeAllRanges();
      selection.addRange(range);
      return;
    }
  }, text);
  await page.mouse.move(box.x + 4, box.y + 4);
  await page.evaluate(() => document.querySelector('[data-message-role]').dispatchEvent(new MouseEvent('mouseup', { bubbles: true })));
}

const FILE_LINE = 'Revenue grew because the data center segment doubled year on year.';

async function openFile(page) {
  await mockAPI(page, {
    ...chatViewOverrides(),
    [`GET /workspaces/${WS}/files`]: { files: ['notes.md'] },
    [`GET /workspaces/${WS}/files/read`]: (route) => route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ workspace_id: WS, path: 'notes.md', content: `# Notes\n\n${FILE_LINE}\n`, mime: 'text/markdown', truncated: false }),
    }),
  });
  await replayNothing();
  await page.goto(`/chat/t/${TH}`);
  await page.waitForSelector('textarea', { timeout: 10000 });
  await page.locator('button[title="Workspace Files"]').click();
  await page.getByRole('treeitem', { name: 'notes.md' }).dispatchEvent('click');
  await expect(page.getByText(FILE_LINE)).toBeVisible({ timeout: 15000 });
}

const MONACO_MIN = path.resolve(import.meta.dirname, '../node_modules/monaco-editor/min');
async function serveMonacoLocally(page) {
  await page.route(/cdn\.jsdelivr\.net\/npm\/monaco-editor@[^/]+\/min\//, (route) => {
    const rel = new URL(route.request().url()).pathname.replace(/^.*?\/min\//, '');
    const file = path.join(MONACO_MIN, rel);
    if (!file.startsWith(MONACO_MIN + path.sep) || !fs.existsSync(file)) return route.fulfill({ status: 404 });
    return route.fulfill({ path: file });
  });
}

/** A real drag across the line, ending past it. */
async function drag(page, locator) {
  const box = await locator.boundingBox();
  await page.mouse.move(box.x + 1, box.y + box.height / 2);
  await page.mouse.down();
  await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2, { steps: 4 });
  await page.mouse.move(box.x + box.width + 40, box.y + box.height / 2, { steps: 4 });
  await page.mouse.up();
}

test.describe('quoting a selection', () => {
  test.beforeEach(async () => { await resetMockServer(); });

  test('a reply and a quote go out with the next message', async ({ page }) => {
    await openWithReply(page);

    await select(page, FIRST);
    const toolbar = page.getByRole('toolbar', { name: 'Selected text' });
    await expect(toolbar).toBeVisible();

    await toolbar.getByRole('button', { name: 'Quote' }).click();
    const popover = page.getByRole('dialog', { name: 'Quote from Analyst' });
    await expect(popover).toBeVisible();
    const reply = popover.getByPlaceholder('Reply to Analyst');
    await expect(reply).toBeFocused();

    // Named for a screen reader, not shown: the text is highlighted beside the box.
    await expect(popover.getByRole('blockquote')).toContainText("#662's three Codex threads");
    expect((await popover.getByRole('blockquote').boundingBox()).height).toBeLessThanOrEqual(1);
    await reply.fill('why this order');
    await reply.press('Enter');
    await expect(popover).toBeHidden();

    // Adding hands focus to the composer, which would take a selection with it.
    await expect(page.locator('.chat-input-container textarea')).toBeFocused();
    // The Enter that added it was not a walk through the page: no focus ring.
    await expect(page.locator('html')).toHaveAttribute('data-pointer-focus', '');

    // A second quote, this one with nothing written about it.
    await select(page, SECOND);
    await toolbar.getByRole('button', { name: 'More' }).click();
    await page.getByRole('menuitem', { name: 'Add to context' }).click();

    const tokens = page.locator('.chat-input-container .draft-token-quote');
    await expect(tokens).toHaveCount(2);
    await expect(tokens.first()).toContainText("#662's three");

    // The reply stays out of the draft, and shows on the token's hover.
    const draft = page.locator('.chat-input-container textarea');
    await expect(draft).not.toHaveValue(/why this order/);
    await tokens.first().hover();
    await expect(page.locator('[data-radix-popper-content-wrapper]')).toContainText('why this order');
    await page.mouse.move(0, 0);

    // Typed words go anywhere around the tokens, the caret stepping over them.
    await draft.press('Home');
    await draft.pressSequentially('first ');
    await draft.press('End');
    await draft.pressSequentially('last');
    const betweenTokens = await draft.evaluate((el) => el.value.indexOf('\u2060 \u2003') + 2);
    expect(betweenTokens).toBeGreaterThan(1);
    await draft.evaluate((el, at) => el.setSelectionRange(at, at), betweenTokens);
    await draft.pressSequentially('middle ');

    await configureSSE({ method: 'POST', path: `/api/v1/threads/${TH}/messages`, events: [sseEvents.messageChunk('ok'), sseEvents.finishStop()], delay: 5 });
    await draft.press('Enter');
    await expect(tokens).toHaveCount(0);

    // The agent gets each quote where it sat, with its reply tagged.
    const requests = await getCapturedRequests();
    const sent = JSON.stringify(requests.filter((r) => r.method === 'POST').at(-1));
    expect(sent).toContain('why this order</reply>');
    const order = ['first ', 'three Codex threads', '</quote>', 'why this order</reply>', 'middle ', 'no longer disappears', 'last'].map((word) => sent.indexOf(word));
    expect(order).toEqual([...order].sort((a, b) => a - b));
    expect(order).not.toContain(-1);

    // Sent, each quote stays folded, its text and reply only in the hover card.
    const bubble = page.locator('[data-message-role="user"]').last();
    await expect(bubble.locator('.composer-quote-chip')).toHaveCount(2);
    await expect(bubble).not.toContainText('why this order');
    await expect(bubble).toContainText('middle');
    const card = page.getByRole('dialog').or(page.locator('[data-radix-popper-content-wrapper]')).first();
    // The bubble redraws as the turn ends, and a still mouse gets no new enter.
    await expect(async () => {
      await page.mouse.move(0, 0);
      await bubble.locator('.composer-quote-chip').first().hover();
      await expect(card).toContainText('on the current main 29ff9cd5', { timeout: 1000 });
    }).toPass();
    await expect(card).toContainText('why this order');
  });

  test('an image takes a reply, and goes out as an attachment with it', async ({ page }) => {
    const svg = '<svg xmlns="http://www.w3.org/2000/svg" width="320" height="180"><rect width="320" height="180" fill="#6a8"/></svg>';
    await mockAPI(page, {
      ...chatViewOverrides(),
      [`GET /workspaces/${WS}/files/download`]: (route) => route.fulfill({ status: 200, contentType: 'image/svg+xml', body: svg }),
    });
    await configureSSE({
      method: 'GET',
      path: `/api/v1/threads/${TH}/messages/replay`,
      events: [sseEvents.userMessage('Chart it', 0), sseEvents.messageChunk('Here it is:\n\n![revenue chart](results/chart.svg)'), sseEvents.finishStop(), sseEvents.replayDone()],
      delay: 5,
    });
    await page.goto(`/chat/t/${TH}`);
    const image = page.getByRole('img', { name: 'revenue chart' });
    await expect(image).toBeVisible({ timeout: 15000 });
    await image.hover();
    await page.getByRole('button', { name: 'Reply to this image' }).click();
    const popover = page.getByRole('dialog', { name: 'Reply to this image' });
    await expect(popover.getByRole('textbox')).toBeFocused();
    await popover.getByRole('textbox').fill('why the dip in Q3?');
    await popover.getByRole('textbox').press('Enter');
    await expect(popover).toBeHidden();
    const draft = page.locator('.chat-input-container textarea');
    await expect(draft).toHaveValue('why the dip in Q3?');
    await expect(page.locator('.chat-input-container img')).toBeVisible();
  });

  test('a quote goes whole, from either side, and the caret cannot rest inside it', async ({ page }) => {
    await openWithReply(page);
    const addQuote = async () => {
      await select(page, SECOND);
      await page.getByRole('toolbar', { name: 'Selected text' }).getByRole('button', { name: 'More' }).click();
      await page.getByRole('menuitem', { name: 'Add to context' }).click();
    };
    await addQuote();
    const token = page.locator('.chat-input-container .draft-token-quote');
    const draft = page.locator('.chat-input-container textarea');
    await expect(token).toHaveCount(1);
    await draft.press('ArrowLeft');
    await draft.press('ArrowLeft');
    await expect.poll(() => draft.evaluate((el) => el.selectionStart)).toBe(0);
    await draft.press('Delete');
    await expect(token).toHaveCount(0);
    await expect(draft).toHaveValue(' ');

    await addQuote();
    await expect(token).toHaveCount(1);
    await draft.press('Backspace');
    await expect(token).toHaveCount(0);
  });

  test('a selection takes a quote whole or not at all, and undo brings it back', async ({ page }) => {
    await openWithReply(page);
    await select(page, SECOND);
    await page.getByRole('toolbar', { name: 'Selected text' }).getByRole('button', { name: 'More' }).click();
    await page.getByRole('menuitem', { name: 'Add to context' }).click();
    const token = page.locator('.chat-input-container .draft-token-quote');
    const draft = page.locator('.chat-input-container textarea');
    await expect(token).toHaveCount(1);
    const range = () => draft.evaluate((el) => [el.selectionStart, el.selectionEnd]);
    const length = await draft.evaluate((el) => el.value.length);

    // Reaching in from the end takes all of it, past the space that follows it.
    await draft.press('Shift+ArrowLeft');
    await draft.press('Shift+ArrowLeft');
    await expect.poll(range).toEqual([0, length]);
    // Drawing back lets go of all of it.
    await draft.press('Shift+ArrowRight');
    await expect.poll(range).toEqual([length - 1, length]);

    // Written over, none of its words stay behind for the agent to read.
    await draft.press('Shift+ArrowLeft');
    await expect.poll(range).toEqual([0, length]);
    await draft.pressSequentially('x');
    await expect(draft).toHaveValue('x');
    await expect(token).toHaveCount(0);

    // An undo that brings the token back brings its quote back.
    await draft.press('ControlOrMeta+z');
    await expect(token).toHaveCount(1);
    await configureSSE({ method: 'POST', path: `/api/v1/threads/${TH}/messages`, events: [sseEvents.messageChunk('ok'), sseEvents.finishStop()], delay: 5 });
    await draft.press('Enter');
    await expect(token).toHaveCount(0);
    const requests = await getCapturedRequests();
    expect(JSON.stringify(requests.filter((r) => r.method === 'POST').at(-1))).toContain('no longer disappears');
    expect(JSON.stringify(requests.filter((r) => r.method === 'POST').at(-1))).toContain('</quote>');
  });

  test('editing a sent message shows its quote as a token and keeps it', async ({ page }) => {
    await mockAPI(page, chatViewOverrides());
    await configureSSE({
      method: 'GET',
      path: `/api/v1/threads/${TH}/messages/replay`,
      events: [
        sseEvents.userMessage('<quote source="Analyst">Revenue grew</quote>\n<reply>by how much?</reply> and why', 0),
        sseEvents.messageChunk('Mostly the data center.'),
        sseEvents.finishStop(),
        sseEvents.replayDone(),
      ],
      delay: 5,
    });
    await configureSSE({ method: 'POST', path: `/api/v1/threads/${TH}/messages`, events: [sseEvents.messageChunk('ok'), sseEvents.finishStop()], delay: 5 });
    await page.goto(`/chat/t/${TH}`);
    await expect(page.getByText('and why')).toBeVisible({ timeout: 15000 });
    await page.getByText('and why').hover();
    await page.locator('button[title="Edit message"]').click();

    const box = page.locator('[data-message-role="user"] textarea');
    await expect(box).toBeFocused();
    await expect(box).not.toHaveValue(/<quote|by how much/);
    await expect(page.locator('[data-message-role="user"] .draft-token-quote')).toContainText('Revenue');
    await box.pressSequentially(' exactly');

    await box.press('Enter');
    await expect(box).toHaveCount(0);
    await expect.poll(async () => {
      const requests = await getCapturedRequests();
      return JSON.stringify(requests.filter((r) => r.method === 'POST').at(-1) ?? '');
    }).toContain('Revenue grew</quote>');
    const requests = await getCapturedRequests();
    const sent = JSON.stringify(requests.filter((r) => r.method === 'POST').at(-1));
    expect(sent).toContain('by how much?</reply>');
    expect(sent).toContain('and why exactly');
  });

  test('a narrower window wraps a quote\'s words with the caret under them', async ({ page }) => {
    await openWithReply(page);
    await select(page, SECOND);
    await page.getByRole('toolbar', { name: 'Selected text' }).getByRole('button', { name: 'More' }).click();
    await page.getByRole('menuitem', { name: 'Add to context' }).click();
    const draft = page.locator('.chat-input-container textarea');
    await expect(page.locator('.chat-input-container .draft-token-quote')).toHaveCount(1);
    await draft.pressSequentially('and a few more words to make this draft wrap at the narrower width');
    const widths = () => page.evaluate(() => {
      const textarea = document.querySelector('.chat-input-container textarea');
      return [textarea.clientWidth, document.querySelector('.chat-input-container .draft-token-layer').offsetWidth];
    });
    const before = await widths();
    // The column holds its width until the window is narrower than it is.
    await page.setViewportSize({ width: 800, height: page.viewportSize().height });
    await expect.poll(async () => (await widths())[0]).toBeLessThan(before[0]);
    await expect.poll(async () => {
      const [textarea, layer] = await widths();
      return layer - textarea;
    }).toBe(0);
  });

  test('a long draft keeps a quote under its words, and the wheel works over one', async ({ page }) => {
    await openWithReply(page);
    const draft = page.locator('.chat-input-container textarea');
    await draft.fill(Array.from({ length: 20 }, (_, i) => `line ${i + 1}`).join('\n'));
    await draft.press('Control+End');
    await select(page, SECOND);
    await page.getByRole('toolbar', { name: 'Selected text' }).getByRole('button', { name: 'More' }).click();
    await page.getByRole('menuitem', { name: 'Add to context' }).click();
    const token = page.locator('.chat-input-container .draft-token-quote');
    await expect(token).toHaveCount(1);

    const scrolled = () => page.evaluate(() => [
      document.querySelector('.chat-input-container textarea').scrollTop,
      document.querySelector('.draft-token-layer').scrollTop,
    ]);
    await expect.poll(async () => { const [text, layer] = await scrolled(); return text > 0 && text === layer; }).toBe(true);
    const [before] = await scrolled();
    await token.hover();
    await page.mouse.wheel(0, -60);
    await expect.poll(async () => (await scrolled())[0]).toBeLessThan(before);
    await expect.poll(async () => { const [text, layer] = await scrolled(); return text === layer; }).toBe(true);
  });

  test('a sent message is as wide as its chip, not as the text the chip clips', async ({ page }) => {
    await openWithReply(page);
    await select(page, FIRST);
    await page.getByRole('toolbar', { name: 'Selected text' }).getByRole('button', { name: 'More' }).click();
    await page.getByRole('menuitem', { name: 'Add to context' }).click();
    await configureSSE({ method: 'POST', path: `/api/v1/threads/${TH}/messages`, events: [sseEvents.messageChunk('ok'), sseEvents.finishStop()], delay: 5 });
    await page.locator('.chat-input-container textarea').press('Enter');
    const chip = page.locator('[data-message-role="user"]').last().locator('.composer-quote-chip');
    await expect(chip).toBeVisible();
    const rowWidth = await chip.evaluate((el) => el.parentElement.getBoundingClientRect().width);
    const chipWidth = (await chip.boundingBox()).width;
    expect(rowWidth).toBeLessThanOrEqual(chipWidth + 1);
  });

  test('a draft with a quote shows what Chinese input is still composing', async ({ page }) => {
    await openWithReply(page);
    await select(page, SECOND);
    await page.getByRole('toolbar', { name: 'Selected text' }).getByRole('button', { name: 'More' }).click();
    await page.getByRole('menuitem', { name: 'Add to context' }).click();
    const layer = page.locator('.chat-input-container .draft-token-layer');
    await expect(layer.locator('.draft-token-quote')).toHaveCount(1);
    await expect(layer.locator('svg')).toHaveCount(1);

    const cdp = await page.context().newCDPSession(page);
    await cdp.send('Input.imeSetComposition', { text: 'ni', selectionStart: 2, selectionEnd: 2 });
    await expect(layer).toContainText('ni');
    await cdp.send('Input.imeSetComposition', { text: 'nihao', selectionStart: 5, selectionEnd: 5 });
    await expect(layer).toContainText('nihao');
    await cdp.send('Input.insertText', { text: '你好' });
    await expect(layer).toContainText('你好');
    await expect(layer).not.toContainText('nihao');
    await expect(page.locator('.chat-input-container textarea')).toHaveValue(/你好$/);
  });

  test('a selection made without a mouse opens the menu once it rests, and leaves it open', async ({ page }) => {
    await openWithReply(page);
    await page.evaluate((needle) => {
      const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
      while (walker.nextNode()) {
        const at = walker.currentNode.data.indexOf(needle);
        if (at < 0) continue;
        const range = document.createRange();
        range.setStart(walker.currentNode, at);
        range.setEnd(walker.currentNode, at + needle.length);
        window.getSelection().removeAllRanges();
        window.getSelection().addRange(range);
        return;
      }
    }, 'How did the round go?');
    const toolbar = page.getByRole('toolbar', { name: 'Selected text' });
    await expect(toolbar).toBeVisible();
    await toolbar.getByRole('button', { name: 'More' }).click();
    const item = page.getByRole('menuitem', { name: 'Add to context' });
    await expect(item).toBeVisible();
    await page.waitForTimeout(500);
    await expect(item).toBeVisible();
  });

  test('the same words from two replies are two quotes', async ({ page }) => {
    await mockAPI(page, chatViewOverrides());
    await configureSSE({
      method: 'GET',
      path: `/api/v1/threads/${TH}/messages/replay`,
      events: [
        sseEvents.userMessage('First?', 0),
        sseEvents.messageChunk('Margins fell again.'),
        sseEvents.finishStop(),
        sseEvents.userMessage('Second?', 1),
        sseEvents.messageChunk('Margins fell again.', 'text', 1),
        sseEvents.finishStop(1),
        sseEvents.replayDone(),
      ],
      delay: 5,
    });
    await page.goto(`/chat/t/${TH}`);
    const replies = page.locator('[data-message-role="assistant"]');
    await expect(replies).toHaveCount(2, { timeout: 15000 });
    for (const reply of [0, 1]) {
      await page.evaluate((index) => {
        const bubble = document.querySelectorAll('[data-message-role="assistant"]')[index];
        const walker = document.createTreeWalker(bubble, NodeFilter.SHOW_TEXT);
        while (walker.nextNode()) {
          if (!walker.currentNode.data.includes('Margins fell')) continue;
          const range = document.createRange();
          range.selectNodeContents(walker.currentNode);
          window.getSelection().removeAllRanges();
          window.getSelection().addRange(range);
          return;
        }
      }, reply);
      await page.evaluate(() => document.querySelector('[data-message-role]').dispatchEvent(new MouseEvent('mouseup', { bubbles: true })));
      const toolbar = page.getByRole('toolbar', { name: 'Selected text' });
      await toolbar.getByRole('button', { name: 'More' }).click();
      await page.getByRole('menuitem', { name: 'Add to context' }).click();
      await expect(page.locator('.draft-token-quote')).toHaveCount(reply + 1);
    }
  });

  test('a quote of the user\'s own message tells the agent whose it is', async ({ page }) => {
    await openWithReply(page);
    await select(page, 'How did the round go?');
    await page.getByRole('toolbar', { name: 'Selected text' }).getByRole('button', { name: 'More' }).click();
    await page.getByRole('menuitem', { name: 'Add to context' }).click();
    await expect(page.locator('.draft-token-quote')).toHaveCount(1);
    await configureSSE({ method: 'POST', path: `/api/v1/threads/${TH}/messages`, events: [sseEvents.messageChunk('ok'), sseEvents.finishStop()], delay: 5 });
    await page.locator('.chat-input-container textarea').press('Enter');
    await expect(page.locator('.draft-token-quote')).toHaveCount(0);
    const requests = await getCapturedRequests();
    expect(JSON.stringify(requests.filter((r) => r.method === 'POST').at(-1))).toContain("source=\\\"User's message\\\"");
  });

  test('resting on a block offers its whole text, and only on finished reply text', async ({ page }) => {
    await page.context().grantPermissions(['clipboard-read', 'clipboard-write']);
    await openWithReply(page);
    const menu = page.getByRole('toolbar', { name: 'Text actions' });

    // Your own bubble has no menu, nor does the page around a reply.
    await page.locator('[data-message-role="user"]').first().getByText('How did the round go?').hover();
    await page.waitForTimeout(400);
    await expect(menu).toBeHidden();

    await page.getByText(SECOND).hover();
    await expect(menu).toBeVisible();
    const block = await page.getByText(SECOND).boundingBox();
    const box = await menu.boundingBox();
    expect(box.y).toBeLessThan(block.y);
    expect(box.y + box.height).toBeGreaterThan(block.y);

    // The pointer can reach it over the block above without losing it.
    await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2, { steps: 4 });
    await page.waitForTimeout(400);
    await expect(menu).toBeVisible();

    await menu.getByRole('button', { name: 'Copy' }).click();
    await expect.poll(() => page.evaluate(() => navigator.clipboard.readText())).toBe(SECOND);

    await menu.getByRole('button', { name: 'Quote' }).click();
    const popover = page.getByRole('dialog', { name: 'Quote from Analyst' });
    await expect(popover.getByRole('blockquote')).toContainText('no longer disappears');
    // Over the block it quotes, not off to the side of the column.
    const popoverBox = await popover.boundingBox();
    expect(popoverBox.x).toBeGreaterThanOrEqual(block.x);
    await popover.getByRole('textbox').fill('why now');
    await popover.getByRole('textbox').press('Enter');
    await expect(page.locator('.chat-input-container .draft-token-quote')).toContainText('The summary line');

    await page.locator('.chat-input-container .draft-token-quote').hover();
    await expect(page.locator('[data-radix-popper-content-wrapper]')).toContainText('why now');
  });

  test('a selection takes over from the block menu', async ({ page }) => {
    await openWithReply(page);
    await page.getByText(SECOND).hover();
    const menu = page.getByRole('toolbar', { name: 'Text actions' });
    await expect(menu).toBeVisible();
    await select(page, FIRST);
    await expect(page.getByRole('toolbar', { name: 'Selected text' })).toBeVisible();
    await expect(menu).toBeHidden();
  });

  test('the toolbar and reply box open where the selection was let go', async ({ page }) => {
    await openWithReply(page);
    const toolbar = page.getByRole('toolbar', { name: 'Selected text' });
    const edges = async () => {
      const rects = await page.evaluate(() => {
        const list = Array.from(getSelection().getRangeAt(0).getClientRects()).filter((r) => r.width > 0);
        return { firstLeft: list[0].left, firstTop: list[0].top, lastRight: list.at(-1).right, lastBottom: list.at(-1).bottom };
      });
      return rects;
    };

    // Dragged forward: under the last line, around its end.
    await select(page, SECOND);
    await expect(toolbar).toBeVisible();
    const forward = await edges();
    await expect.poll(async () => (await toolbar.boundingBox()).y).toBeGreaterThanOrEqual(forward.lastBottom);
    const box = await toolbar.boundingBox();
    expect(Math.abs(box.x + box.width / 2 - forward.lastRight)).toBeLessThan(2);

    await toolbar.getByRole('button', { name: 'Quote' }).click();
    const popover = page.getByRole('dialog', { name: 'Quote from Analyst' });
    await expect.poll(async () => (await popover.boundingBox()).y).toBeGreaterThanOrEqual(forward.lastBottom);
    await popover.getByRole('textbox').press('Escape');
    await expect(popover).toBeHidden();

    // Dragged backward: over the first line, around its start.
    await page.getByText(SECOND).evaluate((el) => {
      const node = el.firstChild;
      getSelection().setBaseAndExtent(node, node.data.length, node, 0);
      el.dispatchEvent(new MouseEvent('mouseup', { bubbles: true }));
    });
    await expect(toolbar).toBeVisible();
    const backward = await edges();
    await expect.poll(async () => { const b = await toolbar.boundingBox(); return b.y + b.height; }).toBeLessThanOrEqual(backward.firstTop);
    const backBox = await toolbar.boundingBox();
    expect(Math.abs(backBox.x + backBox.width / 2 - backward.firstLeft)).toBeLessThan(2);
  });

  test('a press on the page keeps a reply that has been written', async ({ page }) => {
    await openWithReply(page);
    await select(page, FIRST);
    await page.getByRole('toolbar', { name: 'Selected text' }).getByRole('button', { name: 'Quote' }).click();
    const popover = page.getByRole('dialog', { name: 'Quote from Analyst' });
    await popover.getByRole('textbox').fill('draft');
    await page.mouse.click(5, 300);
    await expect(popover).toBeVisible();
    await popover.getByRole('textbox').press('Escape');
    await expect(popover).toBeHidden();
  });

  test('a real drag in a read-only file quotes it with its lines', async ({ page }) => {
    await page.addInitScript(() => localStorage.setItem('theme', 'dark'));
    await openFile(page);
    await drag(page, page.getByText(FILE_LINE));
    const toolbar = page.getByRole('toolbar', { name: 'Selected text' });
    await expect(toolbar).toBeVisible();

    await toolbar.getByRole('button', { name: 'Quote' }).click();
    const popover = page.getByRole('dialog', { name: 'Quote from notes.md:L3' });
    await expect(popover.getByRole('textbox')).toBeFocused();
    await popover.getByRole('textbox').fill('is this organic?');
    await popover.getByRole('textbox').press('Enter');

    await expect(page.locator('.draft-token-quote')).toContainText('Revenue grew because');
  });

  test('a selection in the editor keeps its popover until the reply is done', async ({ page }) => {
    await serveMonacoLocally(page);
    await openFile(page);
    await page.locator('button[title="Edit file"]').click();
    const word = page.locator('.monaco-editor .view-lines').getByText('Revenue');
    await word.dblclick();
    const toolbar = page.getByRole('toolbar', { name: 'Selected text' });
    await expect(toolbar).toBeVisible();
    await toolbar.getByRole('button', { name: 'Quote' }).click();
    const popover = page.getByRole('dialog', { name: /Quote from notes\.md:L3/ });
    await popover.getByRole('textbox').fill('draft');
    await word.click();
    await expect(popover).toBeVisible();
    await popover.getByRole('textbox').press('Enter');
    await page.locator('.draft-token-quote').hover();
    await expect(page.locator('[data-radix-popper-content-wrapper]')).toContainText('draft');
  });

  test('the toolbar follows the text as the transcript scrolls', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 520 });
    await openWithReply(page, 'A line above the quote.\n\n'.repeat(30));
    await select(page, SECOND);
    const toolbar = page.getByRole('toolbar', { name: 'Selected text' });
    await expect(toolbar).toBeVisible();
    const before = (await toolbar.boundingBox()).y;
    const moved = await page.getByText(SECOND).evaluate((el) => {
      let box = el.parentElement;
      while (box && !(box.scrollHeight > box.clientHeight && getComputedStyle(box).overflowY !== 'visible' && getComputedStyle(box).overflowY !== 'hidden')) box = box.parentElement;
      if (!box) return false;
      box.scrollTop -= 40;
      return true;
    });
    expect(moved).toBe(true);
    await expect.poll(async () => (await toolbar.boundingBox())?.y).toBeGreaterThan(before);
  });

  test('a narrow window keeps the popover on screen', async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 780 });
    await openWithReply(page);
    await select(page, SECOND);
    await page.getByRole('toolbar', { name: 'Selected text' }).getByRole('button', { name: 'Quote' }).click();
    const box = await page.getByRole('dialog', { name: 'Quote from Analyst' }).boundingBox();
    expect(box.x).toBeGreaterThanOrEqual(0);
    expect(box.x + box.width).toBeLessThanOrEqual(390);
  });

  test('a skill and a file sit in the draft as tokens and go whole', async ({ page }) => {
    await mockAPI(page, {
      ...chatViewOverrides(),
      [`GET /workspaces/${WS}/files`]: { files: ['results/notes.md', 'report.pdf'] },
      'GET /skills': { skills: [
        { name: 'market-watch', description: 'Register tickers and watch them through the session.' },
        { name: 'dcf', description: 'Build a discounted cash flow model.' },
      ] },
    });
    await replayNothing();
    await page.goto(`/chat/t/${TH}`);
    const draft = page.locator('.chat-input-container textarea');
    await draft.click();
    await draft.pressSequentially('/');
    const menu = page.locator('.mention-autocomplete');
    await expect(menu).toBeVisible();
    await menu.locator('.mention-autocomplete-item').first().click();
    await draft.pressSequentially('compare with @');
    await menu.locator('.mention-autocomplete-item').first().click();
    await expect(page.locator('.draft-token')).toHaveCount(2);

    const value = await draft.inputValue();
    await draft.press('Backspace');
    await expect(page.locator('.draft-token')).toHaveCount(1);
    expect((await draft.inputValue()).length).toBeLessThan(value.length - 1);
    await draft.pressSequentially('@');
    await menu.locator('.mention-autocomplete-item').first().click();

    await configureSSE({ method: 'POST', path: `/api/v1/threads/${TH}/messages`, events: [sseEvents.messageChunk('ok'), sseEvents.finishStop()], delay: 5 });
    await draft.press('Enter');
    await expect(page.locator('[data-message-role="user"] .draft-token')).toHaveCount(2);
  });
});
