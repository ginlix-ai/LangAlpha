/**
 * The main transcript on a send: the reader's message is pinned near the
 * viewport top and the reply fills the room under it, which a floor under the
 * transcript (`data-transcript-floor`) holds open, so nothing scrolls while it
 * fits. A reply that outgrows the view is followed by a glide, one write per
 * frame, which these cases intercept at the moment a test can stage but a
 * reader only meets by chance: between a glide's write and its scroll event,
 * which arrives a frame later.
 */
import { configureSSE, resetMockServer, mockAPI, test, expect } from './fixtures.js';
import { sseEvents } from './helpers/mockResponses.js';
import { TH, WS, chatViewOverrides, openThread, send } from './helpers/chatScenario.js';
import { sampleFrames, stopSampling } from './helpers/frameProbe.js';
import { buildEvents } from './perf/streamFixture.js';

// ANCHOR_OFFSET_PX in utils/scrollDom.ts: the gap left above a pinned message.
const PIN_GAP = 16;
// AT_BOTTOM_PX in utils/scrollHelpers.ts: as close to the end as counts as at it.
const AT_END = 4;
const QUESTION = 'And what changed in the second quarter?';

/** Two settled turns, so the thread opens with history above the composer. */
function historyReplay() {
  const ts = new Date(Date.now() - 600_000).toISOString();
  const out = [];
  for (let i = 0; i < 2; i++) {
    out.push({ event: 'user_message', data: { thread_id: TH, turn_index: i, content: `Question ${i + 1} about the quarter?`, timestamp: ts, metadata: { msg_type: 'ptc', workspace_id: WS } } });
    const md = `Answer ${i + 1}. ` + 'Revenue rose on data-center demand while gross margin eased on early yields. '.repeat(14);
    out.push({ event: 'message_chunk', data: { thread_id: TH, agent: 'model:hist', id: `lc_run--hist${i}`, role: 'assistant', content: md, content_type: 'text', turn_index: i } });
    out.push({ event: 'message_chunk', data: { thread_id: TH, agent: 'model:hist', id: `lc_run--hist${i}`, role: 'assistant', content: '', content_type: 'text', finish_reason: 'stop', turn_index: i } });
  }
  out.push(sseEvents.replayDone());
  return out;
}

/** A short reply to turn 2: a few lines, streamed over about a second and a half. */
function shortReply() {
  const text = 'Mostly yields, not mix. The data-center share rose while margin fell, and a richer mix should have lifted it.';
  const events = [{ ...sseEvents.messageChunk('', 'text', 2), delayAfter: 300 }];
  for (const piece of text.match(/\s*\S+/g)) events.push({ ...sseEvents.messageChunk(piece, 'text', 2), delayAfter: 60 });
  events.push(sseEvents.finishStop(2), sseEvents.creditUsage());
  return events;
}

function viewport(page) {
  return page.locator('main .max-w-3xl.overflow-x-hidden').locator('xpath=ancestor::div[contains(@class, "overflow-auto")][1]');
}

function view(page) {
  return page.evaluate(() => {
    const vp = document.querySelector('main .max-w-3xl.overflow-x-hidden').closest('.overflow-auto');
    const floor = vp.querySelector('[data-transcript-floor]');
    const users = vp.querySelectorAll('[data-message-role="user"]');
    const user = users[users.length - 1];
    return {
      top: vp.scrollTop,
      fromBottom: vp.scrollHeight - vp.scrollTop - vp.clientHeight,
      floor: floor.style.minHeight,
      userTop: user ? user.getBoundingClientRect().top - vp.getBoundingClientRect().top : null,
    };
  });
}

const streaming = (page) => page.locator('button[aria-label="Stop"]');

test.describe('pin on send', () => {
  test.beforeEach(async ({ page }) => {
    await resetMockServer();
    await mockAPI(page, chatViewOverrides());
    await configureSSE({ method: 'GET', path: `/api/v1/threads/${TH}/messages/replay`, events: historyReplay(), delay: 10 });
    await configureSSE({ method: 'POST', path: `/api/v1/threads/${TH}/messages`, events: shortReply(), delay: 10 });
    await page.setViewportSize({ width: 1280, height: 720 });
    await page.goto(`/chat/t/${TH}`);
    await page.waitForSelector('textarea', { timeout: 10000 });
    await page.waitForSelector('[data-message-role="assistant"]', { timeout: 10000 });
  });

  test("pins the reader's message under the top, and a short reply streams in under it without scrolling", async ({ page }) => {
    // A settled thread opens at its end, with nothing held open under it. Not
    // to the pixel: scroll anchoring can take a pixel or two back as the
    // history's rows settle, which still reads as the end.
    await expect.poll(async () => (await view(page)).fromBottom).toBeLessThanOrEqual(AT_END);
    expect((await view(page)).floor).toBe('');

    await page.locator('textarea').fill(QUESTION);
    await page.keyboard.press('Enter');
    await expect(page.getByText(QUESTION)).toBeVisible();
    await expect.poll(async () => (await view(page)).userTop, { timeout: 3000 }).toBeCloseTo(PIN_GAP, 0);
    const pinned = await view(page);
    expect(pinned.floor).not.toBe('');

    // Sampled every frame until the turn ends: the reply fills the room under
    // the message, and the view never moves.
    await sampleFrames(page, () => ({
      top: document.querySelector('main .max-w-3xl.overflow-x-hidden').closest('.overflow-auto').scrollTop,
      live: !!document.querySelector('button[aria-label="Stop"]'),
    }));
    await expect(streaming(page)).toBeHidden({ timeout: 15000 });
    const samples = await stopSampling(page);
    const ended = samples.findIndex((s) => !s.live);
    const tops = samples.slice(0, ended < 0 ? samples.length : ended + 1).map((s) => s.top);
    // The streaming indicator leaves and the reply's actions land.
    await page.waitForTimeout(1200);
    expect(tops.length).toBeGreaterThan(20);
    expect(Math.max(...tops) - Math.min(...tops)).toBeLessThan(1);
    const after = await view(page);
    expect(Math.abs(after.top - pinned.top)).toBeLessThan(1);
    expect(after.userTop).toBeCloseTo(PIN_GAP, 0);
  });

  test('gives the room back as the reader scrolls up out of it, without moving them', async ({ page }) => {
    await page.locator('textarea').fill(QUESTION);
    await page.keyboard.press('Enter');
    await expect.poll(async () => (await view(page)).userTop, { timeout: 3000 }).toBeCloseTo(PIN_GAP, 0);
    await expect(streaming(page)).toBeHidden({ timeout: 15000 });
    await page.waitForTimeout(800);

    const box = await viewport(page).boundingBox();
    await page.mouse.move(box.x + box.width / 2, box.y + 200);
    let prev = await view(page);
    for (let i = 0; i < 4; i++) {
      await page.mouse.wheel(0, -150);
      await page.waitForTimeout(400);
      const now = await view(page);
      // Each notch moves the view by the notch and no more: the floor leaving
      // never clamps it.
      expect(prev.top - now.top).toBeGreaterThan(140);
      expect(prev.top - now.top).toBeLessThan(160);
      prev = now;
    }
    expect(prev.floor).toBe('');
  });

  test('pins at once for a reader who prefers reduced motion', async ({ page }) => {
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await page.locator('textarea').fill(QUESTION);
    await page.keyboard.press('Enter');
    await page.waitForFunction((q) => [...document.querySelectorAll('[data-message-role="user"]')].some((el) => el.textContent.includes(q)), QUESTION);
    // The frame after the message renders already shows it pinned.
    const top = await page.evaluate(() => new Promise((resolve) => requestAnimationFrame(() => {
      const vp = document.querySelector('main .max-w-3xl.overflow-x-hidden').closest('.overflow-auto');
      const users = vp.querySelectorAll('[data-message-role="user"]');
      resolve(users[users.length - 1].getBoundingClientRect().top - vp.getBoundingClientRect().top);
    })));
    expect(top).toBeCloseTo(PIN_GAP, 0);
  });
});

/**
 * Runs `onFollow` once, after the first glide write that moves the view down at
 * least `minMove` px: in a microtask, once the frame that wrote has returned and
 * before the write's scroll event, as a reader's input would land.
 */
async function interceptFollow(page, minMove, onFollow) {
  await page.evaluate(([minMove, body]) => {
    const vp = document.querySelector('main .max-w-3xl.overflow-x-hidden').closest('.overflow-auto');
    const scrollTo = vp.scrollTo;
    const act = new Function('vp', 'top', body);
    window.__intercepted = false;
    vp.scrollTo = function (arg) {
      const before = this.scrollTop;
      scrollTo.apply(this, arguments);
      // Every scroll the transcript makes is a bare position inside the range,
      // and once the reply has outgrown the room under the message only the
      // glide is writing, a frame at a time.
      const glide = typeof arg === 'object' && typeof arg.top === 'number' && !arg.behavior && arg.top < this.scrollHeight - this.clientHeight + 1;
      if (window.__intercepted || !glide || this.scrollTop - before < minMove) return;
      window.__intercepted = true;
      const top = this.scrollTop;
      queueMicrotask(() => act(this, top));
    };
  }, [minMove, onFollow]);
  await page.waitForFunction(() => window.__intercepted, null, { timeout: 8000 });
}

test.describe('streaming follow', () => {
  test.beforeEach(async ({ page }) => {
    await resetMockServer();
    await openThread(page);
    await configureSSE({ method: 'POST', path: `/api/v1/threads/${TH}/messages`, events: buildEvents(8, { sections: 12 }), delay: 20 });
    await page.setViewportSize({ width: 1280, height: 720 });
    await send(page, 'Give me an earnings deep dive on NVDA');
    // The reply is well past the room under the message, and the glide
    // carries its end.
    await page.waitForFunction(() => {
      const vp = document.querySelector('main .max-w-3xl.overflow-x-hidden')?.closest('.overflow-auto');
      return vp && vp.scrollTop > 400 && document.querySelector('button[aria-label="Stop"]');
    }, null, { timeout: 20000 });
  });

  test('keeps following when a tall block lands before the glide is seen', async ({ page }) => {
    // The glide's scroll event then finds the bottom 600 px away, further than
    // the band, though the reader never moved.
    await interceptFollow(page, 0, `
      const block = document.createElement('div');
      block.style.height = '600px';
      vp.querySelector('.max-w-3xl').appendChild(block);
    `);
    await expect.poll(async () => (await view(page)).fromBottom).toBeLessThan(5);
    await expect(page.getByRole('button', { name: 'Scroll to latest message' })).toBeHidden();
  });

  test('keeps following when content above the view closes while the glide trails the end', async ({ page }) => {
    // The browser's scroll anchoring moves the view up by what closed, which
    // left from the end more than AT_END away reads as a reader's scroll up.
    const shrunk = await page.evaluate((atEnd) => new Promise((resolve) => {
      const vp = document.querySelector('main .max-w-3xl.overflow-x-hidden').closest('.overflow-auto');
      const user = vp.querySelector('[data-message-role="user"]');
      const tick = () => {
        if (vp.scrollHeight - vp.scrollTop - vp.clientHeight <= atEnd * 2) {
          requestAnimationFrame(tick);
          return;
        }
        const top = vp.scrollTop;
        user.style.marginTop = '-120px';
        requestAnimationFrame(() => resolve(top - vp.scrollTop));
      };
      requestAnimationFrame(tick);
    }), AT_END);
    expect(shrunk).toBeGreaterThan(60);
    await page.waitForTimeout(1000);
    await expect(streaming(page)).toBeVisible();
    expect((await view(page)).fromBottom).toBeLessThan(160);
    await expect(page.getByRole('button', { name: 'Scroll to latest message' })).toBeHidden();
  });

  test('lets a reader leave by a notch taken before the glide is seen', async ({ page }) => {
    // Measured from where the view was before the write, the notch would read
    // as a move down.
    // A notch comes with its wheel event, as the reader's input would.
    await interceptFollow(page, 1, `
      vp.dispatchEvent(new WheelEvent('wheel', { deltaY: -12, bubbles: true }));
      vp.scrollTop = top - 12;
      window.__nudgedTop = vp.scrollTop;
    `);
    await page.waitForTimeout(1000);
    const nudged = await page.evaluate(() => window.__nudgedTop);
    const after = await view(page);
    expect(Math.abs(after.top - nudged)).toBeLessThan(2);
    expect(after.fromBottom).toBeGreaterThan(12);
    await expect(page.getByRole('button', { name: 'Scroll to latest message' })).toBeVisible();
  });

  test('stays with the end of a fast reply', async ({ page }) => {
    // Sampled every frame for three seconds of streaming: the gap to the end
    // stays within a few lines, never a screen behind.
    await sampleFrames(page, () => {
      const vp = document.querySelector('main .max-w-3xl.overflow-x-hidden').closest('.overflow-auto');
      return { gap: vp.scrollHeight - vp.scrollTop - vp.clientHeight };
    });
    await page.waitForTimeout(3000);
    const gaps = (await stopSampling(page)).map((s) => s.gap);
    expect(gaps.length).toBeGreaterThan(60);
    expect(Math.max(...gaps)).toBeLessThan(160);
    await expect(streaming(page)).toBeVisible();
    await expect(page.getByRole('button', { name: 'Scroll to latest message' })).toBeHidden();
  });
});

/** A Task the main agent spawns, then, once the test has had time to open its
 *  card, a subagent reply streamed fast and long enough to outgrow the view. */
function subagentTurn() {
  const taskChunk = (content) => {
    const event = sseEvents.messageChunk(content, 'text');
    return { ...event, data: { ...event.data, agent: 'task:follow-check' } };
  };
  const events = [
    sseEvents.toolCalls([{ name: 'Task', args: { description: 'Review the filings', prompt: 'Review the filings' }, id: 'toolu_task' }]),
    sseEvents.finishToolCalls(),
    { event: 'artifact', data: { thread_id: TH, artifact_type: 'task', tool_call_id: 'toolu_task', payload: { task_id: 'follow-check', action: 'spawned', description: 'Review the filings', type: 'research' } } },
    { ...sseEvents.messageChunk('Review underway.'), delayAfter: 2500 },
  ];
  for (let i = 0; i < 40; i++) {
    const words = `Finding ${i + 1}: the filings point the same way on margin, mix and the data-center share. `.match(/\S+\s*/g);
    words.forEach((w, j) => events.push({ ...taskChunk(j === words.length - 1 ? `${w}\n\n` : w), delayAfter: 12 }));
  }
  events.push(sseEvents.messageChunk('Review complete.'), sseEvents.finishStop());
  return events;
}

/** The open subagent transcript's scroller, found by its text. */
const SUBAGENT_SCROLLER = `[...document.querySelectorAll('.overflow-auto')].find((el) => el.clientHeight > 0 && el.textContent.includes('Finding 1:'))`;

test.describe('subagent follow', () => {
  test('glides after the end of a subagent reply streaming in its own view', async ({ page }) => {
    await resetMockServer();
    await openThread(page);
    await configureSSE({ method: 'POST', path: `/api/v1/threads/${TH}/messages`, events: subagentTurn(), delay: 5 });
    await page.setViewportSize({ width: 1280, height: 720 });
    await send(page, 'Review the filings');
    await expect(page.getByText('Review underway.', { exact: true })).toBeVisible();
    await page.getByRole('button', { name: /Review the filings/ }).first().click();

    // Sampled every frame from the reply's first line until it outgrows the
    // view and runs a while past it: the gap to the end stays within a few
    // lines, and the view moves a little each frame rather than a line at once.
    await sampleFrames(page, (find) => {
      const vp = (window.__subagentScroller ??= new Function(`return ${find}`)());
      return vp ? { gap: vp.scrollHeight - vp.scrollTop - vp.clientHeight, top: vp.scrollTop, overflow: vp.scrollHeight - vp.clientHeight, frame: document.timeline.currentTime } : {};
    }, SUBAGENT_SCROLLER);
    await page.waitForFunction(() => window.__subagentScroller);
    await page.waitForTimeout(7000);
    const run = (await stopSampling(page)).filter((s) => s.top !== undefined);
    const steps = run.slice(1).map((s, i) => s.top - run[i].top).filter((step) => step !== 0);
    // The sampler's frame runs before the glide's, so each step it reads was
    // drawn over the frame before, timed on the frames' own clock as the glide
    // times its motion.
    const speeds = run.slice(2).map((s, i) => Math.abs(s.top - run[i + 1].top) / (run[i + 1].frame - run[i].frame));
    expect(run[run.length - 1].overflow).toBeGreaterThan(300);
    expect(steps.length).toBeGreaterThan(30);
    expect(Math.max(...run.map((s) => s.gap))).toBeLessThan(160);
    // A few px a frame at this pace, under 12 px a 60 Hz frame: on a runner
    // short of CPU frames drop, and a frame that spans two moves as far as two.
    // A smooth scroll restarted on each message steps 18 px a frame over and
    // over.
    expect(Math.max(...speeds)).toBeLessThan(12 / (1000 / 60));
    // The reply's end: the view lands on it.
    await expect.poll(() => page.evaluate((find) => {
      const vp = new Function(`return ${find}`)();
      return vp.scrollHeight - vp.scrollTop - vp.clientHeight;
    }, SUBAGENT_SCROLLER), { timeout: 15000 }).toBeLessThanOrEqual(AT_END);
  });
});
