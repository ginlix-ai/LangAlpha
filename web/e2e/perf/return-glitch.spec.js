/**
 * Catch-up glitch: what the transcript does when presentation falls behind
 * state and then catches up in a burst. Four ways to fall behind - reload
 * mid-stream, navigate to another thread and back, leave the chat route
 * entirely and come back, and stop painting while SSE keeps landing.
 * Off by default; flags and invocations are in e2e/perf/README.md.
 */
import { configureSSE, resetMockServer, mockAPI, test, expect } from '../fixtures.js';
import { sampleWorkspace, sampleThread, sseEvents } from '../helpers/mockResponses.js';
import { TH, chatViewOverrides } from '../helpers/chatScenario.js';
import { buildEvents, END_MARKER } from './streamFixture.js';
import { installRafFreeze } from './rafFreeze.js';
import { installProbe } from './probe.js';
import { recordUntil, countMagenta, MAGENTA_MARKER_CSS } from './screencast.js';

const TH2 = 'b0000002-0000-4000-8000-000000000002';
const CHUNK_MS = 40;
const CHUNK_CHARS = 8;
const PROMPT = 'Give me an earnings deep dive on NVDA';

// What catching up must never do, whichever way the client fell behind.
// The reply-text series is scoped to the message bubbles, because the page
// around them legitimately loses text: a route unmounting on the way back, a
// status label clearing. Within a bubble, streamed markdown still gives up a
// few characters whenever a syntax run is consumed (a fence line, the stars
// around a bold), so the bound is a lost block, not zero: the smallest block
// in the fixture is several hundred characters and a duplicated-and-collapsed
// bubble is thousands. The scroll bound is generous because the follow scroll
// is allowed to travel - the fixture's transcript is ~14 KB and the whole
// reply can land in one commit - but a fling that shows the top of the
// transcript moves several thousand px, well clear of this.
const MAX_REPLY_DROP_CHARS = 100;
const MAX_SCROLL_JUMP_PX = 2000;

function assertCaughtUpCleanly(r) {
  expect(r.maxReplyDrop, 'catching up must not remove reply text that was on screen').toBeLessThan(MAX_REPLY_DROP_CHARS);
  expect(r.maxScrollJumpPx, 'catching up must not fling the view').toBeLessThan(MAX_SCROLL_JUMP_PX);
}

function overrides(state) {
  const th2 = { ...sampleThread(), id: TH2, thread_id: TH2, title: 'Other thread' };
  return {
    ...chatViewOverrides({
      workspaces: [sampleWorkspace()],
      threads: [sampleThread(), th2],
      threadStatus: (route) => route.fulfill({
        status: 200, contentType: 'application/json',
        body: JSON.stringify(state.reconnectable ? { can_reconnect: true, status: 'streaming', run_id: 'run-1' } : { can_reconnect: false, status: 'idle' }),
      }),
    }),
    [`GET /threads/${TH2}`]: th2,
    [`GET /threads/${TH2}/status`]: { can_reconnect: false, status: 'idle' },
    [`GET /threads/${TH2}/turns`]: { thread_id: TH2, turns: [], retry_checkpoint_id: null },
  };
}

/** Index of the first event whose text carries `marker`. */
function indexOfMarker(events, marker) {
  let acc = '';
  for (let i = 0; i < events.length; i++) {
    const d = events[i]?.data;
    const c = typeof d === 'string' ? (() => { try { return JSON.parse(d); } catch { return null; } })() : d;
    const text = c?.content ?? c?.data?.content ?? '';
    if (typeof text === 'string') acc += text;
    if (acc.includes(marker)) return i;
  }
  return -1;
}

/**
 * Serve the run's first `k` events as a reconnect backlog, the rest live.
 * 2 ms apart so each event is its own socket read, as through a proxy; 0 lets
 * Node coalesce the whole backlog into one read and one React render.
 */
async function serveAsReconnect(state, events, k) {
  const backlog = events.slice(0, k).map((e) => ({ ...e, delayAfter: Number(process.env.PERF_BACKLOG_MS ?? 2) }));
  state.reconnectable = true;
  await configureSSE({
    method: 'GET', path: `/api/v1/threads/${TH}/messages/replay`,
    events: [sseEvents.userMessage(PROMPT), sseEvents.replayDone()], delay: 10,
  });
  // PERF_NO_CAUGHT_UP=1 omits the marker (a server without it): the client
  // falls back to a timer and the typewriter's catch-up rule.
  const boundary = process.env.PERF_NO_CAUGHT_UP ? [] : [{ ...sseEvents.caughtUp(), delayAfter: 0 }];
  await configureSSE({ method: 'GET', path: `/api/v1/threads/${TH}/messages/stream`, events: [...backlog, ...boundary, ...events.slice(k)], delay: CHUNK_MS });
}

async function startStream(page, state, events) {
  await page.addInitScript(installRafFreeze);
  await page.addInitScript(installProbe, { painted: true, reply: true, scrollLog: !!process.env.PERF_SCROLL_LOG });
  await mockAPI(page, overrides(state));
  await configureSSE({ method: 'GET', path: `/api/v1/threads/${TH}/messages/replay`, events: [sseEvents.replayDone()], delay: 10 });
  await configureSSE({ method: 'POST', path: `/api/v1/threads/${TH}/messages`, events, delay: CHUNK_MS });
  await page.goto(`/chat/t/${TH}`);
  await page.waitForSelector('textarea', { timeout: 10000 });
  await page.locator('textarea').fill(PROMPT);
  await page.locator('button[aria-label="Send message"]').click();
}

/**
 * PERF_CAST=1: record compositor frames over the catch-up and count, per
 * frame, the magenta band painted at the top of the transcript. This is the
 * ground truth behind the probe's "painted away" frame, but the band shifts
 * layout and the encoder takes main-thread time, so a PERF_CAST run's layout
 * and frame numbers are not comparable with a plain run's.
 */
async function reportCastFrames(page, doneVisible) {
  await page.addStyleTag({ content: MAGENTA_MARKER_CSS });
  const frames = await recordUntil(page, () => page.waitForFunction(() => !!window.__probe.catchUpWall, null, { timeout: 30_000 }));
  const wall = await page.evaluate(() => window.__probe.catchUpWall);
  const markerStyle = await page.evaluate(() => {
    const el = document.querySelector('main [data-message-id]');
    return el ? [el.matches(':first-of-type'), getComputedStyle(el, '::before').height, getComputedStyle(el, '::before').backgroundColor] : null;
  });
  const span = (f) => Math.round(f.ts * 1000 - wall);
  console.log(`[cast] total frames ${frames.length}, ts range ${frames.length ? span(frames[0]) : '-'}..${frames.length ? span(frames[frames.length - 1]) : '-'} ms vs catch-up; now-wall=${Date.now() - wall}; marker=${JSON.stringify(markerStyle)}`);
  const around = frames.filter((f) => f.ts * 1000 > wall - 300);
  await doneVisible();
  const counts = await countMagenta(page, around.map((f) => f.data));
  const rows = around.map((f, i) => [span(f), counts[i]]);
  console.log('[cast] frames (ms since catch-up, magenta px):', JSON.stringify(rows.filter((r) => r[0] > -400 && r[0] < 600)));
}

// A real background tab needs a real display; everything else is headless.
test.use({ headless: !process.env.PERF_HEADED });

test.describe('catch-up glitch', () => {
  test.skip(!process.env.PERF, 'set PERF=1 to run the catch-up repro');
  test.setTimeout(180_000);
  test.beforeEach(async () => { await resetMockServer(); });

  test('reload mid-stream', async ({ page }) => {
    const state = { reconnectable: false };
    const events = buildEvents(CHUNK_CHARS);
    await startStream(page, state, events);
    await expect(page.getByText('Section 3', { exact: false }).first()).toBeVisible({ timeout: 60_000 });
    // The in-flight run is not in the history replay, so the reconnect stream
    // re-delivers the whole run's buffer in one burst into a fresh bubble.
    await serveAsReconnect(state, events, indexOfMarker(events, 'Section 3') + 40);

    await page.reload();

    if (process.env.PERF_SHOT) {
      await page.locator('.page-loading__wall').waitFor({ state: 'detached', timeout: 10000 }).catch(() => {});
      await page.locator('[role="status"]').first().waitFor({ timeout: 5000 }).catch(() => {});
      await page.screenshot({ path: process.env.PERF_SHOT });
    }
    await page.waitForSelector('textarea', { timeout: 10000 });
    await page.waitForFunction(() => !!window.__probe, null, { timeout: 10000 });
    await page.evaluate(() => window.__probe.watchDrops(8000));
    const done = () => expect(page.getByText(END_MARKER)).toBeVisible({ timeout: 120_000 });
    if (process.env.PERF_CAST) await reportCastFrames(page, done);
    else await done();

    await page.waitForTimeout(1500);
    const r = await page.evaluate(() => window.__probe.report());
    console.log('[reload] ' + JSON.stringify(r));
    const waterfall = await page.evaluate(() => performance.getEntriesByType('resource')
      .filter((e) => e.name.includes('/api/v1/'))
      .map((e) => [e.name.replace(/^.*\/api\/v1/, ''), Math.round(e.startTime), Math.round(e.duration)])
      .sort((a, b) => a[1] - b[1]));
    console.log('[reload-waterfall] ' + JSON.stringify(waterfall));
    console.log('[reload-drop] ' + JSON.stringify(await page.evaluate(() => window.__probe.drop)));
    assertCaughtUpCleanly(r);
  });

  for (const target of ['thread', 'dashboard']) {
    test(`in-app navigation to ${target} and back mid-stream`, async ({ page }) => {
      const state = { reconnectable: false };
      const events = buildEvents(CHUNK_CHARS);
      await configureSSE({ method: 'GET', path: `/api/v1/threads/${TH2}/messages/replay`, events: [sseEvents.replayDone()], delay: 10 });
      await startStream(page, state, events);
      await expect(page.getByText('Section 2', { exact: false }).first()).toBeVisible({ timeout: 60_000 });
      const dest = target === 'thread' ? `/chat/t/${TH2}` : '/dashboard';
      // Leaving the chat route unmounts it and drops the live stream; the
      // return is a reconnect, served here the way the reload case is.
      if (target === 'dashboard') await serveAsReconnect(state, events, indexOfMarker(events, 'Section 3') + 40);
      // Router-level navigation without a document load: pushState + popstate.
      await page.evaluate((d) => { history.pushState({}, '', d); dispatchEvent(new PopStateEvent('popstate')); }, dest);
      await page.waitForTimeout(500);
      console.log(`[nav:${target}] away url=` + await page.evaluate(() => location.pathname) + ' chatTextLen=' + await page.evaluate(() => (document.querySelector('main') || document.body).textContent.length));
      await page.waitForTimeout(6000);
      await page.evaluate(() => { window.__probe.mark('resume'); history.back(); });
      await page.waitForTimeout(50);
      await page.evaluate(() => window.__probe.watchDrops(8000));
      console.log(`[nav:${target}] back url=` + await page.evaluate(() => location.pathname));
      await expect(page.getByText(END_MARKER)).toBeVisible({ timeout: 120_000 });
      await page.waitForTimeout(1500);
      const r = await page.evaluate(() => window.__probe.report('resume'));
      console.log(`[nav:${target}] ` + JSON.stringify(r));
      console.log(`[nav:${target}-drop] ` + JSON.stringify(await page.evaluate(() => window.__probe.drop)));
      assertCaughtUpCleanly(r);
    });
  }

  test('hidden tab mid-stream', async ({ page }) => {
    const state = { reconnectable: false };
    const events = buildEvents(CHUNK_CHARS);
    await startStream(page, state, events);
    await expect(page.getByText('Section 2', { exact: false }).first()).toBeVisible({ timeout: 60_000 });
    if (process.env.PERF_HEADED) {
      // A real background tab: rAF paused, timers throttled, no layout, no
      // smooth-scroll ticks. Needs a display; headless tabs never go hidden.
      const other = await page.context().newPage();
      await other.goto('about:blank');
      await other.bringToFront();
      await page.waitForTimeout(8000);
      const away = await page.evaluate(() => document.visibilityState);
      expect(away, 'the tab must really be hidden or this branch measures nothing').toBe('hidden');
      await page.evaluate(() => window.__probe.mark('resume'));
      await page.bringToFront();
      await other.close();
    } else {
      const len = () => page.evaluate(() => (document.querySelector('main') || document.body).textContent.length);
      await page.evaluate(() => window.__raf.freeze());
      const atFreeze = await len();
      await page.waitForTimeout(6000);
      const atThaw = await len();
      await page.evaluate(() => { window.__probe.mark('resume'); window.__raf.thaw(); });
      await page.waitForTimeout(100); const after100 = await len();
      await page.waitForTimeout(900); const after1000 = await len();
      await page.waitForTimeout(2000); const after3000 = await len();
      console.log(`[hidden] len freeze=${atFreeze} thaw=${atThaw} +100ms=${after100} +1s=${after1000} +3s=${after3000}`);
    }
    await expect(page.getByText(END_MARKER)).toBeVisible({ timeout: 120_000 });
    await page.waitForTimeout(1500);
    const r = await page.evaluate(() => window.__probe.report('resume'));
    console.log('[hidden] ' + JSON.stringify(r));
    assertCaughtUpCleanly(r);
  });
});
