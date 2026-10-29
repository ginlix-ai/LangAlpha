/**
 * The status line: the tool-call layout that draws a turn's calls as one line
 * naming the newest step, not a row per call.
 *
 * The step a block opens on runs in the summary's place, so nothing is ever
 * held empty above the line, and the count takes that place when the next
 * step comes in under it. From then on finished calls only count in the
 * summary, and the line and what sits under it hold their place through every
 * swap and count, and through the gaps between model calls, where the line
 * keeps the step that last finished until the next takes its place.
 */
import { configureSSE, resetMockServer, test, expect } from './fixtures.js';
import { sseEvents } from './helpers/mockResponses.js';
import { TH, openThread, send } from './helpers/chatScenario.js';
import { sampleFrames, stopSampling } from './helpers/frameProbe.js';

const spread = (values) => Math.max(...values) - Math.min(...values);
// The status line on show, wherever it runs: a step lifting out of a swap is
// inert until it has gone.
const LINE = '[data-status-line]:not([inert] *)';

test.describe('status layout', () => {
  test.beforeEach(async () => {
    await resetMockServer();
  });

  test('status line: one line names the newest step, and finished calls only count above it', async ({ page }) => {
    await openThread(page, { layout: 'status' });
    await configureSSE({ method: 'POST', path: `/api/v1/threads/${TH}/messages`, events: [
      sseEvents.toolCalls([{ name: 'Grep', args: { pattern: 'margin', path: 'data' }, id: 'g1' }]),
      { ...sseEvents.finishToolCalls(), delayAfter: 600 },
      { ...sseEvents.toolCallResult('g1', 'data/a.csv:1'), delayAfter: 400 },
      sseEvents.toolCalls([
        { name: 'WebFetch', args: { url: 'https://one.example.com' }, id: 'w1' },
        { name: 'WebFetch', args: { url: 'https://two.example.com' }, id: 'w2' },
      ]),
      { ...sseEvents.finishToolCalls(), delayAfter: 700 },
      // Out of order: the newer call lands first, then the older one fails.
      { ...sseEvents.toolCallResult('w2', 'Fetched'), delayAfter: 700 },
      { ...sseEvents.toolCallResult('w1', 'Error: fetch failed'), delayAfter: 2500 },
      sseEvents.messageChunk('The margin drifted.'),
      sseEvents.finishStop(),
      sseEvents.creditUsage(),
    ], delay: 30 });
    await send(page, 'Check margins');
    await sampleFrames(page, () => {
      const zone = document.querySelector('[data-testid="activity-live-zone"]');
      // The line under the count, once the count has taken the first step's place.
      const line = zone?.querySelector('[data-status-line]');
      const bubble = document.querySelector('[data-message-role="assistant"]');
      const mark = document.querySelector('[data-testid="streaming-indicator"]');
      const top = bubble?.getBoundingClientRect().top ?? 0;
      // The slot over the rows: the first step, then the count.
      const band = document.querySelector('[id^="activity-timeline-"]')?.previousElementSibling;
      return {
        rows: zone ? zone.children.length : 0,
        lineTop: line ? line.getBoundingClientRect().top - top : null,
        markTop: mark ? mark.getBoundingClientRect().top - top : null,
        lead: !!band?.querySelector('.activity-lead[data-status-line]'),
        empty: !!band && band.textContent.trim() === '',
        opened: !!window.__opened,
        // The reply's first paragraph: a thought's own sits inside its row.
        reply: [...(bubble?.querySelectorAll('p') ?? [])].some((el) => !el.closest('.titem')),
      };
    });

    const line = page.locator(LINE);
    // Two calls in flight: the line names the newer one.
    await expect(line).toContainText('two.example.com');
    // It lands first, so the line moves to the call still running.
    await expect(line).toContainText('one.example.com');
    await expect(line).not.toContainText('two.example.com');
    const summary = page.locator('[id^="activity-summary-"]');
    // The failure has no row of its own; the summary keeps it findable, and
    // the line keeps the failed step on screen with its badge.
    await expect(summary).toContainText('1 failed');
    await expect(line.locator('.nrow-badge')).toBeVisible();
    // The frames judged below run to 2400ms past the line's first, timed from
    // that frame rather than from when the polls above caught up.
    await page.waitForFunction(() => {
      const first = window.__samples.find((s) => s.lineTop !== null);
      return !!first && window.__samples.at(-1).t > first.t + 2400;
    });
    // The full list is one click away.
    await page.evaluate(() => { window.__opened = true; });
    await summary.click();
    const zone = page.getByTestId('activity-live-zone');
    await expect(line).toHaveCount(0);
    await expect(zone.locator('.titem')).toHaveCount(3);
    await expect(zone.locator('.nrow-badge')).toHaveCount(1);
    await summary.click();
    await expect(line).toBeVisible();

    await expect(page.getByText('The margin drifted.')).toBeVisible({ timeout: 10000 });
    const samples = await stopSampling(page);
    const before = samples.filter((s) => !s.reply && !s.opened);
    expect(Math.max(...before.map((s) => s.rows)), 'a finished call added a row').toBe(1);
    // The first step runs where the count goes, and nothing is held empty.
    expect(before.some((s) => s.lead), 'the first step did not lead').toBe(true);
    expect(before.some((s) => s.empty), 'an empty band was held over the line').toBe(false);
    // Once the count has taken the first step's place and the line has come
    // in under it, through every swap and count until the failure, neither
    // the line nor anything under it moves.
    const firstLine = before.findIndex((s) => s.lineTop !== null);
    const settled = before.filter((s) => s.t > before[firstLine].t + 600 && s.t < before[firstLine].t + 2400 && s.markTop !== null);
    expect(settled.length).toBeGreaterThan(30);
    const lineTops = settled.map((s) => s.lineTop);
    expect(Math.max(...lineTops) - Math.min(...lineTops)).toBeLessThan(0.5);
    let travel = 0;
    for (let i = 1; i < settled.length; i++) travel += Math.abs(settled[i].markTop - settled[i - 1].markTop);
    expect(travel).toBeLessThan(1);
  });

  test('status line: holds the step that last finished through the gap before the next model call', async ({ page }) => {
    await openThread(page, { layout: 'status' });
    await configureSSE({ method: 'POST', path: `/api/v1/threads/${TH}/messages`, events: [
      sseEvents.toolCalls([{ name: 'Grep', args: { pattern: 'margin', path: 'data' }, id: 'g1' }]),
      { ...sseEvents.finishToolCalls(), delayAfter: 400 },
      { ...sseEvents.toolCallResult('g1', 'data/a.csv:1'), delayAfter: 300 },
      // The model call ends on a content-free finish, which clears the
      // bubble's per-call streaming flag, and the next call is a while coming.
      { ...sseEvents.finishStop(), delayAfter: 1500 },
      sseEvents.toolCalls([{ name: 'WebFetch', args: { url: 'https://one.example.com' }, id: 'w1' }]),
      { ...sseEvents.finishToolCalls(), delayAfter: 600 },
      { ...sseEvents.toolCallResult('w1', 'Fetched'), delayAfter: 300 },
      sseEvents.messageChunk('The margin held.'),
      sseEvents.finishStop(),
      sseEvents.creditUsage(),
    ], delay: 30 });
    await send(page, 'Check margins');
    await sampleFrames(page, () => {
      const bubble = document.querySelector('[data-message-role="assistant"]');
      const top = bubble?.getBoundingClientRect().top ?? 0;
      const line = document.querySelector('[data-status-line]:not([inert] *)');
      const mark = document.querySelector('[data-testid="streaming-indicator"]');
      return {
        live: document.querySelector('[data-turn-fold]')?.getAttribute('data-turn-fold') === 'live',
        line: line?.textContent ?? null,
        state: line?.getAttribute('data-activity-state') ?? null,
        lineTop: line ? line.getBoundingClientRect().top - top : null,
        markTop: mark ? mark.getBoundingClientRect().top - top : null,
      };
    });
    await expect(page.getByText('The margin held.')).toBeVisible({ timeout: 10000 });
    await page.waitForTimeout(600);
    const samples = await stopSampling(page);

    const firstLine = samples.find((s) => s.line !== null);
    expect(firstLine, 'the line never opened').toBeDefined();
    // Past the line's own entrance, for as long as the turn runs.
    const running = samples.filter((s) => s.live && s.t > firstLine.t + 600);
    expect(running.length).toBeGreaterThan(60);
    expect(running.filter((s) => s.line === null).length, 'the line left while the turn ran').toBe(0);
    // Through the gap the line keeps the step that finished, settled, and
    // neither it nor anything under it moves.
    const next = running.find((s) => s.line.includes('one.example.com'));
    expect(next, 'the next step never took the line').toBeDefined();
    const gap = running.filter((s) => s.t < next.t);
    expect(gap.length).toBeGreaterThan(60);
    expect(gap.at(-1).state).toBe('settled');
    expect(spread(gap.map((s) => s.lineTop)), 'the line moved').toBeLessThan(0.5);
    let travel = 0;
    for (let i = 1; i < gap.length; i++) travel += Math.abs(gap[i].markTop - gap[i - 1].markTop);
    expect(travel, 'what is under the line moved').toBeLessThan(1);
  });

  // A block that opens on a thought runs it in the summary's place: an empty
  // band held over it closed when the reply began, and the page dropped by it.
  const thought = [
    sseEvents.messageChunk('start', 'reasoning_signal'),
    { ...sseEvents.messageChunk('Weighing what the question needs.', 'reasoning'), delayAfter: 1200 },
    { ...sseEvents.messageChunk('complete', 'reasoning_signal'), delayAfter: 300 },
  ];
  const bandProbe = () => {
    const bubble = document.querySelector('[data-message-role="assistant"]');
    const top = bubble?.getBoundingClientRect().top ?? 0;
    const band = document.querySelector('[id^="activity-timeline-"]')?.previousElementSibling;
    const line = document.querySelector('[data-testid="activity-live-zone"] [data-status-line]');
    const row = bubble?.querySelector('.titem');
    // The reply's first paragraph: a thought's own sits inside its row.
    const reply = [...(bubble?.querySelectorAll('p') ?? [])].find((el) => !el.closest('.titem'));
    return {
      empty: !!band && band.textContent.trim() === '',
      bandTop: band ? band.getBoundingClientRect().top - top : null,
      bandH: band ? band.getBoundingClientRect().height : null,
      rowTop: row ? row.getBoundingClientRect().top - top : null,
      lineTop: line ? line.getBoundingClientRect().top - top : null,
      replyTop: reply ? reply.getBoundingClientRect().top - top : null,
      // The turn's process folds behind its header once the turn ends.
      live: document.querySelector('[data-turn-fold]')?.getAttribute('data-turn-fold') === 'live',
      summary: !!band?.querySelector('[id^="activity-summary-"]'),
    };
  };

  test('status line: a thought that opens the block settles where it ran when the reply follows', async ({ page }) => {
    await openThread(page, { layout: 'status' });
    await configureSSE({ method: 'POST', path: `/api/v1/threads/${TH}/messages`, events: [
      ...thought,
      // Held open past the typing, so the reply is seen standing under the
      // thought before the turn ends.
      { ...sseEvents.messageChunk('A P/E ratio divides the price by the earnings.'), delayAfter: 1500 },
      sseEvents.finishStop(),
      sseEvents.creditUsage(),
    ], delay: 30 });
    await send(page, 'What is a P/E ratio?');
    await sampleFrames(page, bandProbe);
    await expect(page.getByText('A P/E ratio divides the price by the earnings.')).toBeVisible({ timeout: 10000 });
    await page.waitForTimeout(600);
    const drawn = (await stopSampling(page)).filter((s) => s.rowTop !== null && s.live);
    expect(drawn.some((s) => s.empty), 'an empty band was held over the thought').toBe(false);
    // Past the thought's own entrance, through the reply typing in under it.
    const samples = drawn.filter((s) => s.t > drawn[0].t + 600);
    expect(samples.length).toBeGreaterThan(30);
    const withReply = samples.filter((s) => s.replyTop !== null);
    expect(withReply.length).toBeGreaterThan(10);
    expect(spread(samples.map((s) => s.rowTop)), 'the thought moved').toBeLessThan(0.5);
    expect(spread(withReply.map((s) => s.replyTop)), 'the reply moved').toBeLessThan(0.5);
  });

  test('status line: a step after the opening thought takes the line below it, and nothing slides', async ({ page }) => {
    await openThread(page, { layout: 'status' });
    await configureSSE({ method: 'POST', path: `/api/v1/threads/${TH}/messages`, events: [
      ...thought,
      sseEvents.toolCalls([{ name: 'WebFetch', args: { url: 'https://one.example.com' }, id: 'w1' }]),
      { ...sseEvents.finishToolCalls(), delayAfter: 1500 },
      { ...sseEvents.toolCallResult('w1', 'Fetched'), delayAfter: 300 },
      sseEvents.messageChunk('Fetched it.'),
      sseEvents.finishStop(),
      sseEvents.creditUsage(),
    ], delay: 30 });
    await send(page, 'Fetch one');
    await sampleFrames(page, bandProbe);
    const line = page.locator('[data-testid="activity-live-zone"] [data-status-line]');
    await expect(line).toContainText('one.example.com', { timeout: 5000 });
    // Timed from the line's first drawn frame, not from when the poll above
    // noticed it, which can be any time up to a poll interval later.
    await page.waitForFunction(() => {
      const first = window.__samples.find((s) => s.lineTop !== null);
      return !!first && window.__samples.at(-1).t > first.t + 1000;
    });
    const samples = (await stopSampling(page)).filter((s) => s.bandTop !== null);
    expect(samples.some((s) => s.empty), 'an empty band was held over the thought').toBe(false);
    // The thought's row turns into the count where it sits.
    expect(samples.at(-1).summary).toBe(true);
    expect(spread(samples.map((s) => s.bandTop)), 'the band moved').toBeLessThan(0.5);
    // The thought is the summary's height, so the band that held it is the
    // band the count settles into, and the line, once in, stays.
    const lined = samples.filter((s) => s.lineTop !== null);
    expect(lined.length).toBeGreaterThan(10);
    // Past the band's own entrance, up to the line's.
    const thinking = samples.filter((s) => s.t > samples[0].t + 600 && s.t < lined[0].t);
    expect(thinking.length).toBeGreaterThan(10);
    const thoughtH = thinking.at(-1).bandH;
    expect(spread(thinking.map((s) => s.bandH)), 'the thought changed height').toBeLessThan(0.5);
    // The count's band, less the inset it keeps over the line.
    expect(Math.abs(samples.at(-1).bandH - 8 - thoughtH), 'the count is not the thought\'s height').toBeLessThan(0.5);
    const after = lined.filter((s) => s.t > lined[0].t + 600);
    console.log(`band ${thoughtH.toFixed(1)} -> ${samples.at(-1).bandH.toFixed(1)}px, line entrance ${spread(lined.map((s) => s.lineTop)).toFixed(1)}px`);
    expect(after.length).toBeGreaterThan(10);
    expect(spread(after.map((s) => s.lineTop)), 'the line moved').toBeLessThan(0.5);
  });
});
