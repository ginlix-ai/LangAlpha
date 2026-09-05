/**
 * Typewriter feel: the long reply at a fast model's pace, once as an even
 * token stream and once in lumps (what a proxy or a Redis batch delivers).
 * Records what the eye notices in the reveal - how far the text trails the
 * model, how long it keeps typing after the model stopped, stalls and jumps.
 * Off by default; flags and invocations are in e2e/perf/README.md.
 */
import { configureSSE, resetMockServer, mockAPI, test, expect } from '../fixtures.js';
import { sseEvents } from '../helpers/mockResponses.js';
import { TH, chatViewOverrides } from '../helpers/chatScenario.js';
import { buildReply, chunk, END_MARKER } from './streamFixture.js';
import { installProbe } from './probe.js';
import { label, writeRun } from './run.js';

// ~500 chars/s, the pace of a fast model.
const SCENARIOS = [
  { name: 'even', chars: 16, delayMs: 32 },
  { name: 'lumpy', chars: 160, delayMs: 320 },
];

function analyze(frames, replyChars) {
  // Resample to fixed 50 ms buckets so the numbers do not depend on the
  // machine's frame rate (headless Chromium runs rAF at ~110 fps, no vsync).
  // The text measured is the message bubbles' alone (the probe's reply
  // column), so composer, status and activity text never count as reveal.
  const STEP = 50;
  const t0 = frames[0][0], t1 = frames[frames.length - 1][0];
  const at = [];
  let j = 0;
  for (let t = t0; t <= t1; t += STEP) {
    while (j + 1 < frames.length && frames[j + 1][0] <= t) j++;
    at.push([t, frames[j][5], frames[j][2]]);
  }
  let first = -1, last = -1;
  for (let i = 1; i < at.length; i++) if (at[i][1] > at[i - 1][1]) { if (first < 0) first = i; last = i; }
  if (first < 0) return null;
  const liveFrames = frames.filter((f) => f[2]).length;
  let liveEnd = -1;
  for (let i = first; i < at.length; i++) if (at[i][2] === 0 && at[i - 1][2] === 1) { liveEnd = i; break; }
  const modelDoneAt = liveEnd > 0 ? at[liveEnd][0] : null;
  const gains = [];
  let stalls = 0, stallBuckets = 0, run = 0, maxGain = 0, maxGainAt = 0, firstStallAt = -1;
  for (let i = first; i <= last; i++) {
    const g = at[i][1] - at[i - 1][1];
    gains.push(g);
    if (g > maxGain) { maxGain = g; maxGainAt = Math.round(at[i][0] - at[first][0]); }
    // A stall: no visible progress for 150 ms or more while the model is still talking.
    if (g <= 0 && at[i][2] === 1) { run++; if (run === 3) { stalls++; if (firstStallAt < 0) firstStallAt = Math.round(at[i][0] - at[first][0]); } if (run >= 3) stallBuckets++; } else run = 0;
  }
  const sorted = gains.slice().sort((a, b) => a - b);
  const median = sorted[Math.floor(sorted.length / 2)];
  const mean = gains.reduce((a, b) => a + b, 0) / gains.length;
  const sd = Math.sqrt(gains.reduce((a, b) => a + (b - mean) ** 2, 0) / gains.length);
  // Follow scroll while the model is talking: how far the view sits above the
  // bottom, how often it moves backwards, and how uneven its motion is.
  const live = frames.filter((f) => f[2] === 1 && f[3] >= 0);
  const dist = live.map((f) => f[4] - f[3]).sort((a, b) => a - b);
  let back = 0; const moves = [];
  for (let i = 1; i < live.length; i++) { const d = live[i][3] - live[i - 1][3]; if (d < -1) back++; if (d > 0) moves.push(d); }
  const mmean = moves.reduce((a, b) => a + b, 0) / (moves.length || 1);
  const msd = Math.sqrt(moves.reduce((a, b) => a + (b - mmean) ** 2, 0) / (moves.length || 1));
  const follow = live.length ? {
    distP50: Math.round(dist[Math.floor(dist.length * 0.5)]), distP95: Math.round(dist[Math.floor(dist.length * 0.95)]), distMax: Math.round(dist[dist.length - 1]),
    backwardFrames: back, movingFrames: moves.length, moveCv: +(msd / (mmean || 1)).toFixed(2), maxMovePx: Math.round(Math.max(...moves, 0)),
  } : null;
  const fgaps = [];
  for (let i = 1; i < frames.length; i++) fgaps.push(frames[i][0] - frames[i - 1][0]);
  fgaps.sort((a, b) => a - b);
  return {
    revealMs: Math.round(at[last][0] - at[first][0]),
    // How long the text keeps typing after the model stopped (null: stop control never seen).
    tailAfterModelMs: modelDoneAt === null ? null : Math.round(at[last][0] - modelDoneAt),
    stalls, stallMs: stallBuckets * STEP, firstStallAtMs: firstStallAt,
    // Buckets that showed more than three times the typical amount: a visible jump.
    jumps: gains.filter((g) => g > Math.max(3 * median, 60)).length,
    maxGainPer50ms: maxGain, maxGainAtMs: maxGainAt, medianGainPer50ms: median,
    // Text still hidden when the model finished, against what finally
    // rendered rather than the Markdown source, whose syntax never shows:
    // what the final frame has to pop or type (null: stop control never seen).
    backlogAtModelDone: modelDoneAt === null ? null : at[last][1] - at[liveEnd][1],
    // Unevenness of the reveal, bucket to bucket (0 = perfectly steady).
    speedCv: +(sd / mean).toFixed(2),
    meanCharsPerSec: Math.round(mean * 1000 / STEP),
    frameGapP95: +fgaps[Math.floor(fgaps.length * 0.95)].toFixed(1),
    frames: frames.length, liveFrames, replyChars, follow,
  };
}

// Headed gives real vsync; the reveal is judged at the screen's refresh rate.
test.use({ headless: !process.env.PERF_HEADED });

test.describe('typewriter feel', () => {
  test.skip(!process.env.PERF, 'set PERF=1 to run the typewriter benchmark');
  test.setTimeout(180_000);
  test.beforeEach(async () => { await resetMockServer(); });

  for (const sc of SCENARIOS) {
    test(`fast model, ${sc.name} arrival`, async ({ page }, testInfo) => {
      // `live` adds the stop-control read per frame, which is what stamps when
      // the model finished, and `reply` the bubble-only text length the reveal
      // is measured on; the painted and scroll-log passes stay off so the
      // probe costs the reveal as little as possible.
      await page.addInitScript(installProbe, { live: true, reply: true });
      await mockAPI(page, chatViewOverrides());
      await configureSSE({ method: 'GET', path: `/api/v1/threads/${TH}/messages/replay`, events: [sseEvents.replayDone()], delay: 10 });
      const reply = buildReply();
      const events = [];
      for (const c of chunk(reply, sc.chars)) events.push(sseEvents.messageChunk(c));
      events.push({ ...sseEvents.finishStop(), delayAfter: 0 });
      events.push(sseEvents.creditUsage());
      await configureSSE({ method: 'POST', path: `/api/v1/threads/${TH}/messages`, events, delay: sc.delayMs });

      await page.goto(`/chat/t/${TH}`);
      await page.waitForSelector('textarea', { timeout: 10000 });
      await page.waitForTimeout(500);
      await page.locator('textarea').fill('Give me an earnings deep dive on NVDA');
      await page.evaluate(() => window.__probe.mark('send'));
      await page.locator('button[aria-label="Send message"]').click();
      await expect(page.getByText(END_MARKER)).toBeVisible({ timeout: 150_000 });
      await page.waitForTimeout(1500);
      const frames = await page.evaluate(() => window.__probe.framesSince('send'));
      const m = analyze(frames, reply.length);
      const run = { label: label(), scenario: sc, at: new Date().toISOString(), metrics: m };
      writeRun(`typewriter-${sc.name}`, run, testInfo);
      console.log(`[typewriter ${sc.name} ${run.label}] ${JSON.stringify(m)}`);
      // The sampler has to have run for the bucketed numbers to mean anything:
      // under 10 fps across the reveal, they are noise.
      expect(m.frames).toBeGreaterThan(m.revealMs / 100);
    });
  }
});
