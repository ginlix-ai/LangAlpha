/**
 * Streaming smoothness: a long reply arriving at token cadence under CPU
 * throttling, measured as frame gaps, long animation frames and DOM churn.
 * Off by default; flags and invocations are in e2e/perf/README.md.
 */
import { configureSSE, resetMockServer, mockAPI, test, expect } from '../fixtures.js';
import { sseEvents } from '../helpers/mockResponses.js';
import { TH, chatViewOverrides } from '../helpers/chatScenario.js';
import { buildReply, buildEvents, END_MARKER } from './streamFixture.js';
import { installSmoothProbe } from './metrics.js';
import { label, writeRun } from './run.js';

const CPU_RATE = Number(process.env.PERF_CPU || 4);
const CHUNK_DELAY_MS = Number(process.env.PERF_CHUNK_MS || 8);
const CHUNK_CHARS = Number(process.env.PERF_CHUNK_CHARS || 8);
const PROFILE = !!process.env.PERF_PROFILE;
const TRACE = !!process.env.PERF_TRACE;
// Paths worth naming in the log: the rest are the page-load fetches.
const TOP_REQUEST_PATHS = 12;

/** Self time per module and per function from a V8 CPU profile. */
function summarizeProfile(p) {
  const nodes = new Map(p.nodes.map((n) => [n.id, n]));
  const selfUs = new Map();
  for (let i = 0; i < p.samples.length; i++) {
    selfUs.set(p.samples[i], (selfUs.get(p.samples[i]) || 0) + (p.timeDeltas[i] || 0));
  }
  const moduleOf = (url) => {
    if (!url) return '(native/anonymous)';
    const dep = url.match(/\/node_modules\/\.vite\/deps\/([^?]+)/);
    if (dep) return `dep:${dep[1].replace(/\.js$/, '')}`;
    const nm = url.match(/\/node_modules\/(@[^/]+\/[^/]+|[^/]+)/);
    if (nm) return `dep:${nm[1]}`;
    const src = url.match(/\/src\/(.+?)(\?|$)/);
    if (src) return `src/${src[1]}`;
    return url.replace(/\?.*$/, '').split('/').slice(-2).join('/');
  };
  const byModule = new Map();
  const byFunction = new Map();
  for (const [id, us] of selfUs) {
    const n = nodes.get(id);
    if (!n) continue;
    const { functionName, url, lineNumber } = n.callFrame;
    const mod = moduleOf(url);
    byModule.set(mod, (byModule.get(mod) || 0) + us);
    const fn = `${functionName || '(anonymous)'} ${mod}:${lineNumber}`;
    byFunction.set(fn, (byFunction.get(fn) || 0) + us);
  }
  const top = (m) => [...m].map(([k, us]) => [k, Math.round(us / 1000)]).sort((a, b) => b[1] - a[1]).slice(0, 40);
  return { byModule: top(byModule), byFunction: top(byFunction) };
}

/**
 * Wall time per trace event name, for the page's renderer main thread only.
 * One trace carries every process (other renderers, the browser, the GPU)
 * and every thread in them, and their durations overlap in time, so summing
 * across them would count concurrent work as if it were serial.
 */
function summarizeTrace(events) {
  const isTimeline = (e) => /devtools\.timeline/.test(e.cat || '');
  // The page's main thread is the CrRendererMain that carried the most
  // timeline events; an extension or about:blank renderer carries next to none.
  const mains = new Set();
  for (const e of events) {
    if (e.ph === 'M' && e.name === 'thread_name' && e.args?.name === 'CrRendererMain') mains.add(`${e.pid}:${e.tid}`);
  }
  const load = new Map();
  for (const e of events) {
    const k = `${e.pid}:${e.tid}`;
    if (mains.has(k) && isTimeline(e)) load.set(k, (load.get(k) || 0) + 1);
  }
  const main = [...load].sort((a, b) => b[1] - a[1])[0]?.[0];
  if (!main) return [];
  const byName = new Map();
  const count = new Map();
  // B/E pairs nest like a call stack, same name included, so each name keeps
  // a stack of open starts and an E closes the innermost one.
  const open = new Map();
  const add = (name, us) => {
    byName.set(name, (byName.get(name) || 0) + us);
    count.set(name, (count.get(name) || 0) + 1);
  };
  for (const e of events) {
    if (`${e.pid}:${e.tid}` !== main || !isTimeline(e)) continue;
    if (e.ph === 'X' && typeof e.dur === 'number') {
      add(e.name, e.dur);
    } else if (e.ph === 'B') {
      if (!open.has(e.name)) open.set(e.name, []);
      open.get(e.name).push(e.ts);
    } else if (e.ph === 'E') {
      const start = open.get(e.name)?.pop();
      if (start !== undefined) add(e.name, e.ts - start);
    }
  }
  return [...byName].map(([k, us]) => [k, Math.round(us / 1000), count.get(k)]).sort((a, b) => b[1] - a[1]).slice(0, 25);
}

// PERF_HEADED=1 runs on the real display: headless rAF caps near 110 fps.
test.use({ headless: !process.env.PERF_HEADED });

test.describe('streaming smoothness', () => {
  test.skip(!process.env.PERF, 'set PERF=1 to run the smoothness benchmark');
  test.setTimeout(180_000);

  test.beforeEach(async () => {
    await resetMockServer();
  });

  test('long reply at token cadence', async ({ page }, testInfo) => {
    await page.addInitScript(installSmoothProbe);
    await mockAPI(page, chatViewOverrides());
    await configureSSE({
      method: 'GET',
      path: `/api/v1/threads/${TH}/messages/replay`,
      events: [sseEvents.replayDone()],
      delay: 10,
    });
    const reply = buildReply();
    const events = buildEvents(CHUNK_CHARS);
    await configureSSE({
      method: 'POST',
      path: `/api/v1/threads/${TH}/messages`,
      events,
      delay: CHUNK_DELAY_MS,
    });

    await page.goto(`/chat/t/${TH}`);
    await page.waitForSelector('textarea', { timeout: 10000 });

    const cdp = await page.context().newCDPSession(page);
    if (CPU_RATE > 1) await cdp.send('Emulation.setCPUThrottlingRate', { rate: CPU_RATE });
    if (PROFILE) {
      await cdp.send('Profiler.enable');
      await cdp.send('Profiler.setSamplingInterval', { interval: 500 });
    }
    // Let the throttled page settle before the clock starts.
    await page.waitForTimeout(500);
    if (PROFILE) await cdp.send('Profiler.start');
    const traceEvents = [];
    if (TRACE) {
      cdp.on('Tracing.dataCollected', (d) => traceEvents.push(...d.value));
      await cdp.send('Tracing.start', {
        traceConfig: { includedCategories: ['devtools.timeline', 'disabled-by-default-devtools.timeline'] },
        transferMode: 'ReportEvents',
      });
    }

    const reqs = new Map();
    page.on('request', (r) => { const k = `${r.method()} ${new URL(r.url()).pathname}`; reqs.set(k, (reqs.get(k) || 0) + 1); });
    await page.locator('textarea').fill('Give me an earnings deep dive on NVDA');
    await page.evaluate(() => window.__smooth.start(document.querySelector('main') || document.body));
    await page.locator('button[aria-label="Send message"]').click();

    await expect(page.getByText(END_MARKER)).toBeVisible({ timeout: 150_000 });
    // The typewriter and the last fold animations run past the final chunk.
    await page.waitForTimeout(1500);
    const m = await page.evaluate(() => window.__smooth.stop());
    let profile = null;
    if (PROFILE) {
      const { profile: p } = await cdp.send('Profiler.stop');
      profile = summarizeProfile(p);
    }
    let trace = null;
    if (TRACE) {
      const done = new Promise((resolve) => cdp.once('Tracing.tracingComplete', resolve));
      await cdp.send('Tracing.end');
      await done;
      trace = summarizeTrace(traceEvents);
    }
    if (CPU_RATE > 1) await cdp.send('Emulation.setCPUThrottlingRate', { rate: 1 });

    // Requests are part of every run, not a trace extra: a control that writes
    // and refetches in a loop shows up here long before it shows up in the
    // frame numbers, and the summary diffs the counts between labels.
    const requests = [...reqs].sort((a, b) => b[1] - a[1]);
    const run = {
      label: label(),
      at: new Date().toISOString(),
      config: { cpuRate: CPU_RATE, chunkDelayMs: CHUNK_DELAY_MS, chunkChars: CHUNK_CHARS, events: events.length, replyChars: reply.length },
      metrics: m,
      requests,
      profile,
      trace,
    };
    writeRun('streaming', run, testInfo);

    const { durationMs, fps, gapP95, gapMax, framesOver50, frozenMs, loafCount, loafMaxMs, mutations } = m;
    console.log(`[perf ${run.label}] ${durationMs}ms fps=${fps} p95=${gapP95}ms max=${gapMax}ms >50ms=${framesOver50} frozen=${frozenMs}ms loaf=${loafCount}/${loafMaxMs}ms mutations=${mutations}`);
    console.log(`[perf ${run.label}] requests during the stream:`);
    for (const [k, n] of requests.slice(0, TOP_REQUEST_PATHS)) console.log(`    ${String(n).padStart(6)}  ${k}`);
    if (profile) {
      console.log(`[perf ${run.label}] self time by module (ms):`);
      for (const [mod, ms] of profile.byModule.slice(0, 18)) console.log(`    ${String(ms).padStart(6)}  ${mod}`);
      console.log(`[perf ${run.label}] top functions (ms):`);
      for (const [fn, ms] of profile.byFunction.slice(0, 18)) console.log(`    ${String(ms).padStart(6)}  ${fn}`);
    }
    if (trace) {
      console.log(`[perf ${run.label}] trace time by event (ms, count):`);
      for (const [name, ms, n] of trace) console.log(`    ${String(ms).padStart(6)}  ${String(n).padStart(6)}  ${name}`);
    }
    // The probe has to have sampled at a plausible rate for any of the gap
    // percentiles to mean anything: under 10 fps average, they do not.
    expect(m.frames).toBeGreaterThan(m.durationMs / 100);
  });
});
