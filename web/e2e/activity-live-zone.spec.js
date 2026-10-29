/**
 * The activity live zone must size to its rows after a fold is interrupted.
 *
 * Three tool rows complete together, fold into the accordion, and a fourth
 * call lands while the live zone is still collapsing. The zone must end up
 * as tall as the one row it now holds, not the three it held before.
 */
import { configureSSE, resetMockServer, mockAPI, test, expect } from './fixtures.js';
import { sseEvents, defaultResponses } from './helpers/mockResponses.js';
import { TH, chatViewOverrides, openThread, replayNothing, send } from './helpers/chatScenario.js';
import { sampleFrames, stopSampling } from './helpers/frameProbe.js';
import { ROW_FOLD, MIN_LIVE_EXPOSURE_MS } from '../src/pages/ChatAgent/components/messageList/liveZoneTiming.ts';

// A completed row stays live for MIN_LIVE_EXPOSURE_MS after its call was
// created, then folds on ROW_FOLD, a spring with about a fifth of the height
// left three time constants (1 / sqrt(stiffness)) in, so a call landing then
// lands late inside the collapse. Late, because the fold can only run behind
// its clock (a busy first frame stamps the calls late), never ahead. The clock starts
// at the first event, and the four events before the delay each cost a gap,
// so the delay is measured from the first event, not from the last result.
// The tool must not be an inline-artifact tool (those skip the exposure
// window).
const EVENT_GAP_MS = 30;
const LAND_MID_COLLAPSE_MS = MIN_LIVE_EXPOSURE_MS + Math.round((3 / Math.sqrt(ROW_FOLD.stiffness)) * 1000) - 4 * EVENT_GAP_MS;

test.describe('activity live zone', () => {
  test.beforeEach(async () => {
    await resetMockServer();
  });

  test('sizes to its rows after a call lands mid-fold', async ({ page }) => {
    await openThread(page);

    const batch = ['toolu_a1', 'toolu_a2', 'toolu_a3'];
    await configureSSE({
      method: 'POST',
      path: `/api/v1/threads/${TH}/messages`,
      events: [
        sseEvents.toolCalls(batch.map((id, i) => ({ name: 'bash', args: { command: `echo ${i}` }, id }))),
        sseEvents.finishToolCalls(),
        sseEvents.toolCallResult('toolu_a1', '0'),
        sseEvents.toolCallResult('toolu_a2', '1'),
        { ...sseEvents.toolCallResult('toolu_a3', '2'), delayAfter: LAND_MID_COLLAPSE_MS },
        sseEvents.toolCalls([{ name: 'bash', args: { command: 'echo 3' }, id: 'toolu_b' }]),
        // Hold the fourth call in progress so it stays a live row while measured.
        { ...sseEvents.finishToolCalls(), delayAfter: 3000 },
        sseEvents.toolCallResult('toolu_b', '3'),
        sseEvents.messageChunk('done'),
        sseEvents.finishStop(),
        sseEvents.creditUsage(),
      ],
      delay: EVENT_GAP_MS,
    });

    await page.goto(`/chat/t/${TH}`);
    await page.waitForSelector('textarea', { timeout: 10000 });
    await page.locator('textarea').fill('run four commands');
    await page.locator('button[aria-label="Send message"]').click();

    // The precondition, read in one pass so it can actually fail: the three
    // rows must already be folded behind the accordion summary when the fourth
    // call goes live. If the fourth arrived first there is no interrupted fold
    // and the height below would be measured on a case that never regressed.
    const atFold = await page.waitForFunction(() => {
      if (!document.querySelector('[id^="activity-summary-"]')) return null;
      return { active: document.querySelectorAll('[data-testid="activity-live-zone"] .titem.running').length };
    }, null, { timeout: 15000 }).then((h) => h.jsonValue());
    expect(atFold.active, 'the fourth call went live before the first three folded').toBe(0);

    const zone = page.getByTestId('activity-live-zone');
    await expect(zone.locator('.titem.running')).toHaveCount(1, { timeout: 15000 });
    // Let the zone's own animations settle before measuring.
    await page.waitForTimeout(1000);

    const { zoneHeight, rowsHeight } = await zone.evaluate((el) => ({
      zoneHeight: el.getBoundingClientRect().height,
      rowsHeight: Array.from(el.children).reduce((sum, c) => sum + c.getBoundingClientRect().height, 0),
    }));
    expect(rowsHeight).toBeGreaterThan(0);
    expect(zoneHeight).toBeLessThanOrEqual(rowsHeight + 2);
  });

  test('keeps verbose reasoning open when the completed activity accordion is expanded', async ({ page }) => {
    const prefs = defaultResponses['GET /users/me/preferences'];
    await mockAPI(page, {
      ...chatViewOverrides(),
      'GET /users/me/preferences': { ...prefs, other_preference: { ...prefs.other_preference, turn_display: 'verbose' } },
    });
    await replayNothing();
    await configureSSE({ method: 'POST', path: `/api/v1/threads/${TH}/messages`, events: [
      sseEvents.toolCalls([{ name: 'bash', args: { command: 'echo ready' }, id: 'toolu_ready' }]),
      sseEvents.finishToolCalls(),
      { ...sseEvents.toolCallResult('toolu_ready', 'ready'), delayAfter: MIN_LIVE_EXPOSURE_MS + 300 },
      sseEvents.messageChunk('start', 'reasoning_signal'),
      { ...sseEvents.messageChunk('**Comparing inputs**\n\nChecking the supplied evidence.', 'reasoning'), delayAfter: 3500 },
      sseEvents.messageChunk('complete', 'reasoning_signal'),
      sseEvents.messageChunk('Comparison complete.'),
      sseEvents.finishStop(),
      sseEvents.creditUsage(),
    ], delay: 30 });
    await send(page, 'Compare the inputs');
    const thought = page.getByRole('button', { name: 'Comparing inputs', exact: true });
    await expect(thought).toHaveAttribute('aria-expanded', 'true');
    const body = page.locator('[data-activity-state="live"] .titem-reasoning-card');
    await expect(body).toBeVisible();
    await page.locator('[id^="activity-summary-"]').click();
    await expect(thought).toHaveAttribute('aria-expanded', 'true');
    await expect(body).toBeVisible();
    await expect(page.locator('[data-turn-fold]')).toHaveAttribute('data-turn-fold', 'collapsed', { timeout: 15000 });
    await page.locator('[data-turn-fold] button').click();
    await expect(thought).toHaveAttribute('aria-expanded', 'false');
  });

  test('keeps progress visible while the first paragraph is withheld', async ({ page }) => {
    const prefs = defaultResponses['GET /users/me/preferences'];
    await mockAPI(page, {
      ...chatViewOverrides(),
      'GET /users/me/preferences': { ...prefs, other_preference: { ...prefs.other_preference, response_streaming_mode: 'paragraph' } },
    });
    await replayNothing();
    await configureSSE({ method: 'POST', path: `/api/v1/threads/${TH}/messages`, events: [
      ...Array.from({ length: 40 }, () => sseEvents.messageChunk('Still writing. ')),
      sseEvents.finishStop(),
      sseEvents.creditUsage(),
    ], delay: 100 });
    await send(page, 'Write one paragraph');
    await expect(page.getByTestId('streaming-indicator')).toBeAttached();
    // Chunks keep arriving every 100ms while none of them is shown yet.
    await page.waitForTimeout(1000);
    await expect(page.locator('[data-message-role="assistant"] p')).toHaveCount(0);
    await expect(page.getByTestId('streaming-indicator')).toHaveCSS('opacity', '1');
    await expect(page.getByTestId('streaming-indicator')).toHaveCount(0, { timeout: 10000 });
    await expect(page.locator('[data-message-role="assistant"]')).toContainText('Still writing. '.repeat(40).trim());
  });

  test('keeps the indicator through a model call that ends mid-turn', async ({ page }) => {
    await openThread(page);
    // A call that finishes with a content-free stop clears the bubble's
    // per-call flag, and the turn still has a tool round and a reply to go.
    await configureSSE({ method: 'POST', path: `/api/v1/threads/${TH}/messages`, events: [
      sseEvents.messageChunk('Checking the quote first.'),
      { ...sseEvents.finishStop(), delayAfter: 1500 },
      sseEvents.toolCalls([{ name: 'get_quote', args: { symbol: 'AMD' }, id: 'q1' }]),
      sseEvents.finishToolCalls(),
      sseEvents.toolCallResult('q1', 'Quote'),
      sseEvents.messageChunk('AMD is up on the day.'),
      sseEvents.finishStop(),
      sseEvents.creditUsage(),
    ], delay: 50 });
    await send(page, 'Check AMD');
    const spinner = page.getByTestId('streaming-indicator');
    await expect(page.getByText('Checking the quote first.')).toBeVisible();
    // Inside the pause after the stop, well past the indicator's exit.
    await page.waitForTimeout(800);
    await expect(spinner).toHaveCount(1);
    await expect(spinner).toHaveCSS('opacity', '1');
    await expect(page.getByText('AMD is up on the day.')).toBeVisible({ timeout: 10000 });
    await expect(spinner).toHaveCount(0, { timeout: 10000 });
  });

  test('fades the indicator out while the answer types, and back in for a pause', async ({ page }) => {
    await openThread(page);
    await configureSSE({ method: 'POST', path: `/api/v1/threads/${TH}/messages`, events: [
      ...Array.from({ length: 20 }, () => sseEvents.messageChunk('Still writing. ')),
      { ...sseEvents.messageChunk('Then a pause. '), delayAfter: 2500 },
      ...Array.from({ length: 20 }, () => sseEvents.messageChunk('Writing again. ')),
      sseEvents.finishStop(),
      sseEvents.creditUsage(),
    ], delay: 100 });
    await send(page, 'Write two paragraphs');
    const spinner = page.getByTestId('streaming-indicator');
    await expect(page.getByText('Still writing.', { exact: false }).first()).toBeVisible();
    // Chunks every 100ms: each gap is far inside the still window.
    await page.waitForTimeout(800);
    await expect(spinner).toHaveCount(1);
    await expect(spinner).toHaveCSS('opacity', '0');
    await expect(page.getByText('Then a pause.', { exact: false })).toBeVisible();
    await expect(spinner).toHaveCSS('opacity', '1', { timeout: 2000 });
    await expect(page.getByText('Writing again.', { exact: false }).first()).toBeVisible({ timeout: 5000 });
    await expect(spinner).toHaveCSS('opacity', '0');
    await expect(spinner).toHaveCount(0, { timeout: 10000 });
  });

  for (const turnDisplay of ['lean', 'verbose']) {
    test(`respects ${turnDisplay} reasoning in a live subagent transcript`, async ({ page }) => {
      const prefs = defaultResponses['GET /users/me/preferences'];
      await mockAPI(page, {
        ...chatViewOverrides(),
        'GET /users/me/preferences': { ...prefs, other_preference: { ...prefs.other_preference, turn_display: turnDisplay } },
      });
      await replayNothing();
      const taskChunk = (content, type) => {
        const event = sseEvents.messageChunk(content, type);
        return { ...event, data: { ...event.data, agent: 'task:reasoning-check' } };
      };
      await configureSSE({ method: 'POST', path: `/api/v1/threads/${TH}/messages`, events: [
        sseEvents.toolCalls([{ name: 'Task', args: { description: 'Review the evidence', prompt: 'Review the evidence' }, id: 'toolu_task' }]),
        sseEvents.finishToolCalls(),
        { event: 'artifact', data: { thread_id: TH, artifact_type: 'task', tool_call_id: 'toolu_task', payload: { task_id: 'reasoning-check', action: 'spawned', description: 'Review the evidence', type: 'research' } } },
        taskChunk('start', 'reasoning_signal'),
        taskChunk('**Inspecting evidence**\n\nThe subagent is comparing source documents.', 'reasoning'),
        { ...sseEvents.messageChunk('Review underway.'), delayAfter: 8000 },
        taskChunk('complete', 'reasoning_signal'),
        sseEvents.messageChunk('Review complete.'),
        sseEvents.finishStop(),
      ], delay: 50 });
      await send(page, 'Review the evidence');
      await expect(page.getByText('Review underway.', { exact: true })).toBeVisible();
      await page.getByRole('button', { name: 'Open subagent: Review the evidence', exact: true }).click();
      const thought = page.getByRole('button', { name: 'Inspecting evidence', exact: true });
      await expect(thought).toHaveAttribute('aria-expanded', String(turnDisplay === 'verbose'));
      const body = page.getByText('The subagent is comparing source documents.', { exact: true });
      if (turnDisplay === 'lean') {
        await expect(body).toBeHidden();
        await thought.click();
      }
      await expect(body).toBeVisible();
    });
  }

  test('brings the main transcript back from a subagent tab as it left it', async ({ page }) => {
    await openThread(page);
    // A first turn with nothing to fold: it keeps its row only because it ran here.
    await configureSSE({ method: 'POST', path: `/api/v1/threads/${TH}/messages`, events: [
      sseEvents.messageChunk('Hello there.'),
      sseEvents.finishStop(),
      sseEvents.creditUsage(),
    ], delay: 30 });
    await send(page, 'Say hello');
    await expect(page.getByText('Hello there.', { exact: true })).toBeVisible();
    await expect(page.getByTestId('streaming-indicator')).toHaveCount(0, { timeout: 10000 });
    await expect(page.locator('[data-turn-fold]')).toHaveCount(1);

    // A second turn spawns a subagent and holds a call open while it runs.
    await configureSSE({ method: 'POST', path: `/api/v1/threads/${TH}/messages`, events: [
      sseEvents.toolCalls([{ name: 'Task', args: { description: 'Review the evidence', prompt: 'Review the evidence' }, id: 'toolu_task' }], 1),
      sseEvents.finishToolCalls(1),
      { event: 'artifact', data: { thread_id: TH, artifact_type: 'task', tool_call_id: 'toolu_task', payload: { task_id: 'return-check', action: 'spawned', description: 'Review the evidence', type: 'research' } } },
      sseEvents.toolCalls([{ name: 'WebFetch', args: { url: 'https://example.com/evidence' }, id: 'toolu_fetch' }], 1),
      { ...sseEvents.finishToolCalls(1), delayAfter: 9000 },
      sseEvents.toolCallResult('toolu_fetch', 'Fetched', null, 1),
      sseEvents.messageChunk('Review complete.', 'text', 1),
      sseEvents.finishStop(1),
    ], delay: 50 });
    await page.locator('textarea').fill('Review the evidence');
    await page.getByRole('button', { name: 'Send message', exact: true }).click();
    const zone = page.getByTestId('activity-live-zone');
    await expect(zone.getByText('example.com')).toBeVisible();
    await expect(page.locator('[data-turn-fold]')).toHaveCount(2);

    await page.getByRole('button', { name: 'Open subagent: Review the evidence', exact: true }).click();
    await expect(page.getByRole('button', { name: 'Back to main' })).toBeVisible();
    // Past the moment a block counts as new, so coming back is a redraw.
    await page.waitForTimeout(2500);
    await sampleFrames(page, () => {
      const rows = [...(document.querySelector('[data-testid="activity-live-zone"]')?.children ?? [])];
      return {
        rows: rows.length,
        dimmest: rows.length ? Math.min(...rows.map((row) => Number(getComputedStyle(row).opacity))) : null,
        folds: document.querySelectorAll('[data-turn-fold]').length,
      };
    });
    await page.getByRole('button', { name: 'Back to main' }).click();
    await expect(zone.getByText('example.com')).toBeVisible();
    await page.waitForTimeout(600);
    const back = (await stopSampling(page)).filter((s) => s.rows > 0);
    expect(back.length).toBeGreaterThan(10);
    // Both turns keep their rows, and the running rows are drawn whole from
    // the first frame rather than arriving a second time.
    expect(Math.min(...back.map((s) => s.folds))).toBe(2);
    expect(Math.min(...back.map((s) => s.dimmest))).toBeGreaterThan(0.99);
  });

  test('folds a turn with a retained widget without a final downward step', async ({ page }) => {
    await openThread(page);
    await configureSSE({ method: 'POST', path: `/api/v1/threads/${TH}/messages`, events: [
      sseEvents.toolCalls([{ name: 'show_widget', args: { title: 'Sample table' }, id: 'toolu_widget' }]),
      sseEvents.finishToolCalls(),
      { event: 'artifact', data: { thread_id: TH, artifact_type: 'html_widget', artifact_id: 'sample-widget', payload: { title: 'Sample table', html: '<div style="height:180px">Widget result</div>' } } },
      sseEvents.toolCallResult('toolu_widget', 'Displayed widget'),
      sseEvents.messageChunk('Here is the result.'),
      sseEvents.finishStop(),
      sseEvents.creditUsage(),
    ], delay: 30 });
    await send(page, 'Show a sample table');
    const fold = page.locator('[data-turn-fold="collapsed"] button');
    await expect(fold).toBeVisible();
    const frame = page.locator('.inline-widget-container iframe');
    await expect.poll(() => frame.evaluate(el => el.getBoundingClientRect().height)).toBeGreaterThan(170);
    await fold.click();
    await page.waitForTimeout(1000);
    await page.evaluate(() => {
      const widget = document.querySelector('.inline-widget-container');
      const iframe = widget.querySelector('iframe');
      const row = document.querySelector('[data-turn-fold]');
      const probe = window.widgetFoldProbe = { iframe, samples: [], done: false, clicked: null };
      // The window runs from the click landing, not from here: the click's
      // actionability checks alone can take most of it on a busy runner.
      document.addEventListener('click', () => { probe.clicked = performance.now(); }, { capture: true, once: true });
      const sample = () => {
        // Relative to the toggle, so scroll anchoring cannot mask a layout step.
        probe.samples.push(widget.getBoundingClientRect().top - row.getBoundingClientRect().top);
        if (probe.clicked === null || performance.now() - probe.clicked < 1100) requestAnimationFrame(sample);
        else probe.done = true;
      };
      requestAnimationFrame(sample);
    });
    await page.locator('[data-turn-fold="expanded"] button').click();
    await page.waitForFunction(() => window.widgetFoldProbe.done);
    const result = await page.evaluate(() => {
      const { samples, iframe } = window.widgetFoldProbe;
      return {
        travel: samples[0] - samples.at(-1),
        maxDownwardStep: Math.max(...samples.slice(1).map((y, i) => y - samples[i])),
        sameIframe: iframe === document.querySelector('.inline-widget-container iframe'),
      };
    });
    expect(result.travel).toBeGreaterThan(10);
    expect(result.maxDownwardStep).toBeLessThanOrEqual(0.5);
    expect(result.sameIframe).toBe(true);
    await expect(fold).toBeVisible();
    await expect(frame).toBeVisible();
  });

  test('tracks a reasoning disclosure without a second trailing height animation', async ({ page }) => {
    await openThread(page);
    await configureSSE({ method: 'POST', path: `/api/v1/threads/${TH}/messages`, events: [
      sseEvents.messageChunk('start', 'reasoning_signal'),
      sseEvents.messageChunk('**Checking evidence**\n\n' + 'Compare each source and verify the result.\n\n'.repeat(12), 'reasoning'),
      sseEvents.messageChunk('complete', 'reasoning_signal'),
      sseEvents.messageChunk('The comparison is complete.'),
      sseEvents.finishStop(),
      sseEvents.creditUsage(),
    ], delay: 30 });
    await send(page, 'Compare sources');
    await page.locator('[data-turn-fold="collapsed"] button').click();
    const thought = page.getByRole('button', { name: 'Checking evidence', exact: true });
    await thought.click();
    await page.waitForTimeout(1100);
    await thought.evaluate(button => {
      const row = button.closest('[data-activity-state]').parentElement.parentElement.parentElement;
      const inner = row.firstElementChild;
      const answer = [...document.querySelectorAll('p')].find(p => p.textContent === 'The comparison is complete.');
      window.reasoningFoldProbe = { done: false, lag: [], positions: [], initialHeight: inner.getBoundingClientRect().height };
      const start = performance.now();
      const sample = () => {
        window.reasoningFoldProbe.lag.push(row.getBoundingClientRect().height - inner.getBoundingClientRect().height);
        const body = row.querySelector('.titem-reasoning-card')?.parentElement;
        window.reasoningFoldProbe.positions.push({
          bodyHeight: body?.getBoundingClientRect().height ?? 0,
          answerY: answer.getBoundingClientRect().top - button.getBoundingClientRect().top,
        });
        if (performance.now() - start < 1200) requestAnimationFrame(sample);
        else window.reasoningFoldProbe.done = true;
      };
      requestAnimationFrame(sample);
    });
    await thought.click();
    await page.waitForFunction(() => window.reasoningFoldProbe.done);
    const result = await page.evaluate(() => window.reasoningFoldProbe);
    expect(result.initialHeight).toBeGreaterThan(200);
    // The body owns its spacing: no trailing spring or gap removal after collapse.
    expect(Math.max(...result.lag)).toBeLessThan(0.5);
    const tail = result.positions.filter(sample => sample.bodyHeight < 0.1).map(sample => sample.answerY);
    expect(tail.length).toBeGreaterThan(5);
    expect(Math.max(...tail) - Math.min(...tail)).toBeLessThan(0.5);
    await expect(thought).toHaveAttribute('aria-expanded', 'false');
  });


  test('grows a lookup card out of its row without moving what is under it', async ({ page }) => {
    await openThread(page);
    const quote = { type: 'quote', quotes: ['NVDA', 'AMD', 'AVGO', 'TSM'].map((symbol, i) => ({ symbol, name: `${symbol} Holdings`, price: 100 + i })) };
    await configureSSE({ method: 'POST', path: `/api/v1/threads/${TH}/messages`, events: [
      sseEvents.toolCalls([{ name: 'get_quote', args: { symbol: 'NVDA,AMD,AVGO,TSM' }, id: 'quote1' }]),
      { ...sseEvents.finishToolCalls(), delayAfter: 1500 },
      { ...sseEvents.toolCallResult('quote1', 'Quote', quote), delayAfter: 1500 },
      sseEvents.messageChunk('Quoted.'),
      sseEvents.finishStop(),
      sseEvents.creditUsage(),
    ], delay: 30 });
    await send(page, 'Quote the chip names');
    // The call's row is the block's only item, so the block goes with it.
    await expect(page.getByTestId('activity-live-zone')).toBeVisible();
    // The indicator under the reply, from the reply's top: every frame, and in
    // the commit the card lands in, read before any frame can move it.
    await page.evaluate(() => {
      const below = () => {
        const reply = [...document.querySelectorAll('[data-message-role="assistant"]')].at(-1);
        return document.querySelector('[data-testid="streaming-indicator"]').getBoundingClientRect().top - reply.getBoundingClientRect().top;
      };
      const probe = window.cardArrivalProbe = { frames: [], landed: null, done: false };
      new MutationObserver((_, observer) => {
        if (!document.querySelector('[data-card-arrival]')) return;
        probe.landed = below();
        observer.disconnect();
      }).observe(document.querySelector('main'), { childList: true, subtree: true });
      const sample = () => {
        probe.frames.push({ y: below(), card: !!document.querySelector('[data-card-arrival]') });
        if (probe.frames.filter((f) => f.card).length < 40) requestAnimationFrame(sample);
        else probe.done = true;
      };
      requestAnimationFrame(sample);
    });
    await page.waitForFunction(() => window.cardArrivalProbe.done, null, { timeout: 10000 });
    const { frames, landed } = await page.evaluate(() => window.cardArrivalProbe);
    const before = frames.filter((f) => !f.card).at(-1).y;
    const after = frames.filter((f) => f.card).map((f) => f.y);
    // The card's box starts in the space the row gave up, so the commit that
    // swaps them moves nothing under it...
    expect(Math.abs(landed - before)).toBeLessThan(1);
    // ...and then grows to the card, pushing what is under it down only.
    const steps = [before, ...after].slice(1).map((y, i) => y - [before, ...after][i]);
    expect(Math.min(...steps)).toBeGreaterThan(-0.5);
    expect(after.at(-1) - before).toBeGreaterThan(50);
    await expect(page.locator('[data-arriving]')).toHaveCount(0);
  });

  for (const width of [1280, 390]) {
    test(`spaces live cards, reasoning, batched fetches and the waiting indicator at ${width}px`, async ({ page }) => {
      await page.setViewportSize({ width, height: 1400 });
      await openThread(page);
      const quote = { type: 'quote', quotes: [{ symbol: 'AMD', name: 'Advanced Micro Devices', price: 615.52 }] };
      const call = (id) => sseEvents.toolCalls([{ name: 'get_quote', args: { symbol: 'AMD' }, id }]);
      await configureSSE({ method: 'POST', path: `/api/v1/threads/${TH}/messages`, events: [
        call('quote1'), sseEvents.finishToolCalls(),
        { ...sseEvents.toolCallResult('quote1', 'Quote', quote), delayAfter: 2500 },
        call('missing1'), sseEvents.finishToolCalls(),
        sseEvents.toolCallResult('missing1', 'No card', { type: 'unsupported' }),
        sseEvents.messageChunk('start', 'reasoning_signal'),
        sseEvents.messageChunk('**Researching AMD valuation drivers**\n\nChecking the evidence.', 'reasoning'),
        sseEvents.messageChunk('complete', 'reasoning_signal'),
        call('missing2'), sseEvents.finishToolCalls(),
        sseEvents.toolCallResult('missing2', 'No card', { type: 'unsupported' }),
        call('quote2'), sseEvents.finishToolCalls(),
        sseEvents.toolCallResult('quote2', 'Quote', quote),
        sseEvents.toolCalls(['one', 'two', 'three'].map(id => ({ name: 'WebFetch', args: { url: `https://${id}.example.com` }, id }))),
        { ...sseEvents.finishToolCalls(), delayAfter: 8000 },
        ...['one', 'two', 'three'].map(id => sseEvents.toolCallResult(id, 'Fetched')),
        sseEvents.finishStop(), sseEvents.creditUsage(),
      ], delay: 50 });
      await send(page, 'Check AMD');
      const spinner = page.getByTestId('streaming-indicator');
      await expect(page.getByText('Advanced Micro Devices', { exact: true }).first()).toBeVisible();
      await expect(spinner).toHaveCSS('padding-top', '12px');
      await expect(spinner).toHaveCSS('opacity', '1');
      await page.waitForTimeout(250);
      await page.screenshot({ path: `../output/playwright/live-spinner-after-${width}.png` });
      const fetches = page.locator('.titem.running').filter({ hasText: 'Web Fetch' });
      await expect(fetches).toHaveCount(3);
      await page.waitForTimeout(900);
      const rects = await fetches.evaluateAll(es => es.map(e => ({ y: e.getBoundingClientRect().top, h: e.getBoundingClientRect().height })));
      expect(Math.abs((rects[1].y - rects[0].y) - (rects[2].y - rects[1].y))).toBeLessThan(0.5);
      expect(Math.max(...rects.map(r => r.h)) - Math.min(...rects.map(r => r.h))).toBeLessThan(0.5);
      const reasoning = page.locator('.titem').filter({ hasText: 'Researching AMD valuation drivers' });
      const gaps = await reasoning.evaluate(el => {
        let block = el;
        while (block.parentElement && !block.parentElement.classList.contains('sibling-space-y-3')) block = block.parentElement;
        return { before: block.getBoundingClientRect().top - block.previousElementSibling.getBoundingClientRect().bottom,
          after: block.nextElementSibling.getBoundingClientRect().top - block.getBoundingClientRect().bottom };
      });
      // Subpixel layout: the same tolerance every other geometry check here uses.
      expect(Math.abs(gaps.before - 12)).toBeLessThan(0.5);
      expect(Math.abs(gaps.after - 12)).toBeLessThan(0.5);
      await page.screenshot({ path: `../output/playwright/live-batch-after-${width}.png` });
    });

  }

});

// ---- tool rows and preparing calls ----

/** A tool_call_chunks event: argument chunks for one or more calls, keyed by
 *  the index the model gave each, as the producer streams them. */
const toolChunks = (list) => ({ event: 'tool_call_chunks', data: {
  thread_id: TH, agent: 'model:test', id: 'lc_run--test', role: 'assistant', turn_index: 0,
  tool_call_chunks: list.map(({ index, name = null, id = null, args = '' }) => ({ index, name, id, args, type: 'tool_call_chunk' })),
} });

test.describe('tool rows', () => {
  test.beforeEach(async () => {
    await resetMockServer();
  });

  test('draws no preparing row for a tool the transcript hides', async ({ page }) => {
    await openThread(page);
    const todos = JSON.stringify({ todos: [{ content: 'Read inputs', status: 'in_progress' }] });
    await configureSSE({ method: 'POST', path: `/api/v1/threads/${TH}/messages`, events: [
      sseEvents.toolCalls([{ name: 'Grep', args: { pattern: 'margin', path: 'data' }, id: 'g1' }]),
      { ...sseEvents.finishToolCalls(), delayAfter: 300 },
      // While the search runs, the model streams a todo update, which the
      // transcript never draws.
      toolChunks([{ index: 0, name: 'TodoWrite', id: 'todo1' }]),
      toolChunks([{ index: 0, args: todos.slice(0, 20) }]),
      { ...toolChunks([{ index: 0, args: todos.slice(20) }]), delayAfter: 600 },
      sseEvents.toolCalls([{ name: 'TodoWrite', args: JSON.parse(todos), id: 'todo1' }]),
      sseEvents.finishToolCalls(),
      { ...sseEvents.toolCallResult('todo1', 'Todos updated'), delayAfter: 300 },
      { ...sseEvents.toolCallResult('g1', 'data/a.csv:1'), delayAfter: 300 },
      sseEvents.messageChunk('Done.'),
      sseEvents.finishStop(),
      sseEvents.creditUsage(),
    ], delay: 30 });
    await send(page, 'Plan the work');
    await sampleFrames(page, () => ({
      rows: [...document.querySelectorAll('[data-testid="activity-live-zone"] .titem')].map((el) => el.textContent),
    }));
    await expect(page.getByText('Done.')).toBeVisible({ timeout: 10000 });
    const samples = await stopSampling(page);
    expect(samples.filter((s) => s.rows.length > 0).length, 'the sampler saw the live rows').toBeGreaterThan(30);
    const others = samples.flatMap((s) => s.rows).filter((text) => !text.includes('Grep'));
    expect([...new Set(others)], 'the hidden call drew a row').toEqual([]);
  });

  test('gives each parallel call its own row from its first chunk to its result', async ({ page }) => {
    await openThread(page);
    await configureSSE({ method: 'POST', path: `/api/v1/threads/${TH}/messages`, events: [
      // A sentence first, so the pair streams into a block already drawn; a
      // turn that opens on its call is the case after this one.
      sseEvents.messageChunk('Fetching both pages.'),
      toolChunks([{ index: 0, name: 'WebFetch', id: 'w1' }]),
      toolChunks([{ index: 0, args: '{"url": "https://one.example.com"}' }]),
      toolChunks([{ index: 1, name: 'WebFetch', id: 'w2' }]),
      { ...toolChunks([{ index: 1, args: '{"url": "https://two.example.com"}' }]), delayAfter: 500 },
      // One tool_calls event per call, as the producer sends a parallel pair.
      { ...sseEvents.toolCalls([{ name: 'WebFetch', args: { url: 'https://one.example.com' }, id: 'w1' }]), delayAfter: 500 },
      sseEvents.toolCalls([{ name: 'WebFetch', args: { url: 'https://two.example.com' }, id: 'w2' }]),
      { ...sseEvents.finishToolCalls(), delayAfter: 600 },
      sseEvents.toolCallResult('w1', 'Fetched'),
      { ...sseEvents.toolCallResult('w2', 'Fetched'), delayAfter: 300 },
      sseEvents.messageChunk('Both fetched.'),
      sseEvents.finishStop(),
      sseEvents.creditUsage(),
    ], delay: 30 });
    await send(page, 'Fetch both');
    await sampleFrames(page, () => {
      const zone = document.querySelector('[data-testid="activity-live-zone"]');
      const ids = window.__rowIds || (window.__rowIds = new WeakMap());
      window.__nextRowId = window.__nextRowId || 1;
      const rows = zone ? [...zone.children].map((el) => {
        if (!ids.has(el)) ids.set(el, window.__nextRowId++);
        return ids.get(el);
      }) : [];
      return { rows, running: zone ? zone.querySelectorAll('.titem.running').length : 0 };
    });
    await expect(page.getByText('Both fetched.')).toBeVisible({ timeout: 10000 });
    const samples = (await stopSampling(page)).filter((s) => s.rows.length > 0);
    const both = samples.findIndex((s) => s.rows.length === 2);
    expect(both, 'the second call never got its own row').toBeGreaterThan(-1);
    // Its own row from its first chunk, not only once it lands.
    expect(samples[both].running, 'the second row arrived only with its call').toBe(0);
    // From the moment both calls stream until they fold, the two rows are the
    // same two elements: no row leaves, none replaces another.
    const pair = samples[both].rows.join();
    expect(samples.slice(both).every((s) => s.rows.join() === pair)).toBe(true);
    expect(new Set(samples.flatMap((s) => s.rows)).size).toBe(2);
  });

  for (const layout of ['rows', 'status']) {
    test(`draws a call that opens the turn from its first chunk (${layout})`, async ({ page }) => {
      await openThread(page, { layout: layout === 'status' ? 'status' : undefined });
      await configureSSE({ method: 'POST', path: `/api/v1/threads/${TH}/messages`, events: [
        // No text and no thought ahead of it: the call is the bubble's first content.
        { ...toolChunks([{ index: 0, name: 'WebFetch', id: 'w1' }]), delayAfter: 600 },
        { ...toolChunks([{ index: 0, args: '{"url": "https://one.exa' }]), delayAfter: 600 },
        { ...toolChunks([{ index: 0, args: 'mple.com"}' }]), delayAfter: 600 },
        sseEvents.toolCalls([{ name: 'WebFetch', args: { url: 'https://one.example.com' }, id: 'w1' }]),
        { ...sseEvents.finishToolCalls(), delayAfter: 600 },
        { ...sseEvents.toolCallResult('w1', 'Fetched'), delayAfter: 300 },
        sseEvents.messageChunk('Fetched it.'),
        sseEvents.finishStop(),
        sseEvents.creditUsage(),
      ], delay: 30 });
      await send(page, 'Fetch one');
      await sampleFrames(page, () => {
        const zone = document.querySelector('[data-testid="activity-live-zone"]');
        const bubble = document.querySelector('[data-message-role="assistant"]');
        const ids = window.__rowIds || (window.__rowIds = new WeakMap());
        window.__nextRowId = window.__nextRowId || 1;
        // What holds the call: the status line, which a call that opens the
        // turn runs in the summary's place, or the call's own row.
        let row = bubble?.querySelector('[data-status-line]') ?? null;
        if (!row) {
          row = zone?.querySelector('.titem') ?? null;
          while (row && row.parentElement !== zone) row = row.parentElement;
        }
        if (row && !ids.has(row)) ids.set(row, window.__nextRowId++);
        return {
          row: row ? ids.get(row) : null,
          running: bubble ? bubble.querySelectorAll('.titem.running').length : 0,
          mark: !!document.querySelector('[data-testid="streaming-indicator"]'),
        };
      });
      await expect(page.getByText('Fetched it.')).toBeVisible({ timeout: 10000 });
      const samples = await stopSampling(page);
      const first = samples.findIndex((s) => s.row !== null);
      expect(first, 'the call never drew').toBeGreaterThan(-1);
      expect(samples[first].running, 'the call drew only once it landed').toBe(0);
      // Drawn from its first chunk: through the 1.8s the arguments stream,
      // not from some later one.
      const landed = samples.findIndex((s) => s.running > 0);
      expect(landed).toBeGreaterThan(first);
      expect(samples[landed].t - samples[first].t, 'the call drew late in its stream').toBeGreaterThan(1200);
      // One row, the same element, from its first chunk until its result.
      const lastRunning = samples.findLastIndex((s) => s.running > 0);
      expect(new Set(samples.slice(first, lastRunning + 1).map((s) => s.row))).toEqual(new Set([samples[first].row]));
      // The indicator stays up under it for the whole wait.
      expect(samples.slice(first, lastRunning + 1).every((s) => s.mark), 'the indicator left mid-turn').toBe(true);
    });
  }

  test('grows the summary with the inset that opens under it', async ({ page }) => {
    await openThread(page);
    await configureSSE({ method: 'POST', path: `/api/v1/threads/${TH}/messages`, events: [
      sseEvents.toolCalls([{ name: 'Grep', args: { pattern: 'margin', path: 'data' }, id: 'g1' }]),
      sseEvents.finishToolCalls(),
      // Long enough for the search to fold, leaving the summary with no row
      // under it, before the next call opens the inset.
      { ...sseEvents.toolCallResult('g1', 'data/a.csv:1'), delayAfter: 3500 },
      sseEvents.toolCalls([{ name: 'WebFetch', args: { url: 'https://one.example.com' }, id: 'w1' }]),
      { ...sseEvents.finishToolCalls(), delayAfter: 2500 },
      sseEvents.toolCallResult('w1', 'Fetched'),
      sseEvents.messageChunk('Done.'),
      sseEvents.finishStop(),
      sseEvents.creditUsage(),
    ], delay: 30 });
    await send(page, 'Search then fetch');
    // Sampling from the send, so the first sample with a summary is the frame
    // it arrived in. Started once a poll had found it, the sampler could open
    // as late as the next call, with the inset already opening.
    await sampleFrames(page, () => {
      let inner = document.querySelector('[id^="activity-summary-"]');
      while (inner && !(inner.style.paddingBottom && inner.parentElement?.style.overflow === 'hidden')) inner = inner.parentElement;
      const outer = inner?.parentElement;
      return {
        clipped: outer ? inner.getBoundingClientRect().height - outer.getBoundingClientRect().height : null,
        gap: inner ? parseFloat(getComputedStyle(inner).paddingBottom) : null,
      };
    });
    await expect(page.locator('[id^="activity-summary-"]')).toBeVisible({ timeout: 5000 });
    await expect(page.locator('.titem.running').filter({ hasText: 'Web Fetch' })).toBeVisible({ timeout: 5000 });
    await page.waitForTimeout(600);
    const samples = (await stopSampling(page)).filter((s) => s.clipped !== null);
    expect(samples[0].gap).toBe(0);
    expect(samples.at(-1).gap).toBe(8);
    // A frame inside the inset's tween, so an inset that jumped would not pass.
    // Any frame short of 8px, not a count of them in the steep part: the tween
    // eases out, past 7.5px about 60ms into its 160ms, and its clock starts at
    // the commit that opens it, so a slow commit spends most of that before
    // the first frame paints.
    expect(samples.some((s) => s.gap > 0 && s.gap < 8)).toBe(true);
    // From the inset's first frame on: before it, the summary's own entrance
    // clips it by design.
    const opening = samples.filter((s) => s.gap > 0);
    expect(Math.max(...opening.map((s) => s.clipped)), 'the summary was cut off over its growing inset').toBeLessThan(0.5);
  });

  test('ellipsizes the summary while its width changes', async ({ page }) => {
    await openThread(page);
    await configureSSE({ method: 'POST', path: `/api/v1/threads/${TH}/messages`, events: [
      sseEvents.toolCalls([{ name: 'Grep', args: { pattern: 'margin', path: 'data' }, id: 'g1' }]),
      sseEvents.finishToolCalls(),
      { ...sseEvents.toolCallResult('g1', 'data/a.csv:1'), delayAfter: 300 },
      sseEvents.toolCalls([{ name: 'WebFetch', args: { url: 'https://one.example.com' }, id: 'w1' }]),
      sseEvents.finishToolCalls(),
      { ...sseEvents.toolCallResult('w1', 'Fetched'), delayAfter: 3500 },
      sseEvents.messageChunk('Done.'),
      sseEvents.finishStop(),
      sseEvents.creditUsage(),
    ], delay: 30 });
    await send(page, 'Search then fetch');
    const summary = page.locator('[id^="activity-summary-"]');
    await sampleFrames(page, () => {
      const label = document.querySelector('[id^="activity-summary-"]')?.firstElementChild;
      if (!label) return { overflow: false, text: null };
      return {
        overflow: label.scrollWidth > label.clientWidth + 1,
        // An ellipsis is drawn only on the box's own inline text: a child box
        // that does not fit is clipped whole, with no "…".
        ellipsis: getComputedStyle(label).textOverflow === 'ellipsis' && label.children.length === 0,
        text: label.textContent,
      };
    });
    await expect(summary).toHaveText('Made 1 web call · ran 1 search', { timeout: 5000 });
    await page.waitForTimeout(400);
    const samples = await stopSampling(page);
    // The precondition: the summary was on screen with the shorter text first.
    expect(samples.some((s) => s.text === 'Ran 1 search')).toBe(true);
    const overflowing = samples.filter((s) => s.overflow);
    // The label eases out to the longer text, so for a few frames it is
    // narrower than what it says.
    expect(overflowing.length).toBeGreaterThan(2);
    expect(overflowing.every((s) => s.ellipsis), 'the summary was cut off with no ellipsis').toBe(true);
  });

  test('closes a diff in one measured tween, not its opening spring', async ({ page }) => {
    await openThread(page);
    await configureSSE({ method: 'POST', path: `/api/v1/threads/${TH}/messages`, events: [
      sseEvents.toolCalls([{ name: 'Edit', args: { file_path: 'results/report.md', old_string: 'Margin was flat.\nNo drift.', new_string: 'Margin drifted down.\nAbout 180 bp over four quarters.\nMix explains most of it.' }, id: 'e1' }]),
      sseEvents.finishToolCalls(),
      sseEvents.toolCallResult('e1', 'The file results/report.md has been updated.'),
      sseEvents.messageChunk('Updated the report.'),
      sseEvents.finishStop(),
      sseEvents.creditUsage(),
    ], delay: 30 });
    await send(page, 'Update the report');
    await page.locator('[data-turn-fold="collapsed"] button').click();
    const edit = page.locator('.titem').filter({ hasText: 'report.md' });
    await edit.locator('button[aria-expanded]').first().click();
    const diff = page.getByText('Mix explains most of it.');
    await expect(diff).toBeVisible();
    await page.waitForTimeout(800);
    await sampleFrames(page, () => {
      const panel = [...document.querySelectorAll('.titem')].find((el) => el.textContent.includes('report.md'))
        ?.querySelector('.titem-body > :last-child');
      return { h: panel ? panel.getBoundingClientRect().height : 0, styled: panel?.style.height ?? '' };
    });
    await edit.locator('button[aria-expanded]').first().click();
    await page.waitForTimeout(1000);
    const samples = await stopSampling(page);
    const from = samples.findIndex((s, i) => i > 0 && s.h < samples[0].h - 0.5);
    expect(from).toBeGreaterThan(-1);
    const closing = samples.slice(from - 1);
    // A spring would overshoot and come back; a tween only ever shrinks.
    for (let i = 1; i < closing.length; i++) expect(closing[i].h).toBeLessThanOrEqual(closing[i - 1].h + 0.01);
    expect(closing.at(-1).h).toBeLessThan(0.5);
    expect(closing.every((s) => s.styled !== 'auto')).toBe(true);
  });

  test('settles a tool row in one motion', async ({ page }) => {
    await openThread(page);
    await configureSSE({ method: 'POST', path: `/api/v1/threads/${TH}/messages`, events: [
      sseEvents.toolCalls([{ name: 'WebFetch', args: { url: 'https://one.example.com' }, id: 'w1' }]),
      // Lands and fails well inside its exposure window, so the row is still
      // live, and alone, for every sampled frame.
      { ...sseEvents.finishToolCalls(), delayAfter: 1000 },
      { ...sseEvents.toolCallResult('w1', 'Error: fetch failed'), delayAfter: 2500 },
      sseEvents.messageChunk('The fetch failed.'),
      sseEvents.finishStop(),
      sseEvents.creditUsage(),
    ], delay: 30 });
    await page.goto(`/chat/t/${TH}`);
    await page.locator('textarea').fill('Fetch it');
    // Sampling from before the send, so the window below is cut on the page's
    // own clock. Cut on when a poll for the row and then the badge happened
    // to resolve, it could open after the result had already landed, or run
    // on into the fold, where the summary arriving above moves the row.
    await sampleFrames(page, () => {
      const item = document.querySelector('[data-testid="activity-live-zone"] .titem');
      const cell = item?.querySelector('.titem-body > span');
      const layers = cell ? [...cell.children].map((el) => ({
        o: parseFloat(getComputedStyle(el).opacity), leaving: el.getAttribute('aria-hidden') === 'true',
      })) : [];
      const badge = item?.querySelector('.nrow-badge');
      const m = badge ? getComputedStyle(badge).transform : 'none';
      const scale = m === 'none' ? (badge ? 1 : null) : new DOMMatrixReadOnly(m).a;
      const bubble = document.querySelector('[data-message-role="assistant"]');
      const title = item?.classList.contains('running') ? item.querySelector('.titem-title > span') : null;
      return {
        layers, scale, top: item && bubble ? item.getBoundingClientRect().top - bubble.getBoundingClientRect().top : null,
        shimmer: title ? getComputedStyle(title).animationName : null,
      };
    });
    await page.getByRole('button', { name: 'Send message', exact: true }).click();
    await page.waitForFunction(() => {
      const badge = window.__samples.find((s) => s.scale !== null);
      return !!badge && window.__samples.at(-1).t > badge.t + 350;
    }, null, { timeout: 5000 });
    const sampled = await stopSampling(page);
    const entered = sampled.find((s) => s.top !== null);
    const badged = sampled.find((s) => s.scale !== null);
    // Past the row's entrance, which rises it into place, to 350ms after the
    // badge, which lands about 1s into the row's 1.8s exposure.
    const samples = sampled.filter((s) => s.top !== null && s.t > entered.t + 600 && s.t <= badged.t + 350);
    const swap = samples.findIndex((s) => s.layers.length === 2);
    expect(swap, 'the label did not crossfade').toBeGreaterThan(0);
    // The label crossfades in place: out over the exit, in over quick. Timed
    // on the frames that show a label partway faded, which only the fade's own
    // clock bounds: the curve is within 0.1% of its end from 80% of the way
    // in, so those frames fit inside the duration with no allowance for slow
    // ones. Timed from the swap's first frame, it also counted the frame the
    // fade waits to start on and the commit that unmounts the old label once
    // it is gone, so a few slow frames failed a fade that kept time.
    const fading = (leaving) => samples.filter((s) => s.layers.some((l) => l.leaving === leaving && l.o > 0.001 && l.o < 0.999));
    const span = (frames) => (frames.length ? frames.at(-1).t - frames[0].t : 0);
    expect(fading(true).length, 'the old label did not fade out').toBeGreaterThan(0);
    expect(fading(false).length, 'the new label did not fade in').toBeGreaterThan(0);
    expect(span(fading(true))).toBeLessThan(140);
    expect(span(fading(false))).toBeLessThan(160);
    // No drop: the row does not move as it settles.
    const tops = samples.map((s) => s.top);
    expect(Math.max(...tops) - Math.min(...tops)).toBeLessThan(0.5);
    // The badge grows from .6 and never past 1.
    const scales = samples.map((s) => s.scale).filter((v) => v !== null);
    expect(Math.min(...scales)).toBeLessThan(0.9);
    expect(Math.max(...scales)).toBeLessThanOrEqual(1.0001);
    // And the running title kept its shimmer the whole time it ran.
    const running = samples.filter((s) => s.shimmer !== null);
    expect(running.length).toBeGreaterThan(5);
    expect(running.every((s) => s.shimmer === 'shimmer-sweep'), 'the running title lost its shimmer').toBe(true);
  });
});
