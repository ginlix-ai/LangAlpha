/**
 * How a turn begins and ends in front of the reader.
 *
 * At a send the fold row is already in place, in its running state, so nothing
 * is inserted above the reply later. At the end the turn's work folds away only
 * once the reply has finished typing, and what the ending brings arrives after
 * that, in order: the fold's settled label, then the Sources pill, then the
 * deliverables deck, the last two once the work has finished folding.
 */
import { configureSSE, resetMockServer, mockAPI, test, expect } from './fixtures.js';
import { sseEvents } from './helpers/mockResponses.js';
import { TH, chatViewOverrides, openThread } from './helpers/chatScenario.js';
import { sampleFrames, stopSampling } from './helpers/frameProbe.js';

const provenance = (id, url) => ({
  event: 'provenance',
  data: {
    thread_id: TH,
    agent: 'tools',
    record_id: `rec-${id}`,
    timestamp: new Date().toISOString(),
    source_type: 'web_fetch',
    identifier: url,
    title: url,
    tool_call_id: id,
  },
});

// Long enough that the typewriter is still at it when the stream ends, which
// comes right after its last chunk; short of the typewriter's catch-up size
// (CATCH_UP_CHARS), past which it would show most of it at once.
const SENTENCE = 'The comparison is done and the report is written. ';
const ANSWER = SENTENCE.repeat(10).trim();
const answerChunks = () => Array.from({ length: 5 }, (_, i) => sseEvents.messageChunk(SENTENCE.repeat(2).slice(0, i === 4 ? -1 : undefined)));

// Commentary screens tall, so the process takes longer to fold than the
// ending's own delays: what arrives after it has to wait for the fold itself.
const PARAGRAPH = 'Before writing anything I am going to read the filings, the transcripts and the press releases, line them up quarter by quarter, and check each number against the one the company reported the year before. ';
const COMMENTARY = Array.from({ length: 12 }, () => PARAGRAPH.repeat(2).trim()).join('\n\n') + '\n\n';

/** What the transcript draws in one frame, read in the page by `sampleFrames`
 *  from the send on. `typed` is how much of the paragraph opening with
 *  `answerStart` is drawn. */
const readTurn = (answerStart) => {
  const opacity = (el) => (el ? Number(getComputedStyle(el).opacity) : null);
  const user = document.querySelector('[data-message-role="user"]');
  const assistant = document.querySelector('[data-message-role="assistant"]');
  const answer = answerStart
    ? [...(assistant?.querySelectorAll('p') ?? [])].find((p) => p.textContent?.startsWith(answerStart))
    : null;
  const fold = document.querySelector('[data-turn-fold]');
  const folding = [...(assistant?.querySelectorAll('[data-fold-panel]') ?? [])]
    .filter((panel) => !panel.hidden && panel.getBoundingClientRect().height > 0.5);
  const label = fold?.querySelector('button')?.parentElement;
  const indicator = document.querySelector('[data-testid="streaming-indicator"]');
  const deck = document.querySelector('[data-testid="turn-files"]');
  const pill = [...document.querySelectorAll('[data-message-role="assistant"] button')]
    .find((b) => /source/i.test(b.textContent ?? ''))?.parentElement;
  const userBottom = user?.getBoundingClientRect().bottom;
  const column = user?.firstElementChild;
  const firstContent = assistant?.querySelector('p, .titem, [data-activity-state]');
  return {
    user: !!user,
    rising: !!column && column.getAnimations().length > 0,
    fold: fold?.getAttribute('data-turn-fold') ?? null,
    folding: folding.length,
    foldTop: fold && userBottom !== undefined ? fold.getBoundingClientRect().top - userBottom : null,
    indicatorTop: indicator && userBottom !== undefined ? indicator.getBoundingClientRect().top - userBottom : null,
    typed: answer?.textContent?.length ?? 0,
    content: !!firstContent,
    contentTop: firstContent && userBottom !== undefined ? firstContent.getBoundingClientRect().top - userBottom : null,
    label: opacity(label),
    // Gone once its row has closed: the node leaves a frame after its exit
    // ends, which is React's bookkeeping, not anything the reader sees.
    indicator: !!indicator && indicator.getBoundingClientRect().height > 0.5,
    pill: opacity(pill),
    deck: opacity(deck),
  };
};

const firstAt = (frames, test) => frames.find(test)?.t ?? null;

test.describe('turn start and end', () => {
  test.beforeEach(async () => {
    await resetMockServer();
  });

  test('has its fold row from the first frame of a send, and nothing above the reply moves', async ({ page }) => {
    await openThread(page);
    await configureSSE({ method: 'POST', path: `/api/v1/threads/${TH}/messages`, events: [
      // A quiet first second, so the frames before any content are many.
      { ...sseEvents.keepalive(), delayAfter: 1200 },
      sseEvents.toolCalls([{ name: 'bash', args: { command: 'echo hi' }, id: 'toolu_hi' }]),
      // The call stays running past the window the landing is measured over.
      { ...sseEvents.finishToolCalls(), delayAfter: 1500 },
      sseEvents.toolCallResult('toolu_hi', 'hi'),
      sseEvents.messageChunk('Said hi.'),
      sseEvents.finishStop(),
      sseEvents.creditUsage(),
    ], delay: 30 });

    await page.goto(`/chat/t/${TH}`);
    await page.locator('textarea').fill('Say hi');
    await sampleFrames(page, readTurn);
    await page.getByRole('button', { name: 'Send message', exact: true }).click();
    await expect(page.locator('[data-turn-fold="collapsed"]')).toBeVisible({ timeout: 10000 });
    const frames = await stopSampling(page);

    const withUser = frames.filter((f) => f.user);
    expect(withUser.length).toBeGreaterThan(10);
    // The user's bubble rises into place as it arrives: read from the frames,
    // since the rise can be over before a poll after the click first looks.
    expect(withUser.some((f) => f.rising), 'the message did not rise in').toBe(true);
    // The first frame that draws the message draws its row, running.
    expect(withUser[0].fold).toBe('live');
    // Until the reply has content, the row and the indicator under it hold
    // their place below the message to the pixel.
    const beforeContent = withUser.filter((f) => f.indicatorTop !== null && !f.content);
    expect(beforeContent.length).toBeGreaterThan(10);
    const tops = beforeContent.map((f) => f.indicatorTop);
    const foldTops = beforeContent.map((f) => f.foldTop);
    console.log(`indicator travel before content: ${Math.max(...tops) - Math.min(...tops)}px, fold row travel: ${Math.max(...foldTops) - Math.min(...foldTops)}px`);
    expect(Math.max(...tops) - Math.min(...tops)).toBeLessThan(0.5);
    expect(Math.max(...foldTops) - Math.min(...foldTops)).toBeLessThan(0.5);
    // When the work lands it rises into place where the indicator stood,
    // and stays: it only ever rises, so no row arrives above it and pushes it
    // down.
    const landed = withUser.find((f) => f.contentTop !== null);
    const settling = withUser.filter((f) => f.contentTop !== null && f.t < landed.t + 600);
    const rested = settling.filter((f) => f.t > landed.t + 400).map((f) => f.contentTop);
    console.log(`first content lands ${landed.contentTop - beforeContent.at(-1).indicatorTop}px from the indicator, then rests within ${Math.max(...rested) - Math.min(...rested)}px`);
    for (let i = 1; i < settling.length; i++) {
      expect(settling[i].contentTop, 'the first content was pushed down').toBeLessThanOrEqual(settling[i - 1].contentTop + 0.1);
    }
    expect(rested.length).toBeGreaterThan(5);
    expect(Math.max(...rested) - Math.min(...rested)).toBeLessThan(0.5);
  });

  for (const tall of [false, true]) {
    test(`folds once the reply has typed out, then brings the label, the Sources pill and the deck in order${tall ? ', after a tall process' : ''}`, async ({ page }) => {
      await openThread(page);
      await configureSSE({ method: 'POST', path: `/api/v1/threads/${TH}/messages`, events: [
        ...(tall ? [sseEvents.messageChunk(COMMENTARY), { ...sseEvents.finishToolCalls(), delayAfter: 300 }] : []),
        sseEvents.toolCalls([
          { name: 'WebFetch', args: { url: 'https://example.com/a' }, id: 'toolu_fetch' },
          { name: 'Write', args: { file_path: 'results/report.md', content: '# Report' }, id: 'toolu_write' },
        ]),
        sseEvents.finishToolCalls(),
        provenance('toolu_fetch', 'https://example.com/a'),
        sseEvents.toolCallResult('toolu_fetch', 'Fetched'),
        { ...sseEvents.toolCallResult('toolu_write', 'Wrote results/report.md'), delayAfter: 2200 },
        ...answerChunks(),
        sseEvents.finishStop(),
        sseEvents.creditUsage(),
      ], delay: 30 });

      await page.goto(`/chat/t/${TH}`);
      await page.locator('textarea').fill('Compare and write it up');
      await sampleFrames(page, readTurn, ANSWER.slice(0, 20));
      await page.getByRole('button', { name: 'Send message', exact: true }).click();
      await expect(page.getByTestId('turn-files')).toBeVisible({ timeout: 15000 });
      await page.waitForTimeout(800);
      const frames = await stopSampling(page);

      const typedOut = firstAt(frames, (f) => f.typed >= ANSWER.length);
      const collapsed = firstAt(frames, (f) => f.fold === 'collapsed');
      const indicatorGone = firstAt(frames.filter((f) => f.t > (frames.find((g) => g.indicator)?.t ?? 0)), (f) => !f.indicator);
      const label = firstAt(frames, (f) => (f.label ?? 0) > 0.05);
      const pill = firstAt(frames, (f) => (f.pill ?? 0) > 0.05);
      const deck = firstAt(frames, (f) => (f.deck ?? 0) > 0.05);
      const typingStarted = firstAt(frames, (f) => f.typed > 0);
      const folded = firstAt(frames, (f) => collapsed !== null && f.t > collapsed && f.folding === 0);
      console.log(JSON.stringify({ typingStarted, typedOut, collapsed, folded, indicatorGone, label, pill, deck }));

      for (const t of [typedOut, collapsed, label, pill, deck]) expect(t).not.toBeNull();
      // The work folds away no earlier than the frame the reply is whole.
      expect(collapsed).toBeGreaterThanOrEqual(typedOut - 1);
      // What leaves goes first: the running label and the indicator, over the
      // exit duration. Then what arrives, each after the one before.
      expect(label).toBeGreaterThan(collapsed + 100);
      expect(pill).toBeGreaterThan(label);
      expect(deck).toBeGreaterThan(pill);
      expect(indicatorGone).toBeLessThanOrEqual(label);
      // The pill and the deck wait for the fold that actually runs, however
      // long the process takes to close: neither lands over it. A frame's
      // grace, since the fold's last tick and the pill's first can share one.
      expect(folded).not.toBeNull();
      const frameOf = (t) => frames.findIndex((f) => f.t === t);
      expect(frameOf(pill)).toBeGreaterThanOrEqual(frameOf(folded) - 1);
      expect(frameOf(deck)).toBeGreaterThan(frameOf(folded));
    });
  }

  test('draws a turn opened from history settled from its first frame', async ({ page }) => {
    // A finished turn with work to fold, replayed an event at a time, as a
    // real response arrives: drawn as it went, the turn showed its work open
    // above no answer, then folded it away once the answer and its end landed.
    const asked = Date.now() - 600_000;
    const replay = [
      { ...sseEvents.userMessage('How did the indices close?'), data: {
        ...sseEvents.userMessage('How did the indices close?').data,
        timestamp: new Date(asked).toISOString(),
        run_id: 'run-hist-0',
        run_completed_at: new Date(asked + 7_500).toISOString(),
      } },
      sseEvents.messageChunk('Pulling the closing levels first.'),
      sseEvents.toolCalls([{ name: 'bash', args: { command: 'echo levels' }, id: 'toolu_levels' }]),
      sseEvents.finishToolCalls(),
      sseEvents.toolCallResult('toolu_levels', 'levels'),
      sseEvents.messageChunk('All four closed a touch lower, with energy leading the decline.'),
      sseEvents.finishStop(),
      sseEvents.creditUsage(),
      sseEvents.replayDone(),
    ];
    await mockAPI(page, chatViewOverrides());
    await configureSSE({ method: 'GET', path: `/api/v1/threads/${TH}/messages/replay`, events: replay, delay: 60 });
    await page.addInitScript(() => {
      const frames = [];
      window.historyFrames = frames;
      const sample = () => {
        const assistant = document.querySelector('[data-message-role="assistant"]');
        if (assistant) {
          const fold = document.querySelector('[data-turn-fold]');
          const rows = [...assistant.querySelectorAll('.titem')]
            .filter((row) => row.getBoundingClientRect().height > 0.5 && !row.closest('[hidden]'));
          frames.push({
            fold: fold?.getAttribute('data-turn-fold') ?? null,
            label: fold?.textContent ?? null,
            rows: rows.length,
          });
        }
        requestAnimationFrame(sample);
      };
      requestAnimationFrame(sample);
    });

    await page.goto(`/chat/t/${TH}`);
    await expect(page.locator('[data-turn-fold="collapsed"]')).toBeVisible({ timeout: 10000 });
    await page.waitForTimeout(400);
    const frames = await page.evaluate(() => window.historyFrames);

    expect(frames.length).toBeGreaterThan(5);
    // Every frame that draws the turn draws it folded, under its full label,
    // with none of its work showing.
    const unsettled = frames.filter((f) => f.fold !== 'collapsed' || !/Worked for/.test(f.label ?? '') || f.rows > 0);
    console.log(`frames drawing the turn: ${frames.length}, unsettled: ${unsettled.length}`, JSON.stringify(unsettled.slice(0, 3)));
    expect(unsettled).toEqual([]);
  });
});
