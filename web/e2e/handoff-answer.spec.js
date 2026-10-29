/**
 * An analyst hand-off answered in front of the reader, frame by frame.
 *
 * The Chief of Staff proposes hand-offs side by side, each a card asking. An
 * answer changes the card in place: alone, it resolves into its slip or its
 * declined line; beside a slip or a group, it joins it, the card folding out
 * of the run as the stack takes it in. Whatever the case, the run's height
 * glides once, one way, from what it was to what it is, so nothing below it
 * jumps; and focus on the button pressed lands on what the card became. A
 * thread opened with the hand-off already answered draws it settled.
 */
import { configureSSE, resetMockServer, mockAPI, test, expect } from './fixtures.js';
import { sseEvents } from './helpers/mockResponses.js';
import { TH, chatViewOverrides, replayNothing } from './helpers/chatScenario.js';
import { sampleFrames, stopSampling } from './helpers/frameProbe.js';

const BRIEF = [
  'Rebuild the discounted cash flow model with the latest quarter, then explain what moved the per-share value',
  'since the last version, with a bridge from the old target and the three assumptions it is most sensitive to.',
].join(' ');

const HANDOFFS = [
  { id: 'int-a', call: 'toolu_a', workspace: 'NVDA Deep Dive' },
  { id: 'int-b', call: 'toolu_b', workspace: 'Semis Comps' },
  { id: 'int-c', call: 'toolu_c', workspace: 'Supply Chain' },
  { id: 'int-d', call: 'toolu_d', workspace: 'Macro Rates' },
];

/** The block key the transcript gives a hand-off, which its slip carries. */
const keyOf = (h) => `ptc-agent-${h.id}`;

const interrupt = (h, turnIndex = 0) => ({
  event: 'interrupt',
  data: {
    thread_id: TH,
    interrupt_id: h.id,
    finish_reason: 'interrupt',
    turn_index: turnIndex,
    action_requests: [{
      type: 'ptc_agent',
      workspace_id: `ws-${h.id}`,
      workspace_name: h.workspace,
      question: `${h.workspace}: ${BRIEF}`,
      report_back: true,
      tool_call_id: h.call,
    }],
  },
});

const dispatched = (h, turnIndex = 0) => sseEvents.toolCallResult(
  h.call,
  JSON.stringify({ success: true, thread_id: `th-${h.id}`, workspace_id: `ws-${h.id}`, run_id: `run-${h.id}`, report_back: true }),
  null,
  turnIndex,
);

/** The chat view on TH, with every handed-off run reading as just started. */
async function openChat(page) {
  await mockAPI(page, {
    ...chatViewOverrides(),
    'GET /threads/dispatches/liveness': { liveness: [], runs: [], steers: [] },
  });
}

/** What the run draws in one frame, read in the page by `sampleFrames`. */
const readRun = () => {
  const run = document.querySelector('[data-handoff-run]');
  if (!run) return { run: false };
  const message = run.closest('[data-message-role="assistant"]');
  const box = run.getBoundingClientRect();
  return {
    run: true,
    height: box.height,
    top: message ? box.top - message.getBoundingClientRect().top : null,
    cards: run.querySelectorAll('[data-handoff-approval]').length,
  };
};

/** Samples the run from just before `act` until the page has drawn `ms` of
 *  frames after it. */
async function framesAround(page, act, ms = 900) {
  await sampleFrames(page, readRun);
  // A frame drawn before the answer, so the first sample is where it was.
  await page.waitForFunction(() => window.__samples.length > 0);
  await act();
  await page.waitForFunction((after) => (window.__samples.at(-1)?.t ?? 0) > after, ms);
  return (await stopSampling(page)).filter((f) => f.run);
}

/**
 * Every answer here shrinks the run, so it glides on EASE_COLLAPSE for
 * DURATION.fold (src/lib/motion.ts): 240ms, at its steepest 2.86 times its
 * average pace, so never more than this share of the way in a millisecond.
 */
const PEAK_PACE = 2.857 / 240;

/**
 * How the run's height travelled: the whole way, how many frames drew it
 * between where it was and where it went, and the fastest it went, as a share
 * of the way per millisecond. A card swapped for its row by a cut has no frame
 * between; a glide is on its curve's clock, so a frame that comes late moves
 * it further but never faster.
 */
function travel(frames) {
  const heights = frames.map((f) => f.height);
  const steps = heights.slice(1).map((h, i) => h - heights[i]);
  const total = heights[0] - heights.at(-1);
  // The probe reads in an animation frame callback of its own, which can run
  // before the tween's and read the height it drew a frame earlier, so a step
  // took the longer of its own interval and the one before. Sampling starts
  // at 0, which bounds the first sample's.
  const times = [0, ...frames.map((f) => f.t)];
  const took = steps.map((_, i) => Math.max(times[i + 2] - times[i + 1], times[i + 1] - times[i]));
  return {
    total,
    between: heights.filter((h) => Math.abs(h - heights[0]) > 0.5 && Math.abs(h - heights.at(-1)) > 0.5).length,
    pace: Math.max(0, ...steps.map((s, i) => Math.abs(s / total) / took[i])),
    // Against the way it goes: a box that grew back after it shrank.
    back: Math.max(0, ...steps.map((s) => (total > 0 ? s : -s))),
    tops: frames.map((f) => f.top),
  };
}

function expectOneGlide(frames, label) {
  const t = travel(frames);
  console.log(`${label}: ${t.total.toFixed(1)}px with ${t.between} frames between, at most ${(t.pace / PEAK_PACE).toFixed(2)}x the curve's steepest pace, back ${t.back.toFixed(2)}px`);
  // Something moved, the whole way, in one direction.
  expect(Math.abs(t.total)).toBeGreaterThan(20);
  expect(t.back).toBeLessThan(0.5);
  // Drawn on its way, and never faster than its curve. Paced per millisecond
  // rather than per frame: a busy page draws fewer frames, each further along.
  // The headroom is the tween reading its clock later in the frame than the
  // probe stamps it.
  expect(t.between).toBeGreaterThanOrEqual(1);
  expect(t.pace).toBeLessThan(PEAK_PACE * 1.5);
  // The run itself holds its place; only its height moves.
  expect(Math.max(...t.tops) - Math.min(...t.tops)).toBeLessThan(0.5);
}

/**
 * Opens TH, sends, and waits for a card per hand-off in `asked`. The resume
 * goes out once every card is answered; it reports the dispatches well after
 * the last answer has settled, so nothing it brings moves the run meanwhile.
 */
async function askFor(page, asked) {
  await openChat(page);
  await replayNothing();
  await configureSSE({
    method: 'POST',
    path: `/api/v1/threads/${TH}/messages`,
    once: true,
    events: [
      sseEvents.messageChunk('I split the semis question. Each part needs your go-ahead.'),
      ...asked.map((h) => interrupt(h)),
    ],
    delay: 30,
  });

  await page.goto(`/chat/t/${TH}`);
  await page.locator('textarea').fill('Split the semis question');
  await page.getByRole('button', { name: 'Send message', exact: true }).click();
  await expect(page.locator('[data-handoff-approval]')).toHaveCount(asked.length, { timeout: 10000 });
  // The cards have entered and the turn has settled around them.
  await page.waitForTimeout(800);

  await configureSSE({
    method: 'POST',
    path: `/api/v1/threads/${TH}/messages`,
    events: [
      { ...sseEvents.keepalive(), delayAfter: 2500 },
      ...asked.map((h) => dispatched(h)),
      sseEvents.messageChunk('They are on it.'),
      sseEvents.finishStop(),
      sseEvents.creditUsage(),
    ],
    delay: 30,
  });
}

/** Which hand-off the focused element belongs to, as what it became. */
const focusedMember = (page) => page.evaluate(() => {
  const active = document.activeElement;
  if (!active || active === document.body) return 'body';
  return active.closest('[data-handoff-member], [data-handoff-members]')?.getAttribute('data-handoff-member') ?? 'elsewhere';
});

/** The card asking for `h`, found by the workspace its brief names. */
const card = (page, h) => page.locator('[data-handoff-approval]').filter({ hasText: `${h.workspace}:` });

/** Answers `h` from the keyboard, as `name` on its card. */
const press = (page, h, name) => async () => {
  await card(page, h).getByRole('button', { name, exact: true }).focus();
  await page.keyboard.press('Enter');
};

test.describe('an analyst hand-off answered', () => {
  test.beforeEach(async () => {
    await resetMockServer();
  });

  test('resolves in place, alone, joining a slip, joining a group and declined, and nothing below jumps', async ({ page }) => {
    await askFor(page, HANDOFFS);
    const [a, b, c, d] = HANDOFFS;

    // Alone: its neighbour still asks, so it resolves into a slip of its own.
    expectOneGlide(await framesAround(page, press(page, a, 'Hand off')), 'alone, into its slip');
    await expect(page.locator(`[data-handoff-member="${keyOf(a)}"]`)).toHaveCount(1);
    expect(await focusedMember(page)).toBe(keyOf(a));

    // Beside that slip: the card folds out and the slip becomes a group of two.
    expectOneGlide(await framesAround(page, press(page, b, 'Hand off')), 'joining a slip, into a group');
    await expect(page.getByRole('button', { name: 'Handed off to 2 analysts' })).toBeVisible();
    expect(await focusedMember(page)).toBe(keyOf(b));

    // Declined, beside a card still asking: its declined line, in place.
    expectOneGlide(await framesAround(page, press(page, d, 'Decline')), 'declined, into its line');
    await expect(page.locator(`[data-handoff-member="${keyOf(d)}"]`)).toContainText('Hand-off declined');
    expect(await focusedMember(page)).toBe(keyOf(d));

    // Beside the group: the card folds out and the group takes it in as a row.
    expectOneGlide(await framesAround(page, press(page, c, 'Hand off')), 'joining a group, as a row');
    await expect(page.getByRole('button', { name: 'Handed off to 3 analysts' })).toBeVisible();
    expect(await focusedMember(page)).toBe(keyOf(c));
    await expect(page.locator('[data-handoff-approval]')).toHaveCount(0);
  });

  test('joins the slips on either side into one group, and nothing below jumps', async ({ page }) => {
    const asked = HANDOFFS.slice(0, 3);
    await askFor(page, asked);
    const [a, b, c] = asked;

    expectOneGlide(await framesAround(page, press(page, a, 'Hand off')), 'first, into its slip');
    expectOneGlide(await framesAround(page, press(page, c, 'Hand off')), 'last, into its slip');
    // Between the two: its card and the slip after it both fold out of the run
    // as the first slip becomes a group of all three.
    expectOneGlide(await framesAround(page, press(page, b, 'Hand off')), 'between two slips, into one group');
    await expect(page.getByRole('button', { name: 'Handed off to 3 analysts' })).toBeVisible();
    await expect(page.locator('[data-handoff-run] > *')).toHaveCount(1);
    expect(await focusedMember(page)).toBe(keyOf(b));
  });

  test('a thread opened with its hand-off answered draws it settled, with no motion', async ({ page }) => {
    const [a] = HANDOFFS;
    await openChat(page);
    await configureSSE({
      method: 'GET',
      path: `/api/v1/threads/${TH}/messages/replay`,
      events: [
        sseEvents.userMessage('Hand the NVDA refresh to its analyst', 0),
        sseEvents.messageChunk('Handing this to the NVDA analyst.', 'text', 0),
        interrupt(a, 0),
        {
          event: 'user_message',
          data: {
            thread_id: TH,
            turn_index: 1,
            content: '',
            timestamp: new Date().toISOString(),
            metadata: { msg_type: 'ptc', hitl_interrupt_ids: [a.id] },
          },
        },
        dispatched(a, 1),
        sseEvents.messageChunk('The analyst has it.', 'text', 1),
        sseEvents.finishStop(1),
        sseEvents.replayDone(),
      ],
      delay: 10,
    });

    // Every frame from the document's first, before the transcript draws.
    await page.addInitScript((source) => {
      const read = new Function(`return (${source})();`);
      window.__samples = [];
      const tick = () => {
        const run = document.querySelector('[data-handoff-run]');
        if (run) {
          const slip = run.querySelector('[data-handoff-member]');
          let opacity = 1;
          for (let el = slip; el && el !== run; el = el.parentElement) opacity *= Number(getComputedStyle(el).opacity);
          window.__samples.push({ ...read(), opacity, slip: !!slip });
        }
        requestAnimationFrame(tick);
      };
      requestAnimationFrame(tick);
    }, readRun.toString());

    await page.goto(`/chat/t/${TH}`);
    await expect(page.locator(`[data-handoff-member="${keyOf(a)}"]`)).toBeVisible({ timeout: 10000 });
    await page.waitForFunction(() => window.__samples.length > 40);
    const frames = await page.evaluate(() => window.__samples);

    console.log(`settled run drawn over ${frames.length} frames, heights ${[...new Set(frames.map((f) => f.height.toFixed(1)))].join(', ')}`);
    // Never a card asking, and the slip whole and opaque from its first frame.
    expect(frames.every((f) => f.cards === 0)).toBe(true);
    expect(frames.every((f) => f.slip && f.opacity === 1)).toBe(true);
    expect(Math.max(...frames.map((f) => f.height)) - Math.min(...frames.map((f) => f.height))).toBeLessThan(0.5);
  });
});
