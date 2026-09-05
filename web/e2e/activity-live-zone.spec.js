/**
 * The activity live zone must size to its rows after a fold is interrupted.
 *
 * Three tool rows complete together, fold into the accordion, and a fourth
 * call lands while the live zone is still collapsing. The zone must end up
 * as tall as the one row it now holds, not the three it held before.
 */
import { configureSSE, resetMockServer, mockAPI, test, expect } from './fixtures.js';
import { sseEvents } from './helpers/mockResponses.js';
import { TH, chatViewOverrides } from './helpers/chatScenario.js';
import { MIN_LIVE_EXPOSURE_MS } from '../src/pages/ChatAgent/components/messageList/liveZoneTiming.ts';

// A completed row stays live for MIN_LIVE_EXPOSURE_MS after its call was
// created, then folds; the zone's collapse runs about 180 ms more, so a call
// landing 150 ms past the fold lands inside that collapse. The clock starts
// at the first event, and the four events before the delay each cost a gap,
// so the delay is measured from the first event, not from the last result.
// The tool must not be an inline-artifact tool (those skip the exposure
// window).
const EVENT_GAP_MS = 30;
const LAND_MID_COLLAPSE_MS = MIN_LIVE_EXPOSURE_MS + 150 - 4 * EVENT_GAP_MS;

test.describe('activity live zone', () => {
  test.beforeEach(async () => {
    await resetMockServer();
  });

  test('sizes to its rows after a call lands mid-fold', async ({ page }) => {
    await mockAPI(page, chatViewOverrides());
    await configureSSE({
      method: 'GET',
      path: `/api/v1/threads/${TH}/messages/replay`,
      events: [sseEvents.replayDone()],
      delay: 10,
    });

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
      return { active: document.querySelectorAll('[data-testid="activity-live-zone"] .nrow.state-active').length };
    }, null, { timeout: 15000 }).then((h) => h.jsonValue());
    expect(atFold.active, 'the fourth call went live before the first three folded').toBe(0);

    const zone = page.getByTestId('activity-live-zone');
    await expect(zone.locator('.nrow.state-active')).toHaveCount(1, { timeout: 15000 });
    // Let the zone's own animations settle before measuring.
    await page.waitForTimeout(1000);

    const { zoneHeight, rowsHeight } = await zone.evaluate((el) => ({
      zoneHeight: el.getBoundingClientRect().height,
      rowsHeight: Array.from(el.children).reduce((sum, c) => sum + c.getBoundingClientRect().height, 0),
    }));
    expect(rowsHeight).toBeGreaterThan(0);
    expect(zoneHeight).toBeLessThanOrEqual(rowsHeight + 2);
  });
});
