/**
 * Switching a thread's model after its provider compacted it: the server
 * answers the save with `summary_handoff: true`, and the composer says the next
 * message may start slowly. Only a real page shows whether the answer reaches
 * the toast, since the flag is read off the PATCH response and not off the
 * thread row the rest of the view renders.
 */
import { test, expect, mockAPI, configureSSE } from './fixtures.js';
import { defaultResponses, sampleThread, sseEvents } from './helpers/mockResponses.js';
import { chatViewOverrides, TH } from './helpers/chatScenario.js';

const FROM = 'gpt-handoff-from';
const TO = 'gpt-handoff-to';
const TO_NAME = 'Handoff Target';

const prefs = defaultResponses['GET /users/me/preferences'];

const models = {
  // Starred, so the picker lists it first.
  'GET /users/me/preferences': {
    ...prefs,
    other_preference: { ...prefs.other_preference, starred_models: [TO] },
  },
  'GET /models': {
    model_metadata: {
      [FROM]: { display_name: 'Handoff Source', provider: 'openai' },
      [TO]: { display_name: TO_NAME, provider: 'openai' },
    },
  },
};

/** The thread PATCH, answered as the server would: the row with the new model,
 *  plus whatever the test says about the hand-off. Captures each body. */
function threadPatch(handoff) {
  const bodies = [];
  const route = (r) => {
    const body = JSON.parse(r.request().postData() ?? '{}');
    bodies.push(body);
    return r.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        ...sampleThread({ llm_model: body.llm_model }),
        ...handoff,
      }),
    });
  };
  return { bodies, route };
}

async function openThread(page, patch) {
  await mockAPI(page, {
    ...chatViewOverrides({ threads: [sampleThread({ llm_model: FROM })] }),
    ...models,
    [`PATCH /threads/${TH}`]: patch.route,
  });
  await configureSSE({
    method: 'GET',
    path: `/api/v1/threads/${TH}/messages/replay`,
    events: [sseEvents.replayDone()],
    delay: 10,
  });
  await page.goto(`/chat/t/${TH}`);
  const trigger = page.locator('button[data-pill="model"]');
  await expect(trigger).toBeVisible({ timeout: 10000 });
  await expect(trigger).toContainText('Handoff Source');
  return trigger;
}

async function pickTarget(page, trigger) {
  await trigger.click();
  // A favorite, so it is on the tab the picker opens on.
  await page.getByRole('dialog').getByRole('row').filter({ hasText: TO_NAME }).click();
}

test.describe('switching a thread model', () => {
  test('a save that answers summary_handoff shows the hand-off toast', async ({ page }) => {
    const patch = threadPatch({ summary_handoff: true });
    const trigger = await openThread(page, patch);
    await pickTarget(page, trigger);

    const toast = page.locator('[role="status"]').filter({ hasText: 'writing it out' });
    await expect(toast).toBeVisible();
    await expect(toast).toContainText(TO_NAME);
    expect(patch.bodies).toEqual([{ llm_model: TO }]);
  });

  for (const [label, handoff] of [['false', { summary_handoff: false }], ['absent', {}]]) {
    test(`a save that answers summary_handoff ${label} shows no toast`, async ({ page }) => {
      const patch = threadPatch(handoff);
      const trigger = await openThread(page, patch);
      const answered = page.waitForResponse((r) => r.request().method() === 'PATCH');
      await pickTarget(page, trigger);

      // The absence of a toast is evidence only once the answer has come and
      // been rendered, which takes the response and then a frame or two.
      await answered;
      await page.evaluate(() => new Promise((done) => {
        requestAnimationFrame(() => requestAnimationFrame(done));
      }));
      expect(patch.bodies).toEqual([{ llm_model: TO }]);
      await expect(trigger).toContainText(TO_NAME);
      await expect(page.getByText('writing it out')).toHaveCount(0);
    });
  }
});
