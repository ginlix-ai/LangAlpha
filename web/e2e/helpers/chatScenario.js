/**
 * The one chat scenario every streaming spec mocks: workspace WS holding
 * thread TH, with one completed turn behind it, and the steps that open it and
 * send into it.
 */
import { configureSSE, mockAPI } from '../fixtures.js';
import { sampleWorkspace, sampleThread, defaultResponses, sseEvents } from './mockResponses.js';

export const WS = 'a0000001-0000-4000-8000-000000000001';
export const TH = 'b0000001-0000-4000-8000-000000000001';

/**
 * REST overrides for the chat view on TH. `workspaces` and `threads` default
 * to one of each and their first entry answers the by-id routes; a function
 * passed as `threadStatus` is used as a route handler, which is how a spec
 * makes the thread reconnectable partway through.
 */
export function chatViewOverrides({ workspaces, threads, threadStatus } = {}) {
  const ws = workspaces ?? [sampleWorkspace()];
  const th = threads ?? [sampleThread()];
  return {
    'GET /workspaces': { workspaces: ws, total: ws.length, limit: 20, offset: 0 },
    [`GET /workspaces/${WS}`]: ws[0],
    'GET /threads': { threads: th, total: th.length },
    [`GET /threads/${TH}`]: th[0],
    [`GET /threads/${TH}/status`]: threadStatus ?? { can_reconnect: false, status: 'idle' },
    [`GET /threads/${TH}/turns`]: {
      thread_id: TH,
      turns: [{ turn_index: 0, edit_checkpoint_id: 'cp-edit-0', regenerate_checkpoint_id: 'cp-regen-0' }],
      retry_checkpoint_id: 'cp-retry-0',
    },
    [`GET /workspaces/${WS}/files`]: { files: [] },
  };
}

/** Answers the thread's replay with nothing to catch up on, as a thread that
 *  is not running does when it opens. */
export const replayNothing = () => configureSSE({
  method: 'GET',
  path: `/api/v1/threads/${TH}/messages/replay`,
  events: [sseEvents.replayDone()],
  delay: 10,
});

/**
 * Mocks the chat view on TH with nothing to replay, ready for a send. `layout`
 * sets the account's `tool_call_layout` preference; left out, the default
 * preferences apply.
 */
export async function openThread(page, { layout } = {}) {
  const prefs = defaultResponses['GET /users/me/preferences'];
  await mockAPI(page, {
    ...chatViewOverrides(),
    'GET /users/me/preferences': layout
      ? { ...prefs, other_preference: { ...prefs.other_preference, tool_call_layout: layout } }
      : prefs,
  });
  await replayNothing();
}

/** Opens TH and sends `text` from the composer. */
export async function send(page, text) {
  await page.goto(`/chat/t/${TH}`);
  await page.locator('textarea').fill(text);
  await page.getByRole('button', { name: 'Send message', exact: true }).click();
}
