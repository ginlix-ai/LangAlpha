/**
 * The one chat scenario every streaming spec mocks: workspace WS holding
 * thread TH, with one completed turn behind it.
 */
import { sampleWorkspace, sampleThread } from './mockResponses.js';

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
