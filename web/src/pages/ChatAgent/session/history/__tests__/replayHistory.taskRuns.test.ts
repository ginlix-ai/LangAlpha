// @vitest-environment node
/**
 * Each replayed task launch carries the run it started, as the ledger kept
 * it: its bounds and, when it was not its launcher's, its model. A resumed
 * task has one row per launch over one task, so each launch's facts are
 * filed under its run, linked to the Task call that started it.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const api = vi.hoisted(() => ({ replayThreadHistory: vi.fn() }));

vi.mock('../../../utils/api', () => ({
  replayThreadHistory: api.replayThreadHistory,
}));

import { loadConversationHistory } from '../replayHistory';
import { loadOlderHistoryPage } from '../olderPages';
import { buildRuntime, makeDeps } from './replayHarness';

const USER_TURN = { event: 'user_message', thread_id: 'thread-1', turn_index: 0, content: 'Read the 10-K' };
const GLM = { id: 'glm-5.3-cn', provider: 'z-ai-cn' };

const taskArtifact = (tool_call_id: string, payload: Record<string, unknown>) => ({
  event: 'artifact',
  artifact_type: 'task',
  artifact_id: `art-${tool_call_id}`,
  tool_call_id,
  payload: { task_id: 'T1', type: 'research', description: 'Read the 10-K', ...payload },
});

function replaying(items: Array<Record<string, unknown>>) {
  api.replayThreadHistory.mockImplementationOnce(
    async (_threadId: string, onEvent: (e: Record<string, unknown>) => void) => items.forEach(onEvent),
  );
}

async function runsAfter(items: Array<Record<string, unknown>>) {
  const { rt } = buildRuntime();
  replaying(items);
  await loadConversationHistory(rt, makeDeps());
  return Object.fromEntries(rt.subagentHistory.get().runs);
}

beforeEach(() => vi.clearAllMocks());

describe('history replay: each launch\'s run', () => {
  it('files every launch\'s run under its run id, linked to its Task call', async () => {
    const runs = await runsAfter([
      USER_TURN,
      taskArtifact('launch-1', {
        action: 'spawned',
        status: 'completed',
        task_run_id: 'run-1',
        started_ms: 1_000,
        finished_ms: 3_000,
        model: GLM,
      }),
      // On its launcher's model: the payload names none.
      taskArtifact('launch-2', { action: 'resumed', status: 'running', task_run_id: 'run-2', started_ms: 5_000 }),
    ]);
    expect(runs).toEqual({
      'run-1': { taskId: 'task:T1', launchToolCallId: 'launch-1', startedAtMs: 1_000, finishedAtMs: 3_000, model: GLM },
      'run-2': { taskId: 'task:T1', launchToolCallId: 'launch-2', startedAtMs: 5_000 },
    });
  });

  it('files nothing for a launch from before the ledger, whose row draws its task\'s latest run', async () => {
    // The server stamps such a launch with the facts of its task's latest run,
    // which that run's own launch files.
    const runs = await runsAfter([
      USER_TURN,
      taskArtifact('launch-0', { action: 'spawned', status: 'completed', started_ms: 5_000, finished_ms: 8_000 }),
      taskArtifact('launch-1', {
        action: 'resumed', status: 'completed', task_run_id: 'run-1', started_ms: 5_000, finished_ms: 8_000,
      }),
    ]);
    expect(runs).toEqual({
      'run-1': { taskId: 'task:T1', launchToolCallId: 'launch-1', startedAtMs: 5_000, finishedAtMs: 8_000 },
    });
  });

  it('files nothing for a launch the ledger has no run for', async () => {
    const runs = await runsAfter([USER_TURN, taskArtifact('launch-1', { action: 'spawned', status: 'completed' })]);
    expect(runs).toEqual({});
  });

  it('files an older page\'s runs as it replays', async () => {
    const { rt } = buildRuntime();
    replaying([
      { event: 'history_page', first_turn_index: 0, has_more: false },
      USER_TURN,
      taskArtifact('launch-1', { action: 'spawned', status: 'completed', task_run_id: 'run-1', started_ms: 1_000 }),
      { event: 'replay_done', thread_id: 'thread-1' },
    ]);
    await loadOlderHistoryPage(
      rt,
      { threadId: 'thread-1', beforeTurn: 1, limit: 1, signal: new AbortController().signal },
      { events: [], subagents: new Map(), steeredAgentIds: new Set() },
    );
    expect(rt.subagentHistory.get().runs.get('run-1')).toEqual({
      taskId: 'task:T1', launchToolCallId: 'launch-1', startedAtMs: 1_000,
    });
  });
});
