/**
 * A task that resumed must not replay carrying the reason its previous run
 * stopped for.
 *
 * The live session retracts a credit stop from both writers when the task
 * resumes. Replay rebuilds history from the artifacts instead, and a resumed
 * task's artifact carries no failure at all — so a merge that only overwrites
 * when the incoming value is truthy keeps the earlier run's `credit_stop`, and
 * the notice the resume retracted comes back on the next reload.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const api = vi.hoisted(() => ({ replayThreadHistory: vi.fn() }));

vi.mock('../../../utils/api', () => ({
  replayThreadHistory: api.replayThreadHistory,
}));

import { loadConversationHistory } from '../replayHistory';
import type { HistoryRuntime } from '../../runtime';
import type { HistoryInterruptInfo, MessageRecord } from '../../types';

type Ref<T> = { current: T };
const ref = <T,>(current: T): Ref<T> => ({ current });

function buildRuntime() {
  let messages: MessageRecord[] = [];
  return {
    workspaceId: 'ws-1',
    threadId: 'thread-1',
    get messages() { return messages; },
    t: (key: string) => key,
    updateTodoListCard: null,
    setMessages: ((updater: (prev: MessageRecord[]) => MessageRecord[]) => {
      messages = updater(messages);
    }) as HistoryRuntime['setMessages'],
    setIsLoadingHistory: vi.fn(),
    setIsCompacting: vi.fn(),
    setMessageError: vi.fn(),
    setFallbackSuggestion: vi.fn(),
    setThreadModels: vi.fn(),
    setLastThreadModel: vi.fn(),
    setTokenUsage: vi.fn(),
    setReloadTrigger: vi.fn(),
    setThreadId: vi.fn(),
    historyLoadingRef: ref(false),
    replayedRunIdsRef: ref([] as string[]),
    historyLoadedKeyRef: ref<string | null>(null),
    historyHasUnresolvedInterruptRef: ref(false),
    unresolvedHistoryInterruptRef: ref([] as HistoryInterruptInfo[]),
    lastRenderedTurnIndexRef: ref<number | null>(null),
    newMessagesStartIndexRef: ref(0),
    historyPendingTaskToolCallIdsRef: ref([] as string[]),
    currentMessageRef: ref<string | null>(null),
    lastEventIdRef: ref<number | string | null>(null),
    renderedInterruptIdsRef: ref(new Set<string>()),
    toolCallIdToTaskIdMapRef: ref(new Map<string, string>()),
    recentlySentTrackerRef: ref({ isRecentlySent: () => false }),
    offloadBatchRef: ref(null),
  } as unknown as HistoryRuntime;
}

function replayOf(items: Array<Record<string, unknown>>) {
  return async (_threadId: string, onEvent: (e: Record<string, unknown>) => void) => {
    for (const item of items) onEvent(item);
  };
}

const USER_TURN = {
  event: 'user_message',
  thread_id: 'thread-1',
  turn_index: 0,
  content: 'Analyse the filing',
};

function taskArtifact(
  tool_call_id: string,
  payload: Record<string, unknown>,
) {
  return {
    event: 'artifact',
    artifact_type: 'task',
    artifact_id: `art-${tool_call_id}`,
    tool_call_id,
    payload: { task_id: 'T1', type: 'equity-analyst', description: 'Read the 10-K', ...payload },
  };
}

const STOPPED = taskArtifact('tc-1', {
  action: 'spawned',
  status: 'cancelled',
  error: 'Out of credits.',
  error_type: 'credit_stop',
});

const RESUMED = taskArtifact('tc-2', { action: 'resumed', status: 'running' });

async function historyFor(items: Array<Record<string, unknown>>) {
  const projectSubagentHistory = vi.fn();
  api.replayThreadHistory.mockImplementation(replayOf(items));
  await loadConversationHistory(buildRuntime(), {
    applyFallbackSuggestion: vi.fn(),
    loadFeedback: vi.fn().mockResolvedValue(undefined),
    projectSubagentHistory,
  });
  return projectSubagentHistory.mock.calls[0][0].get('task:T1');
}

beforeEach(() => vi.clearAllMocks());

describe('history replay — a task stop reason', () => {
  it('is dropped once a later artifact shows the task running again', async () => {
    const entry = await historyFor([USER_TURN, STOPPED, RESUMED]);

    expect(entry.status).toBe('running');
    expect(entry.error).toBeUndefined();
    expect(entry.errorType).toBeUndefined();
  });

  it('survives when nothing later contradicts it', async () => {
    // The complement, and the reason the fix is not "never carry a reason
    // forward": a task that really did stop keeps its reason and its type, or
    // the notice the whole surface exists for never renders after a reload.
    const entry = await historyFor([USER_TURN, STOPPED]);

    expect(entry.status).toBe('cancelled');
    expect(entry.error).toBe('Out of credits.');
    expect(entry.errorType).toBe('credit_stop');
  });
});
