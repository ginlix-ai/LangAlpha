/**
 * Locks the failed-Task-spawn terminalization. A background Task tool returns
 * immediately: on success ("Task-N started in background") the result is NOT
 * terminal — the subagent keeps running and settles via its per-task stream
 * closing. But a FAILED spawn returns a bare "Error: …" ToolMessage with no
 * task artifact and opens no channel, so nothing else will ever settle its
 * inline placeholder. Both the live handler and the history handler must
 * stamp it 'error' from that one signal, or the chip spins "Running" forever.
 *
 * Content strings mirror the real backend failure shapes
 * (middleware.py `"Error: could not start {task.display_id} — …"`) — the
 * backend never emits an uppercase "ERROR" prefix.
 */
import { describe, it, expect, vi } from 'vitest';
import { handleToolCallResult } from '../streamEventHandlers';
import { handleHistoryToolCallResult } from '../historyEventHandlers';
import type { MessageRecord, ToolCallResultRecord } from '../types';

type Reducer = (prev: MessageRecord[]) => MessageRecord[];

const refs = {
  contentOrderCounterRef: { current: 0 },
  currentReasoningIdRef: { current: null },
  currentToolCallIdRef: { current: null },
};

function seededMessage(toolCallId: string): MessageRecord {
  return {
    id: 'assistant-1',
    role: 'assistant',
    toolCallProcesses: {
      [toolCallId]: { toolName: 'Task', isInProgress: true, isComplete: false },
    },
    subagentTasks: {
      [toolCallId]: { subagentId: toolCallId, action: 'init', status: 'running' },
    },
  } as unknown as MessageRecord;
}

function applyResult(
  toolCallId: string,
  result: Partial<ToolCallResultRecord>,
): MessageRecord {
  const setMessages = vi.fn<(r: Reducer) => void>();
  handleToolCallResult({
    assistantMessageId: 'assistant-1',
    toolCallId,
    result: { tool_call_id: toolCallId, ...result } as ToolCallResultRecord,
    refs: refs as unknown as Parameters<typeof handleToolCallResult>[0]['refs'],
    setMessages: setMessages as unknown as Parameters<typeof handleToolCallResult>[0]['setMessages'],
  });
  const reducer = setMessages.mock.calls[0][0] as Reducer;
  const next = reducer([seededMessage(toolCallId)]);
  return next[0];
}

function applyHistoryResult(
  toolCallId: string,
  result: Partial<ToolCallResultRecord>,
): MessageRecord {
  const setMessages = vi.fn<(r: Reducer) => void>();
  handleHistoryToolCallResult({
    assistantMessageId: 'assistant-1',
    toolCallId,
    result: { tool_call_id: toolCallId, ...result } as ToolCallResultRecord,
    pairState: {} as Parameters<typeof handleHistoryToolCallResult>[0]['pairState'],
    setMessages: setMessages as unknown as Parameters<typeof handleHistoryToolCallResult>[0]['setMessages'],
  });
  const reducer = setMessages.mock.calls[0][0] as Reducer;
  const next = reducer([seededMessage(toolCallId)]);
  return next[0];
}

describe('handleToolCallResult — subagent Task placeholder (live)', () => {
  it('stamps a failed spawn terminal so it stops spinning', () => {
    const msg = applyResult('tc-1', {
      content: 'Error: could not start Task-a1B2c3 — concurrent task limit reached.',
    });
    const task = (msg.subagentTasks as Record<string, { status?: string }>)['tc-1'];
    expect(task.status).toBe('error');
  });

  it('leaves a successful dispatch non-terminal (subagent still running)', () => {
    const msg = applyResult('tc-2', { content: 'Task-abc123 started in background' });
    const task = (msg.subagentTasks as Record<string, { status?: string }>)['tc-2'];
    // Must NOT be flipped to a terminal state — real completion arrives on the
    // per-task stream close, not this immediate dispatch acknowledgement.
    expect(task.status).toBe('running');
  });

  it('never treats an artifact-carrying result as a failure', () => {
    const msg = applyResult('tc-3', {
      content: 'Error rates by region attached.',
      artifact: { some: 'payload' },
    });
    const task = (msg.subagentTasks as Record<string, { status?: string }>)['tc-3'];
    expect(task.status).toBe('running');
  });
});

describe('handleHistoryToolCallResult — subagent Task placeholder (replay)', () => {
  it('settles a failed spawn as error on reload instead of perpetual Running', () => {
    const msg = applyHistoryResult('tc-1', {
      content: 'Error: could not start Task-a1B2c3 — concurrent task limit reached.',
    });
    const task = (msg.subagentTasks as Record<string, { status?: string }>)['tc-1'];
    expect(task.status).toBe('error');
  });

  it('leaves a successful dispatch non-terminal (artifact stamp settles it later)', () => {
    const msg = applyHistoryResult('tc-2', {
      content: 'Task-abc123 started in background',
    });
    const task = (msg.subagentTasks as Record<string, { status?: string }>)['tc-2'];
    expect(task.status).toBe('running');
  });
});
