/**
 * Locks the failed-Task-spawn terminalization. A background Task tool returns
 * immediately: on success ("Task-N started in background") the result is NOT
 * terminal — the subagent keeps running and settles via its per-task stream
 * closing. But a FAILED spawn (admission/setup error, content prefixed "ERROR")
 * never produces a task artifact or a channel, so nothing else will ever settle
 * its inline placeholder. handleToolCallResult must stamp it 'error' itself, or
 * the chip spins "Running" forever (regression after the inactivateAllSubagents
 * sweep was removed).
 */
import { describe, it, expect, vi } from 'vitest';
import { handleToolCallResult } from '../streamEventHandlers';
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

function applyResult(toolCallId: string, content: string): MessageRecord {
  const setMessages = vi.fn<(r: Reducer) => void>();
  handleToolCallResult({
    assistantMessageId: 'assistant-1',
    toolCallId,
    result: { content, tool_call_id: toolCallId } as ToolCallResultRecord,
    refs: refs as unknown as Parameters<typeof handleToolCallResult>[0]['refs'],
    setMessages: setMessages as unknown as Parameters<typeof handleToolCallResult>[0]['setMessages'],
  });
  const reducer = setMessages.mock.calls[0][0] as Reducer;
  const next = reducer([seededMessage(toolCallId)]);
  return next[0];
}

describe('handleToolCallResult — subagent Task placeholder', () => {
  it('stamps a failed spawn terminal so it stops spinning', () => {
    const msg = applyResult('tc-1', 'ERROR: task admission rejected (concurrent limit)');
    const task = (msg.subagentTasks as Record<string, { status?: string }>)['tc-1'];
    expect(task.status).toBe('error');
  });

  it('leaves a successful dispatch non-terminal (subagent still running)', () => {
    const msg = applyResult('tc-2', 'Task-abc123 started in background');
    const task = (msg.subagentTasks as Record<string, { status?: string }>)['tc-2'];
    // Must NOT be flipped to a terminal state — real completion arrives on the
    // per-task stream close, not this immediate dispatch acknowledgement.
    expect(task.status).toBe('running');
  });
});
