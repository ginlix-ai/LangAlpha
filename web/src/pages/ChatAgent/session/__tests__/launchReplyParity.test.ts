// @vitest-environment node
/**
 * The live stream and history replay stamp a launch card from the same tool
 * result, and must leave the same record behind.
 *
 * They used to differ by a field name alone — `toolCallResult` live,
 * `result` on replay — and every reader then had to know which transport it
 * was looking at. One did not: the Task detail panel read only the replay
 * spelling, so the same task showed a result after a reload and nothing
 * during the turn that produced it. Asserting the two records are equal is
 * what keeps a future stamp from re-splitting them.
 */
import { describe, it, expect } from 'vitest';
import { handleToolCallResult } from '../stream/mainEventHandlers';
import { handleHistoryToolCallResult } from '../history/historyHandlers';
import type { StreamRefs } from '../streamRefs';
import type { AssistantMessage, ChatMessage } from '@/types/chat';

const MESSAGE_ID = 'msg-1';
const TOOL_CALL_ID = 'tc-1';

const REFUSAL = {
  content: "Error: Unknown workflow 'no-such-workflow-xyz'.",
  content_type: 'text',
  tool_call_id: TOOL_CALL_ID,
};

const STOPPED = {
  content: 'Workflow run Task-A1b2C3 was stopped before it started.',
  content_type: 'text',
  tool_call_id: TOOL_CALL_ID,
  status: 'success',
  artifact: { launch: 'cancelled' },
};

const LAUNCHED = {
  content: 'Workflow run started: **Task-A1b2C3**',
  content_type: 'text',
  tool_call_id: TOOL_CALL_ID,
};

/** A message carrying one launch card, mid-flight. */
function seed(): ChatMessage[] {
  return [
    {
      id: MESSAGE_ID,
      role: 'assistant',
      contentSegments: [
        { type: 'subagent_task', subagentId: TOOL_CALL_ID, order: 1 },
      ],
      toolCallProcesses: {
        [TOOL_CALL_ID]: {
          toolName: 'RunWorkflow',
          toolCall: null,
          toolCallResult: null,
          isInProgress: true,
          isComplete: false,
          order: 1,
        },
      },
      subagentTasks: {
        [TOOL_CALL_ID]: {
          subagentId: TOOL_CALL_ID,
          description: 'a workflow',
          prompt: '',
          type: 'workflow',
          action: 'init',
          status: 'running',
        },
      },
    } as unknown as AssistantMessage,
  ];
}

function drive(
  path: 'live' | 'replay',
  result: Record<string, unknown>,
): Record<string, unknown> {
  return driveMessage(path, result).task;
}

function driveMessage(
  path: 'live' | 'replay',
  result: Record<string, unknown>,
): { task: Record<string, unknown>; process: Record<string, unknown> } {
  let messages = seed();
  const setMessages = (update: unknown): void => {
    messages = (update as (prev: ChatMessage[]) => ChatMessage[])(messages);
  };
  const shared = {
    assistantMessageId: MESSAGE_ID,
    toolCallId: TOOL_CALL_ID,
    result: result as never,
    setMessages: setMessages as never,
  };
  if (path === 'live') {
    handleToolCallResult({
      ...shared,
      refs: { currentToolCallIdRef: { current: null } } as unknown as StreamRefs,
    });
  } else {
    handleHistoryToolCallResult({ ...shared, pairState: {} as never });
  }
  const message = messages[0] as AssistantMessage;
  const tasks = message.subagentTasks as unknown as Record<string, Record<string, unknown>>;
  const processes = message.toolCallProcesses as unknown as Record<string, Record<string, unknown>>;
  return { task: tasks[TOOL_CALL_ID], process: processes[TOOL_CALL_ID] };
}

describe('launch card parity across transports', () => {
  it('stamps a refused launch identically live and on replay', () => {
    const live = drive('live', REFUSAL);
    const replay = drive('replay', REFUSAL);

    expect(live).toEqual(replay);
    expect(live.result).toBe(REFUSAL.content);
    // A refusal opens no run and no channel, so this is the only settle.
    expect(live.status).toBe('error');
  });

  it('settles a launch stopped mid-setup as stopped, not failed, on both transports', () => {
    const live = driveMessage('live', STOPPED);
    const replay = driveMessage('replay', STOPPED);

    expect(live.task).toEqual(replay.task);
    // A stop is a cancellation: the card reads "Stopped", and neither the card
    // nor the tool row may read as a failure.
    expect(live.task.status).toBe('cancelled');
    expect(live.process.isFailed).toBe(false);
    expect(replay.process.isFailed).toBe(false);
  });

  it('stamps a launch that started identically, and leaves it running', () => {
    const live = drive('live', LAUNCHED);
    const replay = drive('replay', LAUNCHED);

    expect(live).toEqual(replay);
    expect(live.result).toBe(LAUNCHED.content);
    // The tool returns as soon as the run is dispatched — not a settle.
    expect(live.status).toBe('running');
  });
});
