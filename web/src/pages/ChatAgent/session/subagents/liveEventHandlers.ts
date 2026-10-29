/**
 * Subagent live event handlers — the task-namespace half of the old
 * streamEventHandlers. Every handler here writes through `updateSubagentCard`;
 * none touches the main transcript's `setMessages`.
 */

import { isToolResultFailure, toolNameOf } from './subagentStatus';
import {
  WORKFLOW_TASK_TYPE, applyWorkflowLifecycle, isWorkflowRunTerminal, workflowRunDisplayStatus,
  workflowRunStatusFromLedger,
  type WorkflowLifecycleFrame, type WorkflowRunState,
} from './workflowRunState';
import type { MessageRecord, ToolCallRecord, ToolCallResultRecord } from '../../hooks/utils/types';
import { getOrCreateTaskRefs } from '../streamRefs';
import { isTaskAgentId } from '../../utils/agentId';
import { appendTextChunk } from '../stream/textChunks';
import { addToolCallChunk, hasPreparing, removeLandedToolCall, withoutPending } from '../stream/toolCallChunks';
import { MAIN_AGENT_PROVENANCE } from '../provenance';
import type { ContentSegment, PendingToolCallChunk } from '@/types/chat';
import type {
  StreamRefs, TaskRefs, ToolCallChunkRecord, UpdateSubagentCard,
} from '../streamRefs';

export function isSubagentEvent(event: Record<string, unknown> | null | undefined): boolean {
  return isTaskAgentId(event?.agent);
}

/**
 * Handles subagent message chunks during streaming
 * Similar to main agent handlers but for subagent events
 * @param {Object} params - Handler parameters
 * @param {string} params.taskId - Task ID (e.g., "Task-1")
 * @param {string} params.assistantMessageId - ID of the assistant message being updated
 * @param {string} params.contentType - Content type (reasoning_signal, reasoning, text)
 * @param {string} params.content - Content chunk
 * @param {string} params.finishReason - Optional finish reason
 * @param {Object} params.refs - Refs object with subagent state refs
 * @param {Function} params.updateSubagentCard - Callback to update subagent card
 * @param {number} [params.atMs] - When the writer captured the frame (epoch ms)
 * @returns {boolean} True if event was handled
 */
export function handleSubagentMessageChunk({
  taskId,
  assistantMessageId,
  contentType,
  content,
  finishReason,
  elapsedMs,
  refs,
  updateSubagentCard,
  atMs,
}: {
  taskId: string;
  assistantMessageId: string;
  contentType: string;
  content: string;
  finishReason: string | undefined;
  /** The server's measured duration on a reasoning complete signal. */
  elapsedMs?: number;
  refs: StreamRefs;
  updateSubagentCard: UpdateSubagentCard;
  atMs?: number;
}): boolean {
  if (!taskId || !assistantMessageId || !updateSubagentCard) {
    return false;
  }

  const taskRefs = getOrCreateTaskRefs(refs, taskId);
  const { contentOrderCounterRef, currentReasoningIdRef } = taskRefs;
  // A thought is timed by when the writer captured its frames, as a tool call
  // is: arrival is one instant for a backlog, and a reconnect has no other.
  // Without a capture time a reconnect's thought stays untimed.
  const frameAt = atMs ?? (refs.isReconnect ? undefined : Date.now());
  // Which run a message belongs to, for the run that has to find its closing
  // prose among the task's later ones.
  const opened = atMs !== undefined ? { openedAtMs: atMs } : {};

  // Handle finishReason with no content — model call complete
  if (finishReason && !content && contentType !== 'reasoning_signal') {
    const updatedMessages = [...taskRefs.messages];
    const msgIdx = updatedMessages.findIndex((m: MessageRecord) => m.id === assistantMessageId);
    if (finishReason === 'tool_calls') {
      // Its calls have all landed by now.
      if (msgIdx !== -1 && hasPreparing(updatedMessages[msgIdx])) {
        updatedMessages[msgIdx] = withoutPending(updatedMessages[msgIdx]);
        taskRefs.messages = updatedMessages;
        updateSubagentCard(taskId, { messages: updatedMessages });
      }
      return false; // More work coming, let tool_calls handler process
    }
    // finish_reason: "stop" — subagent's model call is done
    if (msgIdx !== -1) {
      updatedMessages[msgIdx] = { ...withoutPending(updatedMessages[msgIdx]), isStreaming: false };
      taskRefs.messages = updatedMessages;
      updateSubagentCard(taskId, { messages: updatedMessages });
    }
    return true;
  }

  // Handle reasoning_signal
  if (contentType === 'reasoning_signal') {
    const signalContent = content || '';
    if (signalContent === 'start') {
      const reasoningId = `reasoning-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
      currentReasoningIdRef.current = reasoningId;
      contentOrderCounterRef.current++;
      const currentOrder = contentOrderCounterRef.current;

      // Update subagent message
      const updatedMessages = [...taskRefs.messages];
      let messageIndex = updatedMessages.findIndex((m: MessageRecord) => m.id === assistantMessageId);

      if (messageIndex === -1) {
        // Create new message
        updatedMessages.push({
          id: assistantMessageId,
          role: 'assistant',
          contentSegments: [],
          reasoningProcesses: {},
          toolCallProcesses: {},
          isStreaming: true,
          ...opened,
        });
        messageIndex = updatedMessages.length - 1;
      }

      const prev = updatedMessages[messageIndex];
      updatedMessages[messageIndex] = {
        ...prev,
        contentSegments: [
          ...((prev.contentSegments as unknown[]) || []),
          { type: 'reasoning', reasoningId, order: currentOrder },
        ],
        reasoningProcesses: {
          ...((prev.reasoningProcesses as Record<string, unknown>) || {}),
          [reasoningId]: {
            content: '',
            isReasoning: true,
            reasoningComplete: false,
            order: currentOrder,
            _startedAt: frameAt,
          },
        },
      };

      taskRefs.messages = updatedMessages;
      updateSubagentCard(taskId, { messages: updatedMessages });
      return true;
    } else if (signalContent === 'complete') {
      if (currentReasoningIdRef.current) {
        const reasoningId = currentReasoningIdRef.current;
        const updatedMessages = [...taskRefs.messages];
        const messageIndex = updatedMessages.findIndex((m: MessageRecord) => m.id === assistantMessageId);

        if (messageIndex !== -1) {
          const prev = updatedMessages[messageIndex];
          const reasoningProcesses = { ...((prev.reasoningProcesses as Record<string, Record<string, unknown>>) || {}) };
          if (reasoningProcesses[reasoningId]) {
            const startedAt = reasoningProcesses[reasoningId]._startedAt as number | undefined;
            const completedAt = frameAt ?? 1;
            reasoningProcesses[reasoningId] = {
              ...reasoningProcesses[reasoningId],
              isReasoning: false,
              reasoningComplete: true,
              _completedAt: completedAt,
              elapsedMs: elapsedMs ?? (startedAt && frameAt !== undefined ? completedAt - startedAt : undefined),
            };
          }
          updatedMessages[messageIndex] = { ...prev, reasoningProcesses };
          taskRefs.messages = updatedMessages;
          updateSubagentCard(taskId, { messages: updatedMessages });
        }
        currentReasoningIdRef.current = null;
      }
      return true;
    }
  }

  // Handle reasoning content
  if (contentType === 'reasoning' && content && currentReasoningIdRef.current) {
    const reasoningId = currentReasoningIdRef.current;
    const updatedMessages = [...taskRefs.messages];
    let messageIndex = updatedMessages.findIndex((m: MessageRecord) => m.id === assistantMessageId);

    // Create message if it doesn't exist (edge case: reasoning content arrives before start signal)
    if (messageIndex === -1) {
      updatedMessages.push({
        id: assistantMessageId,
        role: 'assistant',
        contentSegments: [],
        reasoningProcesses: {},
        toolCallProcesses: {},
        isStreaming: true,
        ...opened,
      });
      messageIndex = updatedMessages.length - 1;
    }

    const prev = updatedMessages[messageIndex];
    const reasoningProcesses = { ...((prev.reasoningProcesses as Record<string, Record<string, unknown>>) || {}) };
    let nextContentSegments = (prev.contentSegments as unknown[]) || [];

    // Create reasoning process if it doesn't exist (edge case: reasoning content arrives before start signal)
    if (!reasoningProcesses[reasoningId]) {
      contentOrderCounterRef.current++;
      const currentOrder = contentOrderCounterRef.current;

      nextContentSegments = [
        ...nextContentSegments,
        { type: 'reasoning', reasoningId, order: currentOrder },
      ];

      reasoningProcesses[reasoningId] = {
        content: '',
        isReasoning: true,
        reasoningComplete: false,
        order: currentOrder,
      };
    }

    // Update reasoning content - accumulate the content
    const existingContent = (reasoningProcesses[reasoningId]?.content as string) || '';
    const newContent = existingContent + content;

    reasoningProcesses[reasoningId] = {
      ...reasoningProcesses[reasoningId],
      content: newContent,
      isReasoning: true,
    };

    // Replace the message with a fresh object so ``React.memo`` invalidates;
    // mutating ``prev`` in place would freeze the rendered card until the
    // next lifecycle event.
    updatedMessages[messageIndex] = {
      ...prev,
      contentSegments: nextContentSegments,
      reasoningProcesses,
    };
    taskRefs.messages = updatedMessages;
    updateSubagentCard(taskId, { messages: updatedMessages });
    return true;
  }

  // Handle text content
  if (contentType === 'text' && content) {
    contentOrderCounterRef.current++;
    const currentOrder = contentOrderCounterRef.current;

    const updatedMessages = [...taskRefs.messages];
    let messageIndex = updatedMessages.findIndex((m: MessageRecord) => m.id === assistantMessageId);

    if (messageIndex === -1) {
      updatedMessages.push({
        id: assistantMessageId,
        role: 'assistant',
        contentSegments: [],
        reasoningProcesses: {},
        toolCallProcesses: {},
        content: '',
        isStreaming: true,
        ...opened,
      });
      messageIndex = updatedMessages.length - 1;
    }

    // Replace with a fresh object: ``MessageBubble`` is ``React.memo``'d
    // on the message ref, so mutation would skip per-token re-renders and
    // content would only land at the next lifecycle event.
    const prev = updatedMessages[messageIndex];
    updatedMessages[messageIndex] = {
      ...prev,
      contentSegments: appendTextChunk((prev.contentSegments as ContentSegment[]) || [], content, currentOrder),
      content: ((prev.content as string) || '') + content,
      contentType: 'text',
      isStreaming: true,
    };

    taskRefs.messages = updatedMessages;
    updateSubagentCard(taskId, { messages: updatedMessages });
    return true;
  }

  return false;
}

/**
 * Handles subagent tool_call_chunks events during streaming.
 * Updates pendingToolCallChunks on the subagent assistant message to show
 * a "preparing" indicator while the LLM streams tool arguments.
 * @param {Object} params - Handler parameters
 * @param {string} params.taskId - Task ID
 * @param {string} params.assistantMessageId - ID of the assistant message
 * @param {Array} params.chunks - Array of tool call chunk objects
 * @param {Object} params.refs - Refs object with subagent state refs
 * @param {Function} params.updateSubagentCard - Callback to update subagent card
 * @returns {boolean} True if event was handled
 */
export function handleSubagentToolCallChunks({ taskId, assistantMessageId, chunks, refs, updateSubagentCard }: {
  taskId: string;
  assistantMessageId: string;
  chunks: ToolCallChunkRecord[];
  refs: StreamRefs;
  updateSubagentCard: UpdateSubagentCard;
}): boolean {
  if (!taskId || !assistantMessageId || !chunks || !Array.isArray(chunks) || !updateSubagentCard) {
    return false;
  }

  const taskRefs = getOrCreateTaskRefs(refs, taskId);
  const updatedMessages = [...taskRefs.messages];

  let messageIndex = updatedMessages.findIndex((m: MessageRecord) => m.id === assistantMessageId);
  if (messageIndex === -1) {
    updatedMessages.push({
      id: assistantMessageId,
      role: 'assistant',
      contentSegments: [],
      reasoningProcesses: {},
      toolCallProcesses: {},
      pendingToolCallChunks: {},
      isStreaming: true,
    });
    messageIndex = updatedMessages.length - 1;
  }

  const msg = { ...updatedMessages[messageIndex] };
  let pending = (msg.pendingToolCallChunks as Record<string, PendingToolCallChunk> | undefined) ?? {};
  const seenAt = Date.now();

  chunks.forEach((chunk: ToolCallChunkRecord) => {
    pending = addToolCallChunk(pending, chunk, seenAt);
  });

  msg.pendingToolCallChunks = pending;
  updatedMessages[messageIndex] = msg;
  taskRefs.messages = updatedMessages;

  updateSubagentCard(taskId, { messages: taskRefs.messages });
  return true;
}

/**
 * Handles subagent tool_calls events during streaming
 * @param {Object} params - Handler parameters
 * @param {string} params.taskId - Task ID
 * @param {string} params.assistantMessageId - ID of the assistant message
 * @param {Array} params.toolCalls - Array of tool call objects
 * @param {Object} params.refs - Refs object with subagent state refs
 * @param {Function} params.updateSubagentCard - Callback to update subagent card
 * @param {number} [params.atMs] - When the writer captured the frame (epoch ms)
 * @returns {boolean} True if event was handled
 */
export function handleSubagentToolCalls({ taskId, assistantMessageId, toolCalls, refs, updateSubagentCard, atMs }: {
  taskId: string;
  assistantMessageId: string;
  toolCalls: ToolCallRecord[];
  refs: StreamRefs;
  updateSubagentCard: UpdateSubagentCard;
  atMs?: number;
}): boolean {
  if (!taskId || !assistantMessageId || !toolCalls || !Array.isArray(toolCalls) || !updateSubagentCard) {
    return false;
  }

  const taskRefs = getOrCreateTaskRefs(refs, taskId);
  const { contentOrderCounterRef } = taskRefs;

  toolCalls.forEach((toolCall: ToolCallRecord) => {
    const toolCallId = toolCall.id;
    if (toolCallId) {
      const updatedMessages = [...taskRefs.messages];
      let messageIndex = updatedMessages.findIndex((m: MessageRecord) => m.id === assistantMessageId);

      if (messageIndex === -1) {
        updatedMessages.push({
          id: assistantMessageId,
          role: 'assistant',
          contentSegments: [],
          reasoningProcesses: {},
          toolCallProcesses: {},
          isStreaming: true,
          ...(atMs !== undefined ? { openedAtMs: atMs } : {}),
        });
        messageIndex = updatedMessages.length - 1;
      }

      const msg = updatedMessages[messageIndex];
      // A message the call chunks opened carries no capture time, and its
      // first call is the earliest one it will get.
      if (atMs !== undefined && typeof msg.openedAtMs !== 'number') msg.openedAtMs = atMs;
      const toolCallProcesses = { ...((msg.toolCallProcesses as Record<string, Record<string, unknown>>) || {}) };
      const contentSegments = [...((msg.contentSegments as Record<string, unknown>[]) || [])];
      const firstLanding = !toolCallProcesses[toolCallId];

      if (!toolCallProcesses[toolCallId]) {
        contentOrderCounterRef.current++;
        const currentOrder = contentOrderCounterRef.current;

        contentSegments.push({
          type: 'tool_call',
          toolCallId,
          order: currentOrder,
        });

        toolCallProcesses[toolCallId] = {
          toolName: toolCall.name,
          toolCall: toolCall,
          toolCallResult: null,
          isInProgress: true,
          isComplete: false,
          order: currentOrder,
        };
      } else {
        toolCallProcesses[toolCallId] = {
          ...toolCallProcesses[toolCallId],
          toolName: toolCall.name,
          toolCall: toolCall,
          isInProgress: true,
        };
      }

      msg.contentSegments = contentSegments;
      msg.toolCallProcesses = toolCallProcesses;
      // This call's arguments are complete; parallel calls after it keep theirs.
      msg.pendingToolCallChunks = removeLandedToolCall(
        (msg.pendingToolCallChunks as Record<string, PendingToolCallChunk> | undefined) ?? {}, toolCallId, firstLanding);
      taskRefs.messages = updatedMessages;
    }
  });

  // Update subagent card: set currentTool to the first tool being called
  // This ensures the status shows which tool is currently running
  const firstToolCall = toolCalls.length > 0 ? toolCalls[0] : null;
  const currentToolName = firstToolCall?.name || '';

  updateSubagentCard(taskId, {
    messages: taskRefs.messages,
    currentTool: currentToolName, // Update current tool to show what's running
  });
  return true;
}

/**
 * Handles subagent tool_call_result events during streaming
 * @param {Object} params - Handler parameters
 * @param {string} params.taskId - Task ID
 * @param {string} params.assistantMessageId - ID of the assistant message
 * @param {string} params.toolCallId - ID of the tool call
 * @param {Object} params.result - Tool call result object
 * @param {Object} params.refs - Refs object with subagent state refs
 * @param {Function} params.updateSubagentCard - Callback to update subagent card
 * @returns {boolean} True if event was handled
 */
export function handleSubagentToolCallResult({ taskId, assistantMessageId, toolCallId, result, refs, updateSubagentCard }: {
  taskId: string;
  assistantMessageId: string;
  toolCallId: string;
  result: ToolCallResultRecord;
  refs: StreamRefs;
  updateSubagentCard: UpdateSubagentCard;
}): boolean {
  if (!taskId || !toolCallId || !updateSubagentCard) {
    return false;
  }

  const taskRefs = getOrCreateTaskRefs(refs, taskId);
  const { contentOrderCounterRef } = taskRefs;

  const updatedMessages = [...taskRefs.messages];

  // Find the message that contains this tool call
  // tool_call_result events have a different event.id than tool_calls events,
  // so we need to search by tool_call_id instead of message ID
  let messageIndex = -1;
  let targetMessage: MessageRecord | null = null;

  // First, try to find message by assistantMessageId (if provided and matches)
  if (assistantMessageId) {
    messageIndex = updatedMessages.findIndex((m: MessageRecord) => m.id === assistantMessageId);
    if (messageIndex !== -1) {
      targetMessage = updatedMessages[messageIndex];
      // Verify this message actually has the tool call
      if (!(targetMessage.toolCallProcesses as Record<string, unknown>)?.[toolCallId]) {
        if (import.meta.env.DEV) {
          console.warn('[handleSubagentToolCallResult] Message found but tool call not in it:', {
            messageId: assistantMessageId,
            toolCallId,
            availableToolCalls: Object.keys((targetMessage.toolCallProcesses as Record<string, unknown>) || {}),
          });
        }
        messageIndex = -1;
        targetMessage = null;
      }
    }
  }

  // If not found by message ID, search for message containing this tool call
  if (messageIndex === -1) {
    for (let i = 0; i < updatedMessages.length; i++) {
      const msg = updatedMessages[i];
      if ((msg.toolCallProcesses as Record<string, unknown>)?.[toolCallId]) {
        messageIndex = i;
        targetMessage = msg;
        break;
      }
    }
  }

  if (messageIndex === -1) {
    // Tool call doesn't exist yet - create new message with tool call result
    // This can happen if tool_call_result arrives before tool_calls
    contentOrderCounterRef.current++;
    const currentOrder = contentOrderCounterRef.current;

    updatedMessages.push({
      id: assistantMessageId || `subagent-msg-${Date.now()}`,
      role: 'assistant',
      contentSegments: [{
        type: 'tool_call',
        toolCallId,
        order: currentOrder,
      }],
      reasoningProcesses: {},
      toolCallProcesses: {
        [toolCallId]: {
          toolName: 'Unknown Tool',
          toolCall: null,
          toolCallResult: {
            content: result.content,
            content_type: result.content_type,
            tool_call_id: result.tool_call_id,
            artifact: result.artifact,
          },
          isInProgress: false,
          isComplete: true,
          isFailed: isToolResultFailure(result),
          order: currentOrder,
        },
      },
    });

    if (import.meta.env.DEV) {
      console.warn('[handleSubagentToolCallResult] Tool call not found, created new message:', {
        taskId,
        toolCallId,
        assistantMessageId,
      });
    }
  } else {
    // Update existing tool call with result
    const msg = updatedMessages[messageIndex];
    const toolCallProcesses = { ...((msg.toolCallProcesses as Record<string, Record<string, unknown>>) || {}) };

    const isFailed = isToolResultFailure({ ...result, toolName: toolNameOf(toolCallProcesses, toolCallId) });

    if (toolCallProcesses[toolCallId]) {
      toolCallProcesses[toolCallId] = {
        ...toolCallProcesses[toolCallId],
        toolCallResult: {
          content: result.content,
          content_type: result.content_type,
          tool_call_id: result.tool_call_id,
          artifact: result.artifact,
        },
        isInProgress: false,
        isComplete: true,
        isFailed,
      };
    } else {
      // Edge case: message exists but tool call doesn't - add it
      contentOrderCounterRef.current++;
      const currentOrder = contentOrderCounterRef.current;

      const contentSegments = [...((msg.contentSegments as Record<string, unknown>[]) || [])];
      contentSegments.push({
        type: 'tool_call',
        toolCallId,
        order: currentOrder,
      });

      toolCallProcesses[toolCallId] = {
        toolName: 'Unknown Tool',
        toolCall: null,
        toolCallResult: {
          content: result.content,
          content_type: result.content_type,
          tool_call_id: result.tool_call_id,
          artifact: result.artifact,
        },
        isInProgress: false,
        isComplete: true,
        isFailed,
        order: currentOrder,
      };

      msg.contentSegments = contentSegments;
    }

    msg.toolCallProcesses = toolCallProcesses;
  }

  taskRefs.messages = updatedMessages;

  // Detect if the tool call that just completed was a failure
  // We need to check the tool call process that was just updated
  let justCompletedToolFailed = false;

  // Find the tool call that just completed (it should be in updatedMessages now)
  for (const msg of updatedMessages) {
    const toolCallProcesses = (msg.toolCallProcesses as Record<string, Record<string, unknown>>) || {};
    const completedToolCall = toolCallProcesses[toolCallId];
    if (completedToolCall && completedToolCall.isComplete) {
      // This is the tool call that just completed
      justCompletedToolFailed = (completedToolCall.isFailed as boolean) || false;
      break;
    }
  }

  // Update subagent card: clear currentTool when tool call completes
  // Priority:
  // 1. If the tool that just completed failed, clear currentTool immediately
  // 2. Otherwise, check if there are any other in-progress tool calls
  let hasInProgressTool = false;
  let currentToolName = '';

  if (!justCompletedToolFailed) {
    // Only check for in-progress tools if the completed tool didn't fail
    // If it failed, we want to clear currentTool immediately
    for (const msg of updatedMessages) {
      const toolCallProcesses = (msg.toolCallProcesses as Record<string, Record<string, unknown>>) || {};
      for (const [_tcId, tcProcess] of Object.entries(toolCallProcesses)) {
        if (tcProcess.isInProgress && !tcProcess.isComplete) {
          hasInProgressTool = true;
          currentToolName = (tcProcess.toolName as string) || '';
          break;
        }
      }
      if (hasInProgressTool) break;
    }
  }

  // Determine final currentTool value:
  // - If tool just failed, clear it immediately
  // - If there's an in-progress tool, show it
  // - Otherwise, clear it
  const finalCurrentTool = justCompletedToolFailed ? '' : (hasInProgressTool ? currentToolName : '');

  // Update currentTool: clear if tool failed, otherwise use in-progress tool if any
  updateSubagentCard(taskId, {
    messages: updatedMessages,
    currentTool: finalCurrentTool, // Explicitly pass empty string to clear when failed or no tools in progress
  });
  return true;
}

/**
 * Reduces one `workflow_lifecycle` frame into the run task's card state, which
 * the inline WorkflowRunCard and the detail view both read.
 *
 * `run_started` is the lane epoch: a full-lane (re)delivery rebuilds from
 * scratch instead of double-appending onto carried state.
 */
/** Reduce one lifecycle frame into the run's state and stamp its card.
 *
 *  Split out so ledger-driven settlement shares it: `applyWorkflowLifecycle`
 *  must stay the only writer of `workflowRun.status`, or a second writer and
 *  the card's `run?.status ?? status` read can disagree about whether the run
 *  ended. */
function reduceWorkflowRunFrame(
  taskId: string,
  taskRefs: TaskRefs,
  prev: WorkflowRunState | undefined,
  event: WorkflowLifecycleFrame,
  updateSubagentCard: UpdateSubagentCard,
): void {
  const next = applyWorkflowLifecycle(prev, event);
  taskRefs.workflowRun = next;

  const terminal = isWorkflowRunTerminal(next.status);
  updateSubagentCard(taskId, {
    type: WORKFLOW_TASK_TYPE,
    workflowRun: next,
    status: terminal ? workflowRunDisplayStatus(next.status) : 'active',
    ...(terminal ? { isActive: false, ...(next.error ? { error: next.error } : {}) } : {}),
  });
}

/**
 * Settle a workflow run against the ledger outcome its lane closed with.
 *
 * Two cases, one rule — the ledger row is the status authority and the
 * snapshot the detail authority, which is exactly what `workflow_run_items`
 * does on replay:
 *
 *  - No terminal frame at all (a worker that died mid-run emits none, since
 *    the driver mints `workflow_lifecycle`). Recovery appends only `run_end`,
 *    which lands on the card's own status where `workflowRun.status` shadows
 *    it, so the card spins until a reload — and reconnecting does not help,
 *    because the client replays the same frames.
 *  - A terminal frame the ledger then contradicted. The driver stamps its
 *    frame BEFORE the ledger CAS, so a raced cancel or a downgraded event
 *    stream can leave the card reading "completed" against a `cancelled` row.
 *
 * Reconciling means replacing the status, never the frame: the reducer keeps
 * result, totals and duration (`?? state`), so the richer local truth
 * survives while the verdict comes from the ledger.
 */
export function settleWorkflowRunFromClosure({
  taskId, outcome, subagentStateRefs, updateSubagentCard,
}: {
  taskId: string;
  outcome: string | null | undefined;
  subagentStateRefs: Record<string, TaskRefs>;
  updateSubagentCard: UpdateSubagentCard | null;
}): boolean {
  if (!taskId || !updateSubagentCard) return false;
  const taskRefs = subagentStateRefs[taskId];
  const prev = taskRefs?.workflowRun;
  if (!prev) return false;
  const status = workflowRunStatusFromLedger(outcome);
  // Agreement is the common case and needs no write; a non-terminal outcome
  // resolves to nothing to adopt.
  if (!status || prev.status === status) return false;
  // `run_completed` assigns `error` outright rather than defaulting it, so a
  // reconcile has to carry the frame's own detail across or it would erase it.
  // `run_end` carries none of its own — hence the projector's fallback text.
  const error = prev.error ?? (status === 'completed' ? undefined : `run ended: ${outcome}`);
  reduceWorkflowRunFrame(
    taskId,
    taskRefs,
    prev,
    {
      phase: 'run_completed',
      status,
      ...(error ? { error } : {}),
    } as WorkflowLifecycleFrame,
    updateSubagentCard,
  );
  return true;
}

export function handleWorkflowLifecycle({ taskId, event, refs, updateSubagentCard }: {
  taskId: string;
  event: WorkflowLifecycleFrame;
  refs: StreamRefs;
  updateSubagentCard: UpdateSubagentCard;
}): boolean {
  if (!taskId || !updateSubagentCard) {
    return false;
  }

  const taskRefs = getOrCreateTaskRefs(refs, taskId);
  const prev = event.phase === 'run_started' ? undefined : taskRefs.workflowRun;
  reduceWorkflowRunFrame(taskId, taskRefs, prev, event, updateSubagentCard);

  // Stamp identity + ownership on the child's floating card as it is
  // dispatched, before its own lane streams anonymous content — the sidebar
  // hides owner-children, and the label names the drill-in.
  if (event.phase === 'child_started' && typeof event.child_task_id === 'string') {
    const childAgentId = `task:${event.child_task_id}`;
    updateSubagentCard(childAgentId, {
      agentId: childAgentId,
      displayId: `Task-${event.child_task_id}`,
      taskId: childAgentId,
      description: (event.label as string) || '',
      type: (event.subagent_type as string) || 'general-purpose',
      ownerTaskId: taskId,
    });
  }
  return true;
}

/** Who queued an instruction for a subagent: the user, or its main agent. */
export type InstructionSender = 'user' | 'agent';

/** One queue entry a steering delivery took, under the id its sender gave it. */
export interface DeliveredInstruction {
  input_id?: string | null;
  content?: string;
  /** Absent on entries from before the drain recorded it, which get no label. */
  sender?: InstructionSender;
}

/**
 * Renders a user instruction into a subagent transcript — steering
 * follow-ups (steering_delivered) AND run boundaries (the epoch-opening
 * user_message a spawn/resume writes). The mechanics are identical:
 * insert the instruction bubble, finalize the current assistant message,
 * and bump runIndex so subsequent events open a new assistant message
 * below the bubble.
 *
 * @param {Object} params - Handler parameters
 * @param {string} params.taskId - Task ID (e.g., "task:k7Xm2p")
 * @param {string} params.content - The steering instruction content
 * @param {Array} params.entries - The queue entries a delivery took, when it names them
 * @param {boolean} params.steered - Taken into the running run, not the instruction a run opens with
 * @param {Object} params.refs - Refs object with subagentStateRefs
 * @param {Function} params.updateSubagentCard - Callback to update subagent card
 * @returns {boolean} True if event was handled
 */
export function handleTaskSteeringAccepted({ taskId, content, entries, steered, refs, updateSubagentCard }: {
  taskId: string;
  content: string;
  entries?: DeliveredInstruction[];
  steered: boolean;
  refs: StreamRefs;
  updateSubagentCard: UpdateSubagentCard;
}): boolean {
  if (!taskId || !content || !updateSubagentCard) {
    return false;
  }

  const taskRefs = getOrCreateTaskRefs(refs, taskId);
  const updatedMessages = [...taskRefs.messages];

  // Finalize the current assistant message so content before the steering
  // instruction stays above the user bubble
  for (let i = updatedMessages.length - 1; i >= 0; i--) {
    if (updatedMessages[i].role === 'assistant' && updatedMessages[i].isStreaming) {
      updatedMessages[i] = { ...updatedMessages[i], isStreaming: false };
      break;
    }
  }

  // Confirm the pending bubble of each entry this delivery took. An entry with
  // none here, such as the main agent's own follow-up drained in the same
  // step, gets a bubble of its own.
  const taken = new Set<number>();
  const delivered: MessageRecord[] = [];
  const take = (i: number) => {
    taken.add(i);
    delivered.push({ ...updatedMessages[i], isPending: false });
  };
  if (entries?.length) {
    for (const entry of entries) {
      const i = pendingInstructionIndex(updatedMessages, entry.input_id);
      if (i !== -1) take(i);
      else if (entry.content) delivered.push(instructionBubble(entry.content, entry.sender));
    }
  } else {
    const matched = deliveredInstructionIndexes(updatedMessages, content);
    matched.forEach(take);
    if (matched.length === 0) delivered.push(instructionBubble(content));
  }
  // A bubble shows where it was sent, but the subagent reads it here: after
  // everything it streamed since, and ahead of any instruction still waiting.
  // A notice that gave one of these up goes: a send can fail in transport
  // after the server queued it.
  const deliveredIds = new Set(entries?.map((entry) => entry.input_id).filter(Boolean));
  const rest = updatedMessages.filter((m, i) => !taken.has(i) && !(isTaskNotice(m) && deliveredIds.has(m.inputId as string)));
  const messages = [
    ...rest.filter((m) => !isPendingInstruction(m)),
    // Marked as the main transcript marks a steer its turn took in.
    ...(steered ? delivered.map((m) => ({ ...m, steeringDelivered: true })) : delivered),
    ...rest.filter(isPendingInstruction),
  ];

  // Bump runIndex so the next event creates a new assistant message below the user bubble
  taskRefs.runIndex = (taskRefs.runIndex || 0) + 1;
  taskRefs.contentOrderCounterRef.current = 0;
  taskRefs.currentReasoningIdRef.current = null;
  taskRefs.currentToolCallIdRef.current = null;

  taskRefs.messages = messages;
  updateSubagentCard(taskId, { messages });
  return true;
}

/** A delivered instruction with no pending bubble to confirm. Only the main
 *  agent's carry a label: the user's own, sent from another tab or read back
 *  from history, look as they did when sent here. */
function instructionBubble(content: string, sender?: InstructionSender): MessageRecord {
  return {
    id: `followup-${Date.now()}-${Math.random().toString(36).substr(2, 6)}`,
    role: 'user',
    content,
    contentSegments: [{ type: 'text', content, order: 0 }],
    reasoningProcesses: {},
    toolCallProcesses: {},
    ...(sender === 'agent' ? { provenance: MAIN_AGENT_PROVENANCE } : {}),
  };
}

/** An instruction the user sent a running subagent that it has not been given yet. */
function isPendingInstruction(m: MessageRecord): boolean {
  return m.role === 'user' && m.isPending === true;
}

function isTaskNotice(m: MessageRecord): boolean {
  return m.role === 'assistant' && String(m.id).startsWith('task-notice-');
}

function noticeDetail(m: MessageRecord): unknown {
  return (m.contentSegments as Array<{ detail?: string }> | undefined)?.[0]?.detail;
}

function pendingInstructionIndex(messages: MessageRecord[], inputId: string | null | undefined): number {
  return inputId ? messages.findIndex((m) => isPendingInstruction(m) && m.inputId === inputId) : -1;
}

/**
 * The pending bubbles a delivery that names no entries confirms, read off its
 * text alone: an older server's frame, or a captured event from before
 * entries. The subagent takes every instruction queued since its last model
 * call at once, joined by newlines, so the match is a run of bubbles, the
 * latest that fits.
 */
function deliveredInstructionIndexes(messages: MessageRecord[], content: string): number[] {
  const pending = messages.flatMap((m, i) => (isPendingInstruction(m) ? [i] : []));
  for (let start = pending.length - 1; start >= 0; start--) {
    let joined = '';
    for (let end = start; end < pending.length; end++) {
      joined += (end === start ? '' : '\n') + String(messages[pending[end]].content);
      if (joined === content) return pending.slice(start, end + 1);
      if (!content.startsWith(joined)) break;
    }
  }
  return [];
}

/**
 * Shows an instruction the user just sent a running subagent, pending until
 * its delivery confirms it (handleTaskSteeringAccepted). The bubble joins the
 * task's transcript, not only its card: every stream write hands the card that
 * whole transcript, and the card keeps a longer list over a shorter one, so a
 * bubble only the card held froze the running message until the delivery.
 */
export function addPendingTaskInstruction({ taskId, inputId, content, subagentStateRefs, updateSubagentCard }: {
  taskId: string;
  inputId: string;
  content: string;
  subagentStateRefs: Record<string, TaskRefs>;
  updateSubagentCard: UpdateSubagentCard | null;
}): void {
  if (!taskId || !content || !updateSubagentCard) return;
  const taskRefs = getOrCreateTaskRefs({ subagentStateRefs }, taskId);
  taskRefs.messages = [...taskRefs.messages, {
    id: `pending-instruction-${Date.now()}-${Math.random().toString(36).substr(2, 6)}`,
    role: 'user',
    content,
    contentSegments: [{ type: 'text', content, order: 0 }],
    reasoningProcesses: {},
    toolCallProcesses: {},
    isPending: true,
    inputId,
  }];
  updateSubagentCard(taskId, { messages: taskRefs.messages });
}

/** An instruction the subagent was sent, as the frame that settles it names it. */
export interface SentInstruction {
  inputId?: string | null;
  content: string;
}

/**
 * The instruction a task lane's `steering_returned` frame hands back, or null
 * when it names no text. Producers send the text as top-level content;
 * messages[] is a defensive fallback only.
 */
export function returnedTaskInstruction(
  event: { content?: unknown; input_id?: unknown; messages?: unknown },
): SentInstruction | null {
  const fallback = Array.isArray(event.messages)
    ? (event.messages as Array<{ content?: string }>).map((m) => m?.content || '').filter(Boolean).join('\n')
    : '';
  const content = (typeof event.content === 'string' ? event.content : '') || fallback;
  if (!content) return null;
  return { inputId: typeof event.input_id === 'string' ? event.input_id : undefined, content };
}

/** Whether the instruction is still waiting on the subagent, neither delivered nor returned. */
export function hasPendingInstruction(taskRefs: TaskRefs, inputId: string): boolean {
  return pendingInstructionIndex(taskRefs.messages, inputId) !== -1;
}

/**
 * Drops the pending bubble of an instruction the subagent never received. A
 * frame with no id, from an entry an older producer queued, is matched by its
 * text, latest first.
 */
function withdrawPendingInstruction(taskRefs: TaskRefs, { inputId, content }: SentInstruction): void {
  const idx = inputId
    ? pendingInstructionIndex(taskRefs.messages, inputId)
    : taskRefs.messages.findLastIndex((m) => isPendingInstruction(m) && m.content === content);
  if (idx !== -1) taskRefs.messages = taskRefs.messages.filter((_, i) => i !== idx);
}

/**
 * Appends a notice to a task transcript as a message of its own rather than a
 * segment on the last assistant message: a failed task may have no assistant
 * message at all, and a pushed message survives that.
 */
export function pushTaskNotice(taskRefs: TaskRefs, text: string, detail?: string, inputId?: string | null): void {
  const order = ++taskRefs.contentOrderCounterRef.current;
  taskRefs.messages = [...taskRefs.messages, {
    id: `task-notice-${Date.now()}-${Math.random().toString(36).substr(2, 6)}`,
    role: 'assistant',
    content: '',
    contentSegments: [{ type: 'notification', content: text, order, detail }],
    reasoningProcesses: {},
    toolCallProcesses: {},
    ...(inputId ? { inputId } : {}),
  }];
}

/**
 * Takes back an instruction the subagent never received, whether the run
 * ended before taking it or the send itself failed: its pending bubble goes
 * and a notice carrying its text takes its place. The two travel together
 * because the card keeps a longer list over a shorter one, so a withdrawal
 * written on its own would never reach it.
 */
export function returnTaskInstruction(taskRefs: TaskRefs, instruction: SentInstruction, notice: string): void {
  // Once per instruction: a refused send and the run's own return can both
  // name it, and a reconnect re-sends a return history already placed. With
  // no id, a notice of the same text and no pending bubble for it means this
  // return is already shown; a second send of that text would still be
  // pending.
  const { inputId, content } = instruction;
  if (inputId && taskRefs.messages.some((m) => isTaskNotice(m) && m.inputId === inputId)) return;
  if (
    !inputId
    && !taskRefs.messages.some((m) => isPendingInstruction(m) && m.content === content)
    && taskRefs.messages.some((m) => isTaskNotice(m) && !m.inputId && noticeDetail(m) === content)
  ) return;
  withdrawPendingInstruction(taskRefs, instruction);
  pushTaskNotice(taskRefs, notice, instruction.content, inputId);
}
