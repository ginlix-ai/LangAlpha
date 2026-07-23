/**
 * Subagent live event handlers — the task-namespace half of the old
 * streamEventHandlers. Every handler here writes through `updateSubagentCard`;
 * none touches the main transcript's `setMessages`.
 */

import { isToolResultFailure } from './subagentStatus';
import type { MessageRecord, ToolCallRecord, ToolCallResultRecord } from '../../hooks/utils/types';
import { getOrCreateTaskRefs, extractLastReasoningTitle } from '../streamRefs';
import type { StreamRefs, ToolCallChunkRecord, UpdateSubagentCard } from '../streamRefs';

export function isSubagentEvent(event: Record<string, unknown> | null | undefined): boolean {
  const agent = event?.agent;
  if (!agent || typeof agent !== 'string') {
    return false;
  }
  return agent.startsWith('task:');
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
 * @returns {boolean} True if event was handled
 */
export function handleSubagentMessageChunk({
  taskId,
  assistantMessageId,
  contentType,
  content,
  finishReason,
  refs,
  updateSubagentCard
}: {
  taskId: string;
  assistantMessageId: string;
  contentType: string;
  content: string;
  finishReason: string | undefined;
  refs: StreamRefs;
  updateSubagentCard: UpdateSubagentCard;
}): boolean {
  if (!taskId || !assistantMessageId || !updateSubagentCard) {
    return false;
  }

  const taskRefs = getOrCreateTaskRefs(refs, taskId);
  const { contentOrderCounterRef, currentReasoningIdRef } = taskRefs;

  // Handle finishReason with no content — model call complete
  if (finishReason && !content && contentType !== 'reasoning_signal') {
    if (finishReason === 'tool_calls') {
      return false; // More work coming, let tool_calls handler process
    }
    // finish_reason: "stop" — subagent's model call is done
    const updatedMessages = [...taskRefs.messages];
    const msgIdx = updatedMessages.findIndex((m: MessageRecord) => m.id === assistantMessageId);
    if (msgIdx !== -1) {
      updatedMessages[msgIdx] = { ...updatedMessages[msgIdx], isStreaming: false };
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
            reasoningProcesses[reasoningId] = {
              ...reasoningProcesses[reasoningId],
              isReasoning: false,
              reasoningComplete: true,
              reasoningTitle: null,
              _completedAt: refs.isReconnect ? 1 : Date.now(),
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

    const reasoningTitle = extractLastReasoningTitle(newContent) ?? (reasoningProcesses[reasoningId].reasoningTitle as string | null) ?? null;
    reasoningProcesses[reasoningId] = {
      ...reasoningProcesses[reasoningId],
      content: newContent,
      isReasoning: true,
      reasoningTitle,
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
      });
      messageIndex = updatedMessages.length - 1;
    }

    // Replace with a fresh object: ``MessageBubble`` is ``React.memo``'d
    // on the message ref, so mutation would skip per-token re-renders and
    // content would only land at the next lifecycle event.
    const prev = updatedMessages[messageIndex];
    updatedMessages[messageIndex] = {
      ...prev,
      contentSegments: [
        ...((prev.contentSegments as unknown[]) || []),
        { type: 'text', content, order: currentOrder },
      ],
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
  const pending = { ...((msg.pendingToolCallChunks as Record<string, Record<string, unknown>>) || {}) };

  chunks.forEach((chunk: ToolCallChunkRecord) => {
    const key = `${chunk.index ?? 0}`;
    const existing = pending[key] || { toolName: null, chunkCount: 0, argsLength: 0, firstSeenAt: Date.now() };
    pending[key] = {
      toolName: chunk.name || existing.toolName,
      chunkCount: (existing.chunkCount as number) + 1,
      argsLength: (existing.argsLength as number) + (chunk.args?.length || 0),
      firstSeenAt: existing.firstSeenAt,
    };
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
 * @returns {boolean} True if event was handled
 */
export function handleSubagentToolCalls({ taskId, assistantMessageId, toolCalls, refs, updateSubagentCard }: {
  taskId: string;
  assistantMessageId: string;
  toolCalls: ToolCallRecord[];
  refs: StreamRefs;
  updateSubagentCard: UpdateSubagentCard;
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
        });
        messageIndex = updatedMessages.length - 1;
      }

      const msg = updatedMessages[messageIndex];
      const toolCallProcesses = { ...((msg.toolCallProcesses as Record<string, Record<string, unknown>>) || {}) };
      const contentSegments = [...((msg.contentSegments as Record<string, unknown>[]) || [])];

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
      // Clear pending chunks now that the final tool_calls event has arrived
      msg.pendingToolCallChunks = {};
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

    const isFailed = isToolResultFailure(result);

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
 * @param {Object} params.refs - Refs object with subagentStateRefs
 * @param {Function} params.updateSubagentCard - Callback to update subagent card
 * @returns {boolean} True if event was handled
 */
export function handleTaskSteeringAccepted({ taskId, content, refs, updateSubagentCard }: {
  taskId: string;
  content: string;
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

  // Confirm an optimistic pending message if it matches, otherwise insert new one
  const pendingIdx = updatedMessages.findIndex(
    (m: MessageRecord) => m.role === 'user' && m.isPending && m.content === content
  );
  if (pendingIdx !== -1) {
    updatedMessages[pendingIdx] = { ...updatedMessages[pendingIdx], isPending: false };
  } else {
    updatedMessages.push({
      id: `followup-${Date.now()}-${Math.random().toString(36).substr(2, 6)}`,
      role: 'user',
      content,
      contentSegments: [{ type: 'text', content, order: 0 }],
      reasoningProcesses: {},
      toolCallProcesses: {},
    });
  }

  // Bump runIndex so the next event creates a new assistant message below the user bubble
  taskRefs.runIndex = (taskRefs.runIndex || 0) + 1;
  taskRefs.contentOrderCounterRef.current = 0;
  taskRefs.currentReasoningIdRef.current = null;
  taskRefs.currentToolCallIdRef.current = null;

  taskRefs.messages = updatedMessages;
  updateSubagentCard(taskId, { messages: updatedMessages });
  return true;
}

