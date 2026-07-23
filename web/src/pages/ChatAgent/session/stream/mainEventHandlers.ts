/**
 * Main-stream live event handlers — the half of the old streamEventHandlers
 * that drives the primary transcript. Every handler here writes through
 * `setMessages`; subagent (task-namespace) events never enter this module.
 */

import { normalizeAction } from '../../hooks/utils/eventUtils';
import { isToolResultFailure } from '../subagents/subagentStatus';
import type { MessageRecord, SetMessages, ToolCallRecord, ToolCallResultRecord, TodoPayload, HtmlWidgetData } from '../../hooks/utils/types';
import type { ProvenanceEvent } from '@/types/sse';
import type { ProvenanceRecord } from '@/types/chat';
import { provenanceEventToRecord, provenanceRecordKey } from './provenance';
import { extractLastReasoningTitle } from '../streamRefs';
import type { StreamRefs, ToolCallChunkRecord } from '../streamRefs';

/**
 * Handles reasoning signal events during streaming
 * @param {Object} params - Handler parameters
 * @param {string} params.assistantMessageId - ID of the assistant message being updated
 * @param {string} params.signalContent - Signal content ('start' or 'complete')
 * @param {Object} params.refs - Refs object with contentOrderCounterRef, currentReasoningIdRef
 * @param {Function} params.setMessages - State setter for messages
 * @returns {boolean} True if event was handled
 */
export function handleReasoningSignal({ assistantMessageId, signalContent, refs, setMessages, eventId }: {
  assistantMessageId: string;
  signalContent: string;
  refs: StreamRefs;
  setMessages: SetMessages;
  eventId?: number | null;
}): boolean {
  const { contentOrderCounterRef, currentReasoningIdRef } = refs;

  if (signalContent === 'start') {
    // Reasoning process has started - create new reasoning process
    const reasoningId = `reasoning-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    currentReasoningIdRef.current = reasoningId;
    const currentOrder = eventId != null ? eventId : ++contentOrderCounterRef.current;

    setMessages((prev: MessageRecord[]) =>
      prev.map((msg: MessageRecord) => {
        if (msg.id !== assistantMessageId) return msg;

        const newSegments = [
          ...((msg.contentSegments as unknown[]) || []),
          {
            type: 'reasoning',
            reasoningId,
            order: currentOrder,
          },
        ];

        const newReasoningProcesses = {
          ...((msg.reasoningProcesses as Record<string, unknown>) || {}),
          [reasoningId]: {
            content: '',
            isReasoning: true,
            reasoningComplete: false,
            order: currentOrder,
          },
        };

        return {
          ...msg,
          contentSegments: newSegments,
          reasoningProcesses: newReasoningProcesses,
        };
      })
    );
    return true;
  } else if (signalContent === 'complete') {
    // Reasoning process has completed - clear title so icon shows "Reasoning"
    if (currentReasoningIdRef.current) {
      const reasoningId = currentReasoningIdRef.current;
      setMessages((prev: MessageRecord[]) =>
        prev.map((msg: MessageRecord) => {
          if (msg.id !== assistantMessageId) return msg;

          const reasoningProcesses = { ...((msg.reasoningProcesses as Record<string, Record<string, unknown>>) || {}) };
          if (reasoningProcesses[reasoningId]) {
            reasoningProcesses[reasoningId] = {
              ...reasoningProcesses[reasoningId],
              isReasoning: false,
              reasoningComplete: true,
              reasoningTitle: null,
              _completedAt: refs.isReconnect ? 1 : Date.now(),
            };
          }

          return {
            ...msg,
            reasoningProcesses,
          };
        })
      );
      currentReasoningIdRef.current = null;
    }
    return true;
  }
  return false;
}

/**
 * Handles reasoning content chunks during streaming
 * @param {Object} params - Handler parameters
 * @param {string} params.assistantMessageId - ID of the assistant message being updated
 * @param {string} params.content - Reasoning content chunk
 * @param {Object} params.refs - Refs object with currentReasoningIdRef
 * @param {Function} params.setMessages - State setter for messages
 * @returns {boolean} True if event was handled
 */
export function handleReasoningContent({ assistantMessageId, content, refs, setMessages }: {
  assistantMessageId: string;
  content: string;
  refs: StreamRefs;
  setMessages: SetMessages;
}): boolean {
  const { currentReasoningIdRef } = refs;

  if (currentReasoningIdRef.current && content) {
    const reasoningId = currentReasoningIdRef.current;
    setMessages((prev: MessageRecord[]) =>
      prev.map((msg: MessageRecord) => {
        if (msg.id !== assistantMessageId) return msg;

        const reasoningProcesses = { ...((msg.reasoningProcesses as Record<string, Record<string, unknown>>) || {}) };
        if (reasoningProcesses[reasoningId]) {
          const newContent = ((reasoningProcesses[reasoningId].content as string) || '') + content;
          const reasoningTitle = extractLastReasoningTitle(newContent) ?? (reasoningProcesses[reasoningId].reasoningTitle as string | null) ?? null;
          reasoningProcesses[reasoningId] = {
            ...reasoningProcesses[reasoningId],
            content: newContent,
            isReasoning: true,
            reasoningTitle,
          };
        }

        return {
          ...msg,
          reasoningProcesses,
        };
      })
    );
    return true;
  }
  return false;
}

/**
 * Handles text content chunks during streaming
 * @param {Object} params - Handler parameters
 * @param {string} params.assistantMessageId - ID of the assistant message being updated
 * @param {string} params.content - Text content chunk
 * @param {string} params.finishReason - Optional finish reason
 * @param {Object} params.refs - Refs object with contentOrderCounterRef
 * @param {Function} params.setMessages - State setter for messages
 * @returns {boolean} True if event was handled
 */
export function handleTextContent({ assistantMessageId, content, finishReason, refs, setMessages, eventId }: {
  assistantMessageId: string;
  content: string;
  finishReason: string | undefined;
  refs: StreamRefs;
  setMessages: SetMessages;
  eventId?: number | null;
}): boolean {
  const { contentOrderCounterRef } = refs;

  // Handle finish_reason
  if (finishReason) {
    if (finishReason === 'tool_calls' && !content) {
      // Message is requesting tool calls, don't mark as complete yet
      return false; // Let tool_calls handler process this
    } else if (!content) {
      // Metadata chunk with finish_reason but no content. A "stopped" reason
      // (synthetic from a user stop, live or replayed) also stamps the message
      // so the per-message "⏹ Stopped" chip renders, and clears any in-flight
      // tool-call chunks so the "generating (~N chars)…" preparing row stops
      // shimmering (the partial tool call never completed and is discarded).
      const isStopped = finishReason === 'stopped';
      setMessages((prev: MessageRecord[]) =>
        prev.map((msg: MessageRecord) =>
          msg.id === assistantMessageId
            ? { ...msg, isStreaming: false, ...(isStopped ? { stopped: true, pendingToolCallChunks: {} } : {}) }
            : msg
        )
      );
      return true;
    }
    // If finish_reason exists but content also exists, continue to process content
  }

  // Process text content chunks
  if (content) {
    const currentOrder = eventId != null ? eventId : ++contentOrderCounterRef.current;

    setMessages((prev: MessageRecord[]) =>
      prev.map((msg: MessageRecord) => {
        if (msg.id !== assistantMessageId) return msg;

        const newSegments = [
          ...((msg.contentSegments as unknown[]) || []),
          {
            type: 'text',
            content,
            order: currentOrder,
          },
        ];

        const accumulatedText = ((msg.content as string) || '') + content;

        return {
          ...msg,
          contentSegments: newSegments,
          content: accumulatedText,
          contentType: 'text',
          isStreaming: true,
        };
      })
    );
    return true;
  } else if (finishReason) {
    // Message is complete (finish_reason present with no content means end of stream)
    setMessages((prev: MessageRecord[]) =>
      prev.map((msg: MessageRecord) =>
        msg.id === assistantMessageId
          ? { ...msg, isStreaming: false }
          : msg
      )
    );
    return true;
  }
  return false;
}

/**
 * Handles tool_calls events during streaming
 * @param {Object} params - Handler parameters
 * @param {string} params.assistantMessageId - ID of the assistant message being updated
 * @param {Array} params.toolCalls - Array of tool call objects
 * @param {string} params.finishReason - Optional finish reason
 * @param {Object} params.refs - Refs object with contentOrderCounterRef
 * @param {Function} params.setMessages - State setter for messages
 * @returns {boolean} True if event was handled
 */
export function handleToolCalls({ assistantMessageId, toolCalls, finishReason: _finishReason, refs, setMessages, eventId }: {
  assistantMessageId: string;
  toolCalls: ToolCallRecord[];
  finishReason: string | undefined;
  refs: StreamRefs;
  setMessages: SetMessages;
  eventId?: number | null;
}): boolean {
  const { contentOrderCounterRef } = refs;

  if (!toolCalls || !Array.isArray(toolCalls)) {
    return false;
  }

  // Track creation times outside React state so handleToolCallResult can read them synchronously
  if (!refs._toolCreatedAt) refs._toolCreatedAt = {};

  toolCalls.forEach((toolCall: ToolCallRecord, toolIndex: number) => {
    const toolCallId = toolCall.id;

    if (toolCallId) {
      if (!refs.isReconnect && !refs._toolCreatedAt![toolCallId]) {
        refs._toolCreatedAt![toolCallId] = Date.now();
      }
      setMessages((prev: MessageRecord[]) =>
        prev.map((msg: MessageRecord) => {
          if (msg.id !== assistantMessageId) return msg;

          const toolCallProcesses = { ...((msg.toolCallProcesses as Record<string, Record<string, unknown>>) || {}) };
          const contentSegments = [...((msg.contentSegments as Record<string, unknown>[]) || [])];

          let currentOrder: number;

          if (!toolCallProcesses[toolCallId]) {
            currentOrder = eventId != null
              ? eventId + toolIndex * 0.01
              : ++contentOrderCounterRef.current;

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
              _createdAt: refs.isReconnect ? 1 : Date.now(),
              order: currentOrder,
            };
          } else {
            currentOrder = toolCallProcesses[toolCallId].order as number;
            toolCallProcesses[toolCallId] = {
              ...toolCallProcesses[toolCallId],
              toolName: toolCall.name,
              toolCall: toolCall,
              isInProgress: true,
            };
          }

          // If this tool is the Task tool (subagent spawner), also create a subagent_task segment
          // Mirrors historyEventHandlers.js logic for consistency
          const subagentTasks = { ...((msg.subagentTasks as Record<string, Record<string, unknown>>) || {}) };
          const isTaskTool = toolCall.name === 'task' || toolCall.name === 'Task';
          const action = normalizeAction((toolCall.args?.action as string) || (toolCall.args?.task_id ? 'resume' : 'init'));
          const isNewSpawn = action === 'init';
          if (isTaskTool && toolCallId && isNewSpawn) {
            const subagentId = toolCallId;
            const hasExistingSubagentSegment = contentSegments.some(
              (s: Record<string, unknown>) => s.type === 'subagent_task' && s.subagentId === subagentId
            );

            if (!hasExistingSubagentSegment) {
              contentSegments.push({
                type: 'subagent_task',
                subagentId,
                order: currentOrder,
              });
            }

            subagentTasks[subagentId] = {
              ...(subagentTasks[subagentId] || {}),
              subagentId,
              description: (toolCall.args?.description as string) || '',
              prompt: (toolCall.args?.prompt as string) || (toolCall.args?.description as string) || '',
              type: (toolCall.args?.subagent_type as string) || 'general-purpose',
              action: 'init',
              status: 'running',
            };
          } else if (isTaskTool && toolCallId && !isNewSpawn) {
            // Resume/follow-up call — show a new card with "resumed" indicator
            // Normalize to "task:xxx" format to match floating card keys
            const rawTargetId = (toolCall.args?.task_id as string) || '';
            const resumeTargetId = rawTargetId.startsWith('task:') ? rawTargetId : `task:${rawTargetId}`;
            contentSegments.push({
              type: 'subagent_task',
              subagentId: toolCallId,
              resumeTargetId,
              order: currentOrder,
            });
            subagentTasks[toolCallId] = {
              subagentId: toolCallId,
              resumeTargetId,
              description: (toolCall.args?.description as string) || '',
              prompt: (toolCall.args?.prompt as string) || (toolCall.args?.description as string) || '',
              type: (toolCall.args?.subagent_type as string) || 'general-purpose',
              action,
              status: 'running',
            };
          }

          return {
            ...msg,
            contentSegments,
            toolCallProcesses,
            subagentTasks,
            pendingToolCallChunks: {},
          };
        })
      );
    }
  });

  return true;
}

/**
 * Handles tool_call_result events during streaming
 * @param {Object} params - Handler parameters
 * @param {string} params.assistantMessageId - ID of the assistant message being updated
 * @param {string} params.toolCallId - ID of the tool call
 * @param {Object} params.result - Tool call result object
 * @param {Object} params.refs - Refs object with currentToolCallIdRef
 * @param {Function} params.setMessages - State setter for messages
 * @returns {boolean} True if event was handled
 */
export function handleToolCallResult({ assistantMessageId, toolCallId, result, refs, setMessages }: {
  assistantMessageId: string;
  toolCallId: string;
  result: ToolCallResultRecord;
  refs: StreamRefs;
  setMessages: SetMessages;
}): boolean {
  const { currentToolCallIdRef } = refs;

  if (!toolCallId) {
    return false;
  }

  setMessages((prev: MessageRecord[]) =>
    prev.map((msg: MessageRecord) => {
      if (msg.id !== assistantMessageId) return msg;

      const toolCallProcesses = { ...((msg.toolCallProcesses as Record<string, Record<string, unknown>>) || {}) };

      const isFailed = isToolResultFailure(result);

      // Track subagent task status updates
      const subagentTasks = { ...((msg.subagentTasks as Record<string, Record<string, unknown>>) || {}) };

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
        // Orphaned tool_call_result without matching tool_calls (e.g., SubmitPlan
        // result arriving in a HITL resume stream). Skip silently.
        return msg;
      }

      // If this toolCallId is associated with a subagent task, store the tool call result.
      // A SUCCESSFUL Task returns immediately ("Task-N started in background") while the
      // subagent keeps running, so its result is NOT terminal — real completion comes via
      // the per-task SSE stream closing. But a FAILED spawn (admission/setup error — a bare
      // "Error: …" result) never produces a task artifact or a channel, so no chan_close
      // will ever arrive to settle it; stamp it 'error' here or the placeholder spins forever.
      if (subagentTasks[toolCallId]) {
        subagentTasks[toolCallId] = {
          ...subagentTasks[toolCallId],
          toolCallResult: result.content,
          ...(isFailed ? { status: 'error' } : {}),
        };
      }

      return { ...msg, toolCallProcesses, subagentTasks };
    })
  );

  // Reset current tool call ID after result is received
  if (currentToolCallIdRef.current === toolCallId) {
    currentToolCallIdRef.current = null;
  }

  return true;
}

/**
 * Handles provenance events on both the live-stream and history-replay paths
 * (replay callers resolve `assistantMessageId` from the event's `turn_index`).
 *
 * Accumulates the accessed-data record onto the assistant message's
 * `provenanceRecords` map. The provenance event is flat (fields top-level on
 * the event), mirroring the live `tool_call_result` reader. Records are keyed
 * by `provenanceRecordKey` so multiple web_search URLs sharing one
 * `tool_call_id` never collide.
 *
 * @param {Object} params - Handler parameters
 * @param {string} params.assistantMessageId - ID of the assistant message being updated
 * @param {Object} params.event - The flat provenance event
 * @param {Function} params.setMessages - State setter for messages
 * @returns {boolean} True if event was handled
 */
export function handleProvenance({ assistantMessageId, event, setMessages }: {
  assistantMessageId: string;
  event: ProvenanceEvent;
  setMessages: SetMessages;
}): boolean {
  if (!event || !event.record_id) {
    return false;
  }

  const record = provenanceEventToRecord(event);
  const key = provenanceRecordKey(record);

  setMessages((prev: MessageRecord[]) =>
    prev.map((msg: MessageRecord) => {
      if (msg.id !== assistantMessageId) return msg;

      const provenanceRecords = {
        ...((msg.provenanceRecords as Record<string, ProvenanceRecord>) || {}),
        [key]: record,
      };

      return { ...msg, provenanceRecords };
    })
  );

  return true;
}

/**
 * Handles artifact events with artifact_type: "todo_update" during streaming
 * @param {Object} params - Handler parameters
 * @param {string} params.assistantMessageId - ID of the assistant message being updated
 * @param {string} params.artifactType - Type of artifact ("todo_update")
 * @param {string} params.artifactId - ID of the artifact
 * @param {Object} params.payload - Payload containing todos array and status counts
 * @param {Object} params.refs - Refs object with contentOrderCounterRef
 * @param {Function} params.setMessages - State setter for messages
 * @returns {boolean} True if event was handled
 */
export function handleTodoUpdate({ assistantMessageId, artifactType, artifactId, payload, refs, setMessages, eventId }: {
  assistantMessageId: string;
  artifactType: string;
  artifactId: string;
  payload: TodoPayload | null;
  refs: StreamRefs;
  setMessages: SetMessages;
  eventId?: number | null;
}): boolean {
  const { contentOrderCounterRef, updateTodoListCard, isNewConversation } = refs;

  // Only handle todo_update artifacts
  if (artifactType !== 'todo_update' || !payload) {
    return false;
  }

  const { total, completed, in_progress, pending } = payload;
  const todos = Array.isArray(payload.todos) ? payload.todos : [];

  // Update floating card with todo list data (only during live streaming, not history)
  // Do this before setMessages to ensure we have the latest data
  // Always update the card if updateTodoListCard is available, even if todos array is empty
  // This ensures the card persists and shows the latest state
  if (updateTodoListCard) {
    updateTodoListCard(
      {
        todos,
        total: total || 0,
        completed: completed || 0,
        in_progress: in_progress || 0,
        pending: pending || 0,
      },
      isNewConversation || false
    );
  }

  // Use artifactId as the base todoListId to track updates to the same logical todo list
  // But create a unique segmentId for each event to preserve chronological order
  const baseTodoListId = artifactId || `todo-list-base-${Date.now()}`;
  // Create a unique segment ID that includes timestamp to ensure chronological ordering
  const segmentId = `${baseTodoListId}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

  setMessages((prev: MessageRecord[]) => {
    const updated = prev.map((msg: MessageRecord) => {
      if (msg.id !== assistantMessageId) return msg;

      const todoListProcesses = { ...((msg.todoListProcesses as Record<string, unknown>) || {}) };
      const contentSegments = [...((msg.contentSegments as Record<string, unknown>[]) || [])];

      // Always create a new segment for each todo_update event to preserve chronological order
      const currentOrder = eventId != null ? eventId : ++contentOrderCounterRef.current;

      // Add new segment at the current chronological position
      contentSegments.push({
        type: 'todo_list',
        todoListId: segmentId, // Use unique segmentId for this specific event
        order: currentOrder,
      });

      // Store the todo list data with the segmentId
      // If this is an update to an existing logical todo list (same artifactId),
      // we still create a new segment but can reference the base ID for data updates
      todoListProcesses[segmentId] = {
        todos,
        total: total || 0,
        completed: completed || 0,
        in_progress: in_progress || 0,
        pending: pending || 0,
        order: currentOrder,
        baseTodoListId: baseTodoListId, // Keep reference to base ID for potential future use
      };

      const updatedMsg: MessageRecord = {
        ...msg,
        contentSegments,
        todoListProcesses,
      };
      return updatedMsg;
    });
    return updated;
  });

  return true;
}

/**
 * Handles artifact events with artifact_type: "html_widget" during streaming.
 * Creates a content segment for inline rendering of interactive HTML widgets.
 */
export function handleHtmlWidget({ assistantMessageId, artifactType, artifactId, payload, refs, setMessages, eventId }: {
  assistantMessageId: string;
  artifactType: string;
  artifactId: string;
  payload: HtmlWidgetData | null;
  refs: StreamRefs;
  setMessages: SetMessages;
  eventId?: number | null;
}): boolean {
  const { contentOrderCounterRef } = refs;

  if (artifactType !== 'html_widget' || !payload) {
    return false;
  }

  const { html, title } = payload;
  const segmentId = `widget-${artifactId}`;

  setMessages((prev: MessageRecord[]) => {
    const updated = prev.map((msg: MessageRecord) => {
      if (msg.id !== assistantMessageId) return msg;

      const htmlWidgetProcesses = { ...((msg.htmlWidgetProcesses as Record<string, HtmlWidgetData>) || {}) };
      const contentSegments = [...((msg.contentSegments as Record<string, unknown>[]) || [])];

      // Prevent duplicates (e.g. on SSE reconnect replay)
      const segmentExists = contentSegments.some((s: Record<string, unknown>) => s.widgetId === segmentId);
      if (segmentExists) return msg;

      const currentOrder = eventId != null ? eventId : ++contentOrderCounterRef.current;

      contentSegments.push({
        type: 'html_widget',
        widgetId: segmentId,
        order: currentOrder,
      });

      const widgetEntry: HtmlWidgetData = {
        html: html || '',
        title: title || '',
      };
      if (payload.data) {
        widgetEntry.data = payload.data;
      }
      htmlWidgetProcesses[segmentId] = widgetEntry;

      return {
        ...msg,
        contentSegments,
        htmlWidgetProcesses,
      };
    });
    return updated;
  });

  return true;
}

/**
 * Handles tool_call_chunks events during streaming.
 * Tracks pending tool call chunks so the UI can show a "preparing" indicator
 * while the LLM is still generating tool call arguments.
 * @param {Object} params - Handler parameters
 * @param {string} params.assistantMessageId - ID of the assistant message being updated
 * @param {Array} params.chunks - Array of tool_call_chunk objects
 * @param {Function} params.setMessages - State setter for messages
 */
export function handleToolCallChunks({ assistantMessageId, chunks, setMessages }: {
  assistantMessageId: string;
  chunks: ToolCallChunkRecord[];
  setMessages: SetMessages;
}): void {
  if (!chunks || !Array.isArray(chunks)) return;

  chunks.forEach((chunk: ToolCallChunkRecord) => {
    const key = `${chunk.index ?? 0}`;

    setMessages((prev: MessageRecord[]) =>
      prev.map((msg: MessageRecord) => {
        if (msg.id !== assistantMessageId) return msg;
        const pending = { ...((msg.pendingToolCallChunks as Record<string, Record<string, unknown>>) || {}) };
        const existing = pending[key] || { toolName: null, chunkCount: 0, argsLength: 0, firstSeenAt: Date.now() };

        pending[key] = {
          toolName: chunk.name || existing.toolName,
          chunkCount: (existing.chunkCount as number) + 1,
          argsLength: (existing.argsLength as number) + (chunk.args?.length || 0),
          firstSeenAt: existing.firstSeenAt,
        };

        return { ...msg, pendingToolCallChunks: pending };
      })
    );
  });
}

/**
 * Checks if an event is from a subagent.
 * Backend convention:
 * - Main agent: agent.startsWith("model:")
 * - Tool node: agent === "tools"
 * - Subagent: agent contains ":" but does NOT start with "model:" and is NOT "tools"
 * Subagent format: agent_id = "{subagent_type}:{uuid4}" (e.g., "research:550e8400-...")
 * @param {Object} event - Event object
 * @returns {boolean} True if event is from subagent
 */
