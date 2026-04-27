/**
 * History replay event handlers
 * Handles events from history replay (SSE stream of past conversations)
 */

import { normalizeAction } from './eventUtils';
import type { MessageRecord, SetMessages, ToolCallRecord, ToolCallResultRecord, TodoPayload, HtmlWidgetData } from './types';

let _steeringIdCounter = 0;

/** Per-pair mutable state tracked during history replay. */
interface PairState {
  contentOrderCounter: number;
  reasoningId: string | null;
  toolCallId: string | null;
}

/** Shape of an SSE history event. */
interface HistoryEvent {
  agent?: string;
  content?: string;
  timestamp?: string | number;
  metadata?: Record<string, unknown>;
  messages?: Array<Record<string, unknown>>;
  [key: string]: unknown;
}

/** Refs passed to history user message handler. */
interface HistoryUserMessageRefs {
  recentlySentTracker: { isRecentlySent: (content: string) => boolean };
  currentMessageRef: { current: string | null };
  newMessagesStartIndexRef: { current: number };
  historyMessagesRef: { current: Set<string> };
  [key: string]: unknown;
}

/** Refs passed to history steering delivered handler. */
interface HistorySteeringRefs {
  newMessagesStartIndexRef: { current: number };
  historyMessagesRef: { current: Set<string> };
  [key: string]: unknown;
}

/**
 * Helper to check if an event is from a subagent.
 * Backend convention: agent field uses "task:{task_id}" format (e.g., "task:pkyRHQ").
 * This aligns with LangGraph namespace convention (tools:uuid, model:uuid, task:id).
 */
export function isSubagentHistoryEvent(event: HistoryEvent | null | undefined): boolean {
  const agent = event?.agent;
  if (!agent || typeof agent !== 'string') {
    return false;
  }
  return agent.startsWith('task:');
}

/** Handles user_message events from history replay. */
export function handleHistoryUserMessage({
  event,
  pairIndex,
  assistantMessagesByPair,
  pairStateByPair,
  refs,
  messages: _messages,
  setMessages,
}: {
  event: HistoryEvent;
  pairIndex: number;
  assistantMessagesByPair: Map<number, string>;
  pairStateByPair: Map<number, PairState>;
  refs: HistoryUserMessageRefs;
  messages: MessageRecord[];
  setMessages: SetMessages;
}): boolean {
  const { recentlySentTracker, currentMessageRef, newMessagesStartIndexRef, historyMessagesRef } = refs;

  // Check if this is a new pair (not already processed)
  if (assistantMessagesByPair.has(pairIndex)) {
    return false;
  }

  const messageContent = (event.content || '').trim();

  // Check if this message was recently sent (to avoid duplicates)
  const isDuplicate = messageContent && recentlySentTracker.isRecentlySent(messageContent);

  if (isDuplicate) {
    // Check if we're currently streaming a message
    if (currentMessageRef.current) {
      // Initialize pair state for history replay to work correctly
      if (!pairStateByPair.has(pairIndex)) {
        pairStateByPair.set(pairIndex, {
          contentOrderCounter: 0,
          reasoningId: null,
          toolCallId: null,
        });
      }
      // Map turn_index to the streaming assistant message ID
      assistantMessagesByPair.set(pairIndex, currentMessageRef.current);
      return true;
    }
    // If no active streaming, we'll create assistant message below
  } else {
    // Initialize state for this pair
    pairStateByPair.set(pairIndex, {
      contentOrderCounter: 0,
      reasoningId: null,
      toolCallId: null,
    });

    // Create user message bubble (skip for empty content, HITL resume pairs,
    // and system queries like report-back invocations)
    const isSystemQuery = (event as Record<string, unknown>).query_type === 'system';
    if (messageContent && !isSystemQuery) {
      const currentUserMessageId = `history-user-${pairIndex}-${Date.now()}`;
      const userMessage: MessageRecord = {
        id: currentUserMessageId,
        role: 'user',
        content: event.content,
        contentType: 'text',
        timestamp: event.timestamp ? new Date(event.timestamp as string | number) : new Date(),
        isStreaming: false,
        isHistory: true,
      };

      // Restore attachment metadata from persisted query metadata
      const attachments = event.metadata?.attachments as unknown[] | undefined;
      if (attachments && attachments.length > 0) {
        userMessage.attachments = event.metadata!.attachments;
      }

      // Restore widget snapshot metadata so the chip deck re-renders below
      // this user message on history replay. Backend persists `text` so the
      // preview modal can show what the agent saw; `image_jpeg_data_url` is
      // live-only (image rides the multimodal channel and isn't persisted).
      const widgetContexts = event.metadata?.widget_contexts as unknown[] | undefined;
      if (widgetContexts && widgetContexts.length > 0) {
        userMessage.widgetSnapshots = event.metadata!.widget_contexts;
      }

      setMessages((prev: MessageRecord[]) => {
        const insertIndex = newMessagesStartIndexRef.current;
        const newMessages = [
          ...prev.slice(0, insertIndex),
          userMessage,
          ...prev.slice(insertIndex),
        ];
        historyMessagesRef.current.add(currentUserMessageId);
        newMessagesStartIndexRef.current = insertIndex + 1;
        return newMessages;
      });
    }
  }

  // Always create assistant message placeholder for this pair
  if (!assistantMessagesByPair.has(pairIndex)) {
    // Initialize state for this pair if not already done
    if (!pairStateByPair.has(pairIndex)) {
      pairStateByPair.set(pairIndex, {
        contentOrderCounter: 0,
        reasoningId: null,
        toolCallId: null,
      });
    }

    // Create assistant message placeholder
    const currentAssistantMessageId = `history-assistant-${pairIndex}-${Date.now()}`;
    assistantMessagesByPair.set(pairIndex, currentAssistantMessageId);

    const assistantMessage: MessageRecord = {
      id: currentAssistantMessageId,
      role: 'assistant',
      content: '',
      contentType: 'text',
      timestamp: event.timestamp ? new Date(event.timestamp as string | number) : new Date(),
      isStreaming: false,
      isHistory: true,
      contentSegments: [],
      reasoningProcesses: {},
      toolCallProcesses: {},
    };

    setMessages((prev: MessageRecord[]) => {
      const insertIndex = newMessagesStartIndexRef.current;
      const newMessages = [
        ...prev.slice(0, insertIndex),
        assistantMessage,
        ...prev.slice(insertIndex),
      ];
      historyMessagesRef.current.add(currentAssistantMessageId);
      newMessagesStartIndexRef.current = insertIndex + 1;
      return newMessages;
    });

    return true;
  }

  return false;
}

/** Handles reasoning signal events ('start' | 'complete') in history replay. */
export function handleHistoryReasoningSignal({ assistantMessageId, signalContent, pairIndex, pairState, setMessages, eventId }: {
  assistantMessageId: string;
  signalContent: string;
  pairIndex: number;
  pairState: PairState;
  setMessages: SetMessages;
  eventId?: number | null;
}): boolean {
  if (signalContent === 'start') {
    const reasoningId = `history-reasoning-${pairIndex}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    pairState.reasoningId = reasoningId;
    const currentOrder = eventId != null ? eventId : ++pairState.contentOrderCounter;

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
            isReasoning: false, // History: already complete
            reasoningComplete: true,
            order: currentOrder,
            _completedAt: 1,
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
    if (pairState.reasoningId) {
      const reasoningId = pairState.reasoningId;
      setMessages((prev: MessageRecord[]) =>
        prev.map((msg: MessageRecord) => {
          if (msg.id !== assistantMessageId) return msg;

          const reasoningProcesses = { ...((msg.reasoningProcesses as Record<string, Record<string, unknown>>) || {}) };
          if (reasoningProcesses[reasoningId]) {
            reasoningProcesses[reasoningId] = {
              ...reasoningProcesses[reasoningId],
              isReasoning: false,
              reasoningComplete: true,
              _completedAt: 1,
            };
          }

          return {
            ...msg,
            reasoningProcesses,
          };
        })
      );
      pairState.reasoningId = null;
    }
    return true;
  }
  return false;
}

/** Handles reasoning content chunks in history replay. */
export function handleHistoryReasoningContent({ assistantMessageId, content, pairState, setMessages }: {
  assistantMessageId: string;
  content: string;
  pairState: PairState;
  setMessages: SetMessages;
}): boolean {
  if (content && pairState.reasoningId) {
    const reasoningId = pairState.reasoningId;
    setMessages((prev: MessageRecord[]) =>
      prev.map((msg: MessageRecord) => {
        if (msg.id !== assistantMessageId) return msg;

        const reasoningProcesses = { ...((msg.reasoningProcesses as Record<string, Record<string, unknown>>) || {}) };
        if (reasoningProcesses[reasoningId]) {
          reasoningProcesses[reasoningId] = {
            ...reasoningProcesses[reasoningId],
            content: ((reasoningProcesses[reasoningId].content as string) || '') + content,
            isReasoning: false,
            reasoningComplete: true,
            _completedAt: 1,
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

/** Handles text content chunks and finish_reason in history replay. */
export function handleHistoryTextContent({ assistantMessageId, content, finishReason, pairState, setMessages, eventId }: {
  assistantMessageId: string;
  content: string;
  finishReason: string | undefined;
  pairState: PairState;
  setMessages: SetMessages;
  eventId?: number | null;
}): boolean {
  if (content) {
    const currentOrder = eventId != null ? eventId : ++pairState.contentOrderCounter;

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
        };
      })
    );
    return true;
  } else if (finishReason) {
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

/** Handles tool_calls events in history replay. */
export function handleHistoryToolCalls({ assistantMessageId, toolCalls, pairState, setMessages, eventId }: {
  assistantMessageId: string;
  toolCalls: ToolCallRecord[];
  pairState: PairState;
  setMessages: SetMessages;
  eventId?: number | null;
}): boolean {
  if (!toolCalls || !Array.isArray(toolCalls)) {
    return false;
  }

  toolCalls.forEach((toolCall: ToolCallRecord, toolIndex: number) => {
    const toolCallId = toolCall.id;

    if (toolCallId) {
      const currentOrder = eventId != null ? eventId + toolIndex * 0.01 : ++pairState.contentOrderCounter;

      setMessages((prev: MessageRecord[]) =>
        prev.map((msg: MessageRecord) => {
          if (msg.id !== assistantMessageId) return msg;

          const toolCallProcesses = { ...((msg.toolCallProcesses as Record<string, Record<string, unknown>>) || {}) };
          const contentSegments = [...((msg.contentSegments as Record<string, unknown>[]) || [])];
          const subagentTasks = { ...((msg.subagentTasks as Record<string, Record<string, unknown>>) || {}) };

          // Standard tool_call segment/process
          if (!toolCallProcesses[toolCallId]) {
            contentSegments.push({
              type: 'tool_call',
              toolCallId,
              order: currentOrder,
            });

            toolCallProcesses[toolCallId] = {
              toolName: toolCall.name,
              toolCall: toolCall,
              toolCallResult: null,
              isInProgress: false, // History: already complete
              isComplete: false,
              order: currentOrder,
            };
          } else {
            toolCallProcesses[toolCallId] = {
              ...toolCallProcesses[toolCallId],
              toolName: toolCall.name,
              toolCall: toolCall,
            };
          }

          // If this tool is the Task tool (subagent spawner), also create a subagent_task segment
          // Backend uses PascalCase "Task"; accept both for compatibility
          const isTaskTool = toolCall.name === 'task' || toolCall.name === 'Task';
          const action = normalizeAction((toolCall.args?.action as string) || (toolCall.args?.task_id ? 'resume' : 'init'));
          const isNewSpawn = action === 'init';
          if (isTaskTool && toolCallId && isNewSpawn) {
            const subagentId = toolCallId;
            // Only add the segment once per subagentId
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

            // Initialize or update subagent task metadata
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
          };
        })
      );
    }
  });

  return true;
}

/** Handles tool_call_result events in history replay. */
export function handleHistoryToolCallResult({ assistantMessageId, toolCallId, result, pairState: _pairState, setMessages }: {
  assistantMessageId: string;
  toolCallId: string;
  result: ToolCallResultRecord;
  pairState: PairState;
  setMessages: SetMessages;
}): boolean {
  if (!toolCallId) {
    return false;
  }

  setMessages((prev: MessageRecord[]) =>
    prev.map((msg: MessageRecord) => {
      if (msg.id !== assistantMessageId) return msg;

      const toolCallProcesses = { ...((msg.toolCallProcesses as Record<string, Record<string, unknown>>) || {}) };
      const subagentTasks = { ...((msg.subagentTasks as Record<string, Record<string, unknown>>) || {}) };

      // Tool call failed only if content starts with "ERROR" (backend convention)
      const resultContent = (result.content as string) || '';
      const isFailed = typeof resultContent === 'string' && resultContent.trim().startsWith('ERROR');

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
          isFailed: isFailed, // Track if tool call failed
        };
      } else {
        // Orphaned tool_call_result without matching tool_calls (e.g., SubmitPlan
        // result in a HITL resume pair). Skip silently.
        return msg;
      }

      // If this toolCallId is associated with a subagent task, mark it as completed
      // and propagate description from artifact if the inline card's description is empty
      if (subagentTasks[toolCallId]) {
        // Don't set status: 'completed' here — the Task tool returns immediately
        // after spawning, so its tool_call_result doesn't mean the subagent finished.
        // Final status is set by markAllSubagentTasksCompleted() when workflow ends.
        subagentTasks[toolCallId] = {
          ...subagentTasks[toolCallId],
          result: result.content,
        };
      }

      return {
        ...msg,
        toolCallProcesses,
        subagentTasks,
      };
    })
  );

  return true;
}

/**
 * Handles steering_delivered events in history replay. Creates user bubble(s)
 * for each steering message, then a new assistant placeholder so subsequent
 * events render in a fresh assistant bubble.
 */
export function handleHistorySteeringDelivered({
  event,
  pairIndex,
  assistantMessagesByPair,
  pairStateByPair,
  refs,
  setMessages,
}: {
  event: HistoryEvent;
  pairIndex: number;
  assistantMessagesByPair: Map<number, string>;
  pairStateByPair: Map<number, PairState>;
  refs: HistorySteeringRefs;
  setMessages: SetMessages;
}): void {
  const { newMessagesStartIndexRef, historyMessagesRef } = refs;
  const steeringMessages = (event.messages || []) as Array<Record<string, unknown>>;

  // Create user message bubble(s) for each steering message
  const batchId = ++_steeringIdCounter;
  for (let sIdx = 0; sIdx < steeringMessages.length; sIdx++) {
    const qMsg = steeringMessages[sIdx];
    if (!qMsg.content) continue;
    const userMsgId = `history-steering-user-${pairIndex}-${batchId}-${sIdx}`;
    const userMessage: MessageRecord = {
      id: userMsgId,
      role: 'user',
      content: qMsg.content,
      contentType: 'text',
      timestamp: qMsg.timestamp ? new Date((qMsg.timestamp as number) * 1000) : new Date(),
      isStreaming: false,
      isHistory: true,
      steeringDelivered: true,
    };
    setMessages((prev: MessageRecord[]) => {
      const idx = newMessagesStartIndexRef.current;
      const next = [...prev.slice(0, idx), userMessage, ...prev.slice(idx)];
      historyMessagesRef.current.add(userMsgId);
      newMessagesStartIndexRef.current = idx + 1;
      return next;
    });
  }

  // Create new assistant message placeholder
  const newAssistantId = `history-assistant-steering-${pairIndex}-${batchId}`;
  assistantMessagesByPair.set(pairIndex, newAssistantId);

  // Reset pair state for the new assistant message
  pairStateByPair.set(pairIndex, {
    contentOrderCounter: 0,
    reasoningId: null,
    toolCallId: null,
  });

  const assistantMessage: MessageRecord = {
    id: newAssistantId,
    role: 'assistant',
    content: '',
    contentType: 'text',
    timestamp: new Date(),
    isStreaming: false,
    isHistory: true,
    isSteering: true,
    contentSegments: [],
    reasoningProcesses: {},
    toolCallProcesses: {},
  };
  setMessages((prev: MessageRecord[]) => {
    const idx = newMessagesStartIndexRef.current;
    const next = [...prev.slice(0, idx), assistantMessage, ...prev.slice(idx)];
    historyMessagesRef.current.add(newAssistantId);
    newMessagesStartIndexRef.current = idx + 1;
    return next;
  });
}

/** Handles artifact events with artifact_type "todo_update" in history replay. */
export function handleHistoryTodoUpdate({ assistantMessageId, artifactType, artifactId, payload, pairState, setMessages, eventId }: {
  assistantMessageId: string;
  artifactType: string;
  artifactId: string;
  payload: TodoPayload | null;
  pairState: PairState;
  setMessages: SetMessages;
  eventId?: number | null;
}): boolean {
  // Only handle todo_update artifacts
  if (artifactType !== 'todo_update' || !payload) {
    return false;
  }

  const { total, completed, in_progress, pending } = payload;
  const todos = Array.isArray(payload.todos) ? payload.todos : [];

  // Use artifactId as the base todoListId to track updates to the same logical todo list
  // But create a unique segmentId for each event to preserve chronological order
  const baseTodoListId = artifactId || `history-todo-list-base-${Date.now()}`;
  // Create a unique segment ID that includes timestamp to ensure chronological ordering
  const segmentId = `${baseTodoListId}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

  // Use backend event ID when available for consistent ordering across live/reconnect/replay
  const currentOrder = eventId != null ? eventId : ++pairState.contentOrderCounter;

  setMessages((prev: MessageRecord[]) => {
    const updated = prev.map((msg: MessageRecord) => {
      if (msg.id !== assistantMessageId) return msg;

      const todoListProcesses = { ...((msg.todoListProcesses as Record<string, unknown>) || {}) };
      const contentSegments = [...((msg.contentSegments as Record<string, unknown>[]) || [])];

      // Check if this segment already exists (prevent duplicates from React batching)
      const segmentExists = contentSegments.some((s: Record<string, unknown>) => s.todoListId === segmentId);
      if (segmentExists) return msg;

      // Add new segment at the current chronological position
      contentSegments.push({
        type: 'todo_list',
        todoListId: segmentId, // Use unique segmentId for this specific event
        order: currentOrder, // Use the captured order value
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

      return {
        ...msg,
        contentSegments,
        todoListProcesses,
      };
    });

    return updated;
  });

  return true;
}

/**
 * Handles artifact events with artifact_type: "html_widget" in history replay.
 * Creates a content segment for inline rendering of interactive HTML widgets.
 */
export function handleHistoryHtmlWidget({ assistantMessageId, artifactType, artifactId, payload, pairState, setMessages, eventId }: {
  assistantMessageId: string;
  artifactType: string;
  artifactId: string;
  payload: HtmlWidgetData | null;
  pairState: PairState;
  setMessages: SetMessages;
  eventId?: number | null;
}): boolean {
  if (artifactType !== 'html_widget' || !payload) {
    return false;
  }

  const { html, title } = payload;
  const segmentId = `widget-${artifactId}`;
  const currentOrder = eventId != null ? eventId : ++pairState.contentOrderCounter;

  setMessages((prev: MessageRecord[]) => {
    const updated = prev.map((msg: MessageRecord) => {
      if (msg.id !== assistantMessageId) return msg;

      const htmlWidgetProcesses = { ...((msg.htmlWidgetProcesses as Record<string, HtmlWidgetData>) || {}) };
      const contentSegments = [...((msg.contentSegments as Record<string, unknown>[]) || [])];

      // Prevent duplicates
      const segmentExists = contentSegments.some((s: Record<string, unknown>) => s.widgetId === segmentId);
      if (segmentExists) return msg;

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
