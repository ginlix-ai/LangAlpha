/**
 * History replay event handlers
 * Handles events from history replay (SSE stream of past conversations)
 */

/**
 * Helper to check if an event is from a subagent
 * @param {Object} event - The history event
 * @returns {boolean} True if event is from subagent
 */
export function isSubagentHistoryEvent(event) {
  return event.agent && typeof event.agent === 'string' && event.agent.startsWith('tools:');
}

/**
 * Handles user_message events from history replay
 * @param {Object} params - Handler parameters
 * @param {Object} params.event - The history event
 * @param {number} params.pairIndex - The pair index
 * @param {Map} params.assistantMessagesByPair - Map of pair_index to assistant message ID
 * @param {Map} params.pairStateByPair - Map of pair_index to pair state
 * @param {Object} params.refs - Refs object with recentlySentTracker, currentMessageRef, newMessagesStartIndexRef, historyMessagesRef
 * @param {Array} params.messages - Current messages array
 * @param {Function} params.setMessages - State setter for messages
 * @returns {boolean} True if assistant message was created/mapped, false otherwise
 */
export function handleHistoryUserMessage({
  event,
  pairIndex,
  assistantMessagesByPair,
  pairStateByPair,
  refs,
  messages,
  setMessages,
}) {
  const { recentlySentTracker, currentMessageRef, newMessagesStartIndexRef, historyMessagesRef } = refs;

  // Check if this is a new pair (not already processed)
  if (assistantMessagesByPair.has(pairIndex)) {
    return false;
  }

  // Check if this message was recently sent (to avoid duplicates)
  const messageContent = event.content.trim();
  const isDuplicate = recentlySentTracker.isRecentlySent(messageContent);

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
      // Map pair_index to the streaming assistant message ID
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

    // Create user message
    const currentUserMessageId = `history-user-${pairIndex}-${Date.now()}`;
    const userMessage = {
      id: currentUserMessageId,
      role: 'user',
      content: event.content,
      contentType: 'text',
      timestamp: event.timestamp ? new Date(event.timestamp) : new Date(),
      isHistory: true,
    };

    setMessages((prev) => {
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

    const assistantMessage = {
      id: currentAssistantMessageId,
      role: 'assistant',
      content: '',
      contentType: 'text',
      timestamp: event.timestamp ? new Date(event.timestamp) : new Date(),
      isStreaming: false,
      isHistory: true,
      contentSegments: [],
      reasoningProcesses: {},
      toolCallProcesses: {},
    };

    setMessages((prev) => {
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

/**
 * Handles reasoning signal events in history replay
 * @param {Object} params - Handler parameters
 * @param {string} params.assistantMessageId - ID of the assistant message
 * @param {string} params.signalContent - Signal content ('start' or 'complete')
 * @param {number} params.pairIndex - The pair index
 * @param {Object} params.pairState - The pair state object
 * @param {Function} params.setMessages - State setter for messages
 * @returns {boolean} True if event was handled
 */
export function handleHistoryReasoningSignal({ assistantMessageId, signalContent, pairIndex, pairState, setMessages }) {
  if (signalContent === 'start') {
    const reasoningId = `history-reasoning-${pairIndex}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    pairState.reasoningId = reasoningId;
    pairState.contentOrderCounter++;
    const currentOrder = pairState.contentOrderCounter;

    setMessages((prev) =>
      prev.map((msg) => {
        if (msg.id !== assistantMessageId) return msg;

        const newSegments = [
          ...(msg.contentSegments || []),
          {
            type: 'reasoning',
            reasoningId,
            order: currentOrder,
          },
        ];

        const newReasoningProcesses = {
          ...(msg.reasoningProcesses || {}),
          [reasoningId]: {
            content: '',
            isReasoning: false, // History: already complete
            reasoningComplete: true,
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
    if (pairState.reasoningId) {
      const reasoningId = pairState.reasoningId;
      setMessages((prev) =>
        prev.map((msg) => {
          if (msg.id !== assistantMessageId) return msg;

          const reasoningProcesses = { ...(msg.reasoningProcesses || {}) };
          if (reasoningProcesses[reasoningId]) {
            reasoningProcesses[reasoningId] = {
              ...reasoningProcesses[reasoningId],
              isReasoning: false,
              reasoningComplete: true,
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

/**
 * Handles reasoning content in history replay
 * @param {Object} params - Handler parameters
 * @param {string} params.assistantMessageId - ID of the assistant message
 * @param {string} params.content - Reasoning content
 * @param {Object} params.pairState - The pair state object
 * @param {Function} params.setMessages - State setter for messages
 * @returns {boolean} True if event was handled
 */
export function handleHistoryReasoningContent({ assistantMessageId, content, pairState, setMessages }) {
  if (content && pairState.reasoningId) {
    const reasoningId = pairState.reasoningId;
    setMessages((prev) =>
      prev.map((msg) => {
        if (msg.id !== assistantMessageId) return msg;

        const reasoningProcesses = { ...(msg.reasoningProcesses || {}) };
        if (reasoningProcesses[reasoningId]) {
          reasoningProcesses[reasoningId] = {
            ...reasoningProcesses[reasoningId],
            content: (reasoningProcesses[reasoningId].content || '') + content,
            isReasoning: false,
            reasoningComplete: true,
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
 * Handles text content in history replay
 * @param {Object} params - Handler parameters
 * @param {string} params.assistantMessageId - ID of the assistant message
 * @param {string} params.content - Text content
 * @param {string} params.finishReason - Optional finish reason
 * @param {Object} params.pairState - The pair state object
 * @param {Function} params.setMessages - State setter for messages
 * @returns {boolean} True if event was handled
 */
export function handleHistoryTextContent({ assistantMessageId, content, finishReason, pairState, setMessages }) {
  if (content) {
    pairState.contentOrderCounter++;
    const currentOrder = pairState.contentOrderCounter;

    setMessages((prev) =>
      prev.map((msg) => {
        if (msg.id !== assistantMessageId) return msg;

        const newSegments = [
          ...(msg.contentSegments || []),
          {
            type: 'text',
            content,
            order: currentOrder,
          },
        ];

        const accumulatedText = (msg.content || '') + content;

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
    setMessages((prev) =>
      prev.map((msg) =>
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
 * Handles tool_calls events in history replay
 * @param {Object} params - Handler parameters
 * @param {string} params.assistantMessageId - ID of the assistant message
 * @param {Array} params.toolCalls - Array of tool call objects
 * @param {Object} params.pairState - The pair state object
 * @param {Function} params.setMessages - State setter for messages
 * @returns {boolean} True if event was handled
 */
export function handleHistoryToolCalls({ assistantMessageId, toolCalls, pairState, setMessages }) {
  if (!toolCalls || !Array.isArray(toolCalls)) {
    return false;
  }

  toolCalls.forEach((toolCall) => {
    const toolCallId = toolCall.id;

    if (toolCallId) {
      pairState.contentOrderCounter++;
      const currentOrder = pairState.contentOrderCounter;

      setMessages((prev) =>
        prev.map((msg) => {
          if (msg.id !== assistantMessageId) return msg;

          const toolCallProcesses = { ...(msg.toolCallProcesses || {}) };
          const contentSegments = [...(msg.contentSegments || [])];
          const subagentTasks = { ...(msg.subagentTasks || {}) };

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

          // If this tool is the `task` tool, also create a subagent_task segment
          if (toolCall.name === 'task') {
            const subagentId = toolCallId;
            // Only add the segment once per subagentId
            const hasExistingSubagentSegment = contentSegments.some(
              (s) => s.type === 'subagent_task' && s.subagentId === subagentId
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
              description: toolCall.args?.description || '',
              type: toolCall.args?.subagent_type || 'general-purpose',
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

/**
 * Handles tool_call_result events in history replay
 * @param {Object} params - Handler parameters
 * @param {string} params.assistantMessageId - ID of the assistant message
 * @param {string} params.toolCallId - ID of the tool call
 * @param {Object} params.result - Tool call result object
 * @param {Object} params.pairState - The pair state object
 * @param {Function} params.setMessages - State setter for messages
 * @returns {boolean} True if event was handled
 */
export function handleHistoryToolCallResult({ assistantMessageId, toolCallId, result, pairState, setMessages }) {
  if (!toolCallId) {
    return false;
  }

  setMessages((prev) =>
    prev.map((msg) => {
      if (msg.id !== assistantMessageId) return msg;

      const toolCallProcesses = { ...(msg.toolCallProcesses || {}) };
      const subagentTasks = { ...(msg.subagentTasks || {}) };

      if (toolCallProcesses[toolCallId]) {
        toolCallProcesses[toolCallId] = {
          ...toolCallProcesses[toolCallId],
          toolCallResult: {
            content: result.content,
            content_type: result.content_type,
            tool_call_id: result.tool_call_id,
          },
          isInProgress: false,
          isComplete: true,
        };
      } else {
        // Edge case: tool call process doesn't exist, create it
        pairState.contentOrderCounter++;
        const currentOrder = pairState.contentOrderCounter;

        const contentSegments = [
          ...(msg.contentSegments || []),
          {
            type: 'tool_call',
            toolCallId,
            order: currentOrder,
          },
        ];

        toolCallProcesses[toolCallId] = {
          toolName: 'Unknown Tool',
          toolCall: null,
          toolCallResult: {
            content: result.content,
            content_type: result.content_type,
            tool_call_id: result.tool_call_id,
          },
          isInProgress: false,
          isComplete: true,
          order: currentOrder,
        };

        return {
          ...msg,
          contentSegments,
          toolCallProcesses,
          subagentTasks,
        };
      }

      // If this toolCallId is associated with a subagent task, mark it as completed
      if (subagentTasks[toolCallId]) {
        subagentTasks[toolCallId] = {
          ...subagentTasks[toolCallId],
          status: 'completed',
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
 * Handles artifact events with artifact_type: "todo_update" in history replay
 * @param {Object} params - Handler parameters
 * @param {string} params.assistantMessageId - ID of the assistant message
 * @param {string} params.artifactType - Type of artifact ("todo_update")
 * @param {string} params.artifactId - ID of the artifact
 * @param {Object} params.payload - Payload containing todos array and status counts
 * @param {Object} params.pairState - The pair state object
 * @param {Function} params.setMessages - State setter for messages
 * @returns {boolean} True if event was handled
 */
export function handleHistoryTodoUpdate({ assistantMessageId, artifactType, artifactId, payload, pairState, setMessages }) {
  // Only handle todo_update artifacts
  if (artifactType !== 'todo_update' || !payload) {
    return false;
  }

  const { todos, total, completed, in_progress, pending } = payload;

  // Use artifactId as the base todoListId to track updates to the same logical todo list
  // But create a unique segmentId for each event to preserve chronological order
  const baseTodoListId = artifactId || `history-todo-list-base-${Date.now()}`;
  // Create a unique segment ID that includes timestamp to ensure chronological ordering
  const segmentId = `${baseTodoListId}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

  console.log('[handleHistoryTodoUpdate] Processing artifact:', {
    assistantMessageId,
    artifactId,
    segmentId,
    currentCounter: pairState.contentOrderCounter,
  });

  // Capture the order BEFORE incrementing to ensure correct chronological position
  // This is critical because setMessages is asynchronous, and if we increment before,
  // other events might increment the counter further before the state updater runs
  const currentOrder = pairState.contentOrderCounter + 1;
  pairState.contentOrderCounter = currentOrder; // Update the counter for next events
  
  console.log('[handleHistoryTodoUpdate] Creating segment with order:', currentOrder, 'for message:', assistantMessageId);

  setMessages((prev) => {
    const updated = prev.map((msg) => {
      if (msg.id !== assistantMessageId) return msg;

      const todoListProcesses = { ...(msg.todoListProcesses || {}) };
      const contentSegments = [...(msg.contentSegments || [])];

      // Check if this segment already exists (prevent duplicates from React batching)
      const segmentExists = contentSegments.some(s => s.todoListId === segmentId);
      if (segmentExists) {
        console.warn('[handleHistoryTodoUpdate] Segment already exists, skipping:', segmentId);
        return msg;
      }

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
        todos: todos || [],
        total: total || 0,
        completed: completed || 0,
        in_progress: in_progress || 0,
        pending: pending || 0,
        order: currentOrder,
        baseTodoListId: baseTodoListId, // Keep reference to base ID for potential future use
      };

      console.log('[handleHistoryTodoUpdate] Created segment:', {
        segmentId,
        order: currentOrder,
        segmentsCount: contentSegments.length,
        todosCount: todos?.length || 0,
      });

      return {
        ...msg,
        contentSegments,
        todoListProcesses,
      };
    });
    
    console.log('[handleHistoryTodoUpdate] Updated messages, checking segments:', 
      updated.map(m => m.id === assistantMessageId ? {
        id: m.id,
        segments: m.contentSegments?.map(s => ({ type: s.type, order: s.order })) || []
      } : null).filter(Boolean)
    );
    
    return updated;
  });

  return true;
}
