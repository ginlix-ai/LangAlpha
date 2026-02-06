/**
 * Streaming event handlers for live message streaming
 * Handles events from the SSE stream during active message sending
 */

/**
 * Handles reasoning signal events during streaming
 * @param {Object} params - Handler parameters
 * @param {string} params.assistantMessageId - ID of the assistant message being updated
 * @param {string} params.signalContent - Signal content ('start' or 'complete')
 * @param {Object} params.refs - Refs object with contentOrderCounterRef, currentReasoningIdRef
 * @param {Function} params.setMessages - State setter for messages
 * @returns {boolean} True if event was handled
 */
export function handleReasoningSignal({ assistantMessageId, signalContent, refs, setMessages }) {
  const { contentOrderCounterRef, currentReasoningIdRef } = refs;

  if (signalContent === 'start') {
    // Reasoning process has started - create new reasoning process
    const reasoningId = `reasoning-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    currentReasoningIdRef.current = reasoningId;
    contentOrderCounterRef.current++;
    const currentOrder = contentOrderCounterRef.current;

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
    // Reasoning process has completed
    if (currentReasoningIdRef.current) {
      const reasoningId = currentReasoningIdRef.current;
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
export function handleReasoningContent({ assistantMessageId, content, refs, setMessages }) {
  const { currentReasoningIdRef } = refs;

  if (currentReasoningIdRef.current && content) {
    const reasoningId = currentReasoningIdRef.current;
    setMessages((prev) =>
      prev.map((msg) => {
        if (msg.id !== assistantMessageId) return msg;

        const reasoningProcesses = { ...(msg.reasoningProcesses || {}) };
        if (reasoningProcesses[reasoningId]) {
          reasoningProcesses[reasoningId] = {
            ...reasoningProcesses[reasoningId],
            content: (reasoningProcesses[reasoningId].content || '') + content,
            isReasoning: true,
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
export function handleTextContent({ assistantMessageId, content, finishReason, refs, setMessages }) {
  const { contentOrderCounterRef } = refs;

  // Handle finish_reason
  if (finishReason) {
    if (finishReason === 'tool_calls' && !content) {
      // Message is requesting tool calls, don't mark as complete yet
      return false; // Let tool_calls handler process this
    } else if (!content) {
      // Metadata chunk with finish_reason but no content
      setMessages((prev) =>
        prev.map((msg) =>
          msg.id === assistantMessageId
            ? { ...msg, isStreaming: false }
            : msg
        )
      );
      return true;
    }
    // If finish_reason exists but content also exists, continue to process content
  }

  // Process text content chunks
  if (content) {
    contentOrderCounterRef.current++;
    const currentOrder = contentOrderCounterRef.current;

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
          isStreaming: true,
        };
      })
    );
    return true;
  } else if (finishReason) {
    // Message is complete (finish_reason present with no content means end of stream)
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
 * Handles tool_calls events during streaming
 * @param {Object} params - Handler parameters
 * @param {string} params.assistantMessageId - ID of the assistant message being updated
 * @param {Array} params.toolCalls - Array of tool call objects
 * @param {string} params.finishReason - Optional finish reason
 * @param {Object} params.refs - Refs object with contentOrderCounterRef
 * @param {Function} params.setMessages - State setter for messages
 * @returns {boolean} True if event was handled
 */
export function handleToolCalls({ assistantMessageId, toolCalls, finishReason, refs, setMessages }) {
  const { contentOrderCounterRef } = refs;

  if (!toolCalls || !Array.isArray(toolCalls)) {
    return false;
  }

  toolCalls.forEach((toolCall) => {
    const toolCallId = toolCall.id;

    if (toolCallId) {
      setMessages((prev) =>
        prev.map((msg) => {
          if (msg.id !== assistantMessageId) return msg;

          const toolCallProcesses = { ...(msg.toolCallProcesses || {}) };
          const contentSegments = [...(msg.contentSegments || [])];

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

          return {
            ...msg,
            contentSegments,
            toolCallProcesses,
          };
        })
      );
    }
  });

  // Mark all tool calls as waiting for results if finish_reason indicates tool calls are done
  if (finishReason === 'tool_calls') {
    setMessages((prev) =>
      prev.map((msg) => {
        if (msg.id !== assistantMessageId) return msg;

        const toolCallProcesses = { ...(msg.toolCallProcesses || {}) };
        Object.keys(toolCallProcesses).forEach((id) => {
          toolCallProcesses[id] = {
            ...toolCallProcesses[id],
            isInProgress: false, // Tool call sent, waiting for result
          };
        });

        return {
          ...msg,
          toolCallProcesses,
        };
      })
    );
  }

  return true;
}

/**
 * Handles tool_call_result events during streaming
 * @param {Object} params - Handler parameters
 * @param {string} params.assistantMessageId - ID of the assistant message being updated
 * @param {string} params.toolCallId - ID of the tool call
 * @param {Object} params.result - Tool call result object
 * @param {Object} params.refs - Refs object with contentOrderCounterRef, currentToolCallIdRef
 * @param {Function} params.setMessages - State setter for messages
 * @returns {boolean} True if event was handled
 */
export function handleToolCallResult({ assistantMessageId, toolCallId, result, refs, setMessages }) {
  const { contentOrderCounterRef, currentToolCallIdRef } = refs;

  if (!toolCallId) {
    return false;
  }

  setMessages((prev) =>
    prev.map((msg) => {
      if (msg.id !== assistantMessageId) return msg;

      const toolCallProcesses = { ...(msg.toolCallProcesses || {}) };
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
        contentOrderCounterRef.current++;
        const currentOrder = contentOrderCounterRef.current;

        const newSegments = [
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
          contentSegments: newSegments,
          toolCallProcesses,
        };
      }

      return {
        ...msg,
        toolCallProcesses,
      };
    })
  );

  // Reset current tool call ID after result is received
  if (currentToolCallIdRef.current === toolCallId) {
    currentToolCallIdRef.current = null;
  }

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
export function handleTodoUpdate({ assistantMessageId, artifactType, artifactId, payload, refs, setMessages }) {
  const { contentOrderCounterRef, updateTodoListCard, isNewConversation } = refs;

  console.log('[handleTodoUpdate] Called with:', { assistantMessageId, artifactType, artifactId, payload, isNewConversation });

  // Only handle todo_update artifacts
  if (artifactType !== 'todo_update' || !payload) {
    console.log('[handleTodoUpdate] Skipping - artifactType:', artifactType, 'hasPayload:', !!payload);
    return false;
  }

  const { todos, total, completed, in_progress, pending } = payload;
  console.log('[handleTodoUpdate] Extracted data:', { todos, total, completed, in_progress, pending });

  // Update floating card with todo list data (only during live streaming, not history)
  // Do this before setMessages to ensure we have the latest data
  // Always update the card if updateTodoListCard is available, even if todos array is empty
  // This ensures the card persists and shows the latest state
  if (updateTodoListCard) {
    console.log('[handleTodoUpdate] Updating todo list card, isNewConversation:', isNewConversation, 'todos count:', todos?.length || 0);
    updateTodoListCard(
      {
        todos: todos || [],
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
  console.log('[handleTodoUpdate] Using baseTodoListId:', baseTodoListId, 'segmentId:', segmentId);

  setMessages((prev) => {
    console.log('[handleTodoUpdate] Current messages:', prev.map(m => ({ id: m.id, role: m.role, hasSegments: !!m.contentSegments, hasTodoProcesses: !!m.todoListProcesses })));
    const updated = prev.map((msg) => {
      if (msg.id !== assistantMessageId) return msg;

      console.log('[handleTodoUpdate] Found matching message:', msg.id);
      const todoListProcesses = { ...(msg.todoListProcesses || {}) };
      const contentSegments = [...(msg.contentSegments || [])];

      // Always create a new segment for each todo_update event to preserve chronological order
      // Increment order counter to get the current position in the stream
      contentOrderCounterRef.current++;
      const currentOrder = contentOrderCounterRef.current;
      console.log('[handleTodoUpdate] Creating new todo list segment with order:', currentOrder, 'segmentId:', segmentId);

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
        todos: todos || [],
        total: total || 0,
        completed: completed || 0,
        in_progress: in_progress || 0,
        pending: pending || 0,
        order: currentOrder,
        baseTodoListId: baseTodoListId, // Keep reference to base ID for potential future use
      };
      console.log('[handleTodoUpdate] Created new todo list process:', todoListProcesses[segmentId]);

      const updatedMsg = {
        ...msg,
        contentSegments,
        todoListProcesses,
      };
      console.log('[handleTodoUpdate] Updated message:', { 
        id: updatedMsg.id, 
        segmentsCount: updatedMsg.contentSegments?.length,
        todoListIds: Object.keys(updatedMsg.todoListProcesses || {})
      });
      return updatedMsg;
    });
    console.log('[handleTodoUpdate] Final messages after update:', updated.map(m => ({ id: m.id, segmentsCount: m.contentSegments?.length, todoListIds: Object.keys(m.todoListProcesses || {}) })));
    return updated;
  });

  return true;
}
