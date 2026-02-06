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

/**
 * Handles subagent_status events during streaming
 * Creates or updates subagent floating cards based on task status
 * @param {Object} params - Handler parameters
 * @param {Object} params.subagentStatus - Subagent status data with active_tasks and completed_tasks
 * @param {Function} params.updateSubagentCard - Callback to update subagent card
 * @returns {boolean} True if event was handled
 */
export function handleSubagentStatus({ subagentStatus, updateSubagentCard }) {
  if (!subagentStatus || !updateSubagentCard) {
    return false;
  }

  // Validate that subagentStatus has the expected structure
  if (typeof subagentStatus !== 'object') {
    console.warn('[handleSubagentStatus] Invalid subagentStatus format:', subagentStatus);
    return false;
  }

  const { active_tasks = [], completed_tasks = [] } = subagentStatus;
  
  // Ensure active_tasks and completed_tasks are arrays
  if (!Array.isArray(active_tasks) || !Array.isArray(completed_tasks)) {
    console.warn('[handleSubagentStatus] active_tasks or completed_tasks is not an array:', { active_tasks, completed_tasks });
    return false;
  }

  // Update cards for all active tasks
  // Note: We don't set messages here - they will be preserved from previous updates
  // This ensures messages aren't lost when status updates
  active_tasks.forEach((task) => {
    // Only process tasks that have a valid ID
    // Skip tasks without IDs to prevent creating unexpected cards
    if (!task || !task.id) {
      console.warn('[handleSubagentStatus] Skipping task without ID:', task);
      return;
    }
    
    const taskId = task.id;
    updateSubagentCard(taskId, {
      taskId,
      description: task.description || '',
      type: task.type || 'general-purpose',
      toolCalls: task.tool_calls || 0,
      currentTool: task.current_tool || '',
      status: 'active',
      // Don't set messages - preserve existing messages from previous updates
    });
  });

  // Update cards for completed tasks
  // Note: We don't set messages here - they will be preserved from previous updates
  completed_tasks.forEach((task) => {
    // Only process tasks that have a valid ID
    // Skip tasks without IDs to prevent creating unexpected cards
    if (!task || !task.id) {
      console.warn('[handleSubagentStatus] Skipping completed task without ID:', task);
      return;
    }
    
    const taskId = task.id;
    updateSubagentCard(taskId, {
      taskId,
      description: task.description || '',
      type: task.type || 'general-purpose',
      toolCalls: task.tool_calls || 0,
      currentTool: '',
      status: 'completed',
      // Don't set messages - preserve existing messages from previous updates
    });
  });

  return true;
}

/**
 * Checks if an event is from a subagent (agent starts with "tools:")
 * @param {Object} event - Event object
 * @returns {boolean} True if event is from subagent
 */
export function isSubagentEvent(event) {
  return event.agent && typeof event.agent === 'string' && event.agent.startsWith('tools:');
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
}) {
  if (!taskId || !assistantMessageId || !updateSubagentCard) {
    return false;
  }

  // Get or create subagent state refs
  const subagentStateRefs = refs.subagentStateRefs || {};
  if (!subagentStateRefs[taskId]) {
    subagentStateRefs[taskId] = {
      contentOrderCounterRef: { current: 0 },
      currentReasoningIdRef: { current: null },
      currentToolCallIdRef: { current: null },
      messages: [],
    };
  }

  const taskRefs = subagentStateRefs[taskId];
  const { contentOrderCounterRef, currentReasoningIdRef } = taskRefs;

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
      let messageIndex = updatedMessages.findIndex(m => m.id === assistantMessageId);
      
      if (messageIndex === -1) {
        // Create new message
        updatedMessages.push({
          id: assistantMessageId,
          role: 'assistant',
          contentSegments: [],
          reasoningProcesses: {},
          toolCallProcesses: {},
        });
        messageIndex = updatedMessages.length - 1;
      }

      const msg = updatedMessages[messageIndex];
      msg.contentSegments = [
        ...(msg.contentSegments || []),
        {
          type: 'reasoning',
          reasoningId,
          order: currentOrder,
        },
      ];
      msg.reasoningProcesses = {
        ...(msg.reasoningProcesses || {}),
        [reasoningId]: {
          content: '',
          isReasoning: true,
          reasoningComplete: false,
          order: currentOrder,
        },
      };

      taskRefs.messages = updatedMessages;
      updateSubagentCard(taskId, { messages: updatedMessages });
      return true;
    } else if (signalContent === 'complete') {
      if (currentReasoningIdRef.current) {
        const reasoningId = currentReasoningIdRef.current;
        const updatedMessages = [...taskRefs.messages];
        const messageIndex = updatedMessages.findIndex(m => m.id === assistantMessageId);
        
        if (messageIndex !== -1) {
          const msg = updatedMessages[messageIndex];
          const reasoningProcesses = { ...(msg.reasoningProcesses || {}) };
          if (reasoningProcesses[reasoningId]) {
            reasoningProcesses[reasoningId] = {
              ...reasoningProcesses[reasoningId],
              isReasoning: false,
              reasoningComplete: true,
            };
          }
          msg.reasoningProcesses = reasoningProcesses;
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
    let messageIndex = updatedMessages.findIndex(m => m.id === assistantMessageId);
    
    // Create message if it doesn't exist (edge case: reasoning content arrives before start signal)
    if (messageIndex === -1) {
      updatedMessages.push({
        id: assistantMessageId,
        role: 'assistant',
        contentSegments: [],
        reasoningProcesses: {},
        toolCallProcesses: {},
      });
      messageIndex = updatedMessages.length - 1;
    }
    
    const msg = updatedMessages[messageIndex];
    const reasoningProcesses = { ...(msg.reasoningProcesses || {}) };
    
    // Create reasoning process if it doesn't exist (edge case: reasoning content arrives before start signal)
    if (!reasoningProcesses[reasoningId]) {
      // Need to add the reasoning segment to contentSegments as well
      contentOrderCounterRef.current++;
      const currentOrder = contentOrderCounterRef.current;
      
      msg.contentSegments = [
        ...(msg.contentSegments || []),
        {
          type: 'reasoning',
          reasoningId,
          order: currentOrder,
        },
      ];
      
      reasoningProcesses[reasoningId] = {
        content: '',
        isReasoning: true,
        reasoningComplete: false,
        order: currentOrder,
      };
    }
    
    // Update reasoning content - accumulate the content
    const existingContent = reasoningProcesses[reasoningId]?.content || '';
    const newContent = existingContent + content;
    
    if (process.env.NODE_ENV === 'development') {
      console.log('[handleSubagentMessageChunk] Updating reasoning content:', {
        taskId,
        reasoningId,
        existingContentLength: existingContent.length,
        newChunkLength: content.length,
        newContentLength: newContent.length,
      });
    }
    
    reasoningProcesses[reasoningId] = {
      ...reasoningProcesses[reasoningId],
      content: newContent,
      isReasoning: true,
    };
    
    msg.reasoningProcesses = reasoningProcesses;
    taskRefs.messages = updatedMessages;
    updateSubagentCard(taskId, { messages: updatedMessages });
    return true;
  }

  // Handle text content
  if (contentType === 'text' && content) {
    contentOrderCounterRef.current++;
    const currentOrder = contentOrderCounterRef.current;

    const updatedMessages = [...taskRefs.messages];
    let messageIndex = updatedMessages.findIndex(m => m.id === assistantMessageId);
    
    if (messageIndex === -1) {
      updatedMessages.push({
        id: assistantMessageId,
        role: 'assistant',
        contentSegments: [],
        reasoningProcesses: {},
        toolCallProcesses: {},
        content: '',
      });
      messageIndex = updatedMessages.length - 1;
    }

    const msg = updatedMessages[messageIndex];
    msg.contentSegments = [
      ...(msg.contentSegments || []),
      {
        type: 'text',
        content,
        order: currentOrder,
      },
    ];
    msg.content = (msg.content || '') + content;
    msg.contentType = 'text';

    taskRefs.messages = updatedMessages;
    updateSubagentCard(taskId, { messages: updatedMessages });
    return true;
  }

  return false;
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
export function handleSubagentToolCalls({ taskId, assistantMessageId, toolCalls, refs, updateSubagentCard }) {
  if (!taskId || !assistantMessageId || !toolCalls || !Array.isArray(toolCalls) || !updateSubagentCard) {
    return false;
  }

  const subagentStateRefs = refs.subagentStateRefs || {};
  if (!subagentStateRefs[taskId]) {
    subagentStateRefs[taskId] = {
      contentOrderCounterRef: { current: 0 },
      currentReasoningIdRef: { current: null },
      currentToolCallIdRef: { current: null },
      messages: [],
    };
  }

  const taskRefs = subagentStateRefs[taskId];
  const { contentOrderCounterRef } = taskRefs;

  if (process.env.NODE_ENV === 'development') {
    console.log('[handleSubagentToolCalls] Processing tool calls:', {
      taskId,
      assistantMessageId,
      toolCallsCount: toolCalls.length,
      toolCallIds: toolCalls.map(tc => tc.id),
    });
  }

  toolCalls.forEach((toolCall) => {
    const toolCallId = toolCall.id;
    if (toolCallId) {
      const updatedMessages = [...taskRefs.messages];
      let messageIndex = updatedMessages.findIndex(m => m.id === assistantMessageId);
      
      if (messageIndex === -1) {
        updatedMessages.push({
          id: assistantMessageId,
          role: 'assistant',
          contentSegments: [],
          reasoningProcesses: {},
          toolCallProcesses: {},
        });
        messageIndex = updatedMessages.length - 1;
      }

      const msg = updatedMessages[messageIndex];
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
        
        if (process.env.NODE_ENV === 'development') {
          console.log('[handleSubagentToolCalls] Created new tool call:', {
            taskId,
            assistantMessageId,
            toolCallId,
            toolName: toolCall.name,
            order: currentOrder,
          });
        }
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
export function handleSubagentToolCallResult({ taskId, assistantMessageId, toolCallId, result, refs, updateSubagentCard }) {
  if (!taskId || !toolCallId || !updateSubagentCard) {
    return false;
  }

  const subagentStateRefs = refs.subagentStateRefs || {};
  if (!subagentStateRefs[taskId]) {
    subagentStateRefs[taskId] = {
      contentOrderCounterRef: { current: 0 },
      currentReasoningIdRef: { current: null },
      currentToolCallIdRef: { current: null },
      messages: [],
    };
  }

  const taskRefs = subagentStateRefs[taskId];
  const { contentOrderCounterRef } = taskRefs;

  const updatedMessages = [...taskRefs.messages];
  
  // Find the message that contains this tool call
  // tool_call_result events have a different event.id than tool_calls events,
  // so we need to search by tool_call_id instead of message ID
  let messageIndex = -1;
  let targetMessage = null;
  
  if (process.env.NODE_ENV === 'development') {
    console.log('[handleSubagentToolCallResult] Searching for tool call:', {
      taskId,
      toolCallId,
      assistantMessageId,
      existingMessages: updatedMessages.map(m => ({
        id: m.id,
        toolCallIds: Object.keys(m.toolCallProcesses || {}),
      })),
    });
  }
  
  // First, try to find message by assistantMessageId (if provided and matches)
  if (assistantMessageId) {
    messageIndex = updatedMessages.findIndex(m => m.id === assistantMessageId);
    if (messageIndex !== -1) {
      targetMessage = updatedMessages[messageIndex];
      // Verify this message actually has the tool call
      if (!targetMessage.toolCallProcesses?.[toolCallId]) {
        if (process.env.NODE_ENV === 'development') {
          console.warn('[handleSubagentToolCallResult] Message found but tool call not in it:', {
            messageId: assistantMessageId,
            toolCallId,
            availableToolCalls: Object.keys(targetMessage.toolCallProcesses || {}),
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
      if (msg.toolCallProcesses?.[toolCallId]) {
        messageIndex = i;
        targetMessage = msg;
        if (process.env.NODE_ENV === 'development') {
          console.log('[handleSubagentToolCallResult] Found message by tool call ID:', {
            messageId: msg.id,
            toolCallId,
          });
        }
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
          },
          isInProgress: false,
          isComplete: true,
          order: currentOrder,
        },
      },
    });
    
    if (process.env.NODE_ENV === 'development') {
      console.warn('[handleSubagentToolCallResult] Tool call not found, created new message:', {
        taskId,
        toolCallId,
        assistantMessageId,
      });
    }
  } else {
    // Update existing tool call with result
    const msg = updatedMessages[messageIndex];
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
      // Edge case: message exists but tool call doesn't - add it
      contentOrderCounterRef.current++;
      const currentOrder = contentOrderCounterRef.current;
      
      const contentSegments = [...(msg.contentSegments || [])];
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
        },
        isInProgress: false,
        isComplete: true,
        order: currentOrder,
      };
      
      msg.contentSegments = contentSegments;
    }
    
    msg.toolCallProcesses = toolCallProcesses;
  }

  taskRefs.messages = updatedMessages;
  
  // Update subagent card: clear currentTool when tool call completes
  // But only if no other tool calls are in progress
  // Check if there are any other in-progress tool calls
  let hasInProgressTool = false;
  let currentToolName = '';
  for (const msg of updatedMessages) {
    const toolCallProcesses = msg.toolCallProcesses || {};
    for (const [tcId, tcProcess] of Object.entries(toolCallProcesses)) {
      if (tcProcess.isInProgress && !tcProcess.isComplete) {
        hasInProgressTool = true;
        currentToolName = tcProcess.toolName || '';
        break;
      }
    }
    if (hasInProgressTool) break;
  }
  
  // Update currentTool: use the in-progress tool if any, otherwise clear it
  updateSubagentCard(taskId, { 
    messages: updatedMessages,
    currentTool: hasInProgressTool ? currentToolName : '', // Clear if no tools in progress
  });
  return true;
}
