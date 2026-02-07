import { Bot, Loader2, User } from 'lucide-react';
import ReasoningMessageContent from './ReasoningMessageContent';
import SubagentTaskMessageContent from './SubagentTaskMessageContent';
import TextMessageContent from './TextMessageContent';
import TodoListMessageContent from './TodoListMessageContent';
import ToolCallMessageContent from './ToolCallMessageContent';

/**
 * MessageList Component
 * 
 * Displays the chat message history with support for:
 * - Empty state when no messages exist
 * - User and assistant message bubbles
 * - Streaming indicators
 * - Error state styling
 */
function MessageList({ messages, onOpenSubagentTask }) {
  // Empty state - show when no messages exist
  if (messages.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center min-h-full py-12">
        <Bot className="h-12 w-12 mb-4" style={{ color: '#6155F5', opacity: 0.5 }} />
        <p className="text-sm" style={{ color: '#FFFFFF', opacity: 0.65 }}>
          Start a conversation by typing a message below
        </p>
      </div>
    );
  }

  // Render message list
  return (
    <div className="space-y-6">
      {messages.map((message) => (
        <MessageBubble key={message.id} message={message} onOpenSubagentTask={onOpenSubagentTask} />
      ))}
    </div>
  );
}

/**
 * MessageBubble Component
 * 
 * Renders a single message bubble with appropriate styling
 * based on role (user/assistant) and state (streaming/error)
 */
function MessageBubble({ message, onOpenSubagentTask }) {
  const isUser = message.role === 'user';
  const isAssistant = message.role === 'assistant';

  return (
    <div
      className={`flex gap-4 ${isUser ? 'justify-end' : 'justify-start'}`}
    >
      {/* Assistant avatar - shown on the left */}
      {isAssistant && (
        <div
          className="flex-shrink-0 w-8 h-8 rounded-full flex items-center justify-center"
          style={{ backgroundColor: 'rgba(97, 85, 245, 0.2)' }}
        >
          <Bot className="h-4 w-4" style={{ color: '#6155F5' }} />
        </div>
      )}

      {/* Message bubble */}
      <div
        className={`${isUser ? 'max-w-[80%]' : 'w-full min-w-0'} rounded-lg px-4 ${
          isUser ? 'py-3 rounded-tr-none' : 'pb-3 rounded-tl-none'
        } overflow-hidden`}
        style={{
          backgroundColor: isUser
            ? 'var(--color-gray-292929)'
            : message.error
            ? 'rgba(255, 56, 60, 0.1)'
            : 'transparent',
          border: 'none',
          color: '#FFFFFF',
        }}
      >
        {/* Render content segments in chronological order */}
        {message.contentSegments && message.contentSegments.length > 0 ? (
          <MessageContentSegments 
            segments={message.contentSegments}
            reasoningProcesses={message.reasoningProcesses || {}}
            toolCallProcesses={message.toolCallProcesses || {}}
            todoListProcesses={message.todoListProcesses || {}}
            subagentTasks={message.subagentTasks || {}}
            isStreaming={message.isStreaming}
            hasError={message.error}
            onOpenSubagentTask={onOpenSubagentTask}
          />
        ) : (
          // Fallback for messages without segments (backward compatibility)
          <>
            {message.contentType === 'text' || !message.contentType ? (
              <TextMessageContent
                content={message.content}
                isStreaming={message.isStreaming}
                hasError={message.error}
              />
            ) : (
              <p className="text-sm whitespace-pre-wrap break-words">
                {message.content || (message.isStreaming ? '...' : '')}
              </p>
            )}
            {(message.reasoningContent || message.isReasoning) && (
              <ReasoningMessageContent
                reasoningContent={message.reasoningContent || ''}
                isReasoning={message.isReasoning || false}
                reasoningComplete={message.reasoningComplete || false}
              />
            )}
          </>
        )}

        {/* Streaming indicator */}
        {message.isStreaming && (
          <Loader2 className="h-3 w-3 animate-spin mt-2" style={{ color: '#6155F5' }} />
        )}
      </div>

      {/* User avatar - shown on the right */}
      {isUser && (
        <div
          className="flex-shrink-0 w-8 h-8 rounded-full flex items-center justify-center"
          style={{ backgroundColor: 'rgba(97, 85, 245, 0.2)' }}
        >
          <User className="h-4 w-4" style={{ color: '#6155F5' }} />
        </div>
      )}
    </div>
  );
}

/**
 * MessageContentSegments Component
 * 
 * Renders content segments in chronological order.
 * Handles interleaving of text, reasoning, tool call, and todo list content based on when they occurred.
 * 
 * @param {Object} props
 * @param {Array} props.segments - Array of content segments sorted by order
 * @param {Object} props.reasoningProcesses - Object mapping reasoningId to reasoning process data
 * @param {Object} props.toolCallProcesses - Object mapping toolCallId to tool call process data
 * @param {Object} props.todoListProcesses - Object mapping todoListId to todo list process data
 * @param {boolean} props.isStreaming - Whether the message is currently streaming
 * @param {boolean} props.hasError - Whether the message has an error
 */
function MessageContentSegments({ segments, reasoningProcesses, toolCallProcesses, todoListProcesses, subagentTasks, isStreaming, hasError, onOpenSubagentTask }) {
  // Sort segments by order to ensure chronological rendering
  const sortedSegments = [...segments].sort((a, b) => a.order - b.order);
  console.log('[MessageContentSegments] Rendering segments:', {
    totalSegments: sortedSegments.length,
    segmentTypes: sortedSegments.map(s => s.type),
    segmentOrders: sortedSegments.map(s => ({ type: s.type, order: s.order })),
    todoListProcessesKeys: Object.keys(todoListProcesses || {}),
  });
  
  // Group consecutive text segments together for better rendering
  // Text segments are grouped only if they appear consecutively (no reasoning in between)
  const groupedSegments = [];
  let currentTextGroup = null;
  
  for (const segment of sortedSegments) {
    if (segment.type === 'text') {
      if (currentTextGroup) {
        // Append to existing text group
        currentTextGroup.content += segment.content;
        currentTextGroup.lastOrder = segment.order; // Track last order for streaming indicator
      } else {
        // Start new text group
        currentTextGroup = {
          type: 'text',
          content: segment.content,
          order: segment.order,
          lastOrder: segment.order,
        };
        groupedSegments.push(currentTextGroup);
      }
    } else if (segment.type === 'reasoning') {
      // Finalize current text group if exists (reasoning breaks text continuity)
      currentTextGroup = null;
      // Add reasoning segment
      groupedSegments.push(segment);
    } else if (segment.type === 'tool_call') {
      // Finalize current text group if exists (tool call breaks text continuity)
      currentTextGroup = null;
      // Add tool call segment
      groupedSegments.push(segment);
    } else if (segment.type === 'todo_list') {
      // Finalize current text group if exists (todo list breaks text continuity)
      currentTextGroup = null;
      // Add todo list segment
      groupedSegments.push(segment);
    } else if (segment.type === 'subagent_task') {
      // Finalize current text group if exists (subagent task breaks text continuity)
      currentTextGroup = null;
      // Add subagent task segment
      groupedSegments.push(segment);
    }
  }
  
  return (
    <div className="space-y-2">
      {groupedSegments.map((segment, index) => {
        if (segment.type === 'text') {
          // Render text content
          // Only show streaming indicator on the last text segment if it's the last segment overall
          const isLastSegment = index === groupedSegments.length - 1;
          
          return (
            <div key={`text-${segment.order}-${index}`}>
              <TextMessageContent
                content={segment.content}
                isStreaming={isStreaming && isLastSegment}
                hasError={hasError}
              />
            </div>
          );
        } else if (segment.type === 'reasoning') {
          // Render reasoning icon
          const reasoningProcess = reasoningProcesses[segment.reasoningId];
          if (reasoningProcess) {
            return (
              <ReasoningMessageContent
                key={`reasoning-${segment.reasoningId}`}
                reasoningContent={reasoningProcess.content || ''}
                isReasoning={reasoningProcess.isReasoning || false}
                reasoningComplete={reasoningProcess.reasoningComplete || false}
              />
            );
          }
          return null;
        } else if (segment.type === 'tool_call') {
          // Render tool call icon
          const toolCallProcess = toolCallProcesses[segment.toolCallId];
          if (toolCallProcess) {
            return (
              <ToolCallMessageContent
                key={`tool-call-${segment.toolCallId}`}
                toolCallId={segment.toolCallId}
                toolName={toolCallProcess.toolName}
                toolCall={toolCallProcess.toolCall}
                toolCallResult={toolCallProcess.toolCallResult}
                isInProgress={toolCallProcess.isInProgress || false}
                isComplete={toolCallProcess.isComplete || false}
                isFailed={toolCallProcess.isFailed || false}
              />
            );
          }
          return null;
        } else if (segment.type === 'todo_list') {
          // Render todo list
          console.log('[MessageList] Rendering todo_list segment:', { todoListId: segment.todoListId, todoListProcesses });
          const todoListProcess = todoListProcesses[segment.todoListId];
          console.log('[MessageList] Found todoListProcess:', todoListProcess);
          if (todoListProcess) {
            return (
              <TodoListMessageContent
                key={`todo-list-${segment.todoListId}`}
                todos={todoListProcess.todos || []}
                total={todoListProcess.total || 0}
                completed={todoListProcess.completed || 0}
                in_progress={todoListProcess.in_progress || 0}
                pending={todoListProcess.pending || 0}
              />
            );
          }
          console.warn('[MessageList] No todoListProcess found for todoListId:', segment.todoListId);
          return null;
        } else if (segment.type === 'subagent_task') {
          const task = subagentTasks[segment.subagentId];
          if (task) {
            return (
              <SubagentTaskMessageContent
                key={`subagent-task-${segment.subagentId}`}
                subagentId={segment.subagentId}
                description={task.description}
                type={task.type}
                status={task.status}
                onOpen={onOpenSubagentTask}
              />
            );
          }
          return null;
        }
        return null;
      })}
    </div>
  );
}

export default MessageList;
