import { Bot, User } from 'lucide-react';
import logo from '../../../assets/img/logo.svg';
import LogoLoading from '../../../components/LogoLoading';
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
function MessageList({ messages, onOpenSubagentTask, onOpenFile }) {
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
        <MessageBubble key={message.id} message={message} onOpenSubagentTask={onOpenSubagentTask} onOpenFile={onOpenFile} />
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
function MessageBubble({ message, onOpenSubagentTask, onOpenFile }) {
  const isUser = message.role === 'user';
  const isAssistant = message.role === 'assistant';

  return (
    <div
      className={`flex gap-4 ${isUser ? 'justify-end' : 'justify-start'}`}
    >
      {/* Assistant avatar - shown on the left */}
      {isAssistant && (
        <div className="flex-shrink-0 mt-2 w-8 h-8 flex items-center justify-center">
          <img src={logo} alt="Assistant" className="w-8 h-8" />
        </div>
      )}

      {/* Message bubble */}
      <div
        className={`${isUser ? 'max-w-[80%]' : 'w-full min-w-0'} rounded-lg ${
          isUser ? 'px-4 py-3 rounded-tr-none' : 'pl-0 pr-0 pb-3 rounded-tl-none'
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
            onOpenFile={onOpenFile}
            textOnly={true}
          />
        ) : (
          // Fallback for messages without segments (backward compatibility) - main chat shows text only
          (message.contentType === 'text' || !message.contentType) && (
            <TextMessageContent
              content={message.content}
              isStreaming={message.isStreaming}
              hasError={message.error}
            />
          )
        )}

        {/* Streaming indicator */}
        {message.isStreaming && (
          <div className="mt-2 ml-2">
            <LogoLoading size={20} color="#666666" />
          </div>
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
 * Handles interleaving of text, reasoning, and tool call content based on when they occurred.
 *
 * @param {Object} props
 * @param {Array} props.segments - Array of content segments sorted by order
 * @param {Object} props.reasoningProcesses - Object mapping reasoningId to reasoning process data
 * @param {Object} props.toolCallProcesses - Object mapping toolCallId to tool call process data
 * @param {Object} props.todoListProcesses - Object mapping todoListId to todo list data
 * @param {Object} props.subagentTasks - Object mapping subagentId to subagent task data
 * @param {boolean} props.isStreaming - Whether the message is currently streaming
 * @param {boolean} props.hasError - Whether the message has an error
 * @param {boolean} props.textOnly - If true, render text, reasoning, and tool_call segments (for main chat view); todo_list and subagent_task stay in floating cards only
 */
function MessageContentSegments({ segments, reasoningProcesses, toolCallProcesses, todoListProcesses, subagentTasks, isStreaming, hasError, onOpenSubagentTask, onOpenFile, textOnly = false }) {
  const sortedSegments = [...segments].sort((a, b) => a.order - b.order);

  // Group consecutive text segments together for better rendering
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

  // When textOnly (main chat view): show text, reasoning, tool_call (excluding TodoWrite); collapse to one reasoning + one tool_call before each text, with merged content (append new to old)
  let segmentsToRender;
  if (textOnly) {
    const filtered = groupedSegments.filter((s) => {
      if (s.type === 'text' || s.type === 'reasoning') return true;
      if (s.type === 'tool_call') {
        const toolName = toolCallProcesses[s.toolCallId]?.toolName;
        return toolName !== 'TodoWrite';
      }
      return false;
    });
    const collapsed = [];
    let reasoningAccum = [];
    let toolCallAccum = [];
    const pushMerged = () => {
      if (reasoningAccum.length) {
        const ids = reasoningAccum.map((s) => s.reasoningId);
        collapsed.push({ type: 'reasoning', reasoningId: ids[ids.length - 1], mergedReasoningIds: ids });
        reasoningAccum = [];
      }
      if (toolCallAccum.length) {
        const ids = toolCallAccum.map((s) => s.toolCallId);
        collapsed.push({ type: 'tool_call', toolCallId: ids[ids.length - 1], mergedToolCallIds: ids });
        toolCallAccum = [];
      }
    };
    for (const seg of filtered) {
      if (seg.type === 'reasoning') {
        reasoningAccum.push(seg);
      } else if (seg.type === 'tool_call') {
        toolCallAccum.push(seg);
      } else if (seg.type === 'text') {
        pushMerged();
        collapsed.push(seg);
      }
    }
    pushMerged();
    segmentsToRender = collapsed;
  } else {
    segmentsToRender = groupedSegments;
  }

  return (
    <div className="space-y-2">
      {segmentsToRender.map((segment, index) => {
        if (segment.type === 'text') {
          // Render text content
          const isLastSegment = index === segmentsToRender.length - 1;
          
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
          // Render reasoning icon (merged content when segment.mergedReasoningIds in main chat)
          const ids = segment.mergedReasoningIds || [segment.reasoningId];
          const processes = ids.map((id) => reasoningProcesses[id]).filter(Boolean);
          if (processes.length === 0) return null;
          const last = processes[processes.length - 1];
          const mergedContent = processes.map((p) => p.content || '').join('\n\n').trim();
          return (
            <ReasoningMessageContent
              key={`reasoning-${ids.join('-')}`}
              reasoningContent={mergedContent}
              isReasoning={last.isReasoning || false}
              reasoningComplete={last.reasoningComplete || false}
              reasoningTitle={last.reasoningTitle ?? undefined}
            />
          );
        } else if (segment.type === 'tool_call') {
          // Render tool call icon (skip TodoWrite; merged content when segment.mergedToolCallIds in main chat)
          const toolCallProcess = toolCallProcesses[segment.toolCallId];
          if (toolCallProcess?.toolName === 'TodoWrite') return null;
          const ids = segment.mergedToolCallIds || [segment.toolCallId];
          const mergedProcesses = ids
            .map((id) => toolCallProcesses[id])
            .filter((p) => p && p.toolName !== 'TodoWrite');
          if (mergedProcesses.length === 0) return null;
          const lastProcess = mergedProcesses[mergedProcesses.length - 1];
          return (
            <ToolCallMessageContent
              key={`tool-call-${ids.join('-')}`}
              toolCallId={segment.toolCallId}
              toolName={lastProcess.toolName}
              toolCall={lastProcess.toolCall}
              toolCallResult={lastProcess.toolCallResult}
              isInProgress={lastProcess.isInProgress || false}
              isComplete={lastProcess.isComplete || false}
              isFailed={lastProcess.isFailed || false}
              onOpenFile={onOpenFile}
              mergedProcesses={mergedProcesses.length > 1 ? mergedProcesses : undefined}
            />
          );
        } else if (segment.type === 'todo_list') {
          // Render todo list
          const todoListProcess = todoListProcesses[segment.todoListId];
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
export { MessageContentSegments };
