import { CheckCircle2, Circle, Loader2 } from 'lucide-react';
import React from 'react';
import ReasoningMessageContent from './ReasoningMessageContent';
import TextMessageContent from './TextMessageContent';
import ToolCallMessageContent from './ToolCallMessageContent';

/**
 * Normalize text content from backend for proper display in subagent cards.
 * - Unescape literal \n (backslash-n) if backend sends escaped strings
 * - Collapse single newlines (e.g. from chunk boundaries) to spaces to avoid unexpected line breaks
 * - Preserve double newlines (paragraph breaks)
 */
function normalizeSubagentText(content) {
  if (!content || typeof content !== 'string') return '';
  const s = content
    .replace(/\\n/g, '\n')
    .replace(/\r\n/g, '\n')
    .replace(/\r/g, '\n');
  return s
    .split(/\n{2,}/)
    .map((part) => part.replace(/\n/g, ' ').trim())
    .join('\n\n');
}

/**
 * SubagentCardContent Component
 * 
 * Renders subagent work content for the floating card.
 * Displays subagent messages, reasoning, tool calls, and status.
 * 
 * @param {Object} props
 * @param {string} props.taskId - Task ID (e.g., "Task-1")
 * @param {string} props.description - Task description
 * @param {string} props.type - Subagent type (e.g., "general-purpose")
 * @param {number} props.toolCalls - Number of tool calls made
 * @param {string} props.currentTool - Current tool being used
 * @param {string} props.status - Task status ('active', 'completed', etc.)
 * @param {Object} props.messages - Subagent messages state (similar to main chat messages)
 * @param {boolean} props.isHistory - Whether this card is shown from history replay (hides status/header)
 * @param {Function} props.onOpenFile - Callback when user opens a file from a tool call
 */
function SubagentCardContent({ 
  taskId, 
  description, 
  type, 
  toolCalls = 0, 
  currentTool = '', 
  status = 'active',
  messages = [],
  isHistory = false,
  onOpenFile,
}) {
  // Debug: Log status changes
  React.useEffect(() => {
    if (process.env.NODE_ENV === 'development') {
      console.log('[SubagentCardContent] Status update:', {
        taskId,
        status,
        currentTool,
        toolCalls,
        messagesCount: messages.length,
      });
    }
  }, [taskId, status, currentTool, toolCalls, messages.length]);

  /**
   * Get status icon based on current state
   */
  const getStatusIcon = () => {
    if (currentTool) {
      return <Loader2 className="h-3.5 w-3.5 animate-spin" style={{ color: 'rgba(255, 255, 255, 0.4)' }} />;
    }
    if (status === 'active' && messages.length > 0) {
      // Show spinner when actively producing content
      return <Loader2 className="h-3.5 w-3.5 animate-spin" style={{ color: 'rgba(255, 255, 255, 0.4)' }} />;
    }
    if (status === 'completed') {
      return <CheckCircle2 className="h-3.5 w-3.5" style={{ color: 'rgba(255, 255, 255, 0.4)' }} />;
    }
    return <Circle className="h-3.5 w-3.5" style={{ color: 'rgba(255, 255, 255, 0.3)' }} />;
  };

  /**
   * Get status text
   */
  const getStatusText = () => {
    if (currentTool) {
      return `Running: ${currentTool}`;
    }
    if (status === 'completed') {
      if (toolCalls > 0) {
        return `Completed (${toolCalls} tool calls)`;
      }
      return 'Completed';
    }
    if (status === 'active') {
      if (messages.length > 0) {
        // Subagent is actively producing content
        return 'Running';
      }
      return 'Initializing';
    }
    return 'Initializing';
  };

  return (
    <div className="space-y-2.5 w-full overflow-hidden">
      {/* Task description */}
      {description && (
        <div
          className="text-sm leading-relaxed break-words w-full"
          style={{
            color: 'rgba(255, 255, 255, 0.8)',
            wordBreak: 'break-word',
            overflowWrap: 'break-word',
            whiteSpace: 'normal',
          }}
        >
          {normalizeSubagentText(description)}
        </div>
      )}

      {/* Status indicator (hidden for history cards where status is less relevant) */}
      {!isHistory && (
        <div className="flex items-center gap-1.5 text-xs" style={{ color: 'rgba(255, 255, 255, 0.5)' }}>
          {getStatusIcon()}
          <span>{getStatusText()}</span>
        </div>
      )}

      {/* Messages content */}
      {messages.length > 0 && (
        <div className="space-y-2 overflow-y-auto" style={{ borderTop: '0.5px solid rgba(255, 255, 255, 0.04)', paddingTop: '8px' }}>
          {messages.map((msg) => {
            if (msg.role === 'assistant') {
              const segments = (msg.contentSegments || []).sort((a, b) => (a.order || 0) - (b.order || 0));
              // Merge consecutive text segments to avoid unexpected line breaks from chunk boundaries
              const mergedSegments = [];
              let textAccumulator = null;
              for (const segment of segments) {
                if (segment.type === 'text') {
                  const text = segment.content || '';
                  if (textAccumulator === null) {
                    textAccumulator = { type: 'text', content: text, order: segment.order };
                  } else {
                    textAccumulator.content += text;
                  }
                } else {
                  if (textAccumulator !== null) {
                    mergedSegments.push(textAccumulator);
                    textAccumulator = null;
                  }
                  mergedSegments.push(segment);
                }
              }
              if (textAccumulator !== null) {
                mergedSegments.push(textAccumulator);
              }
              return (
                <div key={msg.id} className="space-y-1">
                  {mergedSegments.map((segment, idx) => {
                    if (segment.type === 'reasoning') {
                      const reasoning = msg.reasoningProcesses?.[segment.reasoningId];
                      if (reasoning) {
                        const normalizedReasoning = normalizeSubagentText(reasoning.content || '');
                        return (
                          <ReasoningMessageContent
                            key={`${msg.id}-reasoning-${idx}`}
                            reasoningContent={normalizedReasoning}
                            isReasoning={reasoning.isReasoning || false}
                            reasoningComplete={reasoning.reasoningComplete || false}
                            reasoningTitle={reasoning.reasoningTitle ?? undefined}
                          />
                        );
                      }
                    } else if (segment.type === 'tool_call') {
                      const toolCall = msg.toolCallProcesses?.[segment.toolCallId];
                      if (toolCall) {
                        return (
                          <ToolCallMessageContent
                            key={`${msg.id}-tool-${idx}`}
                            toolName={toolCall.toolName || 'Unknown Tool'}
                            toolCall={toolCall.toolCall}
                            toolCallResult={toolCall.toolCallResult}
                            isInProgress={toolCall.isInProgress || false}
                            isComplete={toolCall.isComplete || false}
                            isFailed={toolCall.isFailed || false}
                            onOpenFile={onOpenFile}
                          />
                        );
                      }
                    } else if (segment.type === 'text' && segment.content !== undefined) {
                      return (
                        <TextMessageContent
                          key={`${msg.id}-text-${idx}`}
                          content={normalizeSubagentText(segment.content)}
                        />
                      );
                    }
                    return null;
                  })}
                </div>
              );
            }
            return null;
          })}
        </div>
      )}
    </div>
  );
}

export default SubagentCardContent;
