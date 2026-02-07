import { Bot, CheckCircle2, Circle, Loader2 } from 'lucide-react';
import React from 'react';
import ReasoningMessageContent from './ReasoningMessageContent';
import TextMessageContent from './TextMessageContent';
import ToolCallMessageContent from './ToolCallMessageContent';

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
            whiteSpace: 'normal'
          }}
        >
          {description}
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
            // Render message based on type
            if (msg.role === 'assistant') {
              // Render content segments in order
              const segments = msg.contentSegments || [];
              return (
                <div key={msg.id} className="space-y-1">
                  {segments
                    .sort((a, b) => (a.order || 0) - (b.order || 0))
                    .map((segment, idx) => {
                      if (segment.type === 'reasoning') {
                        const reasoning = msg.reasoningProcesses?.[segment.reasoningId];
                        if (reasoning) {
                          // Debug: Log reasoning content to help identify issues
                          if (process.env.NODE_ENV === 'development' && !reasoning.content) {
                            console.warn('[SubagentCardContent] Reasoning process exists but content is empty:', {
                              reasoningId: segment.reasoningId,
                              reasoning,
                              messageId: msg.id,
                            });
                          }
                          return (
                            <ReasoningMessageContent
                              key={`${msg.id}-reasoning-${idx}`}
                              reasoningContent={reasoning.content || ''}
                              isReasoning={reasoning.isReasoning || false}
                              reasoningComplete={reasoning.reasoningComplete || false}
                            />
                          );
                        } else if (process.env.NODE_ENV === 'development') {
                          console.warn('[SubagentCardContent] Reasoning segment found but no reasoning process:', {
                            reasoningId: segment.reasoningId,
                            availableReasoningIds: Object.keys(msg.reasoningProcesses || {}),
                            messageId: msg.id,
                          });
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
                            />
                          );
                        }
                      } else if (segment.type === 'text') {
                        return (
                          <TextMessageContent
                            key={`${msg.id}-text-${idx}`}
                            content={segment.content || ''}
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
